use crate::apply::ChangeApplier;
use crate::chunker;
use crate::cli::Cli;
use crate::event::{EventEnvelope, SyncEvent};
use crate::shm::ShmTransport;
use crate::watcher::{self, FsWatcher};
use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Interval between heartbeat events.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Time without a remote heartbeat before declaring the peer offline.
const PEER_TIMEOUT: Duration = Duration::from_secs(15);

/// The sync engine ties together the watcher, shared memory transport, and change applier.
pub struct SyncEngine {
    instance_id: u64,
    root: std::path::PathBuf,
    shm: ShmTransport,
    watcher: FsWatcher,
    applier: ChangeApplier,
    seq: u64,
    chunk_size: usize,
    /// Shared flag for graceful shutdown, set by signal handler.
    running: Arc<AtomicBool>,
    /// Last time we received a heartbeat from the remote instance.
    last_remote_heartbeat: Instant,
    /// Last time we sent our own heartbeat.
    last_heartbeat_sent: Instant,
}

impl SyncEngine {
    /// Create a new sync engine.
    pub fn new(instance_id: u64, cli: &Cli, running: Arc<AtomicBool>) -> Result<Self> {
        let root = cli
            .input
            .canonicalize()
            .unwrap_or_else(|_| cli.input.clone());

        let shm = ShmTransport::create_or_open(&cli.shm_name, cli.shm_size)?;

        let debounce = Duration::from_millis(cli.debounce_ms);
        let watcher = FsWatcher::new(&root, debounce, &cli.ignore)?;

        let applier = ChangeApplier::new(&root, cli.conflict.clone());

        let now = Instant::now();

        Ok(Self {
            instance_id,
            root,
            shm,
            watcher,
            applier,
            seq: 0,
            chunk_size: chunker::DEFAULT_CHUNK_SIZE,
            running,
            last_remote_heartbeat: now,
            last_heartbeat_sent: now,
        })
    }

    /// Push a single event to the SHM with metadata.
    fn push(&mut self, event: SyncEvent) -> Result<()> {
        self.seq += 1;
        let envelope = EventEnvelope {
            instance_id: self.instance_id,
            seq: self.seq,
            timestamp: chrono::Utc::now().timestamp_millis(),
            event,
        };
        self.shm.push_event(self.instance_id, &envelope)
    }

    /// For file-creating/modifying events, also push FileContent chunks.
    fn push_with_content(&mut self, event: SyncEvent) -> Result<()> {
        let needs_content = matches!(
            event,
            SyncEvent::FileCreated { .. } | SyncEvent::FileModified { .. }
        );

        // Push the metadata event first
        self.push(event.clone())?;

        // If it's a file create/modify, also send the content
        if needs_content
            && let Some(rel_path) = event.path()
        {
            let abs_path = self.root.join(rel_path);
            if abs_path.is_file() {
                match chunker::chunk_file(rel_path, &abs_path, self.chunk_size) {
                    Ok(chunks) => {
                        for chunk in chunks {
                            self.push(chunk)?;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to chunk file {}: {}", rel_path.display(), e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Send a heartbeat if enough time has elapsed.
    fn maybe_send_heartbeat(&mut self) -> Result<()> {
        if self.last_heartbeat_sent.elapsed() >= HEARTBEAT_INTERVAL {
            let ts = chrono::Utc::now().timestamp_millis();
            self.push(SyncEvent::Heartbeat { timestamp: ts })?;
            self.last_heartbeat_sent = Instant::now();
            trace_heartbeat_sent(ts);
        }
        Ok(())
    }

    /// Check if the remote peer is still alive.
    fn check_peer_alive(&mut self) {
        if self.last_remote_heartbeat.elapsed() > PEER_TIMEOUT {
            warn!(
                "Peer offline: no heartbeat for {}s",
                self.last_remote_heartbeat.elapsed().as_secs()
            );
        }
    }

    /// Run initial directory scan and push all entries to SHM.
    pub fn initial_sync(&mut self) -> Result<()> {
        info!("Performing initial sync for {}", self.root.display());

        let events = watcher::initial_scan(&self.root, &[]);

        for event in events {
            self.push_with_content(event)?;
        }

        info!("Initial sync complete: {} events pushed", self.seq);
        Ok(())
    }

    /// Start watching for local changes and begin the sync loop.
    pub fn run_sync_loop(&mut self) -> Result<()> {
        // Seed the file state tracker so we can distinguish create vs modify
        self.watcher.seed_tracker(&self.root, &[]);
        self.watcher.watch(&self.root)?;

        info!(
            "Sync loop started (instance_id={}, dir={})",
            self.instance_id,
            self.root.display()
        );

        while self.running.load(Ordering::Relaxed) {
            // 1. Send heartbeat if needed
            self.maybe_send_heartbeat()?;

            // 2. Collect local filesystem events (with short timeout)
            let local_events = self.watcher.collect_events_timeout(&self.root, Duration::from_millis(500));

            if !local_events.is_empty() {
                debug!("Collected {} local events", local_events.len());

                // Push local events to SHM (with file content for creates/modifies)
                for event in &local_events {
                    if let Err(e) = self.push_with_content(event.clone()) {
                        warn!("Failed to push event to SHM: {}", e);
                    }
                }
            }

            // 3. Pop remote events from SHM
            let remote_envelopes = self.shm.pop_events(self.instance_id)?;

            for envelope in &remote_envelopes {
                if envelope.instance_id != self.instance_id {
                    // Track heartbeat from remote
                    if matches!(envelope.event, SyncEvent::Heartbeat { .. }) {
                        self.last_remote_heartbeat = Instant::now();
                        trace_heartbeat_recv(envelope.instance_id);
                    }
                }
            }

            let remote_events: Vec<SyncEvent> = remote_envelopes
                .iter()
                .filter(|e| e.instance_id != self.instance_id)
                .filter(|e| !matches!(e.event, SyncEvent::Heartbeat { .. }))
                .map(|e| e.event.clone())
                .collect();

            if !remote_events.is_empty() {
                debug!("Applying {} remote events", remote_events.len());
                match self.applier.apply_events(&remote_events) {
                    Ok(conflicts) => {
                        for c in &conflicts {
                            info!(
                                "Conflict detected: {} (local={}B, remote={}B)",
                                c.path.display(),
                                c.local_size,
                                c.remote_size
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Failed to apply remote events: {}", e);
                    }
                }
            }

            // 4. Check peer liveness
            self.check_peer_alive();
        }

        info!("Sync loop exiting (shutdown requested)");
        Ok(())
    }
}

fn trace_heartbeat_sent(ts: i64) {
    debug!("Heartbeat sent (ts={})", ts);
}

fn trace_heartbeat_recv(instance_id: u64) {
    debug!("Heartbeat received from instance {}", instance_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ConflictStrategy;
    use tempfile::TempDir;

    fn make_engine(dir: &std::path::Path) -> SyncEngine {
        let cli = Cli {
            input: dir.to_path_buf(),
            shm_name: format!("dirsync_sync_test_{}", std::process::id()),
            shm_size: 65536,
            verbose: 0,
            conflict: ConflictStrategy::LastWriteWins,
            debounce_ms: 50,
            ignore: vec![],
        };
        SyncEngine::new(0, &cli, Arc::new(AtomicBool::new(true))).unwrap()
    }

    #[test]
    fn test_engine_creation() {
        let dir = TempDir::new().unwrap();
        let engine = make_engine(dir.path());
        assert_eq!(engine.instance_id, 0);
        assert_eq!(engine.seq, 0);
    }

    #[test]
    fn test_initial_sync_produces_events() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("test.txt"), b"hello").unwrap();

        let mut engine = make_engine(dir.path());
        engine.initial_sync().unwrap();
        assert!(engine.seq > 0);
    }

    #[test]
    fn test_heartbeat_sends_periodically() {
        let dir = TempDir::new().unwrap();
        let mut engine = make_engine(dir.path());

        // First heartbeat should send immediately (elapsed > interval since now - now = 0, but
        // last_heartbeat_sent was set to Instant::now() at creation, so it won't send yet)
        // Force it by setting last_heartbeat_sent to the past
        engine.last_heartbeat_sent = Instant::now() - HEARTBEAT_INTERVAL;
        engine.maybe_send_heartbeat().unwrap();

        // Should have sent one heartbeat
        let events = engine.shm.pop_events(0).unwrap();
        let heartbeats: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.event, SyncEvent::Heartbeat { .. }))
            .collect();
        assert_eq!(heartbeats.len(), 1);
    }

    #[test]
    fn test_graceful_shutdown_stops_loop() {
        let dir = TempDir::new().unwrap();
        let running = Arc::new(AtomicBool::new(false)); // Already stopped
        let cli = Cli {
            input: dir.path().to_path_buf(),
            shm_name: format!("dirsync_shutdown_test_{}", std::process::id()),
            shm_size: 65536,
            verbose: 0,
            conflict: ConflictStrategy::LastWriteWins,
            debounce_ms: 50,
            ignore: vec![],
        };
        let mut engine = SyncEngine::new(0, &cli, running).unwrap();
        engine.watcher.seed_tracker(dir.path(), &[]);
        engine.watcher.watch(dir.path()).unwrap();

        // Loop should exit immediately since running is false
        engine.run_sync_loop().unwrap();
    }
}
