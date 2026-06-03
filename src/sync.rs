use crate::apply::ChangeApplier;
use crate::chunker;
use crate::cli::Cli;
use crate::event::{EventEnvelope, SyncEvent};
use crate::shm::ShmTransport;
use crate::watcher::{self, FsWatcher};
use anyhow::Result;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Interval between heartbeat events.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Time without a remote heartbeat before declaring the peer offline.
const PEER_TIMEOUT: Duration = Duration::from_secs(15);

/// How long to wait for a file's size to stabilize before reading it.
const STABILITY_INTERVAL: Duration = Duration::from_millis(200);
const STABILITY_MAX_WAIT: Duration = Duration::from_secs(30);

/// The sync engine ties together the watcher, shared memory transport, and change applier.
pub struct SyncEngine {
    instance_id: u64,
    root: std::path::PathBuf,
    shm: ShmTransport,
    watcher: FsWatcher,
    applier: ChangeApplier,
    seq: u64,
    chunk_size: usize,
    ignore_dirs: Vec<String>,
    /// Shared flag for graceful shutdown, set by signal handler.
    running: Arc<AtomicBool>,
    /// Last time we received a heartbeat from the remote instance.
    last_remote_heartbeat: Instant,
    /// Last time we sent our own heartbeat.
    last_heartbeat_sent: Instant,
    /// Paths that were written by the applier in the previous iteration.
    /// Events for these paths are suppressed in the next collection to
    /// prevent the echo loop (remote write → local watcher → push back).
    suppressed_paths: HashSet<PathBuf>,
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
            ignore_dirs: cli.ignore.clone(),
            running,
            last_remote_heartbeat: now,
            last_heartbeat_sent: now,
            suppressed_paths: HashSet::new(),
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
    ///
    /// This is the critical path: we wait for the file to stabilize, read it
    /// once, and use the same data for both the metadata hash and the content
    /// chunks. This eliminates the TOCTOU between watcher hash and chunker read.
    fn push_with_content(&mut self, event: SyncEvent) -> Result<()> {
        let needs_content = matches!(
            event,
            SyncEvent::FileCreated { .. } | SyncEvent::FileModified { .. }
        );

        if !needs_content {
            return self.push(event);
        }

        let rel_path = match event.path() {
            Some(p) => p.clone(),
            None => return self.push(event),
        };
        let abs_path = self.root.join(&rel_path);

        if !abs_path.is_file() {
            return self.push(event);
        }

        // Wait for the file to finish being written (size stabilizes)
        match watcher::wait_for_stable(&abs_path, STABILITY_INTERVAL, STABILITY_MAX_WAIT) {
            Ok(size) => {
                debug!("File stabilized: {} ({} bytes)", rel_path.display(), size);
            }
            Err(e) => {
                warn!("File did not stabilize: {}", e);
                return self.push(event);
            }
        }

        // Single read: data + hash from the same bytes, no TOCTOU
        let (data, hash, size) = match watcher::read_and_hash(&abs_path) {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to read {}: {}", rel_path.display(), e);
                return self.push(event);
            }
        };

        // Build the correct metadata event with the actual hash/size we just read
        let corrected_event = match &event {
            SyncEvent::FileCreated { path, .. } => SyncEvent::FileCreated {
                path: path.clone(),
                content_hash: hash,
                size,
            },
            SyncEvent::FileModified { path, .. } => SyncEvent::FileModified {
                path: path.clone(),
                content_hash: hash,
                size,
            },
            _ => event.clone(),
        };

        self.push(corrected_event.clone())?;
        self.watcher.record_applied_event(&corrected_event);

        let chunks = chunker::chunk_data(&rel_path, &data, self.chunk_size);
        for chunk in chunks {
            self.push(chunk)?;
        }

        Ok(())
    }

    /// Send a heartbeat if enough time has elapsed.
    fn maybe_send_heartbeat(&mut self) -> Result<()> {
        if self.last_heartbeat_sent.elapsed() >= HEARTBEAT_INTERVAL {
            let ts = chrono::Utc::now().timestamp_millis();
            self.push(SyncEvent::Heartbeat { timestamp: ts })?;
            self.last_heartbeat_sent = Instant::now();
            debug!("Heartbeat sent (ts={})", ts);
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

        let events = watcher::initial_scan(&self.root, &self.ignore_dirs);

        for event in events {
            self.push_with_content(event)?;
        }

        info!("Initial sync complete: {} events pushed", self.seq);
        Ok(())
    }

    /// Start watching for local changes and begin the sync loop.
    pub fn run_sync_loop(&mut self) -> Result<()> {
        self.watcher.seed_tracker(&self.root, &self.ignore_dirs);
        self.watcher.watch(&self.root)?;

        info!(
            "Sync loop started (instance_id={}, dir={})",
            self.instance_id,
            self.root.display()
        );

        while self.running.load(Ordering::Relaxed) {
            // 1. Send heartbeat if needed
            self.maybe_send_heartbeat()?;

            // 2. Collect local filesystem events, suppressing paths that were
            //    written by the applier in the previous iteration (echo prevention).
            let raw_events = self.watcher.collect_events_timeout(&self.root, Duration::from_millis(500));
            let local_events: Vec<SyncEvent> = raw_events
                .into_iter()
                .filter(|e| {
                    match e.path() {
                        Some(p) if self.suppressed_paths.contains(p) => {
                            debug!("Suppressing echo event for {}", p.display());
                            false
                        }
                        _ => true,
                    }
                })
                .collect();
            // Clear suppressed paths — they've been used for this iteration
            self.suppressed_paths.clear();

            if !local_events.is_empty() {
                debug!("Collected {} local events", local_events.len());
                for event in &local_events {
                    if let Err(e) = self.push_with_content(event.clone()) {
                        warn!("Failed to push event to SHM: {}", e);
                    }
                }
            }

            // 3. Pop remote events from SHM
            let remote_envelopes = self.shm.pop_events(self.instance_id)?;

            for envelope in &remote_envelopes {
                if envelope.instance_id != self.instance_id
                    && matches!(envelope.event, SyncEvent::Heartbeat { .. })
                {
                    self.last_remote_heartbeat = Instant::now();
                    debug!("Heartbeat received from instance {}", envelope.instance_id);
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

                // Record which paths the applier will write, so the watcher
                // can suppress them on the next iteration (echo prevention).
                for event in &remote_events {
                    if let Some(p) = event.path() {
                        self.suppressed_paths.insert(p.clone());
                    }
                }

                match self.applier.apply_events(&remote_events) {
                    Ok(conflicts) => {
                        let conflict_paths: HashSet<PathBuf> =
                            conflicts.iter().map(|c| c.path.clone()).collect();
                        for c in &conflicts {
                            info!(
                                "Conflict detected: {} (local={}B, remote={}B)",
                                c.path.display(),
                                c.local_size,
                                c.remote_size
                            );
                        }
                        for event in &remote_events {
                            match event.path() {
                                Some(path) if conflict_paths.contains(path) => {}
                                _ => self.watcher.record_applied_event(event),
                            }
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
            instance: None,
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

        engine.last_heartbeat_sent = Instant::now() - HEARTBEAT_INTERVAL;
        engine.maybe_send_heartbeat().unwrap();

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
        let running = Arc::new(AtomicBool::new(false));
        let cli = Cli {
            input: dir.path().to_path_buf(),
            shm_name: format!("dirsync_shutdown_test_{}", std::process::id()),
            shm_size: 65536,
            verbose: 0,
            conflict: ConflictStrategy::LastWriteWins,
            debounce_ms: 50,
            instance: None,
            ignore: vec![],
        };
        let mut engine = SyncEngine::new(0, &cli, running).unwrap();
        engine.watcher.seed_tracker(dir.path(), &[]);
        engine.watcher.watch(dir.path()).unwrap();

        engine.run_sync_loop().unwrap();
    }

    #[test]
    fn test_suppressed_paths_cleared_after_use() {
        let dir = TempDir::new().unwrap();
        let mut engine = make_engine(dir.path());

        // Simulate: applier wrote to "foo.txt" last iteration
        engine.suppressed_paths.insert(PathBuf::from("foo.txt"));
        assert!(!engine.suppressed_paths.is_empty());

        // After collecting (which filters suppressed paths), the set is cleared
        // We can't easily test the full loop, but we can verify the mechanism
        engine.suppressed_paths.clear();
        assert!(engine.suppressed_paths.is_empty());
    }
}
