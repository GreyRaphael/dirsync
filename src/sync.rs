use crate::apply::ChangeApplier;
use crate::chunker;
use crate::cli::Cli;
use crate::event::{EventEnvelope, SyncEvent};
use crate::shm::ShmTransport;
use crate::watcher::{self, FsWatcher};
use anyhow::Result;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const PEER_TIMEOUT: Duration = Duration::from_secs(15);
const STABILITY_INTERVAL: Duration = Duration::from_millis(200);
const STABILITY_MAX_WAIT: Duration = Duration::from_secs(30);
const REMOTE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MIN_CHUNK_SIZE: usize = 16 * 1024;

pub struct SyncEngine {
    instance_id: u64,
    root: std::path::PathBuf,
    shm: ShmTransport,
    watcher: FsWatcher,
    applier: ChangeApplier,
    seq: u64,
    chunk_size: usize,
    ignore_dirs: Vec<String>,
    running: Arc<AtomicBool>,
    last_remote_heartbeat: Instant,
    last_heartbeat_sent: Instant,
    suppressed_paths: HashSet<PathBuf>,
    peer_offline: bool,
}

impl SyncEngine {
    pub fn new(instance_id: u64, cli: &Cli, running: Arc<AtomicBool>) -> Result<Self> {
        let root = cli
            .input()
            .canonicalize()
            .unwrap_or_else(|_| cli.input().clone());

        let mut shm = ShmTransport::create_or_open(cli.shm_name(), cli.shm_size())?;
        shm.register_instance(instance_id)?;

        let chunk_size =
            (shm.capacity() as usize / 4).clamp(MIN_CHUNK_SIZE, chunker::DEFAULT_CHUNK_SIZE);

        let debounce = Duration::from_millis(cli.debounce_ms());
        let watcher = FsWatcher::new(&root, debounce, cli.ignore())?;

        let applier = ChangeApplier::new(&root, cli.conflict().clone());

        let now = Instant::now();

        Ok(Self {
            instance_id,
            root,
            shm,
            watcher,
            applier,
            seq: 0,
            chunk_size,
            ignore_dirs: cli.ignore().to_vec(),
            running,
            last_remote_heartbeat: now,
            last_heartbeat_sent: now,
            suppressed_paths: HashSet::new(),
            peer_offline: false,
        })
    }

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

    fn push_and_maybe_drain(&mut self, event: SyncEvent, drain_remote: bool) -> Result<()> {
        self.push(event)?;
        if drain_remote {
            self.drain_remote_events()?;
        }
        Ok(())
    }

    fn push_with_content_and_drain(&mut self, event: SyncEvent) -> Result<()> {
        self.push_with_content_inner(event, true, true)
    }

    fn push_initial_event(&mut self, event: SyncEvent) -> Result<()> {
        self.push_with_content_inner(event, true, false)
    }

    fn push_with_content_inner(
        &mut self,
        event: SyncEvent,
        drain_remote: bool,
        wait_for_stability: bool,
    ) -> Result<()> {
        let needs_content = matches!(
            event,
            SyncEvent::FileCreated { .. } | SyncEvent::FileModified { .. }
        );

        if !needs_content {
            return self.push_and_maybe_drain(event, drain_remote);
        }

        let rel_path = match event.path() {
            Some(p) => p.clone(),
            None => return self.push_and_maybe_drain(event, drain_remote),
        };
        let abs_path = self.root.join(&rel_path);

        if !abs_path.is_file() {
            return self.push_and_maybe_drain(event, drain_remote);
        }

        if wait_for_stability {
            match watcher::wait_for_stable(&abs_path, STABILITY_INTERVAL, STABILITY_MAX_WAIT) {
                Ok(size) => {
                    debug!("File stabilized: {} ({} bytes)", rel_path.display(), size);
                }
                Err(e) => {
                    warn!("File did not stabilize: {}", e);
                    return self.push_and_maybe_drain(event, drain_remote);
                }
            }
        }

        // First pass: compute hash and size by streaming (constant memory).
        let (hash, size) = match watcher::file_hash_and_size(&abs_path) {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to hash {}: {}", rel_path.display(), e);
                return self.push_and_maybe_drain(event, drain_remote);
            }
        };

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

        self.push_and_maybe_drain(corrected_event.clone(), drain_remote)?;
        self.watcher.record_applied_event(&corrected_event);

        // Second pass: stream file content in chunks (constant memory).
        use std::io::Read;
        let mut file = match std::fs::File::open(&abs_path) {
            Ok(f) => f,
            Err(e) => {
                warn!("Failed to open {} for streaming: {}", rel_path.display(), e);
                return Ok(());
            }
        };
        let mut buf = vec![0u8; self.chunk_size];
        let mut offset: u64 = 0;
        loop {
            let n = match file.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    warn!("Failed to read chunk from {}: {}", rel_path.display(), e);
                    break;
                }
            };
            self.push_and_maybe_drain(
                SyncEvent::FileContent {
                    path: rel_path.clone(),
                    offset,
                    data: buf[..n].to_vec(),
                },
                drain_remote,
            )?;
            offset += n as u64;
        }

        Ok(())
    }

    pub fn wait_for_peer(&self) -> Result<()> {
        let peer_bit = if self.instance_id == 0 { 0b10 } else { 0b01 };
        let mut last_log = Instant::now() - Duration::from_secs(5);

        while self.running.load(Ordering::Relaxed) {
            if self.shm.active_mask() & peer_bit != 0 {
                info!("Peer connected");
                return Ok(());
            }

            if last_log.elapsed() >= Duration::from_secs(5) {
                info!("Waiting for peer instance before initial sync...");
                last_log = Instant::now();
            }
            std::thread::sleep(Duration::from_millis(200));
        }

        anyhow::bail!("Shutdown requested before peer connected")
    }

    fn maybe_send_heartbeat(&mut self) -> Result<()> {
        if self.last_heartbeat_sent.elapsed() >= HEARTBEAT_INTERVAL {
            let ts = chrono::Utc::now().timestamp_millis();
            self.push(SyncEvent::Heartbeat { timestamp: ts })?;
            self.last_heartbeat_sent = Instant::now();
            debug!("Heartbeat sent (ts={})", ts);
        }
        Ok(())
    }

    fn check_peer_alive(&mut self) {
        let elapsed = self.last_remote_heartbeat.elapsed();
        if elapsed > PEER_TIMEOUT && !self.peer_offline {
            self.peer_offline = true;
            let peer_id = 1 - self.instance_id;
            warn!(
                "Peer offline: no heartbeat for {}s — reclaiming peer SHM resources",
                elapsed.as_secs()
            );
            if let Err(e) = self.shm.force_unregister_peer(peer_id) {
                warn!("Failed to reclaim peer resources: {}", e);
            }
        }
    }

    pub fn initial_sync(&mut self) -> Result<()> {
        info!("Performing initial sync for {}", self.root.display());

        let events = watcher::initial_scan(&self.root, &self.ignore_dirs);

        for event in events {
            self.push_initial_event(event)?;
        }

        info!("Initial sync complete: {} events pushed", self.seq);
        Ok(())
    }

    fn drain_remote_events(&mut self) -> Result<()> {
        let remote_envelopes = self.shm.pop_events(self.instance_id)?;
        self.handle_remote_events(&remote_envelopes)
    }

    fn handle_remote_events(&mut self, remote_envelopes: &[EventEnvelope]) -> Result<()> {
        let mut applied = 0usize;

        for envelope in remote_envelopes {
            if envelope.instance_id == self.instance_id {
                continue;
            }

            if matches!(envelope.event, SyncEvent::Heartbeat { .. }) {
                self.last_remote_heartbeat = Instant::now();
                if self.peer_offline {
                    info!("Peer back online");
                    self.peer_offline = false;
                }
                debug!("Heartbeat received from instance {}", envelope.instance_id);
                continue;
            }

            if let Some(path) = envelope.event.path() {
                self.suppressed_paths.insert(path.clone());
            }

            match self.applier.apply_event(&envelope.event, envelope.timestamp) {
                Ok(Some(conflict)) => {
                    info!(
                        "Conflict detected: {} (local={}B, remote={}B)",
                        conflict.path.display(),
                        conflict.local_size,
                        conflict.remote_size
                    );
                }
                Ok(None) => {
                    self.watcher.record_applied_event(&envelope.event);
                }
                Err(e) => {
                    warn!("Failed to apply remote event: {}", e);
                }
            }

            applied += 1;
        }

        if applied > 0 {
            debug!("Applied {} remote events", applied);
        }

        Ok(())
    }

    pub fn run_sync_loop(&mut self) -> Result<()> {
        self.watcher.seed_tracker(&self.root, &self.ignore_dirs);
        self.watcher.watch(&self.root)?;

        info!(
            "Sync loop started (instance_id={}, dir={})",
            self.instance_id,
            self.root.display()
        );

        while self.running.load(Ordering::Relaxed) {
            self.maybe_send_heartbeat()?;

            self.drain_remote_events()?;

            let raw_events = self
                .watcher
                .collect_events_timeout(&self.root, REMOTE_POLL_INTERVAL);
            let local_events: Vec<SyncEvent> = raw_events
                .into_iter()
                .filter(|e| match e.path() {
                    Some(p) if self.suppressed_paths.contains(p) => {
                        debug!("Suppressing echo event for {}", p.display());
                        false
                    }
                    _ => true,
                })
                .collect();
            self.suppressed_paths.clear();

            if !local_events.is_empty() {
                debug!("Collected {} local events", local_events.len());
                for event in &local_events {
                    if let Err(e) = self.push_with_content_and_drain(event.clone()) {
                        warn!("Failed to push event to SHM: {}", e);
                    }
                }
            }

            self.drain_remote_events()?;

            self.check_peer_alive();
        }

        info!("Sync loop exiting (shutdown requested)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Command, ConflictStrategy, RunArgs};
    use tempfile::TempDir;

    fn make_engine(dir: &std::path::Path) -> SyncEngine {
        let cli = Cli {
            command: Command::Host(RunArgs {
                input: dir.to_path_buf(),
                shm_name: format!("dirsync_sync_test_{}", std::process::id()),
                shm_size: 65536,
                verbose: 0,
                conflict: ConflictStrategy::LastWriteWins,
                debounce_ms: 50,
                ignore: vec![],
            }),
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
            command: Command::Host(RunArgs {
                input: dir.path().to_path_buf(),
                shm_name: format!("dirsync_shutdown_test_{}", std::process::id()),
                shm_size: 65536,
                verbose: 0,
                conflict: ConflictStrategy::LastWriteWins,
                debounce_ms: 50,
                ignore: vec![],
            }),
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

        engine.suppressed_paths.insert(PathBuf::from("foo.txt"));
        assert!(!engine.suppressed_paths.is_empty());

        engine.suppressed_paths.clear();
        assert!(engine.suppressed_paths.is_empty());
    }
}
