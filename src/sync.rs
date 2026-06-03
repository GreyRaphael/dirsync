use crate::apply::ChangeApplier;
use crate::chunker;
use crate::cli::Cli;
use crate::event::{EventEnvelope, SyncEvent};
use crate::shm::ShmTransport;
use crate::watcher::{self, FsWatcher};
use anyhow::Result;
use std::time::Duration;
use tracing::{debug, info, warn};

/// The sync engine ties together the watcher, shared memory transport, and change applier.
pub struct SyncEngine {
    instance_id: u64,
    root: std::path::PathBuf,
    shm: ShmTransport,
    watcher: FsWatcher,
    applier: ChangeApplier,
    seq: u64,
    chunk_size: usize,
}

impl SyncEngine {
    /// Create a new sync engine.
    pub fn new(instance_id: u64, cli: &Cli) -> Result<Self> {
        let root = cli
            .input
            .canonicalize()
            .unwrap_or_else(|_| cli.input.clone());

        let shm = ShmTransport::create_or_open(&cli.shm_name, cli.shm_size)?;

        let debounce = Duration::from_millis(cli.debounce_ms);
        let watcher = FsWatcher::new(&root, debounce, &cli.ignore)?;

        let applier = ChangeApplier::new(&root, cli.conflict.clone());

        Ok(Self {
            instance_id,
            root,
            shm,
            watcher,
            applier,
            seq: 0,
            chunk_size: chunker::DEFAULT_CHUNK_SIZE,
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
        self.watcher.watch(&self.root)?;

        info!(
            "Sync loop started (instance_id={}, dir={})",
            self.instance_id,
            self.root.display()
        );

        loop {
            // 1. Collect local filesystem events
            let local_events = self.watcher.collect_events_blocking(&self.root);

            if !local_events.is_empty() {
                debug!("Collected {} local events", local_events.len());

                // 2. Push local events to SHM (with file content for creates/modifies)
                for event in &local_events {
                    if let Err(e) = self.push_with_content(event.clone()) {
                        warn!("Failed to push event to SHM: {}", e);
                    }
                }
            }

            // 3. Pop remote events from SHM
            let remote_envelopes = self.shm.pop_events(self.instance_id)?;

            let remote_events: Vec<SyncEvent> = remote_envelopes
                .iter()
                .filter(|e| e.instance_id != self.instance_id)
                .map(|e| e.event.clone())
                .collect();

            if !remote_events.is_empty() {
                debug!("Applying {} remote events", remote_events.len());
                if let Err(e) = self.applier.apply_events(&remote_events) {
                    warn!("Failed to apply remote events: {}", e);
                }
            }

            // Small sleep to avoid busy-waiting when no events
            if local_events.is_empty() && remote_events.is_empty() {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}
