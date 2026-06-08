use crate::event::SyncEvent;
use anyhow::{Context, Result};
use blake3::Hasher;
use notify::{
    Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
    event::{ModifyKind, RemoveKind, RenameMode},
};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;
use tracing::{debug, info, warn};
use walkdir::WalkDir;

/// Internal workspace for assembling incoming files. It must never be synced.
pub const INTERNAL_TEMP_DIR: &str = ".dirsync_tmp";

/// Cached state for a single file.
#[derive(Debug, Clone)]
struct FileState {
    hash: [u8; 32],
    size: u64,
}

/// Tracks known file hashes to detect true create vs modify.
struct FileStateTracker {
    states: HashMap<PathBuf, FileState>,
}

impl FileStateTracker {
    fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// Record a file's state after a successful sync event.
    fn record(&mut self, path: PathBuf, hash: [u8; 32], size: u64) {
        self.states.insert(path, FileState { hash, size });
    }

    /// Remove a file's tracked state.
    fn remove(&mut self, path: &Path) {
        self.states.remove(path);
    }

    fn is_known_file(&self, path: &Path) -> bool {
        self.states.contains_key(path)
    }

    /// Check if a file is known and whether its content has changed.
    /// Returns: (is_known, has_changed)
    fn check(&self, path: &Path, hash: &[u8; 32], size: &u64) -> (bool, bool) {
        match self.states.get(path) {
            Some(state) => (true, state.hash != *hash || state.size != *size),
            None => (false, false),
        }
    }
}

/// File system watcher that emits SyncEvents for local changes.
pub struct FsWatcher {
    watcher: RecommendedWatcher,
    event_rx: Receiver<notify::Result<Event>>,
    ignore_dirs: Vec<PathBuf>,
    debounce: Duration,
    tracker: FileStateTracker,
}

impl FsWatcher {
    /// Create a new watcher for the given root directory.
    pub fn new(root: &Path, debounce: Duration, ignore_dirs: &[String]) -> Result<Self> {
        let (tx, rx) = mpsc::channel();

        let watcher = RecommendedWatcher::new(
            move |res| {
                let _ = tx.send(res);
            },
            notify::Config::default().with_poll_interval(debounce),
        )
        .context("Failed to create file watcher")?;

        let mut ignore_set: Vec<PathBuf> = ignore_dirs.iter().map(|d| root.join(d)).collect();
        ignore_set.push(root.join(INTERNAL_TEMP_DIR));

        Ok(Self {
            watcher,
            event_rx: rx,
            ignore_dirs: ignore_set,
            debounce,
            tracker: FileStateTracker::new(),
        })
    }

    /// Seed the tracker with the current state of all files in `root`.
    /// Call this after `initial_sync` so that subsequent events are
    /// correctly classified as create vs modify.
    pub fn seed_tracker(&mut self, root: &Path, ignore_dirs: &[String]) {
        for entry in WalkDir::new(root).into_iter().filter_entry(|e| {
            let name = e.file_name().to_string_lossy();
            name.as_ref() != INTERNAL_TEMP_DIR
                && !ignore_dirs.iter().any(|d| name.as_ref() == d.as_str())
        }) {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();
            if path.is_file()
                && let Ok(rel) = path.strip_prefix(root)
                && let Ok((hash, size)) = file_hash_and_size(path)
            {
                self.tracker.record(rel.to_path_buf(), hash, size);
            }
        }
        info!(
            "Seeded file tracker with {} entries",
            self.tracker.states.len()
        );
    }

    /// Start watching the given directory.
    pub fn watch(&mut self, root: &Path) -> Result<()> {
        self.watcher
            .watch(root, RecursiveMode::Recursive)
            .context("Failed to start watching directory")?;
        info!("Watching directory: {}", root.display());
        Ok(())
    }

    /// Collect events with a timeout. Returns empty if no events arrive within
    /// the timeout period. Used in the sync loop to allow periodic heartbeat
    /// checks without blocking indefinitely.
    pub fn collect_events_timeout(&mut self, root: &Path, timeout: Duration) -> Vec<SyncEvent> {
        let first = match self.event_rx.recv_timeout(timeout) {
            Ok(Ok(event)) => event,
            Ok(Err(e)) => {
                warn!("Watch error: {}", e);
                return Vec::new();
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return Vec::new(),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return Vec::new(),
        };

        let mut by_path: HashMap<PathBuf, SyncEvent> = HashMap::new();
        if let Some(sync_events) = self.translate_event(root, &first) {
            for e in sync_events {
                if let Some(p) = e.path().cloned() {
                    by_path.insert(p, e);
                }
            }
        }

        // Debounce: wait, then drain remaining
        std::thread::sleep(self.debounce);
        while let Ok(Ok(event)) = self.event_rx.try_recv() {
            if let Some(sync_events) = self.translate_event(root, &event) {
                for e in sync_events {
                    if let Some(p) = e.path().cloned() {
                        by_path.insert(p, e);
                    }
                }
            }
        }

        by_path
            .into_values()
            .filter(|e| self.should_emit(e))
            .collect()
    }

    /// Block until at least one event arrives, then collect with debounce.
    ///
    /// Events for the same path within the debounce window are merged:
    /// only the latest event per path is kept.
    pub fn collect_events_blocking(&mut self, root: &Path) -> Vec<SyncEvent> {
        // Block for first event
        let first = match self.event_rx.recv() {
            Ok(Ok(event)) => event,
            Ok(Err(e)) => {
                warn!("Watch error: {}", e);
                return Vec::new();
            }
            Err(_) => return Vec::new(),
        };

        // Collect first event into a path-keyed map for dedup
        let mut by_path: HashMap<PathBuf, SyncEvent> = HashMap::new();
        if let Some(sync_events) = self.translate_event(root, &first) {
            for e in sync_events {
                if let Some(p) = e.path().cloned() {
                    by_path.insert(p, e);
                }
            }
        }

        // Debounce: wait, then drain remaining events
        std::thread::sleep(self.debounce);

        while let Ok(Ok(event)) = self.event_rx.try_recv() {
            if let Some(sync_events) = self.translate_event(root, &event) {
                for e in sync_events {
                    if let Some(p) = e.path().cloned() {
                        by_path.insert(p, e);
                    }
                }
            }
        }

        // Filter out events where the file hasn't actually changed
        by_path
            .into_values()
            .filter(|e| self.should_emit(e))
            .collect()
    }

    /// Record a successfully applied remote event in the local tracker.
    ///
    /// Remote filesystem changes are suppressed to avoid echo loops, so they do
    /// not pass through `should_emit`. Without this, later local deletes of
    /// remote-created extensionless files could be misclassified as directories.
    pub fn record_applied_event(&mut self, event: &SyncEvent) {
        match event {
            SyncEvent::FileCreated {
                path,
                content_hash,
                size,
            }
            | SyncEvent::FileModified {
                path,
                content_hash,
                size,
            } => self.tracker.record(path.clone(), *content_hash, *size),
            SyncEvent::FileDeleted { path } => self.tracker.remove(path),
            SyncEvent::DirDeleted { path } => {
                self.tracker.states.retain(|p, _| !p.starts_with(path));
            }
            SyncEvent::DirCreated { .. }
            | SyncEvent::FileContent { .. }
            | SyncEvent::Heartbeat { .. } => {}
        }
    }

    /// Check whether an event should be emitted based on tracked state.
    fn should_emit(&mut self, event: &SyncEvent) -> bool {
        match event {
            SyncEvent::FileCreated {
                path,
                content_hash,
                size,
            } => {
                let (known, changed) = self.tracker.check(path, content_hash, size);
                if known && !changed {
                    return false;
                }
                self.tracker.record(path.clone(), *content_hash, *size);
                true
            }
            SyncEvent::FileModified {
                path,
                content_hash,
                size,
            } => {
                let (known, changed) = self.tracker.check(path, content_hash, size);
                if known && !changed {
                    debug!("Skipping unchanged modify: {}", path.display());
                    return false;
                }
                self.tracker.record(path.clone(), *content_hash, *size);
                true
            }
            SyncEvent::FileDeleted { path } => {
                self.tracker.remove(path);
                true
            }
            SyncEvent::DirCreated { .. } => true,
            SyncEvent::DirDeleted { path } => {
                // Remove all tracked files under this directory
                self.tracker.states.retain(|p, _| !p.starts_with(path));
                true
            }
            SyncEvent::FileContent { .. } | SyncEvent::Heartbeat { .. } => true,
        }
    }

    fn create_event(&self, root: &Path, path: &Path) -> Option<SyncEvent> {
        let rel = path.strip_prefix(root).ok()?.to_path_buf();
        if path.is_dir() {
            return Some(SyncEvent::DirCreated { path: rel });
        }

        if path.is_file() {
            match file_hash_and_size(path) {
                Ok((hash, size)) => {
                    return Some(SyncEvent::FileCreated {
                        path: rel,
                        content_hash: hash,
                        size,
                    });
                }
                Err(e) => warn!("Failed to hash {}: {}", path.display(), e),
            }
        }

        None
    }

    fn modify_event(&self, root: &Path, path: &Path) -> Option<SyncEvent> {
        if !path.is_file() {
            return None;
        }

        let rel = path.strip_prefix(root).ok()?.to_path_buf();
        match file_hash_and_size(path) {
            Ok((hash, size)) => Some(SyncEvent::FileModified {
                path: rel,
                content_hash: hash,
                size,
            }),
            Err(e) => {
                warn!("Failed to hash {}: {}", path.display(), e);
                None
            }
        }
    }

    fn remove_event(
        &self,
        root: &Path,
        path: &Path,
        kind: Option<&RemoveKind>,
    ) -> Option<SyncEvent> {
        let rel = path.strip_prefix(root).ok()?.to_path_buf();
        let is_file = match kind {
            Some(RemoveKind::File) => true,
            Some(RemoveKind::Folder) => false,
            _ => self.tracker.is_known_file(&rel) || rel.extension().is_some(),
        };

        if is_file {
            Some(SyncEvent::FileDeleted { path: rel })
        } else {
            Some(SyncEvent::DirDeleted { path: rel })
        }
    }

    /// Translate a notify event into our SyncEvent types.
    fn translate_event(&self, root: &Path, event: &Event) -> Option<Vec<SyncEvent>> {
        // Skip ignored directories
        for path in &event.paths {
            if self.is_ignored(path) {
                return None;
            }
        }

        let mut result = Vec::new();

        match &event.kind {
            EventKind::Create(_) => {
                for path in &event.paths {
                    if let Some(sync_event) = self.create_event(root, path) {
                        result.push(sync_event);
                    }
                }
            }
            EventKind::Modify(ModifyKind::Name(mode)) => match mode {
                RenameMode::From => {
                    if let Some(path) = event.paths.first()
                        && let Some(sync_event) = self.remove_event(root, path, None)
                    {
                        result.push(sync_event);
                    }
                }
                RenameMode::To => {
                    if let Some(path) = event.paths.last()
                        && let Some(sync_event) = self.create_event(root, path)
                    {
                        result.push(sync_event);
                    }
                }
                RenameMode::Both => {
                    if let Some(path) = event.paths.first()
                        && let Some(sync_event) = self.remove_event(root, path, None)
                    {
                        result.push(sync_event);
                    }
                    if let Some(path) = event.paths.get(1).or_else(|| event.paths.last())
                        && let Some(sync_event) = self.create_event(root, path)
                    {
                        result.push(sync_event);
                    }
                }
                RenameMode::Any | RenameMode::Other => {
                    for path in &event.paths {
                        if let Some(sync_event) = self.create_event(root, path) {
                            result.push(sync_event);
                        } else if let Some(sync_event) = self.remove_event(root, path, None) {
                            result.push(sync_event);
                        }
                    }
                }
            },
            EventKind::Modify(_) => {
                for path in &event.paths {
                    if let Some(sync_event) = self.modify_event(root, path) {
                        result.push(sync_event);
                    }
                }
            }
            EventKind::Remove(kind) => {
                for path in &event.paths {
                    if let Some(sync_event) = self.remove_event(root, path, Some(kind)) {
                        result.push(sync_event);
                    }
                }
            }
            _ => {}
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    fn is_ignored(&self, path: &Path) -> bool {
        self.ignore_dirs
            .iter()
            .any(|ignore| path.starts_with(ignore))
            || path
                .components()
                .any(|c| c.as_os_str() == INTERNAL_TEMP_DIR)
    }
}

/// Compute blake3 hash and size from already-read data.
pub fn hash_data(data: &[u8]) -> ([u8; 32], u64) {
    let size = data.len() as u64;
    let mut hasher = Hasher::new();
    hasher.update(data);
    (*hasher.finalize().as_bytes(), size)
}

/// Read a file and compute its blake3 hash and size.
/// Uses a streaming 1 MB buffer so memory stays constant regardless of file
/// size.  Retries up to 3 times with 100 ms delay on transient failures.
pub fn file_hash_and_size(path: &Path) -> Result<([u8; 32], u64)> {
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..=3u32 {
        match streaming_hash_inner(path) {
            Ok(v) => return Ok(v),
            Err(e) => {
                last_err = Some(e);
                if attempt < 3 {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }
    anyhow::bail!(
        "Failed to hash {} after 3 retries: {}",
        path.display(),
        last_err.unwrap()
    )
}

/// Streaming inner implementation: hash a file by reading it in 1 MB chunks so
/// that memory usage stays constant regardless of file size.
fn streaming_hash_inner(path: &Path) -> Result<([u8; 32], u64)> {
    use std::io::Read;
    let mut file =
        fs::File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    let mut hasher = Hasher::new();
    let mut size = 0u64;
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok((*hasher.finalize().as_bytes(), size))
}

/// Wait until a file has stopped changing for a sustained quiet period.
///
/// This is critical for large files being copied — the watcher may detect
/// creation/modification while the writer is still appending bytes. A single
/// equal-size sample is not enough because large copies can briefly pause; we
/// require both size and modification time to remain unchanged for multiple
/// consecutive polls.
///
/// Returns the file size once stable, or an error if it never stabilizes.
pub fn wait_for_stable(path: &Path, interval: Duration, max_wait: Duration) -> Result<u64> {
    let initial =
        fs::metadata(path).with_context(|| format!("Failed to stat {}", path.display()))?;
    let prev_size = initial.len();
    let prev_modified = initial.modified().ok();
    let required_quiet = std::cmp::max(interval.saturating_mul(10), Duration::from_secs(2));

    // Fast path: if the file's mtime is already older than `required_quiet`,
    // it has been stable long enough — no need to poll at all.  This covers
    // files that finished writing before the watcher event fired (the common
    // case for small/medium files).
    if let Some(mtime) = prev_modified
        && let Ok(elapsed_since_write) = mtime.elapsed()
        && elapsed_since_write >= required_quiet
    {
        return Ok(prev_size);
    }

    // Slow path: poll until the file stops changing for `required_quiet`.
    let mut prev_size = prev_size;
    let mut prev_modified = prev_modified;
    let mut quiet_for = Duration::ZERO;
    let mut elapsed = Duration::ZERO;

    loop {
        std::thread::sleep(interval);
        elapsed += interval;

        let metadata = match fs::metadata(path) {
            Ok(m) => m,
            Err(_) => {
                // File disappeared or is temporarily unavailable — restart wait.
                quiet_for = Duration::ZERO;
                if elapsed >= max_wait {
                    anyhow::bail!(
                        "File {} did not stabilize within {}ms",
                        path.display(),
                        max_wait.as_millis()
                    );
                }
                continue;
            }
        };

        let cur_size = metadata.len();
        let cur_modified = metadata.modified().ok();

        if cur_size == prev_size && cur_modified == prev_modified {
            quiet_for += interval;
            if quiet_for >= required_quiet {
                return Ok(cur_size);
            }
        } else {
            prev_size = cur_size;
            prev_modified = cur_modified;
            quiet_for = Duration::ZERO;
        }

        if elapsed >= max_wait {
            anyhow::bail!(
                "File {} did not stabilize within {}ms (last size: {} bytes)",
                path.display(),
                max_wait.as_millis(),
                cur_size
            );
        }
    }
}



/// Perform initial full scan of a directory, returning SyncEvents for all contents.
pub fn initial_scan(root: &Path, ignore_dirs: &[String]) -> Vec<SyncEvent> {
    let mut events = Vec::new();

    for entry in WalkDir::new(root).into_iter().filter_entry(|e| {
        let name = e.file_name().to_string_lossy();
        name.as_ref() != INTERNAL_TEMP_DIR
            && !ignore_dirs.iter().any(|d| name.as_ref() == d.as_str())
    }) {
        match entry {
            Ok(entry) => {
                let path = entry.path();
                let rel = match path.strip_prefix(root) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                // Skip root itself
                if rel.as_os_str().is_empty() {
                    continue;
                }

                if path.is_dir() {
                    events.push(SyncEvent::DirCreated {
                        path: rel.to_path_buf(),
                    });
                } else if path.is_file() {
                    match file_hash_and_size(path) {
                        Ok((hash, size)) => {
                            events.push(SyncEvent::FileCreated {
                                path: rel.to_path_buf(),
                                content_hash: hash,
                                size,
                            });
                        }
                        Err(e) => {
                            warn!("Failed to hash {}: {}", path.display(), e);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Walk error: {}", e);
            }
        }
    }

    info!("Initial scan found {} entries", events.len());
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    // --- FileStateTracker tests ---

    #[test]
    fn test_tracker_new_file() {
        let mut tracker = FileStateTracker::new();
        let path = PathBuf::from("a.txt");
        let hash = [1u8; 32];
        let (known, _) = tracker.check(&path, &hash, &100);
        assert!(!known);

        tracker.record(path.clone(), hash, 100);
        let (known, changed) = tracker.check(&path, &hash, &100);
        assert!(known);
        assert!(!changed);
    }

    #[test]
    fn test_tracker_detects_change() {
        let mut tracker = FileStateTracker::new();
        let path = PathBuf::from("a.txt");
        let hash1 = [1u8; 32];
        let hash2 = [2u8; 32];

        tracker.record(path.clone(), hash1, 100);
        let (known, changed) = tracker.check(&path, &hash2, &200);
        assert!(known);
        assert!(changed);
    }

    #[test]
    fn test_tracker_remove() {
        let mut tracker = FileStateTracker::new();
        let path = PathBuf::from("a.txt");
        tracker.record(path.clone(), [0u8; 32], 50);
        tracker.remove(&path);
        let (known, _) = tracker.check(&path, &[0u8; 32], &50);
        assert!(!known);
    }

    #[test]
    fn test_remove_extensionless_tracked_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("LICENSE");
        fs::write(&path, b"license").unwrap();

        let mut watcher = FsWatcher::new(dir.path(), Duration::from_millis(10), &[]).unwrap();
        watcher.seed_tracker(dir.path(), &[]);
        fs::remove_file(&path).unwrap();

        let event = Event::new(EventKind::Remove(RemoveKind::Any)).add_path(path);
        let events = watcher.translate_event(dir.path(), &event).unwrap();
        assert!(matches!(
            &events[0],
            SyncEvent::FileDeleted { path } if path == Path::new("LICENSE")
        ));
    }

    // --- initial_scan tests ---

    #[test]
    fn test_initial_scan_empty_dir() {
        let dir = TempDir::new().unwrap();
        let events = initial_scan(dir.path(), &[]);
        assert!(events.is_empty());
    }

    #[test]
    fn test_initial_scan_with_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.txt"), b"hello").unwrap();
        fs::write(dir.path().join("b.txt"), b"world").unwrap();

        let events = initial_scan(dir.path(), &[]);
        assert_eq!(events.len(), 2);

        let paths: Vec<String> = events
            .iter()
            .filter_map(|e| e.path().map(|p| p.display().to_string()))
            .collect();
        assert!(paths.contains(&"a.txt".to_string()));
        assert!(paths.contains(&"b.txt".to_string()));
    }

    #[test]
    fn test_initial_scan_with_subdirs() {
        let dir = TempDir::new().unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/file.txt"), b"data").unwrap();

        let events = initial_scan(dir.path(), &[]);
        assert!(events.len() >= 2);

        let has_dir = events
            .iter()
            .any(|e| matches!(e, SyncEvent::DirCreated { path } if path == Path::new("sub")));
        let has_file = events.iter().any(|e| {
            matches!(e, SyncEvent::FileCreated { path, .. } if path == Path::new("sub/file.txt"))
        });
        assert!(has_dir, "Expected DirCreated for 'sub'");
        assert!(has_file, "Expected FileCreated for 'sub/file.txt'");
    }

    #[test]
    fn test_initial_scan_ignores_dirs() {
        let dir = TempDir::new().unwrap();
        fs::create_dir(dir.path().join(".git")).unwrap();
        fs::write(dir.path().join(".git/config"), b"data").unwrap();
        fs::write(dir.path().join("real.txt"), b"data").unwrap();

        let events = initial_scan(dir.path(), &[".git".to_string()]);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            SyncEvent::FileCreated { path, .. } if path == Path::new("real.txt")
        ));
    }

    // --- file_hash_and_size tests ---

    #[test]
    fn test_file_hash_and_size() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("test.txt");
        fs::write(&file, b"hello world").unwrap();

        let (hash, size) = file_hash_and_size(&file).unwrap();
        assert_eq!(size, 11);
        let (hash2, _) = file_hash_and_size(&file).unwrap();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_file_hash_different_content() {
        let dir = TempDir::new().unwrap();
        let f1 = dir.path().join("a.txt");
        let f2 = dir.path().join("b.txt");
        fs::write(&f1, b"hello").unwrap();
        fs::write(&f2, b"world").unwrap();

        let (h1, _) = file_hash_and_size(&f1).unwrap();
        let (h2, _) = file_hash_and_size(&f2).unwrap();
        assert_ne!(h1, h2);
    }
}
