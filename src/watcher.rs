use crate::event::SyncEvent;
use anyhow::{Context, Result};
use blake3::Hasher;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;
use tracing::{info, warn};
use walkdir::WalkDir;

/// File system watcher that emits SyncEvents for local changes.
pub struct FsWatcher {
    watcher: RecommendedWatcher,
    event_rx: Receiver<notify::Result<Event>>,
    ignore_dirs: HashSet<PathBuf>,
    debounce: Duration,
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

        let ignore_set: HashSet<PathBuf> = ignore_dirs
            .iter()
            .map(|d| root.join(d))
            .collect();

        Ok(Self {
            watcher,
            event_rx: rx,
            ignore_dirs: ignore_set,
            debounce,
        })
    }

    /// Start watching the given directory.
    pub fn watch(&mut self, root: &Path) -> Result<()> {
        self.watcher
            .watch(root, RecursiveMode::Recursive)
            .context("Failed to start watching directory")?;
        info!("Watching directory: {}", root.display());
        Ok(())
    }

    /// Collect pending events with debounce.
    ///
    /// Returns deduplicated events after waiting for the debounce window.
    #[expect(dead_code)]
    pub fn collect_events(&self, root: &Path) -> Vec<SyncEvent> {
        let mut events = Vec::new();
        let mut seen_paths = HashSet::new();

        // Drain all immediately available events
        while let Ok(result) = self.event_rx.try_recv() {
            match result {
                Ok(event) => {
                    if let Some(sync_events) = self.translate_event(root, &event) {
                        for e in sync_events {
                            let key = event_key(&e);
                            if seen_paths.insert(key) {
                                events.push(e);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Watch error: {}", e);
                }
            }
        }

        events
    }

    /// Block until at least one event arrives, then collect with debounce.
    pub fn collect_events_blocking(&self, root: &Path) -> Vec<SyncEvent> {
        // Block for first event
        match self.event_rx.recv() {
            Ok(Ok(event)) => {
                let mut events = Vec::new();
                let mut seen_paths = HashSet::new();

                if let Some(sync_events) = self.translate_event(root, &event) {
                    for e in sync_events {
                        let key = event_key(&e);
                        if seen_paths.insert(key) {
                            events.push(e);
                        }
                    }
                }

                // Debounce: wait then drain remaining
                std::thread::sleep(self.debounce);

                while let Ok(Ok(event)) = self.event_rx.try_recv() {
                    if let Some(sync_events) = self.translate_event(root, &event) {
                        for e in sync_events {
                            let key = event_key(&e);
                            if seen_paths.insert(key) {
                                events.push(e);
                            }
                        }
                    }
                }

                events
            }
            Ok(Err(e)) => {
                warn!("Watch error: {}", e);
                Vec::new()
            }
            Err(_) => Vec::new(),
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

        match event.kind {
            EventKind::Create(_) => {
                let mut result = Vec::new();
                for path in &event.paths {
                    let rel = path.strip_prefix(root).ok()?;
                    if path.is_dir() {
                        result.push(SyncEvent::DirCreated { path: rel.to_path_buf() });
                    } else {
                        match file_hash_and_size(path) {
                            Ok((hash, size)) => {
                                result.push(SyncEvent::FileCreated {
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
                Some(result)
            }
            EventKind::Modify(_) => {
                let mut result = Vec::new();
                for path in &event.paths {
                    if path.is_file() {
                        let rel = path.strip_prefix(root).ok()?;
                        match file_hash_and_size(path) {
                            Ok((hash, size)) => {
                                result.push(SyncEvent::FileModified {
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
                if result.is_empty() { None } else { Some(result) }
            }
            EventKind::Remove(_) => {
                let mut result = Vec::new();
                for path in &event.paths {
                    let rel = path.strip_prefix(root).ok()?;
                    // Heuristic: if path has an extension, treat as file
                    if rel.extension().is_some() {
                        result.push(SyncEvent::FileDeleted { path: rel.to_path_buf() });
                    } else {
                        result.push(SyncEvent::DirDeleted { path: rel.to_path_buf() });
                    }
                }
                Some(result)
            }
            _ => None,
        }
    }

    fn is_ignored(&self, path: &Path) -> bool {
        for ignore in &self.ignore_dirs {
            if path.starts_with(ignore) {
                return true;
            }
        }
        false
    }
}

/// Compute blake3 hash and size of a file.
pub fn file_hash_and_size(path: &Path) -> Result<([u8; 32], u64)> {
    let data = fs::read(path).context("Failed to read file for hashing")?;
    let size = data.len() as u64;
    let mut hasher = Hasher::new();
    hasher.update(&data);
    let hash = hasher.finalize();
    Ok((*hash.as_bytes(), size))
}

/// Perform initial full scan of a directory, returning SyncEvents for all contents.
pub fn initial_scan(root: &Path, ignore_dirs: &[String]) -> Vec<SyncEvent> {
    let mut events = Vec::new();

    for entry in WalkDir::new(root)
        .into_iter()
        .filter_entry(|e| {
            let name = e.file_name().to_string_lossy();
            !ignore_dirs.iter().any(|d| name.as_ref() == d.as_str())
        })
    {
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

/// Generate a deduplication key for a SyncEvent.
fn event_key(event: &SyncEvent) -> String {
    match event {
        SyncEvent::FileCreated { path, .. } => format!("C:{}", path.display()),
        SyncEvent::FileModified { path, .. } => format!("M:{}", path.display()),
        SyncEvent::FileDeleted { path } => format!("D:{}", path.display()),
        SyncEvent::DirCreated { path } => format!("DC:{}", path.display()),
        SyncEvent::DirDeleted { path } => format!("DD:{}", path.display()),
        SyncEvent::FileContent { path, offset, .. } => format!("FC:{}:{}", path.display(), offset),
        SyncEvent::Heartbeat { .. } => "HB".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

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
        // Should have: DirCreated("sub"), FileCreated("sub/file.txt")
        assert!(events.len() >= 2);

        let has_dir = events.iter().any(|e| matches!(e, SyncEvent::DirCreated { path } if path == std::path::Path::new("sub")));
        let has_file = events.iter().any(|e| matches!(e, SyncEvent::FileCreated { path, .. } if path == std::path::Path::new("sub/file.txt")));
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
        // Only real.txt should appear
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            SyncEvent::FileCreated { path, .. } if path == std::path::Path::new("real.txt")
        ));
    }

    #[test]
    fn test_file_hash_and_size() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("test.txt");
        fs::write(&file, b"hello world").unwrap();

        let (hash, size) = file_hash_and_size(&file).unwrap();
        assert_eq!(size, 11);
        // Hash should be deterministic
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

    #[test]
    fn test_event_key_dedup() {
        let e1 = SyncEvent::FileCreated {
            path: std::path::PathBuf::from("a.txt"),
            content_hash: [0u8; 32],
            size: 100,
        };
        let e2 = SyncEvent::FileModified {
            path: std::path::PathBuf::from("a.txt"),
            content_hash: [1u8; 32],
            size: 200,
        };
        // Different event types for same path should have different keys
        assert_ne!(event_key(&e1), event_key(&e2));
    }

    #[test]
    fn test_event_key_heartbeat() {
        let e = SyncEvent::Heartbeat { timestamp: 123 };
        assert_eq!(event_key(&e), "HB");
    }
}
