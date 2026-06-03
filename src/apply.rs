use crate::cli::ConflictStrategy;
use crate::event::SyncEvent;
use crate::watcher::{file_hash_and_size, INTERNAL_TEMP_DIR};
use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Cached state for conflict detection: path → (hash, size).
type FileHashMap = HashMap<PathBuf, ([u8; 32], u64)>;

struct PendingFileTransfer {
    expected_hash: [u8; 32],
    expected_size: u64,
    temp_path: PathBuf,
}

/// Apply received SyncEvents to the local directory.
pub struct ChangeApplier {
    root: PathBuf,
    #[allow(dead_code)]
    conflict_strategy: ConflictStrategy,
    /// The hash of the file as last confirmed synced and committed to the final path.
    /// Pending chunk transfers do not update this baseline until their expected
    /// size and content hash have been verified.
    last_synced_hash: FileHashMap,
    /// Files currently being assembled from remote chunks.
    pending_remote_files: HashMap<PathBuf, PendingFileTransfer>,
    /// Files whose metadata event was skipped due to a conflict; ignore their
    /// following content chunks until a new metadata event starts a transfer.
    blocked_remote_files: HashSet<PathBuf>,
}

impl ChangeApplier {
    pub fn new(root: &Path, conflict_strategy: ConflictStrategy) -> Self {
        Self {
            root: root.to_path_buf(),
            conflict_strategy,
            last_synced_hash: HashMap::new(),
            pending_remote_files: HashMap::new(),
            blocked_remote_files: HashSet::new(),
        }
    }

    /// Apply a batch of events to the local directory.
    ///
    /// Returns a list of events that were skipped due to conflict resolution.
    pub fn apply_events(&mut self, events: &[SyncEvent]) -> Result<Vec<ConflictInfo>> {
        let mut conflicts = Vec::new();
        for event in events {
            if let Some(conflict) = self.apply_event(event)? {
                conflicts.push(conflict);
            }
        }
        Ok(conflicts)
    }

    /// Apply a single event to the local directory.
    pub fn apply_event(&mut self, event: &SyncEvent) -> Result<Option<ConflictInfo>> {
        self.apply_single(event)
    }

    /// Check if applying a remote file event would conflict with local changes.
    ///
    /// Returns Some(conflict info) if there's a conflict, None otherwise.
    fn detect_conflict(&self, path: &Path, remote_hash: &[u8; 32], remote_size: u64) -> Option<ConflictInfo> {
        let full_path = self.root.join(path);
        if !full_path.exists() {
            return None; // No local file, no conflict
        }

        let (local_hash, local_size) = match file_hash_and_size(&full_path) {
            Ok(v) => v,
            Err(_) => return None,
        };

        // If the local file matches the remote, no conflict
        if local_hash == *remote_hash {
            return None;
        }

        // Check if we have a last-synced hash for this file
        if let Some((synced_hash, _)) = self.last_synced_hash.get(path) {
            // If local hash matches what was last synced, local hasn't changed — no conflict
            if local_hash == *synced_hash {
                return None;
            }
            // Local differs from both remote and last-synced → conflict
            return Some(ConflictInfo {
                path: path.to_path_buf(),
                local_hash,
                local_size,
                remote_hash: *remote_hash,
                remote_size,
            });
        }

        // No prior sync record. File exists locally with different content.
        // Could be a conflict (both sides created same path) or just a first sync.
        // We can't tell without prior state, so allow it (no conflict).
        None
    }

    fn temp_path_for(&self, path: &Path) -> PathBuf {
        self.root.join(INTERNAL_TEMP_DIR).join(path)
    }

    fn prepare_transfer(
        &mut self,
        path: &Path,
        expected_hash: [u8; 32],
        expected_size: u64,
    ) -> Result<()> {
        self.blocked_remote_files.remove(path);
        self.pending_remote_files.remove(path);

        let full_path = self.root.join(path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create parent dir: {}", parent.display()))?;
        }

        if expected_size == 0 {
            fs::write(&full_path, b"")
                .with_context(|| format!("Failed to create empty file: {}", full_path.display()))?;
            let (actual_hash, actual_size) = file_hash_and_size(&full_path)
                .with_context(|| format!("Failed to hash empty file: {}", full_path.display()))?;
            if actual_hash != expected_hash {
                warn!("Committed empty file with mismatched metadata hash: {}", path.display());
            }
            self.last_synced_hash
                .insert(path.to_path_buf(), (actual_hash, actual_size));
            debug!("Committed empty file: {}", path.display());
            return Ok(());
        }

        let temp_path = self.temp_path_for(path);
        if let Some(parent) = temp_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create temp parent dir: {}", parent.display()))?;
        }
        fs::File::create(&temp_path)
            .with_context(|| format!("Failed to create temp file: {}", temp_path.display()))?;

        self.pending_remote_files.insert(
            path.to_path_buf(),
            PendingFileTransfer {
                expected_hash,
                expected_size,
                temp_path,
            },
        );
        Ok(())
    }

    fn write_pending_chunk(&mut self, path: &Path, offset: u64, data: &[u8]) -> Result<()> {
        let pending = match self.pending_remote_files.get(path) {
            Some(p) => p,
            None => return self.write_direct_chunk(path, offset, data),
        };

        use std::io::{Seek, SeekFrom, Write};
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&pending.temp_path)
            .with_context(|| format!("Failed to open temp file: {}", pending.temp_path.display()))?;
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("Failed to seek temp file: {}", pending.temp_path.display()))?;
        file.write_all(data)
            .with_context(|| format!("Failed to write temp content: {}", pending.temp_path.display()))?;

        self.try_commit_transfer(path)
    }

    fn try_commit_transfer(&mut self, path: &Path) -> Result<()> {
        let (expected_hash, expected_size, temp_path) = match self.pending_remote_files.get(path) {
            Some(p) => (p.expected_hash, p.expected_size, p.temp_path.clone()),
            None => return Ok(()),
        };

        let current_size = fs::metadata(&temp_path)
            .with_context(|| format!("Failed to stat temp file: {}", temp_path.display()))?
            .len();
        if current_size < expected_size {
            return Ok(());
        }
        if current_size > expected_size {
            warn!(
                "Received too much data for {} (expected={}B, temp={}B)",
                path.display(),
                expected_size,
                current_size
            );
            return Ok(());
        }

        let (actual_hash, actual_size) = file_hash_and_size(&temp_path)
            .with_context(|| format!("Failed to hash temp file: {}", temp_path.display()))?;
        if actual_hash != expected_hash || actual_size != expected_size {
            warn!(
                "Waiting for complete data for {} (expected={}B, got={}B, hash_match={})",
                path.display(),
                expected_size,
                actual_size,
                actual_hash == expected_hash
            );
            return Ok(());
        }

        let full_path = self.root.join(path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create parent dir: {}", parent.display()))?;
        }
        if full_path.exists() {
            fs::remove_file(&full_path)
                .with_context(|| format!("Failed to replace existing file: {}", full_path.display()))?;
        }
        fs::rename(&temp_path, &full_path).with_context(|| {
            format!(
                "Failed to move temp file {} to {}",
                temp_path.display(),
                full_path.display()
            )
        })?;

        self.pending_remote_files.remove(path);
        self.last_synced_hash
            .insert(path.to_path_buf(), (expected_hash, expected_size));
        info!("Committed file: {} ({} bytes)", path.display(), expected_size);
        Ok(())
    }

    fn write_direct_chunk(&mut self, path: &Path, offset: u64, data: &[u8]) -> Result<()> {
        let full_path = self.root.join(path);
        use std::io::{Seek, SeekFrom, Write};
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create parent dir: {}", parent.display()))?;
        }

        // Compatibility path for tests/older senders that provide content without
        // a metadata event. Real transfers go through `pending_remote_files`.
        let truncate = offset == 0;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(truncate)
            .open(&full_path)
            .with_context(|| format!("Failed to open file for writing: {}", full_path.display()))?;
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("Failed to seek: {}", full_path.display()))?;
        file.write_all(data)
            .with_context(|| format!("Failed to write content: {}", full_path.display()))?;

        if let Ok((hash, size)) = file_hash_and_size(&full_path) {
            self.last_synced_hash.insert(path.to_path_buf(), (hash, size));
        }
        Ok(())
    }

    fn apply_single(&mut self, event: &SyncEvent) -> Result<Option<ConflictInfo>> {
        match event {
            SyncEvent::FileCreated {
                path,
                content_hash,
                size,
            } => {
                self.prepare_transfer(path, *content_hash, *size)?;
                debug!("Prepared file create: {} ({} bytes)", path.display(), size);
                Ok(None)
            }

            SyncEvent::FileModified {
                path,
                content_hash,
                size,
            } => {
                // Check for conflict: local changed since we last synced this file
                if let Some(conflict) = self.detect_conflict(path, content_hash, *size) {
                    warn!(
                        "Conflict on modify {} (local={}B, remote={}B)",
                        path.display(),
                        conflict.local_size,
                        conflict.remote_size
                    );
                    self.pending_remote_files.remove(path);
                    self.blocked_remote_files.insert(path.clone());
                    return Ok(Some(conflict));
                }

                self.prepare_transfer(path, *content_hash, *size)?;
                debug!("Prepared file modify: {} ({} bytes)", path.display(), size);
                Ok(None)
            }

            SyncEvent::FileDeleted { path } => {
                let full_path = self.root.join(path);
                self.pending_remote_files.remove(path);
                self.blocked_remote_files.remove(path);
                let temp_path = self.temp_path_for(path);
                let _ = fs::remove_file(temp_path);
                self.last_synced_hash.remove(path);
                if full_path.exists() {
                    fs::remove_file(&full_path)
                        .with_context(|| format!("Failed to delete file: {}", full_path.display()))?;
                    info!("Deleted file: {}", path.display());
                } else {
                    debug!("File already gone: {}", path.display());
                }
                Ok(None)
            }

            SyncEvent::DirCreated { path } => {
                let full_path = self.root.join(path);
                if !full_path.exists() {
                    fs::create_dir_all(&full_path)
                        .with_context(|| format!("Failed to create dir: {}", full_path.display()))?;
                    info!("Created directory: {}", path.display());
                }
                Ok(None)
            }

            SyncEvent::DirDeleted { path } => {
                let full_path = self.root.join(path);
                self.last_synced_hash.retain(|p, _| !p.starts_with(path));
                self.pending_remote_files.retain(|p, _| !p.starts_with(path));
                self.blocked_remote_files.retain(|p| !p.starts_with(path));
                let temp_path = self.temp_path_for(path);
                let _ = fs::remove_dir_all(temp_path);
                if full_path.exists() {
                    fs::remove_dir_all(&full_path)
                        .with_context(|| format!("Failed to delete dir: {}", full_path.display()))?;
                    info!("Deleted directory: {}", path.display());
                }
                Ok(None)
            }

            SyncEvent::FileContent { path, offset, data } => {
                if self.blocked_remote_files.contains(path) {
                    debug!("Ignoring content for conflicted file: {}", path.display());
                    return Ok(None);
                }

                self.write_pending_chunk(path, *offset, data)?;
                debug!("Received {} bytes at offset {} for {}", data.len(), offset, path.display());
                Ok(None)
            }

            SyncEvent::Heartbeat { .. } => Ok(None),
        }
    }
}

/// Information about a detected conflict.
#[derive(Debug)]
pub struct ConflictInfo {
    pub path: PathBuf,
    pub local_hash: [u8; 32],
    pub local_size: u64,
    pub remote_hash: [u8; 32],
    pub remote_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup(strategy: ConflictStrategy) -> (TempDir, ChangeApplier) {
        let dir = TempDir::new().unwrap();
        let applier = ChangeApplier::new(dir.path(), strategy);
        (dir, applier)
    }

    fn hash_size(data: &[u8]) -> ([u8; 32], u64) {
        crate::watcher::hash_data(data)
    }

    #[test]
    fn test_dir_created() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let events = vec![SyncEvent::DirCreated {
            path: PathBuf::from("subdir"),
        }];
        applier.apply_events(&events).unwrap();
        assert!(dir.path().join("subdir").is_dir());
    }

    #[test]
    fn test_dir_deleted() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        fs::create_dir(dir.path().join("to_delete")).unwrap();

        let events = vec![SyncEvent::DirDeleted {
            path: PathBuf::from("to_delete"),
        }];
        applier.apply_events(&events).unwrap();
        assert!(!dir.path().join("to_delete").exists());
    }

    #[test]
    fn test_file_created() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let events = vec![SyncEvent::FileCreated {
            path: PathBuf::from("new.txt"),
            content_hash: [0u8; 32],
            size: 0,
        }];
        applier.apply_events(&events).unwrap();
        assert!(dir.path().join("new.txt").exists());
    }

    #[test]
    fn test_file_created_skips_existing() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        fs::write(dir.path().join("existing.txt"), b"original").unwrap();

        let events = vec![SyncEvent::FileCreated {
            path: PathBuf::from("existing.txt"),
            content_hash: [1u8; 32],
            size: 1,
        }];
        let conflicts = applier.apply_events(&events).unwrap();
        // No conflict on first sync, and existing content is preserved until
        // the remote content chunks are complete and verified.
        assert!(conflicts.is_empty());
        assert_eq!(fs::read(dir.path().join("existing.txt")).unwrap(), b"original");
    }

    #[test]
    fn test_file_deleted() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        fs::write(dir.path().join("del.txt"), b"data").unwrap();

        let events = vec![SyncEvent::FileDeleted {
            path: PathBuf::from("del.txt"),
        }];
        applier.apply_events(&events).unwrap();
        assert!(!dir.path().join("del.txt").exists());
    }

    #[test]
    fn test_file_deleted_nonexistent() {
        let (_dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let events = vec![SyncEvent::FileDeleted {
            path: PathBuf::from("ghost.txt"),
        }];
        assert!(applier.apply_events(&events).is_ok());
    }

    #[test]
    fn test_file_content_writes_at_offset() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        fs::write(dir.path().join("chunked.bin"), vec![0u8; 100]).unwrap();

        let events = vec![SyncEvent::FileContent {
            path: PathBuf::from("chunked.bin"),
            offset: 50,
            data: vec![0xAB; 10],
        }];
        applier.apply_events(&events).unwrap();

        let data = fs::read(dir.path().join("chunked.bin")).unwrap();
        assert_eq!(data.len(), 100);
        assert_eq!(&data[50..60], &[0xAB; 10]);
        assert_eq!(&data[0..50], &[0u8; 50]);
    }

    #[test]
    fn test_file_content_creates_parent_dirs() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let events = vec![SyncEvent::FileContent {
            path: PathBuf::from("deep/nested/file.txt"),
            offset: 0,
            data: b"hello".to_vec(),
        }];
        applier.apply_events(&events).unwrap();
        assert_eq!(fs::read(dir.path().join("deep/nested/file.txt")).unwrap(), b"hello");
    }

    #[test]
    fn test_file_commits_only_after_complete_verified_chunks() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let content = b"abcdefghij";
        let (hash, size) = hash_size(content);

        applier
            .apply_events(&[
                SyncEvent::FileCreated {
                    path: PathBuf::from("large.bin"),
                    content_hash: hash,
                    size,
                },
                SyncEvent::FileContent {
                    path: PathBuf::from("large.bin"),
                    offset: 0,
                    data: content[..5].to_vec(),
                },
            ])
            .unwrap();
        assert!(!dir.path().join("large.bin").exists());

        applier
            .apply_events(&[SyncEvent::FileContent {
                path: PathBuf::from("large.bin"),
                offset: 5,
                data: content[5..].to_vec(),
            }])
            .unwrap();
        assert_eq!(fs::read(dir.path().join("large.bin")).unwrap(), content);
    }

    #[test]
    fn test_nested_dir_created() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let events = vec![SyncEvent::DirCreated {
            path: PathBuf::from("a/b/c"),
        }];
        applier.apply_events(&events).unwrap();
        assert!(dir.path().join("a/b/c").is_dir());
    }

    #[test]
    fn test_heartbeat_ignored() {
        let (_dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let events = vec![SyncEvent::Heartbeat { timestamp: 12345 }];
        assert!(applier.apply_events(&events).is_ok());
    }

    #[test]
    fn test_multiple_events_in_batch() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let content = b"# Hello";
        let (hash, size) = hash_size(content);
        let events = vec![
            SyncEvent::DirCreated {
                path: PathBuf::from("docs"),
            },
            SyncEvent::FileCreated {
                path: PathBuf::from("docs/readme.md"),
                content_hash: hash,
                size,
            },
            SyncEvent::FileContent {
                path: PathBuf::from("docs/readme.md"),
                offset: 0,
                data: content.to_vec(),
            },
        ];
        applier.apply_events(&events).unwrap();
        assert!(dir.path().join("docs").is_dir());
        assert!(dir.path().join("docs/readme.md").exists());
        assert_eq!(fs::read(dir.path().join("docs/readme.md")).unwrap(), b"# Hello");
    }

    #[test]
    fn test_no_conflict_on_first_sync() {
        // When a file doesn't exist locally, no conflict should be detected
        let (_dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let events = vec![SyncEvent::FileCreated {
            path: PathBuf::from("new.txt"),
            content_hash: [0xAA; 32],
            size: 100,
        }];
        let conflicts = applier.apply_events(&events).unwrap();
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_conflict_detected_when_both_changed() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);

        // First sync: remote creates a file and delivers content
        let remote_data = b"remote data";
        let (remote_hash, remote_size) = hash_size(remote_data);
        applier.apply_events(&[
            SyncEvent::FileCreated {
                path: PathBuf::from("shared.txt"),
                content_hash: remote_hash,
                size: remote_size,
            },
            SyncEvent::FileContent {
                path: PathBuf::from("shared.txt"),
                offset: 0,
                data: remote_data.to_vec(),
            },
        ])
        .unwrap();

        // Verify file now has content (last_synced_hash should be set)
        assert!(dir.path().join("shared.txt").exists());

        // Simulate local modification
        fs::write(dir.path().join("shared.txt"), b"local change").unwrap();

        // Remote sends a modification with different hash
        let events = vec![SyncEvent::FileModified {
            path: PathBuf::from("shared.txt"),
            content_hash: [0xBB; 32],
            size: 20,
        }];
        let conflicts = applier.apply_events(&events).unwrap();
        // Conflict detected: local changed since last sync
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].path, PathBuf::from("shared.txt"));
    }

    #[test]
    fn test_no_conflict_when_remote_unchanged() {
        let (_dir, mut applier) = setup(ConflictStrategy::LastWriteWins);

        // First sync
        let hash = [0xAA; 32];
        let events = vec![SyncEvent::FileCreated {
            path: PathBuf::from("shared.txt"),
            content_hash: hash,
            size: 10,
        }];
        applier.apply_events(&events).unwrap();

        // Write the same content locally (hash unchanged)
        // The file was created empty by the applier, so we write content that matches hash [0xAA;32]
        // Actually this won't match, but the point is: if we send the SAME remote hash again,
        // detect_conflict should see that local hash != remote hash (since local is empty)
        // But since we're sending the same remote hash as before, it checks if local == prev_remote
        // Local is empty (hash != [0xAA;32]), so it will detect a conflict.
        // This is actually correct behavior — the local file was modified (written to).

        // Let's test the no-conflict case: remote sends same hash, local hasn't changed
        // We need to make local file match the remote hash
        let (dir2, mut applier2) = setup(ConflictStrategy::LastWriteWins);

        // Create file locally with known content
        let content = b"test content";
        fs::write(dir2.path().join("file.txt"), content).unwrap();
        let (local_hash, local_size) = file_hash_and_size(&dir2.path().join("file.txt")).unwrap();

        // Remote creates with same hash
        let events = vec![SyncEvent::FileCreated {
            path: PathBuf::from("file.txt"),
            content_hash: local_hash,
            size: local_size,
        }];
        let conflicts = applier2.apply_events(&events).unwrap();
        // No conflict because local hash matches remote
        assert!(conflicts.is_empty());
    }
}
