use crate::cli::ConflictStrategy;
use crate::event::SyncEvent;
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use tracing::{debug, info};

/// Apply received SyncEvents to the local directory.
pub struct ChangeApplier {
    root: std::path::PathBuf,
    #[allow(dead_code)]
    conflict_strategy: ConflictStrategy,
}

impl ChangeApplier {
    pub fn new(root: &Path, conflict_strategy: ConflictStrategy) -> Self {
        Self {
            root: root.to_path_buf(),
            conflict_strategy,
        }
    }

    /// Apply a batch of events to the local directory.
    pub fn apply_events(&self, events: &[SyncEvent]) -> Result<()> {
        for event in events {
            self.apply_single(event)?;
        }
        Ok(())
    }

    fn apply_single(&self, event: &SyncEvent) -> Result<()> {
        match event {
            SyncEvent::FileCreated { path, .. } => {
                let full_path = self.root.join(path);
                if full_path.exists() {
                    debug!("File already exists, skipping create: {}", path.display());
                    return Ok(());
                }
                // Ensure parent directory exists
                if let Some(parent) = full_path.parent() {
                    fs::create_dir_all(parent)
                        .with_context(|| format!("Failed to create parent dir: {}", parent.display()))?;
                }
                // Create empty file (content transfer handled separately)
                fs::write(&full_path, "")
                    .with_context(|| format!("Failed to create file: {}", full_path.display()))?;
                info!("Created file: {}", path.display());
            }

            SyncEvent::FileModified { path, .. } => {
                let full_path = self.root.join(path);
                if !full_path.exists() {
                    debug!("File doesn't exist for modify, creating: {}", path.display());
                    if let Some(parent) = full_path.parent() {
                        fs::create_dir_all(parent)
                            .with_context(|| format!("Failed to create parent dir: {}", parent.display()))?;
                    }
                    fs::write(&full_path, "")
                        .with_context(|| format!("Failed to create file: {}", full_path.display()))?;
                }
                // Content update will be handled by FileContent events
                info!("Modified file placeholder: {}", path.display());
            }

            SyncEvent::FileDeleted { path } => {
                let full_path = self.root.join(path);
                if full_path.exists() {
                    fs::remove_file(&full_path)
                        .with_context(|| format!("Failed to delete file: {}", full_path.display()))?;
                    info!("Deleted file: {}", path.display());
                } else {
                    debug!("File already gone: {}", path.display());
                }
            }

            SyncEvent::DirCreated { path } => {
                let full_path = self.root.join(path);
                if !full_path.exists() {
                    fs::create_dir_all(&full_path)
                        .with_context(|| format!("Failed to create dir: {}", full_path.display()))?;
                    info!("Created directory: {}", path.display());
                }
            }

            SyncEvent::DirDeleted { path } => {
                let full_path = self.root.join(path);
                if full_path.exists() {
                    fs::remove_dir_all(&full_path)
                        .with_context(|| format!("Failed to delete dir: {}", full_path.display()))?;
                    info!("Deleted directory: {}", path.display());
                }
            }

            SyncEvent::FileContent { path, offset, data } => {
                let full_path = self.root.join(path);
                use std::io::{Seek, SeekFrom, Write};
                if let Some(parent) = full_path.parent() {
                    fs::create_dir_all(parent)
                        .with_context(|| format!("Failed to create parent dir: {}", parent.display()))?;
                }
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&full_path)
                    .with_context(|| format!("Failed to open file for writing: {}", full_path.display()))?;
                file.seek(SeekFrom::Start(*offset))
                    .with_context(|| format!("Failed to seek: {}", full_path.display()))?;
                file.write_all(data)
                    .with_context(|| format!("Failed to write content: {}", full_path.display()))?;
                debug!("Wrote {} bytes at offset {} to {}", data.len(), offset, path.display());
            }

            SyncEvent::Heartbeat { .. } => {
                // Heartbeat handled at protocol level, not applied to filesystem
            }
        }

        Ok(())
    }

    /// Resolve a conflict when both sides modified the same file.
    #[allow(dead_code)]
    pub fn resolve_conflict(
        &self,
        _path: &Path,
        local_hash: &[u8; 32],
        remote_hash: &[u8; 32],
        local_mtime: i64,
        remote_mtime: i64,
    ) -> ConflictResolution {
        // Same content, no conflict
        if local_hash == remote_hash {
            return ConflictResolution::NoChange;
        }

        match self.conflict_strategy {
            ConflictStrategy::LastWriteWins => {
                if remote_mtime >= local_mtime {
                    ConflictResolution::UseRemote
                } else {
                    ConflictResolution::KeepLocal
                }
            }
            ConflictStrategy::KeepBoth => {
                ConflictResolution::KeepBoth {
                    remote_suffix: ".remote".to_string(),
                }
            }
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum ConflictResolution {
    NoChange,
    KeepLocal,
    UseRemote,
    KeepBoth { remote_suffix: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup() -> (TempDir, ChangeApplier) {
        let dir = TempDir::new().unwrap();
        let applier = ChangeApplier::new(dir.path(), ConflictStrategy::LastWriteWins);
        (dir, applier)
    }

    #[test]
    fn test_dir_created() {
        let (dir, applier) = setup();
        let events = vec![SyncEvent::DirCreated {
            path: std::path::PathBuf::from("subdir"),
        }];
        applier.apply_events(&events).unwrap();
        assert!(dir.path().join("subdir").is_dir());
    }

    #[test]
    fn test_dir_deleted() {
        let (dir, applier) = setup();
        fs::create_dir(dir.path().join("to_delete")).unwrap();

        let events = vec![SyncEvent::DirDeleted {
            path: std::path::PathBuf::from("to_delete"),
        }];
        applier.apply_events(&events).unwrap();
        assert!(!dir.path().join("to_delete").exists());
    }

    #[test]
    fn test_file_created() {
        let (dir, applier) = setup();
        let events = vec![SyncEvent::FileCreated {
            path: std::path::PathBuf::from("new.txt"),
            content_hash: [0u8; 32],
            size: 0,
        }];
        applier.apply_events(&events).unwrap();
        assert!(dir.path().join("new.txt").exists());
    }

    #[test]
    fn test_file_created_skips_existing() {
        let (dir, applier) = setup();
        fs::write(dir.path().join("existing.txt"), b"original").unwrap();

        let events = vec![SyncEvent::FileCreated {
            path: std::path::PathBuf::from("existing.txt"),
            content_hash: [0u8; 32],
            size: 0,
        }];
        applier.apply_events(&events).unwrap();
        // Should not overwrite
        assert_eq!(fs::read(dir.path().join("existing.txt")).unwrap(), b"original");
    }

    #[test]
    fn test_file_deleted() {
        let (dir, applier) = setup();
        fs::write(dir.path().join("del.txt"), b"data").unwrap();

        let events = vec![SyncEvent::FileDeleted {
            path: std::path::PathBuf::from("del.txt"),
        }];
        applier.apply_events(&events).unwrap();
        assert!(!dir.path().join("del.txt").exists());
    }

    #[test]
    fn test_file_deleted_nonexistent() {
        let (_dir, applier) = setup();
        // Deleting a non-existent file should not error
        let events = vec![SyncEvent::FileDeleted {
            path: std::path::PathBuf::from("ghost.txt"),
        }];
        assert!(applier.apply_events(&events).is_ok());
    }

    #[test]
    fn test_file_content_writes_at_offset() {
        let (dir, applier) = setup();
        fs::write(dir.path().join("chunked.bin"), vec![0u8; 100]).unwrap();

        let events = vec![SyncEvent::FileContent {
            path: std::path::PathBuf::from("chunked.bin"),
            offset: 50,
            data: vec![0xAB; 10],
        }];
        applier.apply_events(&events).unwrap();

        let data = fs::read(dir.path().join("chunked.bin")).unwrap();
        assert_eq!(data.len(), 100);
        assert_eq!(&data[50..60], &[0xAB; 10]);
        // First 50 bytes should be unchanged
        assert_eq!(&data[0..50], &[0u8; 50]);
    }

    #[test]
    fn test_file_content_creates_parent_dirs() {
        let (dir, applier) = setup();
        let events = vec![SyncEvent::FileContent {
            path: std::path::PathBuf::from("deep/nested/file.txt"),
            offset: 0,
            data: b"hello".to_vec(),
        }];
        applier.apply_events(&events).unwrap();
        assert_eq!(
            fs::read(dir.path().join("deep/nested/file.txt")).unwrap(),
            b"hello"
        );
    }

    #[test]
    fn test_nested_dir_created() {
        let (dir, applier) = setup();
        let events = vec![SyncEvent::DirCreated {
            path: std::path::PathBuf::from("a/b/c"),
        }];
        applier.apply_events(&events).unwrap();
        assert!(dir.path().join("a/b/c").is_dir());
    }

    #[test]
    fn test_heartbeat_ignored() {
        let (_dir, applier) = setup();
        let events = vec![SyncEvent::Heartbeat { timestamp: 12345 }];
        assert!(applier.apply_events(&events).is_ok());
    }

    #[test]
    fn test_multiple_events_in_batch() {
        let (dir, applier) = setup();
        let events = vec![
            SyncEvent::DirCreated {
                path: std::path::PathBuf::from("docs"),
            },
            SyncEvent::FileCreated {
                path: std::path::PathBuf::from("docs/readme.md"),
                content_hash: [0u8; 32],
                size: 0,
            },
            SyncEvent::FileContent {
                path: std::path::PathBuf::from("docs/readme.md"),
                offset: 0,
                data: b"# Hello".to_vec(),
            },
        ];
        applier.apply_events(&events).unwrap();
        assert!(dir.path().join("docs").is_dir());
        assert!(dir.path().join("docs/readme.md").exists());
        assert_eq!(
            fs::read(dir.path().join("docs/readme.md")).unwrap(),
            b"# Hello"
        );
    }
}
