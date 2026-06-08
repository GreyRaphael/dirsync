use crate::cli::ConflictStrategy;
use crate::event::SyncEvent;
use crate::watcher::{INTERNAL_TEMP_DIR, file_hash_and_size};
use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Component, Path, PathBuf};
use tracing::{debug, info, warn};

type FileHashMap = HashMap<PathBuf, ([u8; 32], u64)>;

struct PendingFileTransfer {
    expected_hash: [u8; 32],
    expected_size: u64,
    temp_path: PathBuf,
    /// Cached file handle — kept open across chunks to avoid per-chunk
    /// open/close syscalls that dominate transfer time for large files.
    file: std::fs::File,
}

pub struct ChangeApplier {
    root: PathBuf,
    root_canonical: PathBuf,
    conflict_strategy: ConflictStrategy,
    last_synced_hash: FileHashMap,
    pending_remote_files: HashMap<PathBuf, PendingFileTransfer>,
    blocked_remote_files: HashSet<PathBuf>,
}

impl ChangeApplier {
    pub fn new(root: &Path, conflict_strategy: ConflictStrategy) -> Self {
        let root_canonical = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
        Self {
            root: root.to_path_buf(),
            root_canonical,
            conflict_strategy,
            last_synced_hash: HashMap::new(),
            pending_remote_files: HashMap::new(),
            blocked_remote_files: HashSet::new(),
        }
    }

    fn validate_event_path(path: &Path) -> Result<()> {
        if path.as_os_str().is_empty() {
            anyhow::bail!("Remote event path must not be empty");
        }

        for component in path.components() {
            match component {
                Component::Normal(name) if name != OsStr::new(INTERNAL_TEMP_DIR) => {}
                Component::Normal(_) => {
                    anyhow::bail!(
                        "Remote event path must not target internal directory: {}",
                        path.display()
                    );
                }
                Component::Prefix(_)
                | Component::RootDir
                | Component::CurDir
                | Component::ParentDir => {
                    anyhow::bail!("Unsafe remote event path: {}", path.display());
                }
            }
        }

        Ok(())
    }

    fn deepest_existing_ancestor(path: &Path) -> Option<PathBuf> {
        let mut current = Some(path);
        while let Some(candidate) = current {
            if candidate.exists() {
                return Some(candidate.to_path_buf());
            }
            current = candidate.parent();
        }
        None
    }

    fn ensure_existing_ancestor_within(base_canonical: &Path, target: &Path) -> Result<()> {
        let ancestor = Self::deepest_existing_ancestor(target)
            .ok_or_else(|| anyhow::anyhow!("No existing ancestor for {}", target.display()))?;
        let ancestor_canonical = ancestor
            .canonicalize()
            .with_context(|| format!("Failed to canonicalize {}", ancestor.display()))?;

        if !ancestor_canonical.starts_with(base_canonical) {
            anyhow::bail!("Remote event path escapes sync root: {}", target.display());
        }

        Ok(())
    }

    fn safe_target_path(&self, path: &Path) -> Result<PathBuf> {
        Self::validate_event_path(path)?;
        let target = self.root.join(path);
        Self::ensure_existing_ancestor_within(&self.root_canonical, &target)?;
        Ok(target)
    }

    fn temp_path_for(&self, path: &Path) -> Result<PathBuf> {
        Self::validate_event_path(path)?;
        let temp_root = self.root.join(INTERNAL_TEMP_DIR);

        if let Ok(metadata) = fs::symlink_metadata(&temp_root)
            && metadata.file_type().is_symlink()
        {
            anyhow::bail!(
                "Internal temp directory is a symlink: {}",
                temp_root.display()
            );
        }

        fs::create_dir_all(&temp_root)
            .with_context(|| format!("Failed to create temp root: {}", temp_root.display()))?;

        let temp_root_canonical = temp_root.canonicalize().with_context(|| {
            format!("Failed to canonicalize temp root: {}", temp_root.display())
        })?;
        if !temp_root_canonical.starts_with(&self.root_canonical) {
            anyhow::bail!(
                "Internal temp directory escapes sync root: {}",
                temp_root.display()
            );
        }

        let target = temp_root.join(path);
        Self::ensure_existing_ancestor_within(&temp_root_canonical, &target)?;
        Ok(target)
    }

    pub fn apply_events(
        &mut self,
        events: &[SyncEvent],
        remote_timestamp: i64,
    ) -> Result<Vec<ConflictInfo>> {
        let mut conflicts = Vec::new();
        for event in events {
            if let Some(conflict) = self.apply_event(event, remote_timestamp)? {
                conflicts.push(conflict);
            }
        }
        Ok(conflicts)
    }

    pub fn apply_event(
        &mut self,
        event: &SyncEvent,
        remote_timestamp: i64,
    ) -> Result<Option<ConflictInfo>> {
        self.apply_single(event, remote_timestamp)
    }

    fn local_mtime_millis(path: &Path) -> i64 {
        fs::metadata(path)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    fn detect_conflict(
        &self,
        path: &Path,
        remote_hash: &[u8; 32],
        remote_size: u64,
        remote_timestamp: i64,
    ) -> Option<ConflictInfo> {
        let full_path = match self.safe_target_path(path) {
            Ok(path) => path,
            Err(_) => return None,
        };
        if !full_path.exists() {
            return None;
        }

        let (local_hash, local_size) = match file_hash_and_size(&full_path) {
            Ok(v) => v,
            Err(_) => return None,
        };

        if local_hash == *remote_hash {
            return None;
        }

        if let Some((synced_hash, _)) = self.last_synced_hash.get(path) {
            if local_hash == *synced_hash {
                return None;
            }

            return match self.conflict_strategy {
                ConflictStrategy::LastWriteWins => {
                    let local_mtime = Self::local_mtime_millis(&full_path);
                    if remote_timestamp >= local_mtime {
                        None
                    } else {
                        Some(ConflictInfo {
                            path: path.to_path_buf(),
                            local_hash,
                            local_size,
                            remote_hash: *remote_hash,
                            remote_size,
                        })
                    }
                }
                ConflictStrategy::KeepBoth => Some(ConflictInfo {
                    path: path.to_path_buf(),
                    local_hash,
                    local_size,
                    remote_hash: *remote_hash,
                    remote_size,
                }),
            };
        }

        None
    }

    fn prepare_transfer(
        &mut self,
        path: &Path,
        expected_hash: [u8; 32],
        expected_size: u64,
    ) -> Result<()> {
        self.blocked_remote_files.remove(path);
        self.pending_remote_files.remove(path);

        let full_path = self.safe_target_path(path)?;
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
                warn!(
                    "Committed empty file with mismatched metadata hash: {}",
                    path.display()
                );
            }
            self.last_synced_hash
                .insert(path.to_path_buf(), (actual_hash, actual_size));
            debug!("Committed empty file: {}", path.display());
            return Ok(());
        }

        let temp_path = self.temp_path_for(path)?;
        if let Some(parent) = temp_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create temp parent dir: {}", parent.display())
            })?;
        }
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .with_context(|| format!("Failed to create temp file: {}", temp_path.display()))?;

        self.pending_remote_files.insert(
            path.to_path_buf(),
            PendingFileTransfer {
                expected_hash,
                expected_size,
                temp_path,
                file,
            },
        );
        Ok(())
    }

    fn write_pending_chunk(&mut self, path: &Path, offset: u64, data: &[u8]) -> Result<()> {
        let pending = match self.pending_remote_files.get_mut(path) {
            Some(p) => p,
            None => return self.write_direct_chunk(path, offset, data),
        };

        use std::io::{Seek, SeekFrom, Write};
        pending.file.seek(SeekFrom::Start(offset)).with_context(|| {
            format!("Failed to seek temp file: {}", pending.temp_path.display())
        })?;
        pending.file.write_all(data).with_context(|| {
            format!(
                "Failed to write temp content: {}",
                pending.temp_path.display()
            )
        })?;

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

        // File is complete — flush the cached handle so all buffered
        // writes hit disk before we hash.
        {
            use std::io::Write;
            if let Some(pending) = self.pending_remote_files.get_mut(path) {
                pending.file.flush().with_context(|| {
                    format!("Failed to flush temp file: {}", pending.temp_path.display())
                })?;
            }
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

        let full_path = self.safe_target_path(path)?;
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create parent dir: {}", parent.display()))?;
        }
        // Drop the cached file handle before renaming — Windows requires
        // the handle to be closed.
        if let Some(pending) = self.pending_remote_files.remove(path) {
            drop(pending.file);
        }
        if full_path.exists() {
            fs::remove_file(&full_path).with_context(|| {
                format!("Failed to replace existing file: {}", full_path.display())
            })?;
        }
        fs::rename(&temp_path, &full_path).with_context(|| {
            format!(
                "Failed to move temp file {} to {}",
                temp_path.display(),
                full_path.display()
            )
        })?;

        self.last_synced_hash
            .insert(path.to_path_buf(), (expected_hash, expected_size));
        info!(
            "Committed file: {} ({} bytes)",
            path.display(),
            expected_size
        );
        Ok(())
    }

    fn write_direct_chunk(&mut self, path: &Path, offset: u64, data: &[u8]) -> Result<()> {
        let full_path = self.safe_target_path(path)?;
        use std::io::{Seek, SeekFrom, Write};
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create parent dir: {}", parent.display()))?;
        }

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
            self.last_synced_hash
                .insert(path.to_path_buf(), (hash, size));
        }
        Ok(())
    }

    fn apply_single(
        &mut self,
        event: &SyncEvent,
        remote_timestamp: i64,
    ) -> Result<Option<ConflictInfo>> {
        match event {
            SyncEvent::FileCreated {
                path,
                content_hash,
                size,
            } => {
                let mut resolved_conflict = None;
                if let Some(conflict) =
                    self.detect_conflict(path, content_hash, *size, remote_timestamp)
                {
                    match self.conflict_strategy {
                        ConflictStrategy::LastWriteWins => {
                            warn!(
                                "Conflict on create {} — keeping local (LWW)",
                                path.display()
                            );
                            self.blocked_remote_files.insert(path.clone());
                            return Ok(Some(conflict));
                        }
                        ConflictStrategy::KeepBoth => {
                            info!(
                                "Conflict on create {} — keeping both versions",
                                path.display()
                            );
                            if let Ok(full_path) = self.safe_target_path(path)
                                && full_path.exists()
                            {
                                let copy = keep_both_path(&full_path, "local");
                                if let Err(e) = fs::rename(&full_path, &copy) {
                                    warn!("Failed to rename local copy: {}", e);
                                } else {
                                    info!("Renamed local copy to {}", copy.display());
                                }
                            }
                            resolved_conflict = Some(conflict);
                        }
                    }
                }
                self.prepare_transfer(path, *content_hash, *size)?;
                debug!("Prepared file create: {} ({} bytes)", path.display(), size);
                Ok(resolved_conflict)
            }

            SyncEvent::FileModified {
                path,
                content_hash,
                size,
            } => {
                let mut resolved_conflict = None;
                if let Some(conflict) =
                    self.detect_conflict(path, content_hash, *size, remote_timestamp)
                {
                    match self.conflict_strategy {
                        ConflictStrategy::LastWriteWins => {
                            warn!(
                                "Conflict on modify {} (local={}B, remote={}B) — keeping local (LWW)",
                                path.display(),
                                conflict.local_size,
                                conflict.remote_size
                            );
                            self.pending_remote_files.remove(path);
                            self.blocked_remote_files.insert(path.clone());
                            return Ok(Some(conflict));
                        }
                        ConflictStrategy::KeepBoth => {
                            info!(
                                "Conflict on modify {} — keeping both versions",
                                path.display()
                            );
                            if let Ok(full_path) = self.safe_target_path(path)
                                && full_path.exists()
                            {
                                let copy = keep_both_path(&full_path, "local");
                                if let Err(e) = fs::rename(&full_path, &copy) {
                                    warn!("Failed to rename local copy: {}", e);
                                } else {
                                    info!("Renamed local copy to {}", copy.display());
                                }
                            }
                            resolved_conflict = Some(conflict);
                        }
                    }
                }

                self.prepare_transfer(path, *content_hash, *size)?;
                debug!("Prepared file modify: {} ({} bytes)", path.display(), size);
                Ok(resolved_conflict)
            }

            SyncEvent::FileDeleted { path } => {
                let full_path = self.safe_target_path(path)?;
                self.pending_remote_files.remove(path);
                self.blocked_remote_files.remove(path);
                let temp_path = self.temp_path_for(path)?;
                let _ = fs::remove_file(temp_path);
                self.last_synced_hash.remove(path);
                if full_path.exists() {
                    fs::remove_file(&full_path).with_context(|| {
                        format!("Failed to delete file: {}", full_path.display())
                    })?;
                    info!("Deleted file: {}", path.display());
                } else {
                    debug!("File already gone: {}", path.display());
                }
                Ok(None)
            }

            SyncEvent::DirCreated { path } => {
                let full_path = self.safe_target_path(path)?;
                if !full_path.exists() {
                    fs::create_dir_all(&full_path).with_context(|| {
                        format!("Failed to create dir: {}", full_path.display())
                    })?;
                    info!("Created directory: {}", path.display());
                }
                Ok(None)
            }

            SyncEvent::DirDeleted { path } => {
                let full_path = self.safe_target_path(path)?;
                self.last_synced_hash.retain(|p, _| !p.starts_with(path));
                self.pending_remote_files
                    .retain(|p, _| !p.starts_with(path));
                self.blocked_remote_files.retain(|p| !p.starts_with(path));
                let temp_path = self.temp_path_for(path)?;
                let _ = fs::remove_dir_all(temp_path);
                if full_path.exists() {
                    fs::remove_dir_all(&full_path).with_context(|| {
                        format!("Failed to delete dir: {}", full_path.display())
                    })?;
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
                debug!(
                    "Received {} bytes at offset {} for {}",
                    data.len(),
                    offset,
                    path.display()
                );
                Ok(None)
            }

            SyncEvent::Heartbeat { .. } => Ok(None),
        }
    }
}

fn keep_both_path(original: &Path, tag: &str) -> PathBuf {
    let parent = original.parent().unwrap_or(Path::new("."));
    let file_name = original
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();

    let make_name = |suffix: &str| -> String {
        match file_name.rfind('.') {
            Some(pos) => {
                let (stem, ext) = file_name.split_at(pos);
                format!("{}.{}{}", stem, suffix, ext)
            }
            None => format!("{}.{}", file_name, suffix),
        }
    };

    let candidate = parent.join(make_name(tag));
    if !candidate.exists() {
        return candidate;
    }

    for i in 1..1000 {
        let numbered = parent.join(make_name(&format!("{}.{}", tag, i)));
        if !numbered.exists() {
            return numbered;
        }
    }

    candidate
}

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

    const NOW: i64 = 1_700_000_000_000;

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
        applier.apply_events(&events, NOW).unwrap();
        assert!(dir.path().join("subdir").is_dir());
    }

    #[test]
    fn test_dir_deleted() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        fs::create_dir(dir.path().join("to_delete")).unwrap();

        let events = vec![SyncEvent::DirDeleted {
            path: PathBuf::from("to_delete"),
        }];
        applier.apply_events(&events, NOW).unwrap();
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
        applier.apply_events(&events, NOW).unwrap();
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
        let conflicts = applier.apply_events(&events, NOW).unwrap();
        assert!(conflicts.is_empty());
        assert_eq!(
            fs::read(dir.path().join("existing.txt")).unwrap(),
            b"original"
        );
    }

    #[test]
    fn test_file_deleted() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        fs::write(dir.path().join("del.txt"), b"data").unwrap();

        let events = vec![SyncEvent::FileDeleted {
            path: PathBuf::from("del.txt"),
        }];
        applier.apply_events(&events, NOW).unwrap();
        assert!(!dir.path().join("del.txt").exists());
    }

    #[test]
    fn test_file_deleted_nonexistent() {
        let (_dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let events = vec![SyncEvent::FileDeleted {
            path: PathBuf::from("ghost.txt"),
        }];
        assert!(applier.apply_events(&events, NOW).is_ok());
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
        applier.apply_events(&events, NOW).unwrap();

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
        applier.apply_events(&events, NOW).unwrap();
        assert_eq!(
            fs::read(dir.path().join("deep/nested/file.txt")).unwrap(),
            b"hello"
        );
    }

    #[test]
    fn test_rejects_parent_path_escape() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let outside_name = format!("dirsync_escape_{}_parent.txt", std::process::id());
        let outside_path = dir.path().parent().unwrap().join(&outside_name);
        let _ = fs::remove_file(&outside_path);

        let result = applier.apply_events(
            &[SyncEvent::FileContent {
                path: PathBuf::from("..").join(&outside_name),
                offset: 0,
                data: b"escape".to_vec(),
            }],
            NOW,
        );

        assert!(result.is_err());
        assert!(!outside_path.exists());
    }

    #[test]
    fn test_rejects_absolute_event_path() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let absolute_path = dir.path().join("absolute.txt");

        let result = applier.apply_events(
            &[SyncEvent::FileContent {
                path: absolute_path.clone(),
                offset: 0,
                data: b"absolute".to_vec(),
            }],
            NOW,
        );

        assert!(result.is_err());
        assert!(!absolute_path.exists());
    }

    #[test]
    fn test_rejects_internal_temp_event_path() {
        let (_dir, mut applier) = setup(ConflictStrategy::LastWriteWins);

        let result = applier.apply_events(
            &[SyncEvent::FileContent {
                path: PathBuf::from(INTERNAL_TEMP_DIR).join("payload.txt"),
                offset: 0,
                data: b"internal".to_vec(),
            }],
            NOW,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_file_commits_only_after_complete_verified_chunks() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let content = b"abcdefghij";
        let (hash, size) = hash_size(content);

        applier
            .apply_events(
                &[
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
                ],
                NOW,
            )
            .unwrap();
        assert!(!dir.path().join("large.bin").exists());

        applier
            .apply_events(
                &[SyncEvent::FileContent {
                    path: PathBuf::from("large.bin"),
                    offset: 5,
                    data: content[5..].to_vec(),
                }],
                NOW,
            )
            .unwrap();
        assert_eq!(fs::read(dir.path().join("large.bin")).unwrap(), content);
    }

    #[test]
    fn test_nested_dir_created() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let events = vec![SyncEvent::DirCreated {
            path: PathBuf::from("a/b/c"),
        }];
        applier.apply_events(&events, NOW).unwrap();
        assert!(dir.path().join("a/b/c").is_dir());
    }

    #[test]
    fn test_heartbeat_ignored() {
        let (_dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let events = vec![SyncEvent::Heartbeat { timestamp: 12345 }];
        assert!(applier.apply_events(&events, NOW).is_ok());
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
        applier.apply_events(&events, NOW).unwrap();
        assert!(dir.path().join("docs").is_dir());
        assert!(dir.path().join("docs/readme.md").exists());
        assert_eq!(
            fs::read(dir.path().join("docs/readme.md")).unwrap(),
            b"# Hello"
        );
    }

    #[test]
    fn test_no_conflict_on_first_sync() {
        let (_dir, mut applier) = setup(ConflictStrategy::LastWriteWins);
        let events = vec![SyncEvent::FileCreated {
            path: PathBuf::from("new.txt"),
            content_hash: [0xAA; 32],
            size: 100,
        }];
        let conflicts = applier.apply_events(&events, NOW).unwrap();
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_conflict_detected_when_both_changed() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);

        let remote_data = b"remote data";
        let (remote_hash, remote_size) = hash_size(remote_data);
        applier
            .apply_events(
                &[
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
                ],
                NOW,
            )
            .unwrap();

        assert!(dir.path().join("shared.txt").exists());

        fs::write(dir.path().join("shared.txt"), b"local change").unwrap();

        // Remote timestamp far in the past — local wins under LWW
        let events = vec![SyncEvent::FileModified {
            path: PathBuf::from("shared.txt"),
            content_hash: [0xBB; 32],
            size: 20,
        }];
        let conflicts = applier.apply_events(&events, 1000).unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].path, PathBuf::from("shared.txt"));
    }

    #[test]
    fn test_no_conflict_when_remote_unchanged() {
        let (dir2, mut applier2) = setup(ConflictStrategy::LastWriteWins);

        let content = b"test content";
        fs::write(dir2.path().join("file.txt"), content).unwrap();
        let (local_hash, local_size) = file_hash_and_size(&dir2.path().join("file.txt")).unwrap();

        let events = vec![SyncEvent::FileCreated {
            path: PathBuf::from("file.txt"),
            content_hash: local_hash,
            size: local_size,
        }];
        let conflicts = applier2.apply_events(&events, NOW).unwrap();
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_lww_remote_wins_when_newer() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);

        let v1 = b"version one";
        let (h1, s1) = hash_size(v1);
        applier
            .apply_events(
                &[
                    SyncEvent::FileCreated {
                        path: PathBuf::from("f.txt"),
                        content_hash: h1,
                        size: s1,
                    },
                    SyncEvent::FileContent {
                        path: PathBuf::from("f.txt"),
                        offset: 0,
                        data: v1.to_vec(),
                    },
                ],
                NOW,
            )
            .unwrap();

        fs::write(dir.path().join("f.txt"), b"local edit").unwrap();

        // Remote timestamp far in the future — remote wins under LWW
        let future_ts: i64 = 9_999_999_999_999;
        let conflicts = applier
            .apply_events(
                &[SyncEvent::FileModified {
                    path: PathBuf::from("f.txt"),
                    content_hash: [0xCC; 32],
                    size: 30,
                }],
                future_ts,
            )
            .unwrap();
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_lww_local_wins_when_newer() {
        let (dir, mut applier) = setup(ConflictStrategy::LastWriteWins);

        let v1 = b"version one";
        let (h1, s1) = hash_size(v1);
        applier
            .apply_events(
                &[
                    SyncEvent::FileCreated {
                        path: PathBuf::from("f.txt"),
                        content_hash: h1,
                        size: s1,
                    },
                    SyncEvent::FileContent {
                        path: PathBuf::from("f.txt"),
                        offset: 0,
                        data: v1.to_vec(),
                    },
                ],
                NOW,
            )
            .unwrap();

        fs::write(dir.path().join("f.txt"), b"local edit").unwrap();

        // Remote timestamp far in the past — local wins under LWW
        let conflicts = applier
            .apply_events(
                &[SyncEvent::FileModified {
                    path: PathBuf::from("f.txt"),
                    content_hash: [0xDD; 32],
                    size: 50,
                }],
                1000,
            )
            .unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            fs::read(dir.path().join("f.txt")).unwrap(),
            b"local edit"
        );
    }

    #[test]
    fn test_keep_both_creates_local_copy() {
        let (dir, mut applier) = setup(ConflictStrategy::KeepBoth);

        let original = b"original  ";
        let (oh, os) = hash_size(original);
        applier
            .apply_events(
                &[
                    SyncEvent::FileCreated {
                        path: PathBuf::from("doc.txt"),
                        content_hash: oh,
                        size: os,
                    },
                    SyncEvent::FileContent {
                        path: PathBuf::from("doc.txt"),
                        offset: 0,
                        data: original.to_vec(),
                    },
                ],
                NOW,
            )
            .unwrap();

        fs::write(dir.path().join("doc.txt"), b"my changes").unwrap();

        let remote_hash = [0xEE; 32];
        let conflicts = applier
            .apply_events(
                &[SyncEvent::FileModified {
                    path: PathBuf::from("doc.txt"),
                    content_hash: remote_hash,
                    size: 15,
                }],
                NOW,
            )
            .unwrap();

        assert_eq!(conflicts.len(), 1);
        assert!(dir.path().join("doc.local.txt").exists());
        assert_eq!(
            fs::read(dir.path().join("doc.local.txt")).unwrap(),
            b"my changes"
        );
    }

    #[test]
    fn test_keep_both_avoids_overwrite() {
        let (dir, mut applier) = setup(ConflictStrategy::KeepBoth);

        // Pre-existing .local.txt should cause numbered suffix
        fs::write(dir.path().join("f.txt"), b"old local").unwrap();
        fs::write(dir.path().join("f.local.txt"), b"already exists").unwrap();

        let original = b"original";
        let (oh, os) = hash_size(original);
        applier
            .apply_events(
                &[
                    SyncEvent::FileCreated {
                        path: PathBuf::from("f.txt"),
                        content_hash: oh,
                        size: os,
                    },
                    SyncEvent::FileContent {
                        path: PathBuf::from("f.txt"),
                        offset: 0,
                        data: original.to_vec(),
                    },
                ],
                NOW,
            )
            .unwrap();

        fs::write(dir.path().join("f.txt"), b"new local").unwrap();

        applier
            .apply_events(
                &[SyncEvent::FileModified {
                    path: PathBuf::from("f.txt"),
                    content_hash: [0xFF; 32],
                    size: 20,
                }],
                NOW,
            )
            .unwrap();

        assert!(dir.path().join("f.local.1.txt").exists());
        assert_eq!(
            fs::read(dir.path().join("f.local.1.txt")).unwrap(),
            b"new local"
        );
    }
}
