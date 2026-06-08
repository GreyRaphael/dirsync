use dirsync::apply::ChangeApplier;
use dirsync::chunker;
use dirsync::cli::ConflictStrategy;
use dirsync::event::{EventEnvelope, SyncEvent};
use dirsync::shm::ShmTransport;
use dirsync::watcher::{file_hash_and_size, hash_data, initial_scan};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

const NOW: i64 = 1_700_000_000_000;

fn unique_shm_name(suffix: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/dirsync_itest_{}_{}", ts, suffix)
}

fn hash_size(data: &[u8]) -> ([u8; 32], u64) {
    hash_data(data)
}

fn seq_envelope(instance_id: u64, seq: u64, event: SyncEvent) -> EventEnvelope {
    EventEnvelope {
        instance_id,
        seq,
        timestamp: chrono::Utc::now().timestamp_millis(),
        event,
    }
}

// ------------------------------------------------------------------
// SHM transport: two-instance read/write
// ------------------------------------------------------------------

#[test]
fn test_two_instances_create_and_read() {
    let name = unique_shm_name("two_inst");
    let shm_a = ShmTransport::create(&name, 65536).unwrap();
    let shm_b = ShmTransport::open(&name).unwrap();

    let env = seq_envelope(
        0,
        1,
        SyncEvent::FileCreated {
            path: PathBuf::from("shared.txt"),
            content_hash: [0xAA; 32],
            size: 42,
        },
    );
    shm_a.push_event(0, &env).unwrap();

    let events = shm_b.pop_events(1).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].instance_id, 0);
    assert!(matches!(
        &events[0].event,
        SyncEvent::FileCreated { path, .. } if path == Path::new("shared.txt")
    ));
}

#[test]
fn test_bidirectional_sync() {
    let name = unique_shm_name("bidir");
    let shm = ShmTransport::create(&name, 65536).unwrap();

    shm.push_event(
        0,
        &seq_envelope(
            0,
            1,
            SyncEvent::DirCreated {
                path: PathBuf::from("docs"),
            },
        ),
    )
    .unwrap();

    shm.push_event(
        1,
        &seq_envelope(
            1,
            1,
            SyncEvent::FileCreated {
                path: PathBuf::from("readme.md"),
                content_hash: [0xBB; 32],
                size: 100,
            },
        ),
    )
    .unwrap();

    let ev0 = shm.pop_events(0).unwrap();
    assert_eq!(ev0.len(), 2);
    let remote_for_0: Vec<_> = ev0.iter().filter(|e| e.instance_id == 1).collect();
    assert_eq!(remote_for_0.len(), 1);

    let ev1 = shm.pop_events(1).unwrap();
    assert_eq!(ev1.len(), 2);
    let remote_for_1: Vec<_> = ev1.iter().filter(|e| e.instance_id == 0).collect();
    assert_eq!(remote_for_1.len(), 1);
}

// ------------------------------------------------------------------
// Apply: full lifecycle
// ------------------------------------------------------------------

#[test]
fn test_full_file_lifecycle() {
    let dir = TempDir::new().unwrap();
    let mut applier = ChangeApplier::new(dir.path(), ConflictStrategy::LastWriteWins);

    applier
        .apply_events(
            &[SyncEvent::DirCreated {
                path: PathBuf::from("project"),
            }],
            NOW,
        )
        .unwrap();
    assert!(dir.path().join("project").is_dir());

    let initial_content = b"Hello, World!";
    let (initial_hash, initial_size) = hash_size(initial_content);
    applier
        .apply_events(
            &[
                SyncEvent::FileCreated {
                    path: PathBuf::from("project/data.txt"),
                    content_hash: initial_hash,
                    size: initial_size,
                },
                SyncEvent::FileContent {
                    path: PathBuf::from("project/data.txt"),
                    offset: 0,
                    data: initial_content.to_vec(),
                },
            ],
            NOW,
        )
        .unwrap();
    assert_eq!(
        fs::read(dir.path().join("project/data.txt")).unwrap(),
        b"Hello, World!"
    );

    applier
        .apply_events(
            &[SyncEvent::FileContent {
                path: PathBuf::from("project/data.txt"),
                offset: 7,
                data: b"Rust!".to_vec(),
            }],
            NOW,
        )
        .unwrap();
    assert_eq!(
        fs::read(dir.path().join("project/data.txt")).unwrap(),
        b"Hello, Rust!!"
    );

    applier
        .apply_events(
            &[SyncEvent::FileDeleted {
                path: PathBuf::from("project/data.txt"),
            }],
            NOW,
        )
        .unwrap();
    assert!(!dir.path().join("project/data.txt").exists());

    applier
        .apply_events(
            &[SyncEvent::DirDeleted {
                path: PathBuf::from("project"),
            }],
            NOW,
        )
        .unwrap();
    assert!(!dir.path().join("project").exists());
}

// ------------------------------------------------------------------
// Chunker: round-trip through SHM
// ------------------------------------------------------------------

#[test]
fn test_chunked_file_through_shm() {
    let name = unique_shm_name("chunk_shm");
    let shm = ShmTransport::create(&name, 131072).unwrap();

    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("big.bin");
    let content = vec![0xABu8; 200];
    fs::write(&file_path, &content).unwrap();

    let chunks = chunker::chunk_data(Path::new("big.bin"), &content, 64);
    assert_eq!(chunks.len(), 4);

    for (i, chunk) in chunks.into_iter().enumerate() {
        shm.push_event(0, &seq_envelope(0, (i + 1) as u64, chunk))
            .unwrap();
    }

    let events = shm.pop_events(1).unwrap();
    assert_eq!(events.len(), 4);

    let out_dir = TempDir::new().unwrap();
    let mut applier = ChangeApplier::new(out_dir.path(), ConflictStrategy::LastWriteWins);
    let sync_events: Vec<SyncEvent> = events.into_iter().map(|e| e.event).collect();
    applier.apply_events(&sync_events, NOW).unwrap();

    let out_content = fs::read(out_dir.path().join("big.bin")).unwrap();
    assert_eq!(out_content, content);
}

// ------------------------------------------------------------------
// Initial scan → apply round-trip
// ------------------------------------------------------------------

#[test]
fn test_initial_scan_and_apply() {
    let src = TempDir::new().unwrap();
    fs::create_dir(src.path().join("src")).unwrap();
    fs::write(src.path().join("src/main.rs"), b"fn main() {}").unwrap();
    fs::write(src.path().join("Cargo.toml"), b"[package]").unwrap();

    let events = initial_scan(src.path(), &[]);
    assert!(events.len() >= 3);

    let mut all_events = events;
    for entry in walkdir::WalkDir::new(src.path()) {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            let rel = entry.path().strip_prefix(src.path()).unwrap();
            let data = std::fs::read(entry.path()).unwrap();
            let chunks = chunker::chunk_data(rel, &data, 65536);
            all_events.extend(chunks);
        }
    }

    let dst = TempDir::new().unwrap();
    let mut applier = ChangeApplier::new(dst.path(), ConflictStrategy::LastWriteWins);
    applier.apply_events(&all_events, NOW).unwrap();

    assert!(dst.path().join("src").is_dir());
    assert_eq!(
        fs::read(dst.path().join("src/main.rs")).unwrap(),
        b"fn main() {}"
    );
    assert_eq!(
        fs::read(dst.path().join("Cargo.toml")).unwrap(),
        b"[package]"
    );
}

#[test]
fn test_initial_scan_with_ignore() {
    let src = TempDir::new().unwrap();
    fs::create_dir(src.path().join(".git")).unwrap();
    fs::write(src.path().join(".git/HEAD"), b"ref: refs/heads/main").unwrap();
    fs::write(src.path().join("real.txt"), b"data").unwrap();

    let events = initial_scan(src.path(), &[".git".to_string()]);

    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        SyncEvent::FileCreated { path, .. } if path == Path::new("real.txt")
    ));
}

// ------------------------------------------------------------------
// Hash consistency
// ------------------------------------------------------------------

#[test]
fn test_hash_consistency_across_operations() {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("data.bin");
    let content = b"consistent content for hashing";
    fs::write(&file, content).unwrap();

    let (hash1, size1) = file_hash_and_size(&file).unwrap();

    let mut applier = ChangeApplier::new(dir.path(), ConflictStrategy::LastWriteWins);
    applier
        .apply_events(
            &[SyncEvent::FileContent {
                path: PathBuf::from("data.bin"),
                offset: 0,
                data: content.to_vec(),
            }],
            NOW,
        )
        .unwrap();

    let (hash2, size2) = file_hash_and_size(&file).unwrap();
    assert_eq!(hash1, hash2);
    assert_eq!(size1, size2);
}

// ------------------------------------------------------------------
// Large file: many chunks
// ------------------------------------------------------------------

#[test]
fn test_large_file_chunked_transfer() {
    let name = unique_shm_name("large_chunk");
    let shm = ShmTransport::create(&name, 262144).unwrap();

    let dir = TempDir::new().unwrap();
    let file = dir.path().join("large.bin");
    let content = vec![0x42u8; 100_000];
    fs::write(&file, &content).unwrap();

    let chunks = chunker::chunk_data(Path::new("large.bin"), &content, 65536);
    assert_eq!(chunks.len(), 2);

    for (i, chunk) in chunks.into_iter().enumerate() {
        shm.push_event(0, &seq_envelope(0, (i + 1) as u64, chunk))
            .unwrap();
    }

    let events = shm.pop_events(1).unwrap();
    let out_dir = TempDir::new().unwrap();
    let mut applier = ChangeApplier::new(out_dir.path(), ConflictStrategy::LastWriteWins);
    let sync_events: Vec<SyncEvent> = events.into_iter().map(|e| e.event).collect();
    applier.apply_events(&sync_events, NOW).unwrap();

    let out = fs::read(out_dir.path().join("large.bin")).unwrap();
    assert_eq!(out, content);
}

// ------------------------------------------------------------------
// Conflict detection and resolution
// ------------------------------------------------------------------

#[test]
fn test_conflict_last_write_wins_local_kept() {
    let dir = TempDir::new().unwrap();
    let mut applier = ChangeApplier::new(dir.path(), ConflictStrategy::LastWriteWins);

    let remote_version = b"remote version";
    let (remote_hash, remote_size) = hash_size(remote_version);
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
                    data: remote_version.to_vec(),
                },
            ],
            NOW,
        )
        .unwrap();

    fs::write(dir.path().join("shared.txt"), b"local version").unwrap();

    // Remote timestamp far in the past — local wins
    let hash_b = [0xBB; 32];
    let conflicts = applier
        .apply_events(
            &[SyncEvent::FileModified {
                path: PathBuf::from("shared.txt"),
                content_hash: hash_b,
                size: 20,
            }],
            1000,
        )
        .unwrap();

    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].path, PathBuf::from("shared.txt"));
    assert_eq!(
        conflicts[0].local_hash,
        file_hash_and_size(&dir.path().join("shared.txt"))
            .unwrap()
            .0
    );
    assert_eq!(conflicts[0].remote_hash, hash_b);
    // Local file preserved
    assert_eq!(fs::read(dir.path().join("shared.txt")).unwrap(), b"local version");
}

#[test]
fn test_conflict_last_write_wins_remote_overwrites() {
    let dir = TempDir::new().unwrap();
    let mut applier = ChangeApplier::new(dir.path(), ConflictStrategy::LastWriteWins);

    let remote_version = b"remote version";
    let (remote_hash, remote_size) = hash_size(remote_version);
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
                    data: remote_version.to_vec(),
                },
            ],
            NOW,
        )
        .unwrap();

    fs::write(dir.path().join("shared.txt"), b"local version").unwrap();

    // Remote timestamp far in the future — remote wins
    let hash_b = [0xBB; 32];
    let conflicts = applier
        .apply_events(
            &[SyncEvent::FileModified {
                path: PathBuf::from("shared.txt"),
                content_hash: hash_b,
                size: 20,
            }],
            9_999_999_999_999,
        )
        .unwrap();

    assert!(conflicts.is_empty());
}

#[test]
fn test_conflict_keep_both_strategy() {
    let dir = TempDir::new().unwrap();
    let mut applier = ChangeApplier::new(dir.path(), ConflictStrategy::KeepBoth);

    let original = b"original  ";
    let (original_hash, original_size) = hash_size(original);
    applier
        .apply_events(
            &[
                SyncEvent::FileCreated {
                    path: PathBuf::from("doc.txt"),
                    content_hash: original_hash,
                    size: original_size,
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

    let hash_b = [0xCC; 32];
    let conflicts = applier
        .apply_events(
            &[SyncEvent::FileModified {
                path: PathBuf::from("doc.txt"),
                content_hash: hash_b,
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
fn test_no_conflict_same_content() {
    let dir = TempDir::new().unwrap();
    let mut applier = ChangeApplier::new(dir.path(), ConflictStrategy::LastWriteWins);

    let content = b"identical content";
    fs::write(dir.path().join("same.txt"), content).unwrap();
    let (hash, size) = file_hash_and_size(&dir.path().join("same.txt")).unwrap();

    let conflicts = applier
        .apply_events(
            &[SyncEvent::FileCreated {
                path: PathBuf::from("same.txt"),
                content_hash: hash,
                size,
            }],
            NOW,
        )
        .unwrap();
    assert!(conflicts.is_empty());
}

#[test]
fn test_no_conflict_on_new_file() {
    let dir = TempDir::new().unwrap();
    let mut applier = ChangeApplier::new(dir.path(), ConflictStrategy::LastWriteWins);

    let brand_new = b"brand new content";
    let (brand_new_hash, brand_new_size) = hash_size(brand_new);
    let conflicts = applier
        .apply_events(
            &[
                SyncEvent::FileCreated {
                    path: PathBuf::from("brand_new.txt"),
                    content_hash: brand_new_hash,
                    size: brand_new_size,
                },
                SyncEvent::FileContent {
                    path: PathBuf::from("brand_new.txt"),
                    offset: 0,
                    data: brand_new.to_vec(),
                },
            ],
            NOW,
        )
        .unwrap();
    assert!(conflicts.is_empty());
    assert!(dir.path().join("brand_new.txt").exists());
}

#[test]
fn test_conflict_on_second_modify() {
    let dir = TempDir::new().unwrap();
    let mut applier = ChangeApplier::new(dir.path(), ConflictStrategy::LastWriteWins);

    let version_one = b"version one";
    let (version_one_hash, version_one_size) = hash_size(version_one);
    applier
        .apply_events(
            &[
                SyncEvent::FileCreated {
                    path: PathBuf::from("evolving.txt"),
                    content_hash: version_one_hash,
                    size: version_one_size,
                },
                SyncEvent::FileContent {
                    path: PathBuf::from("evolving.txt"),
                    offset: 0,
                    data: version_one.to_vec(),
                },
            ],
            NOW,
        )
        .unwrap();

    // Remote modifies (local unchanged) — no conflict
    let conflicts = applier
        .apply_events(
            &[SyncEvent::FileModified {
                path: PathBuf::from("evolving.txt"),
                content_hash: [0x22; 32],
                size: 10,
            }],
            NOW,
        )
        .unwrap();
    assert!(conflicts.is_empty());

    // Local modifies the file
    fs::write(dir.path().join("evolving.txt"), b"local edit here").unwrap();

    // Remote modifies with timestamp in the past — conflict
    let conflicts = applier
        .apply_events(
            &[SyncEvent::FileModified {
                path: PathBuf::from("evolving.txt"),
                content_hash: [0x33; 32],
                size: 15,
            }],
            1000,
        )
        .unwrap();
    assert_eq!(conflicts.len(), 1);
}

#[test]
fn test_full_sync_simulation_two_dirs() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    fs::write(dir_a.path().join("file_a.txt"), b"from A").unwrap();
    fs::create_dir(dir_a.path().join("subdir")).unwrap();
    fs::write(dir_a.path().join("subdir/nested.txt"), b"nested from A").unwrap();

    let events_a = initial_scan(dir_a.path(), &[]);
    let mut all_a = events_a;
    for entry in walkdir::WalkDir::new(dir_a.path()) {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            let rel = entry.path().strip_prefix(dir_a.path()).unwrap();
            let data = std::fs::read(entry.path()).unwrap();
            let chunks = chunker::chunk_data(rel, &data, 65536);
            all_a.extend(chunks);
        }
    }

    let mut applier_b = ChangeApplier::new(dir_b.path(), ConflictStrategy::LastWriteWins);
    let conflicts = applier_b.apply_events(&all_a, NOW).unwrap();
    assert!(conflicts.is_empty());

    assert_eq!(
        fs::read(dir_b.path().join("file_a.txt")).unwrap(),
        b"from A"
    );
    assert!(dir_b.path().join("subdir").is_dir());
    assert_eq!(
        fs::read(dir_b.path().join("subdir/nested.txt")).unwrap(),
        b"nested from A"
    );

    fs::write(dir_b.path().join("file_a.txt"), b"modified by B").unwrap();
    fs::write(dir_b.path().join("new_in_b.txt"), b"new from B").unwrap();

    let events_b = initial_scan(dir_b.path(), &[]);
    let mut all_b = events_b;
    for entry in walkdir::WalkDir::new(dir_b.path()) {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            let rel = entry.path().strip_prefix(dir_b.path()).unwrap();
            let data = std::fs::read(entry.path()).unwrap();
            let chunks = chunker::chunk_data(rel, &data, 65536);
            all_b.extend(chunks);
        }
    }
    let mut applier_a = ChangeApplier::new(dir_a.path(), ConflictStrategy::LastWriteWins);
    let _conflicts = applier_a.apply_events(&all_b, NOW).unwrap();

    assert!(dir_a.path().join("new_in_b.txt").exists());
}
