//! End-to-end integration tests for dirsync.
//!
//! These tests simulate two sync instances communicating through shared memory,
//! verifying that file creates, modifies, and deletes are properly propagated.

use dirsync::apply::ChangeApplier;
use dirsync::chunker;
use dirsync::cli::ConflictStrategy;
use dirsync::event::{EventEnvelope, SyncEvent};
use dirsync::shm::ShmTransport;
use dirsync::watcher::{file_hash_and_size, initial_scan};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

fn unique_shm_name(suffix: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = std::env::temp_dir();
    path.push(format!("dirsync_itest_{}_{}", ts, suffix));
    path.to_string_lossy().into_owned()
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

    // Instance A writes
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

    // Instance B reads
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

    // Instance 0 writes
    shm.push_event(0, &seq_envelope(0, 1, SyncEvent::DirCreated {
        path: PathBuf::from("docs"),
    }))
    .unwrap();

    // Instance 1 writes
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

    // Instance 0 reads — sees both its own and instance 1's events
    let ev0 = shm.pop_events(0).unwrap();
    assert_eq!(ev0.len(), 2);
    // Filter to only remote events (like the sync engine does)
    let remote_for_0: Vec<_> = ev0.iter().filter(|e| e.instance_id == 1).collect();
    assert_eq!(remote_for_0.len(), 1);

    // Instance 1 reads — sees both its own and instance 0's events
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
    let applier = ChangeApplier::new(dir.path(), ConflictStrategy::LastWriteWins);

    // 1. Create directory
    applier
        .apply_events(&[SyncEvent::DirCreated {
            path: PathBuf::from("project"),
        }])
        .unwrap();
    assert!(dir.path().join("project").is_dir());

    // 2. Create file
    applier
        .apply_events(&[SyncEvent::FileCreated {
            path: PathBuf::from("project/data.txt"),
            content_hash: [0u8; 32],
            size: 0,
        }])
        .unwrap();
    assert!(dir.path().join("project/data.txt").exists());

    // 3. Write content
    applier
        .apply_events(&[SyncEvent::FileContent {
            path: PathBuf::from("project/data.txt"),
            offset: 0,
            data: b"Hello, World!".to_vec(),
        }])
        .unwrap();
    assert_eq!(
        fs::read(dir.path().join("project/data.txt")).unwrap(),
        b"Hello, World!"
    );

    // 4. Modify content at offset
    applier
        .apply_events(&[SyncEvent::FileContent {
            path: PathBuf::from("project/data.txt"),
            offset: 7,
            data: b"Rust!".to_vec(),
        }])
        .unwrap();
    // Writing "Rust!" at offset 7 overwrites bytes 7-11, but original '!' at
    // byte 12 remains since we didn't truncate.
    assert_eq!(
        fs::read(dir.path().join("project/data.txt")).unwrap(),
        b"Hello, Rust!!"
    );

    // 5. Delete file
    applier
        .apply_events(&[SyncEvent::FileDeleted {
            path: PathBuf::from("project/data.txt"),
        }])
        .unwrap();
    assert!(!dir.path().join("project/data.txt").exists());

    // 6. Delete directory
    applier
        .apply_events(&[SyncEvent::DirDeleted {
            path: PathBuf::from("project"),
        }])
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

    // Create a temp file with known content
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("big.bin");
    let content = vec![0xABu8; 200];
    fs::write(&file_path, &content).unwrap();

    // Chunk it
    let chunks = chunker::chunk_file(Path::new("big.bin"), &file_path, 64).unwrap();
    assert_eq!(chunks.len(), 4); // 200 / 64 = 4 chunks

    // Push all chunks to SHM as instance 0
    for (i, chunk) in chunks.into_iter().enumerate() {
        shm.push_event(0, &seq_envelope(0, (i + 1) as u64, chunk))
            .unwrap();
    }

    // Instance 1 reads and applies
    let events = shm.pop_events(1).unwrap();
    assert_eq!(events.len(), 4);

    let out_dir = TempDir::new().unwrap();
    let applier = ChangeApplier::new(out_dir.path(), ConflictStrategy::LastWriteWins);
    let sync_events: Vec<SyncEvent> = events.into_iter().map(|e| e.event).collect();
    applier.apply_events(&sync_events).unwrap();

    // Verify content matches
    let out_content = fs::read(out_dir.path().join("big.bin")).unwrap();
    assert_eq!(out_content, content);
}

// ------------------------------------------------------------------
// Initial scan → apply round-trip
// ------------------------------------------------------------------

#[test]
fn test_initial_scan_and_apply() {
    // Set up source directory
    let src = TempDir::new().unwrap();
    fs::create_dir(src.path().join("src")).unwrap();
    fs::write(src.path().join("src/main.rs"), b"fn main() {}").unwrap();
    fs::write(src.path().join("Cargo.toml"), b"[package]").unwrap();

    // Scan source (produces DirCreated + FileCreated events)
    let events = initial_scan(src.path(), &[]);
    assert!(events.len() >= 3);

    // Also produce FileContent events for file data (like the sync engine does)
    let mut all_events = events;
    for entry in walkdir::WalkDir::new(src.path()) {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            let rel = entry.path().strip_prefix(src.path()).unwrap();
            let chunks = chunker::chunk_file(rel, entry.path(), 65536).unwrap();
            all_events.extend(chunks);
        }
    }

    // Apply to destination
    let dst = TempDir::new().unwrap();
    let applier = ChangeApplier::new(dst.path(), ConflictStrategy::LastWriteWins);
    applier.apply_events(&all_events).unwrap();

    // Verify structure and content
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

    // Only real.txt should appear
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

    // Apply the same content via SyncEvent
    let applier = ChangeApplier::new(dir.path(), ConflictStrategy::LastWriteWins);
    applier
        .apply_events(&[SyncEvent::FileContent {
            path: PathBuf::from("data.bin"),
            offset: 0,
            data: content.to_vec(),
        }])
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
    let shm = ShmTransport::create(&name, 262144).unwrap(); // 256KB SHM

    let dir = TempDir::new().unwrap();
    let file = dir.path().join("large.bin");
    let content = vec![0x42u8; 100_000]; // 100KB
    fs::write(&file, &content).unwrap();

    let chunks = chunker::chunk_file(Path::new("large.bin"), &file, 65536).unwrap();
    // 100KB / 64KB = 2 chunks
    assert_eq!(chunks.len(), 2);

    for (i, chunk) in chunks.into_iter().enumerate() {
        shm.push_event(0, &seq_envelope(0, (i + 1) as u64, chunk))
            .unwrap();
    }

    let events = shm.pop_events(1).unwrap();
    let out_dir = TempDir::new().unwrap();
    let applier = ChangeApplier::new(out_dir.path(), ConflictStrategy::LastWriteWins);
    let sync_events: Vec<SyncEvent> = events.into_iter().map(|e| e.event).collect();
    applier.apply_events(&sync_events).unwrap();

    let out = fs::read(out_dir.path().join("large.bin")).unwrap();
    assert_eq!(out, content);
}
