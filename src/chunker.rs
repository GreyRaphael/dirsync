use crate::event::SyncEvent;
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use tracing::debug;

/// Default chunk size for file content transfer (64KB).
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// Read a file and produce a sequence of `SyncEvent::FileContent` events
/// that carry the file data in chunks small enough for the ring buffer.
pub fn chunk_file(
    relative_path: &Path,
    absolute_path: &Path,
    chunk_size: usize,
) -> Result<Vec<SyncEvent>> {
    let data = fs::read(absolute_path)
        .with_context(|| format!("Failed to read file for chunking: {}", absolute_path.display()))?;

    let total_size = data.len();
    let mut events = Vec::new();
    let mut offset: u64 = 0;

    for chunk in data.chunks(chunk_size) {
        events.push(SyncEvent::FileContent {
            path: relative_path.to_path_buf(),
            offset,
            data: chunk.to_vec(),
        });
        offset += chunk.len() as u64;
    }

    if events.is_empty() {
        // Empty file — send a single zero-length chunk
        events.push(SyncEvent::FileContent {
            path: relative_path.to_path_buf(),
            offset: 0,
            data: Vec::new(),
        });
    }

    debug!(
        "Chunked {} ({} bytes) into {} events (chunk_size={})",
        relative_path.display(),
        total_size,
        events.len(),
        chunk_size
    );

    Ok(events)
}

/// Estimate the serialized size of a FileContent event to decide
/// whether chunking is needed.
#[allow(dead_code)]
pub fn estimated_event_size(data_len: usize) -> usize {
    // bincode overhead: enum variant (1) + path prefix + hash(32) + offset(8) + data len prefix
    // Rough estimate: ~100 bytes overhead + data
    100 + data_len
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

    #[test]
    fn test_chunk_small_file() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello world").unwrap();

        let events = chunk_file(Path::new("test.txt"), f.path(), DEFAULT_CHUNK_SIZE).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            SyncEvent::FileContent { path, offset, data } => {
                assert_eq!(path, &PathBuf::from("test.txt"));
                assert_eq!(*offset, 0);
                assert_eq!(data, b"hello world");
            }
            _ => panic!("Expected FileContent"),
        }
    }

    #[test]
    fn test_chunk_large_file() {
        let mut f = NamedTempFile::new().unwrap();
        let data = vec![0xABu8; 200];
        f.write_all(&data).unwrap();

        // Chunk size 64 → 200/64 = 4 chunks
        let events = chunk_file(Path::new("big.bin"), f.path(), 64).unwrap();
        assert_eq!(events.len(), 4);

        // Verify offsets and lengths
        assert_eq!(events[0].offset(), 0);
        assert_eq!(events[0].data_len(), 64);
        assert_eq!(events[1].offset(), 64);
        assert_eq!(events[1].data_len(), 64);
        assert_eq!(events[2].offset(), 128);
        assert_eq!(events[2].data_len(), 64);
        assert_eq!(events[3].offset(), 192);
        assert_eq!(events[3].data_len(), 8);
    }

    #[test]
    fn test_chunk_empty_file() {
        let f = NamedTempFile::new().unwrap();
        let events = chunk_file(Path::new("empty.txt"), f.path(), DEFAULT_CHUNK_SIZE).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data_len(), 0);
    }
}
