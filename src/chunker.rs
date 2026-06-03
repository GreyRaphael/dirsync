use crate::event::SyncEvent;
use std::path::Path;
use tracing::debug;

/// Default chunk size for file content transfer (64KB).
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// Produce a sequence of `SyncEvent::FileContent` events from pre-read file data.
///
/// This is the preferred entry point — the caller reads the file once and passes
/// the data here, eliminating the TOCTOU between hashing and chunking.
pub fn chunk_data(
    relative_path: &Path,
    data: &[u8],
    chunk_size: usize,
) -> Vec<SyncEvent> {
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

    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_chunk_small_data() {
        let events = chunk_data(Path::new("test.txt"), b"hello world", DEFAULT_CHUNK_SIZE);
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
    fn test_chunk_large_data() {
        let data = vec![0xABu8; 200];
        let events = chunk_data(Path::new("big.bin"), &data, 64);
        assert_eq!(events.len(), 4);

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
    fn test_chunk_empty_data() {
        let events = chunk_data(Path::new("empty.txt"), b"", DEFAULT_CHUNK_SIZE);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data_len(), 0);
    }
}
