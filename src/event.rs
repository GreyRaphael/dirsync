use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Unique identifier for a sync instance
pub type InstanceId = u64;

/// Sequence number for ordering events
pub type SeqNum = u64;

/// Synchronization events exchanged via shared memory
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SyncEvent {
    /// A new file was created
    FileCreated {
        path: PathBuf,
        content_hash: [u8; 32],
        size: u64,
    },
    /// An existing file was modified
    FileModified {
        path: PathBuf,
        content_hash: [u8; 32],
        size: u64,
    },
    /// A file was deleted
    FileDeleted {
        path: PathBuf,
    },
    /// A new directory was created
    DirCreated {
        path: PathBuf,
    },
    /// A directory was deleted
    DirDeleted {
        path: PathBuf,
    },
    /// Chunk of file content for large file transfer
    FileContent {
        path: PathBuf,
        offset: u64,
        data: Vec<u8>,
    },
    /// Heartbeat to detect process liveness
    Heartbeat {
        timestamp: i64,
    },
}

impl SyncEvent {
    /// Returns the relative path affected by this event, if any.
    pub fn path(&self) -> Option<&PathBuf> {
        match self {
            Self::FileCreated { path, .. }
            | Self::FileModified { path, .. }
            | Self::FileDeleted { path }
            | Self::DirCreated { path }
            | Self::DirDeleted { path }
            | Self::FileContent { path, .. } => Some(path),
            Self::Heartbeat { .. } => None,
        }
    }

    /// For FileContent events, returns the data offset.
    #[allow(dead_code)]
    pub fn offset(&self) -> u64 {
        match self {
            Self::FileContent { offset, .. } => *offset,
            _ => 0,
        }
    }

    /// For FileContent events, returns the data length.
    #[allow(dead_code)]
    pub fn data_len(&self) -> usize {
        match self {
            Self::FileContent { data, .. } => data.len(),
            _ => 0,
        }
    }
}

/// Envelope wrapping an event with metadata for the shared memory protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Which instance produced this event
    pub instance_id: InstanceId,
    /// Monotonic sequence number
    pub seq: SeqNum,
    /// Wall-clock timestamp (Unix millis)
    pub timestamp: i64,
    /// The actual event payload
    pub event: SyncEvent,
}

/// Shared memory header stored at offset 0
///
/// Layout (all fields little-endian):
///   0x00  [u8; 4]  magic        "DSYN"
///   0x04  u32      version      protocol version
///   0x08  u64      seq_a        instance A write counter
///   0x10  u64      seq_b        instance B write counter
///   0x18  u32      lock_a       spinlock for A
///   0x1C  u32      lock_b       spinlock for B
///   0x20  u32      rb_write     ring buffer write offset
///   0x24  u32      rb_read_a    ring buffer read cursor for A
///   0x28  u32      rb_read_b    ring buffer read cursor for B
///   0x2C  u32      rb_capacity  ring buffer capacity in bytes
///   0x30  u32      dp_write     data pool write offset
///   0x34  u32      dp_size      data pool total size
#[repr(C)]
#[derive(Clone)]
pub struct ShmHeader {
    pub magic: [u8; 4],
    pub version: u32,
    pub seq_a: u64,
    pub seq_b: u64,
    pub lock_a: u32,
    pub lock_b: u32,
    pub rb_write: u32,
    pub rb_read_a: u32,
    pub rb_read_b: u32,
    pub rb_capacity: u32,
    pub dp_write: u32,
    pub dp_size: u32,
}

pub const SHM_MAGIC: &[u8; 4] = b"DSYN";
pub const SHM_VERSION: u32 = 1;
pub const SHM_HEADER_SIZE: usize = 0x38;

impl ShmHeader {
    pub fn new(rb_capacity: u32, dp_size: u32) -> Self {
        Self {
            magic: *SHM_MAGIC,
            version: SHM_VERSION,
            seq_a: 0,
            seq_b: 0,
            lock_a: 0,
            lock_b: 0,
            rb_write: 0,
            rb_read_a: 0,
            rb_read_b: 0,
            rb_capacity,
            dp_write: 0,
            dp_size,
        }
    }

    /// Validate magic and version
    pub fn validate(&self) -> anyhow::Result<()> {
        if &self.magic != SHM_MAGIC {
            anyhow::bail!("Invalid SHM magic: expected DSYN");
        }
        if self.version != SHM_VERSION {
            anyhow::bail!(
                "Unsupported SHM version: {} (expected {})",
                self.version,
                SHM_VERSION
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_serde_roundtrip() {
        let event = SyncEvent::FileCreated {
            path: PathBuf::from("foo/bar.txt"),
            content_hash: [0u8; 32],
            size: 1024,
        };
        let encoded = bincode::serialize(&event).unwrap();
        let decoded: SyncEvent = bincode::deserialize(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_envelope_serde_roundtrip() {
        let envelope = EventEnvelope {
            instance_id: 1,
            seq: 42,
            timestamp: 1700000000000,
            event: SyncEvent::Heartbeat { timestamp: 1700000000000 },
        };
        let encoded = bincode::serialize(&envelope).unwrap();
        let decoded: EventEnvelope = bincode::deserialize(&encoded).unwrap();
        assert_eq!(envelope.instance_id, decoded.instance_id);
        assert_eq!(envelope.seq, decoded.seq);
        assert_eq!(envelope.event, decoded.event);
    }

    #[test]
    fn test_shm_header_validate() {
        let header = ShmHeader::new(1024, 2048);
        assert!(header.validate().is_ok());
    }
}
