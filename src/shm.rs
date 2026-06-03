use crate::event::{EventEnvelope, ShmHeader, SHM_HEADER_SIZE};
use anyhow::{Context, Result};
use shared_memory::{Shmem, ShmemConf};
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{debug, trace};

/// Sentinel length value indicating a wrap-around padding in the ring buffer.
/// When the reader encounters this value, it resets its cursor to offset 0.
const WRAP_SENTINEL: u32 = 0xFFFFFFFF;

/// Ring buffer based shared memory transport for SyncEvents.
pub struct ShmTransport {
    shmem: Shmem,
    shm_name: String,
    #[allow(dead_code)]
    total_size: usize,
}

impl ShmTransport {
    /// Create a new shared memory segment (first instance).
    pub fn create(name: &str, total_size: usize) -> Result<Self> {
        let shmem = ShmemConf::new()
            .size(total_size)
            .flink(name)
            .force_create_flink()
            .create()
            .context("Failed to create shared memory")?;

        let shm = Self {
            shmem,
            shm_name: name.to_string(),
            total_size,
        };

        // Initialize header
        let rb_capacity = (total_size - SHM_HEADER_SIZE) as u32;
        let header = ShmHeader::new(rb_capacity, 0);
        unsafe {
            shm.write_header(&header);
        }

        debug!("Created SHM '{}' ({} bytes)", name, total_size);
        Ok(shm)
    }

    /// Open an existing shared memory segment (second instance).
    pub fn open(name: &str) -> Result<Self> {
        let shmem = ShmemConf::new()
            .flink(name)
            .open()
            .context("Failed to open shared memory")?;

        let total_size = shmem.len();
        let shm = Self {
            shmem,
            shm_name: name.to_string(),
            total_size,
        };

        // Validate header
        let header = unsafe { shm.read_header() };
        header.validate()?;

        debug!("Opened SHM '{}' ({} bytes)", name, total_size);
        Ok(shm)
    }

    /// Create or open shared memory depending on whether it already exists.
    pub fn create_or_open(name: &str, total_size: usize) -> Result<Self> {
        match Self::open(name) {
            Ok(shm) => Ok(shm),
            Err(_) => Self::create(name, total_size),
        }
    }

    /// Get a pointer to the shared memory base.
    fn base_ptr(&self) -> *mut u8 {
        self.shmem.as_ptr()
    }

    /// Read the header from shared memory.
    ///
    /// # Safety
    /// Caller must ensure no concurrent mutable access to the header region.
    unsafe fn read_header(&self) -> &ShmHeader {
        unsafe { &*(self.base_ptr() as *const ShmHeader) }
    }

    /// Write header to shared memory.
    ///
    /// # Safety
    /// Caller must ensure no concurrent access to the header region.
    unsafe fn write_header(&self, header: &ShmHeader) {
        unsafe {
            let dst = self.base_ptr() as *mut ShmHeader;
            *dst = header.clone();
        }
    }

    /// Get pointer to the ring buffer region (right after header).
    fn rb_ptr(&self) -> *mut u8 {
        unsafe { self.base_ptr().add(SHM_HEADER_SIZE) }
    }

    /// Acquire the spinlock for the given instance.
    ///
    /// Returns a reference to the lock atomic so the caller can release it.
    fn acquire_lock(&self, instance_id: u64) -> &'static AtomicU32 {
        let header = self.base_ptr() as *const ShmHeader;
        let lock_ptr = unsafe {
            if instance_id == 0 {
                &(*header).lock_a as *const u32
            } else {
                &(*header).lock_b as *const u32
            }
        };
        let lock = unsafe { &*(lock_ptr as *const AtomicU32) };

        while lock.compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed).is_err() {
            std::hint::spin_loop();
        }

        lock
    }

    /// Release a spinlock.
    fn release_lock(lock: &AtomicU32) {
        lock.store(0, Ordering::Release);
    }

    /// Compute how many free bytes remain in the ring buffer.
    ///
    /// The buffer is considered full when the next write would overlap the
    /// slowest reader's cursor.
    fn free_bytes(&self) -> u32 {
        let header = unsafe { self.read_header() };
        let cap = header.rb_capacity;
        let w = header.rb_write;
        let r_a = header.rb_read_a;
        let r_b = header.rb_read_b;

        // Use the slowest (minimum) read cursor to compute free space
        let slowest = std::cmp::min(r_a, r_b);

        if w >= slowest {
            cap - (w - slowest)
        } else {
            slowest - w
        }
    }

    /// Push an event envelope into the ring buffer.
    ///
    /// Frame layout: `[u32 len][bytes data]`
    /// If the remaining space at the end is too small for the frame, a
    /// WRAP_SENTINEL is written and the frame is placed at offset 0.
    pub fn push_event(&self, instance_id: u64, envelope: &EventEnvelope) -> Result<()> {
        let encoded = bincode::serialize(envelope).context("Failed to serialize event")?;
        let data_len = encoded.len() as u32;
        let frame_len = 4 + data_len; // u32 prefix + data

        let lock = self.acquire_lock(instance_id);

        let header = unsafe { self.read_header() };
        let cap = header.rb_capacity;
        let w = header.rb_write;

        // Check if there is enough free space
        let free = self.free_bytes();
        if frame_len + 4 > free {
            // +4 for potential wrap sentinel
            Self::release_lock(lock);
            anyhow::bail!(
                "Ring buffer overflow: need {} bytes, {} free",
                frame_len,
                free
            );
        }

        let rb = self.rb_ptr();

        // Determine if we need to wrap
        let actual_write = if w + frame_len > cap {
            // Not enough room at the tail — write a wrap sentinel and start at 0
            let sentinel = WRAP_SENTINEL.to_le_bytes();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    sentinel.as_ptr(),
                    rb.add(w as usize),
                    4,
                );
            }
            0u32
        } else {
            w
        };

        // Write frame: [u32 len][data]
        unsafe {
            std::ptr::copy_nonoverlapping(
                data_len.to_le_bytes().as_ptr(),
                rb.add(actual_write as usize),
                4,
            );
            std::ptr::copy_nonoverlapping(
                encoded.as_ptr(),
                rb.add(actual_write as usize + 4),
                data_len as usize,
            );
        }

        // Update header: write cursor and sequence
        let header_mut = unsafe { &mut *(self.base_ptr() as *mut ShmHeader) };
        header_mut.rb_write = (actual_write + frame_len) % cap;

        if instance_id == 0 {
            header_mut.seq_a += 1;
        } else {
            header_mut.seq_b += 1;
        }

        Self::release_lock(lock);

        trace!(
            "Pushed event ({} bytes) at offset {}, next write={}",
            data_len,
            actual_write,
            header_mut.rb_write
        );
        Ok(())
    }

    /// Pop all pending events from the ring buffer for the given reader instance.
    pub fn pop_events(&self, instance_id: u64) -> Result<Vec<EventEnvelope>> {
        let header = unsafe { self.read_header() };
        let cap = header.rb_capacity;

        let read_offset = if instance_id == 0 {
            header.rb_read_a
        } else {
            header.rb_read_b
        };

        let write_offset = header.rb_write;

        if read_offset == write_offset {
            return Ok(Vec::new());
        }

        let mut events = Vec::new();
        let rb = self.rb_ptr();
        let mut cursor = read_offset;

        loop {
            if cursor == write_offset {
                break;
            }

            // Read length prefix
            let mut len_buf = [0u8; 4];
            unsafe {
                std::ptr::copy_nonoverlapping(rb.add(cursor as usize), len_buf.as_mut_ptr(), 4);
            }
            let len = u32::from_le_bytes(len_buf);

            // Check for wrap sentinel
            if len == WRAP_SENTINEL {
                trace!("Hit wrap sentinel at offset {}, resetting to 0", cursor);
                cursor = 0;
                if cursor == write_offset {
                    break;
                }
                // Re-read length at offset 0
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        rb.add(cursor as usize),
                        len_buf.as_mut_ptr(),
                        4,
                    );
                }
                let len2 = u32::from_le_bytes(len_buf);
                if len2 == WRAP_SENTINEL {
                    break; // Should not happen
                }
                // Fall through to read the frame at offset 0 with len2
                let data_len = len2 as usize;
                if cursor + 4 + data_len as u32 > cap {
                    break;
                }
                let mut data = vec![0u8; data_len];
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        rb.add(cursor as usize + 4),
                        data.as_mut_ptr(),
                        data_len,
                    );
                }
                match bincode::deserialize::<EventEnvelope>(&data) {
                    Ok(env) => events.push(env),
                    Err(e) => tracing::warn!("Failed to deserialize event: {}", e),
                }
                cursor = (cursor + 4 + data_len as u32) % cap;
                continue;
            }

            let data_len = len as usize;

            // Bounds check
            if cursor + 4 + len > cap {
                tracing::warn!(
                    "Incomplete frame at offset {} (need {} bytes, {} available)",
                    cursor,
                    len,
                    cap - cursor
                );
                break;
            }

            // Read data
            let mut data = vec![0u8; data_len];
            unsafe {
                std::ptr::copy_nonoverlapping(
                    rb.add(cursor as usize + 4),
                    data.as_mut_ptr(),
                    data_len,
                );
            }

            match bincode::deserialize::<EventEnvelope>(&data) {
                Ok(env) => events.push(env),
                Err(e) => tracing::warn!("Failed to deserialize event at offset {}: {}", cursor, e),
            }

            cursor = (cursor + 4 + len) % cap;
        }

        // Update read cursor for this instance
        let header_mut = unsafe { &mut *(self.base_ptr() as *mut ShmHeader) };
        if instance_id == 0 {
            header_mut.rb_read_a = cursor;
        } else {
            header_mut.rb_read_b = cursor;
        }

        debug!("Popped {} events, read cursor now at {}", events.len(), cursor);
        Ok(events)
    }

    /// Return total capacity of the ring buffer in bytes.
    #[allow(dead_code)]
    pub fn capacity(&self) -> u32 {
        let header = unsafe { self.read_header() };
        header.rb_capacity
    }
}

impl Drop for ShmTransport {
    fn drop(&mut self) {
        debug!("Dropping SHM '{}'", self.shm_name);
        // shared_memory crate handles cleanup
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::SyncEvent;
    use std::path::PathBuf;

    fn unique_name(suffix: &str) -> String {
        // Use a temp directory for the flink path to avoid invalid chars on Windows
        let mut path = std::env::temp_dir();
        path.push(format!("dirsync_test_{}_{}", std::process::id(), suffix));
        path.to_string_lossy().into_owned()
    }

    fn make_envelope(seq: u64, path: &str) -> EventEnvelope {
        EventEnvelope {
            instance_id: 0,
            seq,
            timestamp: 1700000000000 + seq as i64,
            event: SyncEvent::FileCreated {
                path: PathBuf::from(path),
                content_hash: [0u8; 32],
                size: 100 * seq,
            },
        }
    }

    #[test]
    fn test_shm_create_and_open() {
        let name = unique_name("create_open");
        let shm = ShmTransport::create(&name, 4096).unwrap();
        let shm2 = ShmTransport::open(&name).unwrap();
        assert_eq!(shm.total_size, shm2.total_size);
    }

    #[test]
    fn test_create_or_open_first_creates() {
        let name = unique_name("first_creates");
        let shm = ShmTransport::create_or_open(&name, 8192).unwrap();
        assert!(shm.capacity() > 0);
    }

    #[test]
    fn test_create_or_open_second_opens() {
        let name = unique_name("second_opens");
        let shm1 = ShmTransport::create_or_open(&name, 8192).unwrap();
        let cap1 = shm1.capacity();
        let shm2 = ShmTransport::create_or_open(&name, 8192).unwrap();
        assert_eq!(shm1.capacity(), shm2.capacity());
        assert_eq!(cap1, shm2.capacity());
    }

    #[test]
    fn test_push_and_pop_single_event() {
        let name = unique_name("push_pop_single");
        let shm = ShmTransport::create(&name, 65536).unwrap();

        let envelope = make_envelope(1, "test.txt");
        shm.push_event(0, &envelope).unwrap();

        let events = shm.pop_events(1).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 1);
    }

    #[test]
    fn test_push_and_pop_multiple_events() {
        let name = unique_name("push_pop_multi");
        let shm = ShmTransport::create(&name, 65536).unwrap();

        for i in 1..=10 {
            let envelope = make_envelope(i, &format!("file_{i}.txt"));
            shm.push_event(0, &envelope).unwrap();
        }

        let events = shm.pop_events(1).unwrap();
        assert_eq!(events.len(), 10);
        for (i, ev) in events.iter().enumerate() {
            assert_eq!(ev.seq, (i + 1) as u64);
        }
    }

    #[test]
    fn test_pop_empty_returns_empty() {
        let name = unique_name("pop_empty");
        let shm = ShmTransport::create(&name, 4096).unwrap();
        let events = shm.pop_events(0).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_pop_advances_cursor() {
        let name = unique_name("pop_advances");
        let shm = ShmTransport::create(&name, 65536).unwrap();

        shm.push_event(0, &make_envelope(1, "a.txt")).unwrap();
        shm.push_event(0, &make_envelope(2, "b.txt")).unwrap();

        let first = shm.pop_events(1).unwrap();
        assert_eq!(first.len(), 2);

        // Second pop should return empty (cursor advanced)
        let second = shm.pop_events(1).unwrap();
        assert!(second.is_empty());
    }

    #[test]
    fn test_ring_buffer_wrap_around() {
        // Use a small buffer to force wrap-around quickly
        let name = unique_name("wrap_around");
        let shm = ShmTransport::create(&name, 4096).unwrap();

        // Fill and drain the buffer multiple times to exercise wrap
        for round in 0..20 {
            let envelope = make_envelope(round, &format!("round_{round}.txt"));
            shm.push_event(0, &envelope).unwrap();
            let events = shm.pop_events(1).unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].seq, round);
        }
    }

    #[test]
    fn test_two_instances_independent_read_cursors() {
        let name = unique_name("independent_cursors");
        let shm = ShmTransport::create(&name, 65536).unwrap();

        shm.push_event(0, &make_envelope(1, "shared.txt")).unwrap();

        // Instance 0 reads
        let ev0 = shm.pop_events(0).unwrap();
        assert_eq!(ev0.len(), 1);

        // Instance 1 still sees the event
        let ev1 = shm.pop_events(1).unwrap();
        assert_eq!(ev1.len(), 1);

        // Both now empty
        assert!(shm.pop_events(0).unwrap().is_empty());
        assert!(shm.pop_events(1).unwrap().is_empty());
    }

    #[test]
    fn test_overflow_returns_error() {
        let name = unique_name("overflow");
        // Small buffer: header(56) + ~900 bytes ring buffer
        let shm = ShmTransport::create(&name, 1024).unwrap();

        // Push events until the buffer fills up
        let mut pushed = 0;
        for i in 0..100 {
            let event = EventEnvelope {
                instance_id: 0,
                seq: i,
                timestamp: 1700000000000 + i as i64,
                event: SyncEvent::FileCreated {
                    path: PathBuf::from(format!("file_{i}.txt")),
                    content_hash: [0u8; 32],
                    size: 100,
                },
            };
            match shm.push_event(0, &event) {
                Ok(()) => pushed += 1,
                Err(_) => break,
            }
        }
        // Should have pushed some but not all 100
        assert!(pushed > 0, "Expected at least one push to succeed");
        assert!(pushed < 100, "Expected overflow before 100 events");
    }

    #[test]
    fn test_heartbeat_event_roundtrip() {
        let name = unique_name("heartbeat");
        let shm = ShmTransport::create(&name, 4096).unwrap();

        let envelope = EventEnvelope {
            instance_id: 1,
            seq: 99,
            timestamp: 1700000000000,
            event: SyncEvent::Heartbeat {
                timestamp: 1700000000000,
            },
        };
        shm.push_event(1, &envelope).unwrap();
        let events = shm.pop_events(0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 99);
        assert!(matches!(events[0].event, SyncEvent::Heartbeat { .. }));
    }
}
