use crate::event::{EventEnvelope, ShmHeader, SHM_HEADER_SIZE};
use anyhow::{Context, Result};
use shared_memory::{Shmem, ShmemConf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, trace};

/// Sentinel length value indicating a wrap-around padding in the ring buffer.
/// When the reader encounters this value, it resets its cursor to offset 0.
const WRAP_SENTINEL: u32 = 0xFFFFFFFF;

/// Maximum spin iterations before declaring lock deadlock.
const MAX_SPIN: u32 = 2_000_000;

/// How long a producer waits for readers to free ring-buffer space.
const PUSH_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const PUSH_RETRY_SLEEP: Duration = Duration::from_millis(10);

/// Ring buffer based shared memory transport for SyncEvents.
pub struct ShmTransport {
    shmem: Shmem,
    shm_name: String,
    registered_instance: Option<u64>,
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
            registered_instance: None,
            total_size,
        };

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
            registered_instance: None,
            total_size,
        };

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

    fn base_ptr(&self) -> *mut u8 {
        self.shmem.as_ptr()
    }

    /// # Safety
    /// Caller must ensure the header region is not being concurrently mutated.
    unsafe fn read_header(&self) -> &ShmHeader {
        unsafe { &*(self.base_ptr() as *const ShmHeader) }
    }

    /// # Safety
    /// Caller must ensure no concurrent access to the header region.
    unsafe fn write_header(&self, header: &ShmHeader) {
        unsafe {
            let dst = self.base_ptr() as *mut ShmHeader;
            *dst = header.clone();
        }
    }

    fn rb_ptr(&self) -> *mut u8 {
        unsafe { self.base_ptr().add(SHM_HEADER_SIZE) }
    }

    /// Get a pointer to the global ring-buffer lock.
    fn lock_ptr(&self, _instance_id: u64) -> *const AtomicU32 {
        let header = self.base_ptr() as *const ShmHeader;
        unsafe { &(*header).lock_a as *const u32 as *const AtomicU32 }
    }

    /// Acquire the spinlock with a timeout to prevent deadlock.
    ///
    /// Returns `Some(lock)` on success, `None` on timeout.
    fn try_acquire_lock(&self, instance_id: u64) -> Option<&AtomicU32> {
        let lock = unsafe { &*self.lock_ptr(instance_id) };
        let mut spins = 0;
        while lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            spins += 1;
            if spins >= MAX_SPIN {
                // Force-reclaim the lock: the holder likely crashed
                lock.store(0, Ordering::Relaxed);
                tracing::warn!("Lock timeout for instance {}, force-reclaimed", instance_id);
                // Try once more
                if lock
                    .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    return Some(lock);
                }
                return None;
            }
            std::hint::spin_loop();
        }
        Some(lock)
    }

    fn release_lock(lock: &AtomicU32) {
        lock.store(0, Ordering::Release);
    }

    /// Mark an instance as active in the shared-memory header.
    ///
    /// The read cursor starts at the current write cursor so a restarted process
    /// does not try to consume stale frames from before it joined. The initial
    /// directory scan is responsible for reconciling state after startup.
    pub fn register_instance(&mut self, instance_id: u64) -> Result<()> {
        if instance_id > 1 {
            anyhow::bail!("Invalid instance_id {}, expected 0 or 1", instance_id);
        }

        let lock = self
            .try_acquire_lock(instance_id)
            .ok_or_else(|| anyhow::anyhow!("Failed to acquire lock for register"))?;

        let active_mask = {
            let header_mut = unsafe { &mut *(self.base_ptr() as *mut ShmHeader) };
            let bit = 1u32 << instance_id;
            let write = header_mut.rb_write;
            header_mut.active_mask |= bit;
            if instance_id == 0 {
                header_mut.rb_read_a = write;
            } else {
                header_mut.rb_read_b = write;
            }
            header_mut.active_mask
        };

        Self::release_lock(lock);
        self.registered_instance = Some(instance_id);
        debug!(
            "Registered instance {} (active_mask={:#04b})",
            instance_id, active_mask
        );
        Ok(())
    }

    /// Return the current active-instance bitmask.
    pub fn active_mask(&self) -> u32 {
        let header = unsafe { self.read_header() };
        header.active_mask
    }

    fn unregister_instance(&mut self) {
        let Some(instance_id) = self.registered_instance.take() else {
            return;
        };

        let Some(lock) = self.try_acquire_lock(instance_id) else {
            tracing::warn!("Failed to acquire lock while unregistering instance {}", instance_id);
            return;
        };

        let header_mut = unsafe { &mut *(self.base_ptr() as *mut ShmHeader) };
        header_mut.active_mask &= !(1u32 << instance_id);
        Self::release_lock(lock);
        debug!(
            "Unregistered instance {} (active_mask={:#04b})",
            instance_id, header_mut.active_mask
        );
    }

    /// Compute free bytes in the ring buffer.
    /// Must be called while holding the lock.
    fn free_bytes_locked(&self) -> u32 {
        fn used_between(read: u32, write: u32, cap: u32) -> u32 {
            if write >= read {
                write - read
            } else {
                cap - read + write
            }
        }

        let header = unsafe { self.read_header() };
        let cap = header.rb_capacity;
        let w = header.rb_write;
        let active_mask = header.active_mask & 0b11;
        let mut used = 0;

        // If no instances have registered (unit tests and direct transport
        // usage), preserve the original two-reader behavior.
        if active_mask == 0 || active_mask & 0b01 != 0 {
            used = used.max(used_between(header.rb_read_a, w, cap));
        }
        if active_mask == 0 || active_mask & 0b10 != 0 {
            used = used.max(used_between(header.rb_read_b, w, cap));
        }

        // Keep one byte empty so rb_read == rb_write always means empty.
        cap.saturating_sub(used).saturating_sub(1)
    }

    /// Push an event envelope into the ring buffer.
    ///
    /// Frame layout: `[u32 len][bytes data]`
    /// Wrap sentinel: `[u32 0xFFFFFFFF]` written at tail when not enough space.
    pub fn push_event(&self, instance_id: u64, envelope: &EventEnvelope) -> Result<()> {
        let encoded = bincode::serialize(envelope).context("Failed to serialize event")?;
        let data_len = encoded.len() as u32;
        let frame_len = 4 + data_len; // u32 prefix + data
        let deadline = Instant::now() + PUSH_WAIT_TIMEOUT;

        loop {
            let lock = self
                .try_acquire_lock(instance_id)
                .ok_or_else(|| anyhow::anyhow!("Failed to acquire lock"))?;

            let header = unsafe { self.read_header() };
            let cap = header.rb_capacity;
            let w = header.rb_write;

            // Reject frames that can never fit. Reserve a few bytes for wrap
            // sentinel/empty-space disambiguation.
            if frame_len > cap.saturating_sub(4) {
                Self::release_lock(lock);
                anyhow::bail!("Event too large ({} bytes) for buffer ({} bytes)", frame_len, cap);
            }

            let tail_remaining = cap - w;
            let would_leave_tiny_tail = w + frame_len < cap && cap - (w + frame_len) < 4;
            let needs_wrap = w + frame_len > cap || would_leave_tiny_tail;
            let required = if needs_wrap {
                tail_remaining + frame_len
            } else {
                frame_len
            };
            let free = self.free_bytes_locked();

            if required > free {
                Self::release_lock(lock);
                if Instant::now() >= deadline {
                    anyhow::bail!(
                        "Ring buffer full for {}ms: need {} bytes, {} free",
                        PUSH_WAIT_TIMEOUT.as_millis(),
                        required,
                        free
                    );
                }
                std::thread::sleep(PUSH_RETRY_SLEEP);
                continue;
            }

            let rb = self.rb_ptr();
            let actual_write = if needs_wrap {
                if w != 0 {
                    if tail_remaining < 4 {
                        Self::release_lock(lock);
                        anyhow::bail!(
                            "Ring buffer tail too small for wrap sentinel (w={}, cap={})",
                            w,
                            cap
                        );
                    }
                    let sentinel = WRAP_SENTINEL.to_le_bytes();
                    unsafe {
                        std::ptr::copy_nonoverlapping(sentinel.as_ptr(), rb.add(w as usize), 4);
                    }
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

            // Update header atomically: rb_write first (data is visible), then seq.
            // The reader checks rb_write to know where data ends; seq is informational.
            let new_write = (actual_write + frame_len) % cap;
            let header_mut = unsafe { &mut *(self.base_ptr() as *mut ShmHeader) };
            header_mut.rb_write = new_write;

            // Memory fence: ensure write cursor is visible before incrementing seq
            std::sync::atomic::fence(Ordering::Release);

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
                new_write
            );
            return Ok(());
        }
    }

    /// Pop all pending events from the ring buffer for the given reader instance.
    ///
    /// Acquires the lock to prevent reading partially-written frames.
    pub fn pop_events(&self, instance_id: u64) -> Result<Vec<EventEnvelope>> {
        let lock = self
            .try_acquire_lock(instance_id)
            .ok_or_else(|| anyhow::anyhow!("Failed to acquire lock for pop"))?;

        let header = unsafe { self.read_header() };
        let cap = header.rb_capacity;

        let read_offset = if instance_id == 0 {
            header.rb_read_a
        } else {
            header.rb_read_b
        };

        let write_offset = header.rb_write;

        if read_offset == write_offset {
            Self::release_lock(lock);
            return Ok(Vec::new());
        }

        let mut events = Vec::new();
        let rb = self.rb_ptr();
        let mut cursor = read_offset;

        while cursor != write_offset {
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
                        rb.add(0),
                        len_buf.as_mut_ptr(),
                        4,
                    );
                }
                let len2 = u32::from_le_bytes(len_buf);
                if len2 == WRAP_SENTINEL {
                    break;
                }
                // Read frame at offset 0
                let data_len = len2 as usize;
                if 4 + data_len as u32 > cap {
                    break;
                }
                let mut data = vec![0u8; data_len];
                unsafe {
                    std::ptr::copy_nonoverlapping(rb.add(4), data.as_mut_ptr(), data_len);
                }
                if let Ok(env) = bincode::deserialize::<EventEnvelope>(&data) {
                    events.push(env);
                } else {
                    tracing::warn!("Failed to deserialize event after sentinel");
                }
                cursor = (4 + data_len as u32) % cap;
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

        // Update read cursor
        let header_mut = unsafe { &mut *(self.base_ptr() as *mut ShmHeader) };
        if instance_id == 0 {
            header_mut.rb_read_a = cursor;
        } else {
            header_mut.rb_read_b = cursor;
        }

        Self::release_lock(lock);

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
        self.unregister_instance();
        debug!("Dropping SHM '{}'", self.shm_name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::SyncEvent;
    use std::path::PathBuf;

    fn unique_name(suffix: &str) -> String {
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

        let second = shm.pop_events(1).unwrap();
        assert!(second.is_empty());
    }

    #[test]
    fn test_ring_buffer_wrap_around() {
        let name = unique_name("wrap_around");
        let shm = ShmTransport::create(&name, 4096).unwrap();

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

        let ev0 = shm.pop_events(0).unwrap();
        assert_eq!(ev0.len(), 1);

        let ev1 = shm.pop_events(1).unwrap();
        assert_eq!(ev1.len(), 1);

        assert!(shm.pop_events(0).unwrap().is_empty());
        assert!(shm.pop_events(1).unwrap().is_empty());
    }

    #[test]
    fn test_overflow_returns_error() {
        let name = unique_name("overflow");
        let shm = ShmTransport::create(&name, 1024).unwrap();

        let event = EventEnvelope {
            instance_id: 0,
            seq: 1,
            timestamp: 1700000000000,
            event: SyncEvent::FileContent {
                path: PathBuf::from("too_large.bin"),
                offset: 0,
                data: vec![0u8; 2048],
            },
        };

        assert!(shm.push_event(0, &event).is_err());
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
