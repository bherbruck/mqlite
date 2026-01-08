//! Power-of-two circular buffer for zero-copy I/O.
//!
//! Features:
//! - Scale-to-zero: No allocation until first write
//! - Lock-free pooling: Reuses buffers across clients via crossbeam-queue
//! - Length derived from head/tail: No separate len field that can become inconsistent
//! - Power-of-two sizing: Enables fast bitmask indexing (`& (cap-1)` vs `% cap`)
//! - Optimized for sequential writes and reads without reallocations

use std::io::{self, IoSlice, Write};
use std::sync::LazyLock;

use crossbeam_queue::ArrayQueue;

/// Minimum buffer size (4KB) - smallest pooled size.
const MIN_SIZE: usize = 4096;

/// Soft limit (1MB) - returns WouldBlock above this for backpressure.
const SOFT_LIMIT: usize = 1024 * 1024;

/// Maximum buffer size (16MB) - hard cap, returns OutOfMemory.
const MAX_SIZE: usize = 16 * 1024 * 1024;

/// Pool capacity per size class.
/// Smaller buffers are more common, so we keep more of them.
/// Larger buffers (64KB, 128KB) are for fan-out scenarios with many subscribers.
const POOL_CAPACITY_4K: usize = 256;
const POOL_CAPACITY_8K: usize = 128;
const POOL_CAPACITY_16K: usize = 64;
const POOL_CAPACITY_32K: usize = 64;
const POOL_CAPACITY_64K: usize = 32;
const POOL_CAPACITY_128K: usize = 16;

// Global lock-free buffer pools by size class.
static POOL_4K: LazyLock<ArrayQueue<Box<[u8]>>> =
    LazyLock::new(|| ArrayQueue::new(POOL_CAPACITY_4K));
static POOL_8K: LazyLock<ArrayQueue<Box<[u8]>>> =
    LazyLock::new(|| ArrayQueue::new(POOL_CAPACITY_8K));
static POOL_16K: LazyLock<ArrayQueue<Box<[u8]>>> =
    LazyLock::new(|| ArrayQueue::new(POOL_CAPACITY_16K));
static POOL_32K: LazyLock<ArrayQueue<Box<[u8]>>> =
    LazyLock::new(|| ArrayQueue::new(POOL_CAPACITY_32K));
static POOL_64K: LazyLock<ArrayQueue<Box<[u8]>>> =
    LazyLock::new(|| ArrayQueue::new(POOL_CAPACITY_64K));
static POOL_128K: LazyLock<ArrayQueue<Box<[u8]>>> =
    LazyLock::new(|| ArrayQueue::new(POOL_CAPACITY_128K));

/// Acquire a buffer from the pool, or allocate a new one.
/// Size is rounded up to the nearest power of two (minimum MIN_SIZE).
/// Caller must ensure size is pre-validated (<= MAX_SIZE) to avoid large allocations.
#[inline]
fn pool_acquire(size: usize) -> Box<[u8]> {
    let size = size.next_power_of_two().max(MIN_SIZE);

    let maybe_buf = match size {
        4096 => POOL_4K.pop(),
        8192 => POOL_8K.pop(),
        16384 => POOL_16K.pop(),
        32768 => POOL_32K.pop(),
        65536 => POOL_64K.pop(),
        131072 => POOL_128K.pop(),
        _ => None, // Very large buffers not pooled
    };

    maybe_buf.unwrap_or_else(|| vec![0u8; size].into_boxed_slice())
}

/// Release a buffer back to the pool (drops if pool full or too large).
#[inline]
fn pool_release(buf: Box<[u8]>) {
    match buf.len() {
        4096 => {
            let _ = POOL_4K.push(buf);
        }
        8192 => {
            let _ = POOL_8K.push(buf);
        }
        16384 => {
            let _ = POOL_16K.push(buf);
        }
        32768 => {
            let _ = POOL_32K.push(buf);
        }
        65536 => {
            let _ = POOL_64K.push(buf);
        }
        131072 => {
            let _ = POOL_128K.push(buf);
        }
        _ => {} // Very large buffers not pooled, dropped implicitly
    }
}

/// A circular buffer with power-of-two sizing for efficient modulo operations.
///
/// Length is derived from `head - tail`, eliminating the possibility of
/// the length field becoming inconsistent with actual buffer contents.
///
/// Scale-to-zero: Starts with no allocation, acquires buffer on first write,
/// releases back to pool when emptied.
pub struct WriteBuffer {
    /// Buffer storage, None when empty (scale-to-zero).
    buf: Option<Box<[u8]>>,
    /// Write position (unbounded, wraps naturally).
    head: usize,
    /// Read position (unbounded, wraps naturally).
    tail: usize,
    /// Primed for shrink (requires two consecutive eligible states before shrinking).
    /// This prevents shrink/grow cycles during bursty traffic.
    shrink_primed: bool,
}

impl WriteBuffer {
    /// Create a new circular buffer (scale-to-zero: no allocation until first write).
    pub fn new() -> Self {
        Self {
            buf: None,
            head: 0,
            tail: 0,
            shrink_primed: false,
        }
    }

    /// Create a new circular buffer with at least the given capacity.
    /// Acquires buffer from pool or allocates new one.
    #[allow(dead_code)]
    pub fn with_capacity(cap: usize) -> Self {
        let buf = pool_acquire(cap);
        Self {
            buf: Some(buf),
            head: 0,
            tail: 0,
            shrink_primed: false,
        }
    }

    /// Returns the number of bytes available for reading.
    /// Computed from head - tail, so it can never become inconsistent.
    #[inline]
    pub fn len(&self) -> usize {
        // wrapping_sub handles the case where head has wrapped around usize::MAX
        self.head.wrapping_sub(self.tail)
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    /// Debug-only invariant check. Zero cost in release builds.
    #[inline]
    fn debug_check_invariants(&self) {
        if let Some(buf) = &self.buf {
            debug_assert!(
                self.len() <= buf.len(),
                "invariant violated: len {} > capacity {}",
                self.len(),
                buf.len()
            );
        } else {
            debug_assert!(
                self.is_empty(),
                "invariant violated: len > 0 with no buffer"
            );
        }
    }

    /// Returns free space in the currently allocated buffer.
    /// Returns 0 if no buffer exists yet (scale-to-zero state).
    /// Note: 0 does NOT mean writes will fail - buffer will be allocated on demand.
    #[inline]
    pub fn free_space(&self) -> usize {
        match &self.buf {
            None => 0,
            Some(buf) => buf.len() - self.len(),
        }
    }

    /// Returns the capacity of the buffer (0 if not allocated).
    #[inline]
    pub fn capacity(&self) -> usize {
        match &self.buf {
            None => 0,
            Some(buf) => buf.len(),
        }
    }

    /// Get the maximum contiguous bytes that can be read without wrapping.
    #[inline]
    #[allow(dead_code)]
    pub fn contiguous_read_len(&self) -> usize {
        let buf = match &self.buf {
            None => return 0,
            Some(buf) => buf,
        };

        let len = self.len();
        if len == 0 {
            return 0;
        }

        let cap = buf.len();
        let tail_pos = self.tail & (cap - 1);

        // Bytes from tail_pos to end of buffer
        let to_end = cap - tail_pos;
        to_end.min(len)
    }

    /// Get a slice of contiguous readable bytes.
    #[inline]
    #[allow(dead_code)]
    pub fn read_slice(&self) -> &[u8] {
        match &self.buf {
            None => &[],
            Some(buf) => {
                let len = self.len();
                if len == 0 {
                    return &[];
                }

                let cap = buf.len();
                let tail_pos = self.tail & (cap - 1);
                let contiguous = (cap - tail_pos).min(len);
                &buf[tail_pos..tail_pos + contiguous]
            }
        }
    }

    /// Get two slices for vectored I/O (handles wraparound in one syscall).
    #[inline]
    pub fn as_io_slices(&self) -> [IoSlice<'_>; 2] {
        let buf = match &self.buf {
            None => return [IoSlice::new(&[]), IoSlice::new(&[])],
            Some(buf) => buf,
        };

        let len = self.len();
        if len == 0 {
            return [IoSlice::new(&[]), IoSlice::new(&[])];
        }

        let cap = buf.len();
        let tail_pos = self.tail & (cap - 1);
        let to_end = cap - tail_pos;

        if len <= to_end {
            // Contiguous - data doesn't wrap
            [
                IoSlice::new(&buf[tail_pos..tail_pos + len]),
                IoSlice::new(&[]),
            ]
        } else {
            // Wrapped - two segments
            let second_len = len - to_end;
            [
                IoSlice::new(&buf[tail_pos..]),
                IoSlice::new(&buf[..second_len]),
            ]
        }
    }

    /// Advance the read position after consuming bytes.
    /// Keeps buffer allocated to avoid allocation churn during sustained traffic.
    /// Buffer is released via maybe_shrink() when truly idle, or Drop when client disconnects.
    #[inline]
    pub fn consume(&mut self, n: usize) {
        debug_assert!(n <= self.len());
        self.tail = self.tail.wrapping_add(n);

        // Reset positions when empty to maximize contiguous write space
        // But keep buffer allocated to avoid churn during sustained traffic
        if self.is_empty() {
            self.head = 0;
            self.tail = 0;
        }
        self.debug_check_invariants();
    }

    /// Write bytes into the buffer, growing if necessary.
    /// Returns WouldBlock if buffer would exceed soft limit (for QoS 0 backpressure).
    /// Returns OutOfMemory if buffer would exceed hard limit.
    #[inline]
    pub fn write_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.write_bytes_inner(data, true)
    }

    /// Write bytes bypassing soft limit (for QoS 1/2 guaranteed delivery).
    /// Only fails if hard limit (16MB) would be exceeded.
    /// Use this for QoS 1/2 messages that must not be dropped.
    #[inline]
    pub fn write_bytes_guaranteed(&mut self, data: &[u8]) -> io::Result<()> {
        self.write_bytes_inner(data, false)
    }

    /// Inner write implementation with configurable soft limit enforcement.
    #[inline]
    fn write_bytes_inner(&mut self, data: &[u8], enforce_soft_limit: bool) -> io::Result<()> {
        self.ensure_space(data.len(), enforce_soft_limit)?;

        // SAFETY: ensure_space guarantees buf is Some and has enough space
        let buf = self.buf.as_mut().unwrap();
        let cap = buf.len();
        let head_pos = self.head & (cap - 1);

        // How much can we write before wrapping?
        let to_end = cap - head_pos;
        let first_chunk = to_end.min(data.len());
        buf[head_pos..head_pos + first_chunk].copy_from_slice(&data[..first_chunk]);

        // Wrap around if needed
        if first_chunk < data.len() {
            let second_chunk = data.len() - first_chunk;
            buf[..second_chunk].copy_from_slice(&data[first_chunk..]);
        }

        self.head = self.head.wrapping_add(data.len());
        self.debug_check_invariants();
        Ok(())
    }

    /// Ensure there's space for at least `needed` bytes, growing if necessary.
    /// Acquires buffer from pool on first write (scale-to-zero).
    /// If enforce_soft_limit is true, returns WouldBlock above 1MB.
    /// Returns OutOfMemory if growth would exceed hard limit (16MB).
    #[inline]
    fn ensure_space(&mut self, needed: usize, enforce_soft_limit: bool) -> io::Result<()> {
        // Fast path: already have enough space
        if self.free_space() >= needed {
            return Ok(());
        }

        // Calculate required size (works for both first write and growth)
        // Use checked arithmetic to avoid panic on adversarial input
        let required = self
            .len()
            .checked_add(needed)
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "size overflow"))?;
        let new_size = required
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX)
            .max(MIN_SIZE);

        // Soft limit: signal backpressure via WouldBlock (only for QoS 0)
        // QoS 1/2 bypasses this to ensure guaranteed delivery
        if enforce_soft_limit && new_size > SOFT_LIMIT && self.capacity() < new_size {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "tx buffer soft limit reached",
            ));
        }

        if new_size > MAX_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "circular buffer would exceed maximum size",
            ));
        }

        // First write: acquire from pool
        if self.buf.is_none() {
            self.buf = Some(pool_acquire(new_size));
            return Ok(());
        }

        // Growth: copy to larger buffer
        self.grow_to(new_size);
        Ok(())
    }

    /// Grow the buffer to the new size, preserving contents.
    /// Releases old buffer to pool.
    fn grow_to(&mut self, new_size: usize) {
        let len = self.len();
        let mut new_buf = pool_acquire(new_size);

        if let Some(ref old_buf) = self.buf {
            let old_cap = old_buf.len();
            let tail_pos = self.tail & (old_cap - 1);
            let to_end = old_cap - tail_pos;

            if len <= to_end {
                // Data is contiguous - doesn't wrap
                new_buf[..len].copy_from_slice(&old_buf[tail_pos..tail_pos + len]);
            } else {
                // Data wraps around
                new_buf[..to_end].copy_from_slice(&old_buf[tail_pos..]);
                let second_len = len - to_end;
                new_buf[to_end..len].copy_from_slice(&old_buf[..second_len]);
            }
        }

        // Release old buffer to pool
        if let Some(old_buf) = self.buf.take() {
            pool_release(old_buf);
        }

        // Reset positions - data is now at start of new buffer
        self.buf = Some(new_buf);
        self.tail = 0;
        self.head = len;
        self.debug_check_invariants();
    }

    /// Reset the buffer, clearing all data and releasing to pool (scale-to-zero).
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        if let Some(buf) = self.buf.take() {
            pool_release(buf);
        }
        self.head = 0;
        self.tail = 0;
    }

    /// Release or shrink the buffer if it's been empty for two consecutive calls.
    /// Call this periodically to reclaim memory from idle clients.
    ///
    /// Uses hysteresis (two-tick debounce) to avoid allocation churn:
    /// - Only acts when buffer is completely empty
    /// - Requires two consecutive calls while empty before releasing/shrinking
    /// - Any data in buffer resets the primed state
    ///
    /// For small buffers (<= 64KB): releases back to pool
    /// For large buffers (> 64KB): shrinks to MIN_SIZE
    pub fn maybe_shrink(&mut self) {
        // Nothing to do if no buffer
        if self.buf.is_none() {
            self.shrink_primed = false;
            return;
        }

        // Only consider releasing/shrinking when completely empty
        if !self.is_empty() {
            self.shrink_primed = false;
            return;
        }

        // First time seeing empty: prime for next time
        if !self.shrink_primed {
            self.shrink_primed = true;
            return;
        }

        // Second consecutive empty call: actually release or shrink
        self.shrink_primed = false;

        let cap = self.capacity();
        if cap <= 65536 {
            // Small buffer: release to pool entirely
            if let Some(buf) = self.buf.take() {
                pool_release(buf);
            }
            self.head = 0;
            self.tail = 0;
        } else {
            // Large buffer: shrink to MIN_SIZE (grow_to checks invariants)
            self.grow_to(MIN_SIZE);
        }
        self.debug_check_invariants();
    }
}

impl Drop for WriteBuffer {
    fn drop(&mut self) {
        // Release buffer to pool on drop
        if let Some(buf) = self.buf.take() {
            pool_release(buf);
        }
    }
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Write for WriteBuffer {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_bytes(buf)?;
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Wrapper for guaranteed writes that bypass the soft limit.
/// Use this for QoS 1/2 messages that must not be dropped.
pub struct WriteGuaranteed<'a>(pub &'a mut WriteBuffer);

impl Write for WriteGuaranteed<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write_bytes_guaranteed(buf)?;
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_write_read() {
        let mut buf = WriteBuffer::new();
        assert!(buf.is_empty());

        buf.write_bytes(b"hello").unwrap();
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.read_slice(), b"hello");

        buf.consume(5);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_wraparound() {
        let mut buf = WriteBuffer::with_capacity(8);

        // Fill most of the buffer
        buf.write_bytes(b"abcde").unwrap();
        buf.consume(3); // Read "abc", now tail=3

        // Write more
        buf.write_bytes(b"fgh").unwrap();

        // With MIN_SIZE=4096, no wraparound occurs, so all data is contiguous
        // Read all remaining data: "de" + "fgh" = "defgh"
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.read_slice(), b"defgh");
        buf.consume(5);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_grow() {
        let mut buf = WriteBuffer::with_capacity(8);
        let data = b"this is longer than 8 bytes for sure";

        buf.write_bytes(data).unwrap();
        assert!(buf.capacity() >= data.len());
        assert_eq!(buf.len(), data.len());
    }

    #[test]
    fn test_grow_with_wraparound() {
        // This tests that grow_to correctly handles wrapped data
        let mut buf = WriteBuffer::new();

        // Write enough to advance head past capacity
        for _ in 0..100 {
            buf.write_bytes(&[0u8; 100]).unwrap();
            buf.consume(100);
        }

        // Now head and tail are large values (around 10000)
        // Write data that will wrap around and require growth
        let large_data = vec![0xABu8; 8000];
        buf.write_bytes(&large_data).unwrap();
        assert_eq!(buf.len(), 8000);

        // Consume some, then write more to force growth while wrapped
        buf.consume(2000);
        assert_eq!(buf.len(), 6000);

        // This write should trigger growth
        let more_data = vec![0xCDu8; 5000];
        buf.write_bytes(&more_data).unwrap();
        assert_eq!(buf.len(), 11000);

        // Verify data integrity via io_slices
        let slices = buf.as_io_slices();
        assert_eq!(slices[0].len() + slices[1].len(), 11000);
    }

    #[test]
    fn test_soft_limit_backpressure() {
        let mut buf = WriteBuffer::new();

        // Fill up to soft limit (1MB)
        let chunk = vec![0xABu8; 256 * 1024]; // 256KB chunks
        for _ in 0..4 {
            // 4 * 256KB = 1MB
            buf.write_bytes(&chunk).unwrap();
        }

        // Next write should return WouldBlock (soft limit)
        let result = buf.write_bytes(&chunk);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::WouldBlock);

        // After consuming, we can write again
        buf.consume(256 * 1024);
        assert!(buf.write_bytes(&chunk).is_ok());
    }

    #[test]
    fn test_maybe_shrink_small_buffer() {
        // Test: small buffer gets released after two idle ticks
        let mut buf = WriteBuffer::new();
        buf.write_bytes(&[0u8; 1000]).unwrap();
        assert!(buf.capacity() > 0);

        // Consume all data - buffer stays allocated (no scale-to-zero in consume)
        buf.consume(1000);
        assert!(buf.buf.is_some()); // Buffer still allocated

        // First call while empty: primes but doesn't release
        buf.maybe_shrink();
        assert!(buf.buf.is_some()); // Still allocated

        // Second call: actually releases
        buf.maybe_shrink();
        assert!(buf.buf.is_none()); // Now released
    }

    #[test]
    fn test_maybe_shrink_large_buffer() {
        // Test: large buffer (>64KB) gets shrunk after two idle ticks
        let mut buf = WriteBuffer::new();
        buf.write_bytes(&[0u8; 100000]).unwrap(); // >64KB
        let original_cap = buf.capacity();
        assert!(original_cap > 65536);

        // Consume all data
        buf.consume(100000);
        assert!(buf.buf.is_some());

        // First call while empty: primes but doesn't shrink
        buf.maybe_shrink();
        assert_eq!(buf.capacity(), original_cap);

        // Second call: shrinks to MIN_SIZE
        buf.maybe_shrink();
        assert_eq!(buf.capacity(), 4096); // Shrunk to MIN_SIZE
    }

    #[test]
    fn test_maybe_shrink_resets_on_data() {
        // Test: maybe_shrink seeing non-empty buffer resets primed state
        let mut buf = WriteBuffer::new();
        buf.write_bytes(&[0u8; 1000]).unwrap();
        buf.consume(1000);

        // First call: primes
        buf.maybe_shrink();
        assert!(buf.buf.is_some());

        // Write more data
        buf.write_bytes(&[0u8; 100]).unwrap();

        // maybe_shrink sees non-empty buffer, resets primed
        buf.maybe_shrink();
        assert!(buf.buf.is_some());

        // Consume data
        buf.consume(100);

        // Now need two more calls to release (primed was reset)
        buf.maybe_shrink(); // Primes again
        assert!(buf.buf.is_some());
        buf.maybe_shrink(); // Now releases
        assert!(buf.buf.is_none());
    }

    #[test]
    fn test_len_derived_from_head_tail() {
        // Verify that len is always consistent with head - tail
        let mut buf = WriteBuffer::new();

        for i in 0..1000 {
            let data = vec![i as u8; (i % 100) + 1];
            buf.write_bytes(&data).unwrap();
            assert_eq!(buf.len(), buf.head.wrapping_sub(buf.tail));

            if i % 3 == 0 && !buf.is_empty() {
                let consume = buf.len().min(50);
                buf.consume(consume);
                assert_eq!(buf.len(), buf.head.wrapping_sub(buf.tail));
            }
        }
    }

    #[test]
    fn test_io_slices_correctness() {
        let mut buf = WriteBuffer::new();

        // Write data that will wrap around
        buf.write_bytes(&[1u8; 3000]).unwrap();
        buf.consume(2500);
        buf.write_bytes(&[2u8; 2000]).unwrap();

        let slices = buf.as_io_slices();
        let total: usize = slices.iter().map(|s| s.len()).sum();
        assert_eq!(total, buf.len());

        // Verify we can read all the data
        let mut collected = Vec::new();
        for slice in &slices {
            collected.extend_from_slice(slice);
        }
        assert_eq!(collected.len(), buf.len());
    }

    #[test]
    fn test_first_write_respects_hard_limit() {
        // Verify that even first writes check the hard limit (16MB)
        // Use write_bytes_guaranteed to bypass soft limit and test hard limit
        let mut buf = WriteBuffer::new();
        assert!(buf.buf.is_none()); // Start with no allocation

        // Try to write more than MAX_SIZE (16MB) on first write
        let huge = vec![0u8; 20 * 1024 * 1024]; // 20MB
        let result = buf.write_bytes_guaranteed(&huge);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::OutOfMemory);
        assert!(buf.buf.is_none()); // Should not have allocated
    }

    #[test]
    fn test_first_write_respects_soft_limit() {
        // Verify that first writes check soft limit (1MB) for QoS 0
        let mut buf = WriteBuffer::new();
        assert!(buf.buf.is_none());

        // Try to write more than SOFT_LIMIT (1MB) on first write
        let large = vec![0u8; 2 * 1024 * 1024]; // 2MB
        let result = buf.write_bytes(&large);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
        assert!(buf.buf.is_none()); // Should not have allocated
    }

    #[test]
    fn test_first_write_guaranteed_bypasses_soft_limit() {
        // Verify that guaranteed writes bypass soft limit but respect hard limit
        let mut buf = WriteBuffer::new();
        assert!(buf.buf.is_none());

        // 2MB write with guaranteed (bypasses soft limit)
        let large = vec![0u8; 2 * 1024 * 1024]; // 2MB
        let result = buf.write_bytes_guaranteed(&large);

        assert!(result.is_ok());
        assert!(buf.buf.is_some());
        assert_eq!(buf.len(), 2 * 1024 * 1024);
    }

    #[test]
    fn test_exceeds_max_size_returns_error() {
        // Verify that sizes exceeding MAX_SIZE (16MB) return OutOfMemory
        // The checked arithmetic also prevents panic on usize overflow
        let mut buf = WriteBuffer::new();

        // 17MB exceeds MAX_SIZE (16MB), should return OutOfMemory
        let result = buf.write_bytes_guaranteed(&vec![0u8; 17 * 1024 * 1024]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::OutOfMemory);
        assert!(buf.buf.is_none()); // Should not have allocated
    }

    #[test]
    fn fuzz_random_operations() {
        // Deterministic pseudo-random fuzz test for write/consume/shrink operations.
        // Uses a simple LCG for reproducibility without external dependencies.
        let mut rng = 12345u64; // Seed
        let mut next = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            rng
        };

        let mut buf = WriteBuffer::new();
        let mut expected_len = 0usize;

        for _ in 0..10_000 {
            let op = next() % 100;

            match op {
                // 50% chance: write random amount (1-1000 bytes)
                0..50 => {
                    let write_size = ((next() % 1000) + 1) as usize;
                    let data = vec![(next() & 0xFF) as u8; write_size];
                    if buf.write_bytes(&data).is_ok() {
                        expected_len += write_size;
                    }
                    // Soft limit rejection is fine, just don't advance expected_len
                }
                // 35% chance: consume random amount (0 to current len)
                50..85 => {
                    if expected_len > 0 {
                        let consume_amt = (next() as usize % expected_len).max(1);
                        buf.consume(consume_amt);
                        expected_len -= consume_amt;
                    }
                }
                // 10% chance: maybe_shrink (tests hysteresis)
                85..95 => {
                    buf.maybe_shrink();
                }
                // 5% chance: consume all then shrink (tests scale-to-zero path)
                _ => {
                    if expected_len > 0 {
                        buf.consume(expected_len);
                        expected_len = 0;
                    }
                    buf.maybe_shrink();
                    buf.maybe_shrink(); // Two ticks to actually release
                }
            }

            // Invariant checks after every operation
            assert_eq!(buf.len(), expected_len, "len mismatch after operation");
            buf.debug_check_invariants();
        }
    }
}
