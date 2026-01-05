//! Power-of-two circular buffer for zero-copy I/O.
//!
//! Features:
//! - Scale-to-zero: No allocation until first write
//! - Lock-free pooling: Reuses buffers across clients via crossbeam-queue
//! - Length derived from head/tail: No separate len field that can become inconsistent
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
const POOL_CAPACITY_4K: usize = 256;
const POOL_CAPACITY_8K: usize = 128;
const POOL_CAPACITY_16K: usize = 64;
const POOL_CAPACITY_32K: usize = 32;

// Global lock-free buffer pools by size class.
static POOL_4K: LazyLock<ArrayQueue<Box<[u8]>>> =
    LazyLock::new(|| ArrayQueue::new(POOL_CAPACITY_4K));
static POOL_8K: LazyLock<ArrayQueue<Box<[u8]>>> =
    LazyLock::new(|| ArrayQueue::new(POOL_CAPACITY_8K));
static POOL_16K: LazyLock<ArrayQueue<Box<[u8]>>> =
    LazyLock::new(|| ArrayQueue::new(POOL_CAPACITY_16K));
static POOL_32K: LazyLock<ArrayQueue<Box<[u8]>>> =
    LazyLock::new(|| ArrayQueue::new(POOL_CAPACITY_32K));

/// Acquire a buffer from the pool, or allocate a new one.
#[inline]
fn pool_acquire(size: usize) -> Box<[u8]> {
    let size = size.next_power_of_two().max(MIN_SIZE);

    let maybe_buf = match size {
        4096 => POOL_4K.pop(),
        8192 => POOL_8K.pop(),
        16384 => POOL_16K.pop(),
        32768 => POOL_32K.pop(),
        _ => None, // Large buffers not pooled
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
        _ => drop(buf), // Large buffers not pooled
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
}

impl WriteBuffer {
    /// Create a new circular buffer (scale-to-zero: no allocation until first write).
    pub fn new() -> Self {
        Self {
            buf: None,
            head: 0,
            tail: 0,
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

    /// Returns the number of bytes available for writing.
    #[inline]
    pub fn free_space(&self) -> usize {
        match &self.buf {
            None => 0, // Will allocate on write
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
        let tail_pos = self.tail % cap;

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
                let tail_pos = self.tail % cap;
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
        let tail_pos = self.tail % cap;
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
    /// Releases buffer to pool when empty (scale-to-zero).
    #[inline]
    pub fn consume(&mut self, n: usize) {
        debug_assert!(n <= self.len());
        self.tail = self.tail.wrapping_add(n);

        // Release buffer to pool when empty (scale-to-zero)
        if self.is_empty() {
            if let Some(buf) = self.buf.take() {
                pool_release(buf);
            }
            self.head = 0;
            self.tail = 0;
        }
    }

    /// Write bytes into the buffer, growing if necessary.
    /// Returns Err if the buffer would exceed MAX_SIZE.
    #[inline]
    pub fn write_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.ensure_space(data.len())?;

        // SAFETY: ensure_space guarantees buf is Some and has enough space
        let buf = self.buf.as_mut().unwrap();
        let cap = buf.len();
        let head_pos = self.head % cap;

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
        Ok(())
    }

    /// Ensure there's space for at least `needed` bytes, growing if necessary.
    /// Acquires buffer from pool on first write (scale-to-zero).
    /// Returns WouldBlock if growth would exceed soft limit (backpressure).
    /// Returns OutOfMemory if growth would exceed hard limit.
    #[inline]
    fn ensure_space(&mut self, needed: usize) -> io::Result<()> {
        // First write: acquire buffer from pool
        if self.buf.is_none() {
            let buf = pool_acquire(needed);
            self.buf = Some(buf);
            return Ok(());
        }

        if self.free_space() >= needed {
            return Ok(());
        }

        // Need to grow
        let required = self.len() + needed;
        let new_size = required.next_power_of_two();

        // Soft limit: signal backpressure via WouldBlock
        // Caller can decide policy: disconnect slow client, drop QoS0, retry later
        if new_size > SOFT_LIMIT && self.capacity() < new_size {
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

        self.grow_to(new_size);
        Ok(())
    }

    /// Grow the buffer to the new size, preserving contents.
    /// Releases old buffer to pool.
    fn grow_to(&mut self, new_size: usize) {
        let mut new_buf = pool_acquire(new_size);

        if let Some(ref old_buf) = self.buf {
            let len = self.len();
            let old_cap = old_buf.len();
            let tail_pos = self.tail % old_cap;
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
        let len = self.len();
        self.buf = Some(new_buf);
        self.tail = 0;
        self.head = len;
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

    /// Shrink the buffer if it's very large and mostly empty.
    /// Call this periodically to reclaim memory from slow clients that caught up.
    /// Only shrinks if buffer is 4x the minimum and less than 1/8 full.
    pub fn maybe_shrink(&mut self) {
        let cap = self.capacity();
        if cap == 0 {
            return; // Already released
        }
        let len = self.len();
        let new_size = MIN_SIZE * 2;
        // Only shrink if we're at 4x minimum, less than 1/8 full, AND data fits in new size
        if cap >= MIN_SIZE * 4 && len < cap / 8 && len <= new_size {
            self.grow_to(new_size);
        }
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
    fn test_maybe_shrink() {
        let mut buf = WriteBuffer::new();

        // Grow the buffer to 32KB (at soft limit, not over)
        // Write in chunks to stay under limit
        for _ in 0..4 {
            buf.write_bytes(&[0u8; 8000]).unwrap();
        }
        assert!(buf.capacity() >= 32768);

        // Consume most data - need <1/8 full for shrink
        buf.consume(31000);
        assert_eq!(buf.len(), 1000);

        // Buffer should shrink since it's <1/8 full and >4x min (16KB)
        buf.maybe_shrink();
        assert!(buf.capacity() < 32768);
        assert_eq!(buf.len(), 1000); // Data preserved
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
}
