//! Cross-thread client write handle.
//!
//! This module implements cross-thread writes:
//! - Any thread can write to any client's buffer (mutex-protected)
//! - epoll_ctl is thread-safe, so we update poll interest directly
//! - No channels needed for publish delivery

use std::cell::RefCell;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU8, Ordering};

use mio::Token;
use parking_lot::Mutex;

use mqlite_core::packet::{self, Packet, QoS};
use crate::publish_encoder::PublishEncoder;
use crate::write_buffer::{WriteBuffer, WriteGuaranteed};

// Thread-local buffer for packet encoding (avoids allocation per packet).
thread_local! {
    static ENCODE_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(256));
}

/// Shared write handle for cross-thread client access.
///
/// This allows any thread to write to a client's output buffer
/// and wake the owning worker via epoll_ctl.
pub struct ClientWriteHandle {
    /// Mutex-protected write buffer.
    write_buf: Mutex<WriteBuffer>,
    /// Atomic flag to avoid redundant epoll_ctl calls.
    ready_for_writing: AtomicBool,
    /// Count of consecutive empty flushes. Used to detect truly idle clients
    /// and clear EPOLLOUT only after many cycles, avoiding race conditions under load.
    idle_flush_count: AtomicU16,
    /// Atomic packet ID counter for cross-thread QoS allocation.
    next_packet_id: AtomicU16,
    /// MQTT protocol version (4=3.1.1, 5=5.0). Set after CONNECT.
    protocol_version: AtomicU8,
    /// The epoll fd (owned by the worker's Poll).
    epoll_fd: RawFd,
    /// The client's socket fd.
    socket_fd: RawFd,
    /// The client's token for epoll events.
    token: Token,
    /// The worker ID that owns this client.
    worker_id: usize,
}

impl ClientWriteHandle {
    /// Create a new write handle.
    pub fn new(worker_id: usize, epoll_fd: RawFd, socket_fd: RawFd, token: Token) -> Self {
        Self {
            write_buf: Mutex::new(WriteBuffer::new()),
            ready_for_writing: AtomicBool::new(false),
            idle_flush_count: AtomicU16::new(0),
            next_packet_id: AtomicU16::new(1),
            protocol_version: AtomicU8::new(4), // Default to 3.1.1
            epoll_fd,
            socket_fd,
            token,
            worker_id,
        }
    }

    /// Set the protocol version (called after CONNECT is processed).
    #[inline]
    pub fn set_protocol_version(&self, version: u8) {
        self.protocol_version.store(version, Ordering::Relaxed);
    }

    /// Get the protocol version.
    #[inline]
    #[allow(dead_code)]
    pub fn protocol_version(&self) -> u8 {
        self.protocol_version.load(Ordering::Relaxed)
    }

    /// Check if this client uses MQTT v5.
    #[inline]
    pub fn is_v5(&self) -> bool {
        self.protocol_version.load(Ordering::Relaxed) == 5
    }

    /// Allocate the next packet ID atomically. Can be called from any thread.
    /// Returns values 1-65535 (0 is invalid per MQTT spec).
    #[inline]
    pub fn allocate_packet_id(&self) -> u16 {
        // Use fetch_add for speed - only loops on rare wrap from 65535->0
        loop {
            let id = self.next_packet_id.fetch_add(1, Ordering::Relaxed);
            if id != 0 {
                return id;
            }
            // Got 0 (invalid), loop to get 1
        }
    }

    /// Get the worker ID.
    #[inline]
    pub fn worker_id(&self) -> usize {
        self.worker_id
    }

    /// Queue a packet for sending. Can be called from any thread.
    /// Returns WouldBlock if client's TX buffer is full (slow client).
    /// Use `queue_control_packet` for protocol control packets that must not be dropped.
    #[inline]
    pub fn queue_packet(&self, packet: &Packet) -> std::io::Result<()> {
        ENCODE_BUF.with(|buf| {
            let mut tmp = buf.borrow_mut();
            tmp.clear();
            packet::encode_packet(packet, &mut tmp);
            let mut write_buf = self.write_buf.lock();
            write_buf.write_bytes(&tmp)?;
            drop(write_buf);
            self.set_ready_for_writing(true);
            Ok(())
        })
    }

    /// Queue a control packet for sending, bypassing soft limit.
    /// Use this for protocol control packets (CONNACK, PINGRESP, PUBACK, PUBREC,
    /// PUBREL, PUBCOMP, SUBACK, UNSUBACK) that must not be dropped.
    /// Only fails if hard limit (16MB) would be exceeded.
    #[inline]
    pub fn queue_control_packet(&self, packet: &Packet) -> std::io::Result<()> {
        ENCODE_BUF.with(|buf| {
            let mut tmp = buf.borrow_mut();
            tmp.clear();
            packet::encode_packet(packet, &mut tmp);
            let mut write_buf = self.write_buf.lock();
            write_buf.write_bytes_guaranteed(&tmp)?;
            drop(write_buf);
            self.set_ready_for_writing(true);
            Ok(())
        })
    }

    /// Queue a publish packet using the copy factory. Can be called from any thread.
    /// Returns WouldBlock if client's TX buffer is full (slow client).
    #[inline]
    pub fn queue_publish(
        &self,
        factory: &mut PublishEncoder,
        effective_qos: QoS,
        packet_id: Option<u16>,
        retain: bool,
    ) -> std::io::Result<()> {
        self.queue_publish_with_sub_id(factory, effective_qos, packet_id, retain, None)
    }

    /// Queue a publish packet with optional subscription identifier.
    /// Used for MQTT v5 when forwarding to subscribers with subscription_id.
    ///
    /// For QoS 0: Uses soft limit (1MB) - drops if buffer full (acceptable for at-most-once).
    /// For QoS 1/2: Bypasses soft limit (up to 16MB) - guaranteed delivery per MQTT spec.
    #[inline]
    pub fn queue_publish_with_sub_id(
        &self,
        factory: &mut PublishEncoder,
        effective_qos: QoS,
        packet_id: Option<u16>,
        retain: bool,
        subscription_id: Option<u32>,
    ) -> std::io::Result<()> {
        let is_v5 = self.is_v5();
        let mut buf = self.write_buf.lock();

        // QoS 0: enforce soft limit (can drop messages)
        // QoS 1/2: bypass soft limit for guaranteed delivery
        match effective_qos {
            QoS::AtMostOnce => {
                factory.write_to_with_sub_id(
                    &mut *buf,
                    effective_qos,
                    packet_id,
                    retain,
                    is_v5,
                    subscription_id,
                )?;
            }
            QoS::AtLeastOnce | QoS::ExactlyOnce => {
                factory.write_to_with_sub_id(
                    &mut WriteGuaranteed(&mut buf),
                    effective_qos,
                    packet_id,
                    retain,
                    is_v5,
                    subscription_id,
                )?;
            }
        }

        drop(buf);
        self.set_ready_for_writing(true);
        Ok(())
    }

    /// Flush the write buffer to the socket. Called by owning worker only.
    /// Returns Ok(true) if all data was written, Ok(false) if would block.
    pub fn flush(&self, socket: &mut impl std::io::Write) -> std::io::Result<bool> {
        // Threshold for clearing EPOLLOUT. Only clear after this many consecutive
        // empty flushes to avoid race conditions under active load. Under fan-out,
        // the counter resets every time data is queued, so we never clear.
        // When truly idle, ~1000 wakeups is acceptable before clearing.
        const IDLE_THRESHOLD: u16 = 1000;

        loop {
            // Lock and write directly from buffer - no temp copy needed
            // This is safe because only the owning worker calls flush()
            let mut buf = self.write_buf.lock();

            if buf.is_empty() {
                // Buffer is empty. Instead of clearing EPOLLOUT immediately (which
                // causes race conditions under load), count consecutive empty flushes.
                // Only clear EPOLLOUT for truly idle clients.
                let count = self.idle_flush_count.fetch_add(1, Ordering::Relaxed);

                // Periodically try to shrink idle buffers (requires 2 consecutive calls)
                // Call every ~500 cycles to allow shrink hysteresis to work
                if count.is_multiple_of(500) {
                    buf.maybe_shrink();
                }

                if count >= IDLE_THRESHOLD {
                    // Client has been idle for many cycles, safe to clear EPOLLOUT
                    self.idle_flush_count.store(0, Ordering::Relaxed);
                    drop(buf);
                    self.set_ready_for_writing(false);

                    // Handle rare race: if data was queued during our epoll_ctl,
                    // ensure EPOLLOUT is restored
                    if !self.write_buf.lock().is_empty() {
                        self.ready_for_writing.store(true, Ordering::Release);
                        self.update_epoll(true);
                    }
                }
                return Ok(true);
            }

            // Data exists, reset idle counter
            self.idle_flush_count.store(0, Ordering::Relaxed);

            // Use vectored I/O to handle wraparound in one syscall
            let slices = buf.as_io_slices();
            match socket.write_vectored(&slices) {
                Ok(0) => return Ok(false), // Connection closed
                Ok(n) => {
                    buf.consume(n);
                    buf.maybe_shrink(); // Reclaim memory from recovered slow clients
                                        // Continue loop to write more if buffer not empty
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(false);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Check if there's data waiting to be written.
    #[inline]
    #[allow(dead_code)]
    pub fn has_pending_writes(&self) -> bool {
        !self.write_buf.lock().is_empty()
    }

    /// Update epoll interest. Called after writes or flushes.
    ///
    /// Uses level-triggered epoll for writes. This means:
    /// - EPOLLOUT fires continuously while socket is writable AND we have data
    /// - No need to re-arm after each write (saves ~5% CPU in fan-out scenarios)
    /// - Safe to skip epoll_ctl if state hasn't changed
    #[inline]
    fn set_ready_for_writing(&self, val: bool) {
        // When setting ready (data queued), reset the idle counter.
        // This prevents clearing EPOLLOUT while client is actively receiving data.
        if val {
            self.idle_flush_count.store(0, Ordering::Relaxed);
        }

        // Fast path: skip if state hasn't changed (safe with level-triggered)
        if self.ready_for_writing.swap(val, Ordering::Release) == val {
            return;
        }

        self.update_epoll(val);
    }

    /// Unconditionally update epoll interest for EPOLLOUT.
    #[inline]
    fn update_epoll(&self, include_out: bool) {
        // Level-triggered for writes (no EPOLLET) - fires while socket is writable
        let events = if include_out {
            (libc::EPOLLIN | libc::EPOLLOUT) as u32
        } else {
            libc::EPOLLIN as u32
        };

        let mut ev = libc::epoll_event {
            events,
            u64: self.token.0 as u64,
        };

        // epoll_ctl is thread-safe
        unsafe {
            libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_MOD, self.socket_fd, &mut ev);
        }
    }

    /// Get the token.
    pub fn token(&self) -> Token {
        self.token
    }
}

// Safety: The write buffer is mutex-protected, and epoll_ctl is thread-safe.
unsafe impl Send for ClientWriteHandle {}
unsafe impl Sync for ClientWriteHandle {}

#[cfg(test)]
impl ClientWriteHandle {
    /// Test helper: check if EPOLLOUT would be set.
    fn is_ready_for_writing(&self) -> bool {
        self.ready_for_writing.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};

    /// Mock writer that accepts all writes.
    struct MockWriter;

    impl Write for MockWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Mock writer that always returns WouldBlock.
    struct WouldBlockWriter;

    impl Write for WouldBlockWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::WouldBlock, "would block"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn write_vectored(&mut self, _bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::WouldBlock, "would block"))
        }
    }

    fn make_test_handle() -> ClientWriteHandle {
        // Use invalid fds - epoll_ctl will fail but we ignore return value
        ClientWriteHandle::new(0, -1, -1, Token(1))
    }

    #[test]
    fn test_ready_for_writing_set_after_queue() {
        let handle = make_test_handle();
        assert!(!handle.is_ready_for_writing(), "should start false");

        let packet = Packet::Pingresp;
        handle.queue_packet(&packet).unwrap();

        assert!(handle.is_ready_for_writing(), "should be true after queue");
    }

    #[test]
    fn test_ready_for_writing_not_cleared_immediately() {
        let handle = make_test_handle();

        // Queue a packet
        let packet = Packet::Pingresp;
        handle.queue_packet(&packet).unwrap();
        assert!(handle.is_ready_for_writing());

        // Flush writes data, buffer becomes empty
        let mut writer = MockWriter;
        let result = handle.flush(&mut writer).unwrap();
        assert!(result, "flush should complete");

        // ready_for_writing still true (threshold not reached)
        // This is by design - we don't clear EPOLLOUT immediately to avoid
        // race conditions under load. Only truly idle clients get cleared.
        assert!(handle.is_ready_for_writing(),
            "should still be true - threshold-based clearing");
    }

    #[test]
    fn test_ready_for_writing_kept_on_wouldblock() {
        let handle = make_test_handle();

        // Queue a packet
        let packet = Packet::Pingresp;
        handle.queue_packet(&packet).unwrap();
        assert!(handle.is_ready_for_writing());

        // Try to flush but get WouldBlock
        let mut writer = WouldBlockWriter;
        let result = handle.flush(&mut writer).unwrap();
        assert!(!result, "flush should return false on WouldBlock");

        // ready_for_writing should stay true (data still pending)
        assert!(handle.is_ready_for_writing(),
            "should stay true when data pending");
    }

    #[test]
    fn test_idle_counter_reset_by_queue() {
        let handle = make_test_handle();

        // Queue and flush a packet
        let packet = Packet::Pingresp;
        handle.queue_packet(&packet).unwrap();
        let mut writer = MockWriter;
        handle.flush(&mut writer).unwrap();

        // Idle counter has started, ready_for_writing still true
        assert!(handle.is_ready_for_writing());

        // Queue another packet - this resets the idle counter
        handle.queue_packet(&packet).unwrap();

        // Another flush writes the new data
        handle.flush(&mut writer).unwrap();

        // Still true because idle counter was reset by queue_packet
        assert!(handle.is_ready_for_writing(),
            "should still be true - idle counter reset by queue");
    }
}
