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
use crate::write_buffer::WriteBuffer;

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
        factory.write_to_with_sub_id(
            &mut *buf,
            effective_qos,
            packet_id,
            retain,
            is_v5,
            subscription_id,
        )?;
        drop(buf);
        self.set_ready_for_writing(true);
        Ok(())
    }

    /// Flush the write buffer to the socket. Called by owning worker only.
    /// Returns Ok(true) if all data was written, Ok(false) if would block.
    pub fn flush(&self, socket: &mut impl std::io::Write) -> std::io::Result<bool> {
        loop {
            // Lock and write directly from buffer - no temp copy needed
            // This is safe because only the owning worker calls flush()
            let mut buf = self.write_buf.lock();
            if buf.is_empty() {
                // Clear the ready flag while still holding the lock to prevent race.
                // If another thread writes after we release, it will set the flag again.
                self.set_ready_for_writing(false);
                return Ok(true);
            }

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
    #[inline]
    fn set_ready_for_writing(&self, val: bool) {
        // Fast path: cheap load to avoid swap when state matches
        if self.ready_for_writing.load(Ordering::Relaxed) == val {
            return;
        }
        // Use Release ordering (cheaper than SeqCst) - we just need writes to be visible
        if self.ready_for_writing.swap(val, Ordering::Release) == val {
            return;
        }

        let events = if val {
            (libc::EPOLLIN | libc::EPOLLOUT | libc::EPOLLET) as u32
        } else {
            (libc::EPOLLIN | libc::EPOLLET) as u32
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
