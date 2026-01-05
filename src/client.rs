//! Per-client state and buffer management.

use std::io::{self, Read};
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::AHashMap;
use mio::net::TcpStream;
use mio::Token;

use crate::client_handle::ClientWriteHandle;
use crate::error::Result;
use crate::packet;
use crate::packet::{Packet, Publish, QoS, Will};
use crate::publish_encoder::PublishEncoder;
use crate::util::{QuotaTracker, RateLimitedCounter};

/// Pending outgoing QoS 1/2 message awaiting acknowledgment.
#[derive(Debug, Clone)]
#[allow(dead_code)] // sent_at reserved for future retransmission
pub struct PendingPublish {
    pub publish: Publish,
    pub sent_at: Instant,
}

/// Initial buffer size (1KB as per spec).
const INITIAL_BUFFER_SIZE: usize = 1024;

/// Client connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientState {
    /// Waiting for CONNECT packet.
    Connecting,
    /// Connected and authenticated.
    Connected,
    /// Disconnecting (graceful or error).
    Disconnecting,
}

/// Per-client state and buffers.
#[allow(dead_code)] // token field kept for debugging
pub struct Client {
    pub token: Token,
    pub socket: TcpStream,
    pub remote_addr: SocketAddr,
    pub state: ClientState,
    pub client_id: Option<String>,
    pub keep_alive: u16,
    pub clean_session: bool,
    /// Will message to publish on abnormal disconnect.
    pub will: Option<Will>,
    /// Whether the client sent a DISCONNECT packet (graceful disconnect).
    pub graceful_disconnect: bool,

    // Authentication info
    /// Username (if authenticated with one).
    pub username: Option<String>,
    /// Role assigned during authentication (for ACL lookups).
    pub role: Option<String>,
    /// Whether this is an anonymous (unauthenticated) connection.
    pub is_anonymous: bool,

    /// MQTT protocol version (3=3.1, 4=3.1.1, 5=5.0).
    pub protocol_version: u8,

    /// Next packet ID for outgoing QoS 1/2 messages (1-65535, 0 is invalid).
    pub next_packet_id: u16,

    /// Last time any packet was received from this client.
    pub last_packet_time: Instant,

    /// Pending outgoing QoS 1 messages awaiting PUBACK.
    pub pending_qos1: AHashMap<u16, PendingPublish>,

    /// Pending outgoing QoS 2 messages awaiting PUBREC.
    pub pending_qos2: AHashMap<u16, PendingPublish>,

    // MQTT 5 Flow Control
    /// Client's maximum packet size (from CONNECT properties, 0 = unlimited).
    pub client_max_packet_size: u32,
    /// Outgoing quota tracker for QoS 1/2 flow control.
    pub quota: QuotaTracker,

    /// Rate-limited backpressure logging.
    pub backpressure_log: RateLimitedCounter,

    /// Read buffer for incoming data.
    read_buf: Vec<u8>,
    read_pos: usize,

    /// Shared write handle for cross-thread writes.
    /// Any thread can write to this handle, and it will update epoll directly.
    pub handle: Arc<ClientWriteHandle>,
}

impl Client {
    /// Create a new client with a shared write handle.
    pub fn new(
        token: Token,
        socket: TcpStream,
        remote_addr: SocketAddr,
        worker_id: usize,
        epoll_fd: i32,
    ) -> Self {
        let socket_fd = socket.as_raw_fd();
        let handle = Arc::new(ClientWriteHandle::new(
            worker_id, epoll_fd, socket_fd, token,
        ));

        Self {
            token,
            socket,
            remote_addr,
            state: ClientState::Connecting,
            client_id: None,
            keep_alive: 0,
            clean_session: true,
            will: None,
            graceful_disconnect: false,
            username: None,
            role: None,
            is_anonymous: true,  // Default to anonymous until authenticated
            protocol_version: 4, // Default to 3.1.1, updated on CONNECT
            next_packet_id: 1,
            last_packet_time: Instant::now(),
            pending_qos1: AHashMap::new(),
            pending_qos2: AHashMap::new(),
            client_max_packet_size: 0,       // 0 = unlimited
            quota: QuotaTracker::new(65535), // Default per MQTT 5 spec
            backpressure_log: RateLimitedCounter::new(Duration::from_secs(10)),
            read_buf: vec![0u8; INITIAL_BUFFER_SIZE],
            read_pos: 0,
            handle,
        }
    }

    /// Record a backpressure drop and log if interval has passed.
    /// Returns true if a log was emitted (for callers that want to add context).
    pub fn record_backpressure_drop(&mut self, context: &str) -> bool {
        if let Some(count) = self.backpressure_log.increment() {
            log::warn!(
                "Backpressure: dropped {} messages to slow client (token={:?}, {})",
                count,
                self.token,
                context
            );
            true
        } else {
            false
        }
    }

    /// Read data from socket into buffer.
    /// Returns Ok(true) if data was read, Ok(false) if would block.
    pub fn read(&mut self) -> Result<bool> {
        loop {
            // Grow buffer if needed
            if self.read_pos >= self.read_buf.len() {
                let new_size = self.read_buf.len() * 2;
                self.read_buf.resize(new_size, 0);
            }

            match self.socket.read(&mut self.read_buf[self.read_pos..]) {
                Ok(0) => {
                    // Connection closed - if there's data to process (e.g., DISCONNECT packet),
                    // return true so packets get processed. Otherwise mark as disconnecting.
                    if self.read_pos > 0 {
                        return Ok(true);
                    } else {
                        self.state = ClientState::Disconnecting;
                        return Ok(false);
                    }
                }
                Ok(n) => {
                    self.read_pos += n;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(self.read_pos > 0);
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Try to decode the next packet from the read buffer.
    /// max_packet_size: Maximum allowed packet size (0 = no limit).
    pub fn decode_packet(&mut self, max_packet_size: u32) -> Result<Option<Packet>> {
        if self.read_pos == 0 {
            return Ok(None);
        }

        let data = &self.read_buf[..self.read_pos];
        match packet::decode_packet(data, self.protocol_version, max_packet_size)? {
            Some((packet, consumed)) => {
                // Remove consumed bytes from buffer
                self.read_buf.copy_within(consumed..self.read_pos, 0);
                self.read_pos -= consumed;
                Ok(Some(packet))
            }
            None => Ok(None),
        }
    }

    /// Get next available packet ID, wrapping from 65535 to 1 (0 is invalid).
    pub fn allocate_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        self.next_packet_id = if id == 65535 { 1 } else { id + 1 };
        id
    }

    /// Set MQTT 5 flow control values from CONNECT properties.
    pub fn set_flow_control(&mut self, receive_max: u16, max_packet_size: u32) {
        self.quota.set_max(receive_max);
        self.client_max_packet_size = max_packet_size;
    }

    /// Check if we can send a QoS 1/2 message (quota available).
    #[inline]
    #[allow(dead_code)]
    pub fn has_quota(&self) -> bool {
        self.quota.has_quota()
    }

    /// Consume one quota slot when sending QoS 1/2. Returns false if no quota.
    #[inline]
    pub fn consume_quota(&mut self) -> bool {
        self.quota.consume()
    }

    /// Restore one quota slot when receiving ACK (PUBACK/PUBCOMP).
    #[inline]
    pub fn restore_quota(&mut self) {
        self.quota.restore()
    }

    /// Queue a packet for sending.
    /// This writes to the shared handle, which updates epoll directly.
    /// Returns WouldBlock if client's TX buffer is full (slow client).
    pub fn queue_packet(&self, packet: &Packet) -> std::io::Result<()> {
        self.handle.queue_packet(packet)
    }

    /// Queue a publish packet using the copy factory (zero-copy for payload).
    /// Returns WouldBlock if client's TX buffer is full (slow client).
    #[allow(dead_code)]
    pub fn queue_publish(
        &self,
        factory: &mut PublishEncoder,
        effective_qos: QoS,
        packet_id: Option<u16>,
        retain: bool,
    ) -> std::io::Result<()> {
        self.handle
            .queue_publish(factory, effective_qos, packet_id, retain)
    }

    /// Write queued data to socket.
    /// Returns Ok(true) if all data was written, Ok(false) if would block.
    pub fn flush(&mut self) -> Result<bool> {
        match self.handle.flush(&mut self.socket) {
            Ok(true) => Ok(true),
            Ok(false) => {
                // Check if connection was closed (0 bytes written)
                // The handle.flush returns false for WouldBlock too
                Ok(false)
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::WriteZero {
                    self.state = ClientState::Disconnecting;
                    return Ok(false);
                }
                Err(e.into())
            }
        }
    }

    /// Check if there's data waiting to be written.
    #[allow(dead_code)]
    pub fn has_pending_writes(&self) -> bool {
        self.handle.has_pending_writes()
    }
}
