//! MQTT client implementation.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::{TcpStream as StdTcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};

use mqlite_core::packet::{
    decode_packet, encode_connect, encode_disconnect, encode_pingreq, encode_publish,
    encode_subscribe, encode_unsubscribe, Connack, ConnackCode, Connect, Packet, Publish, QoS,
    Suback, Subscribe, SubscriptionOptions, Unsuback, Unsubscribe,
};

use crate::config::{ClientConfig, ConnectOptions};
use crate::error::{ClientError, Result};
use crate::events::{ClientEvent, ConnectionState};

const CLIENT: Token = Token(0);
const DEFAULT_BUFFER_SIZE: usize = 8192;

/// MQTT client.
pub struct Client {
    config: ClientConfig,
    state: ConnectionState,
    poll: Poll,
    stream: Option<TcpStream>,
    read_buf: BytesMut,
    write_buf: Vec<u8>,
    events: VecDeque<ClientEvent>,
    next_packet_id: u16,
    last_packet_time: Instant,
    pending_pings: u8,
}

impl Client {
    /// Create a new MQTT client with the given configuration.
    pub fn new(config: ClientConfig) -> Result<Self> {
        let poll = Poll::new()?;

        Ok(Self {
            config,
            state: ConnectionState::Disconnected,
            poll,
            stream: None,
            read_buf: BytesMut::with_capacity(DEFAULT_BUFFER_SIZE),
            write_buf: Vec::with_capacity(DEFAULT_BUFFER_SIZE),
            events: VecDeque::new(),
            next_packet_id: 1,
            last_packet_time: Instant::now(),
            pending_pings: 0,
        })
    }

    /// Connect to the broker.
    pub fn connect(&mut self, options: Option<ConnectOptions>) -> Result<()> {
        if self.state != ConnectionState::Disconnected {
            return Err(ClientError::InvalidState(
                "Already connected or connecting".to_string(),
            ));
        }

        // Resolve address
        let addr = self
            .config
            .address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| {
                ClientError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Could not resolve address",
                ))
            })?;

        // Create non-blocking TCP connection
        let std_stream = StdTcpStream::connect_timeout(&addr, self.config.connect_timeout)?;
        std_stream.set_nonblocking(true)?;
        std_stream.set_nodelay(true)?;

        let mut stream = TcpStream::from_std(std_stream);

        // Register with poll
        self.poll
            .registry()
            .register(&mut stream, CLIENT, Interest::READABLE | Interest::WRITABLE)?;

        self.stream = Some(stream);
        self.state = ConnectionState::Connecting;

        // Build CONNECT packet
        let opts = options.unwrap_or_default();
        let client_id = opts
            .client_id
            .unwrap_or_else(|| self.config.client_id.clone());
        let clean_session = opts.clean_session.unwrap_or(self.config.clean_session);

        let protocol_name = "MQTT".to_string();

        let connect = Connect {
            protocol_name,
            protocol_version: self.config.protocol_version,
            clean_session,
            keep_alive: self.config.keep_alive,
            client_id,
            username: self.config.username.clone(),
            password: self.config.password.clone(),
            will: None,
            properties: None,
        };

        // Encode and queue CONNECT
        encode_connect(&connect, &mut self.write_buf);
        self.last_packet_time = Instant::now();

        Ok(())
    }

    /// Disconnect from the broker.
    pub fn disconnect(&mut self) -> Result<()> {
        if self.state == ConnectionState::Disconnected {
            return Ok(());
        }

        // Send DISCONNECT packet
        encode_disconnect(0, &mut self.write_buf);

        // Try to flush
        let _ = self.flush_write_buffer();

        self.cleanup();
        self.events
            .push_back(ClientEvent::Disconnected { reason: None });

        Ok(())
    }

    /// Subscribe to topics with default options.
    ///
    /// For MQTT 5.0 subscription options (NoLocal, RetainAsPublished, etc.),
    /// use `subscribe_with_options` instead.
    pub fn subscribe(&mut self, topics: &[(&str, QoS)]) -> Result<u16> {
        let subscriptions: Vec<_> = topics
            .iter()
            .map(|(topic, qos)| {
                (
                    *topic,
                    SubscriptionOptions {
                        qos: *qos,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: 0,
                    },
                )
            })
            .collect();
        self.subscribe_with_options(&subscriptions)
    }

    /// Subscribe to topics with full MQTT 5.0 subscription options.
    ///
    /// # Options
    /// - `no_local`: Don't receive messages published by this client (loop prevention)
    /// - `retain_as_published`: Keep original retain flag on forwarded messages
    /// - `retain_handling`: 0=send retained on subscribe, 1=send if new sub, 2=don't send
    ///
    /// # Example
    /// ```ignore
    /// use mqlite_client::{Client, SubscriptionOptions, QoS};
    ///
    /// // Subscribe with NoLocal to prevent message reflection (useful for bridges)
    /// client.subscribe_with_options(&[
    ///     ("sensors/#", SubscriptionOptions {
    ///         qos: QoS::AtLeastOnce,
    ///         no_local: true,
    ///         retain_as_published: false,
    ///         retain_handling: 0,
    ///     }),
    /// ])?;
    /// ```
    pub fn subscribe_with_options(&mut self, topics: &[(&str, SubscriptionOptions)]) -> Result<u16> {
        if self.state != ConnectionState::Connected {
            return Err(ClientError::NotConnected);
        }

        let packet_id = self.next_packet_id();
        let subscribe = Subscribe {
            packet_id,
            topics: topics
                .iter()
                .map(|(topic, opts)| (topic.to_string(), *opts))
                .collect(),
            subscription_id: None,
        };

        encode_subscribe(&subscribe, &mut self.write_buf);
        self.last_packet_time = Instant::now();

        Ok(packet_id)
    }

    /// Unsubscribe from topics.
    pub fn unsubscribe(&mut self, topics: &[&str]) -> Result<u16> {
        if self.state != ConnectionState::Connected {
            return Err(ClientError::NotConnected);
        }

        let packet_id = self.next_packet_id();
        let unsubscribe = Unsubscribe {
            packet_id,
            topics: topics.iter().map(|t| t.to_string()).collect(),
        };

        encode_unsubscribe(&unsubscribe, &mut self.write_buf);
        self.last_packet_time = Instant::now();

        Ok(packet_id)
    }

    /// Publish a message.
    pub fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<Option<u16>> {
        if self.state != ConnectionState::Connected {
            return Err(ClientError::NotConnected);
        }

        let packet_id = if qos != QoS::AtMostOnce {
            Some(self.next_packet_id())
        } else {
            None
        };

        let publish = Publish {
            dup: false,
            qos,
            retain,
            topic: Bytes::from(topic.to_string()),
            packet_id,
            payload: Bytes::copy_from_slice(payload),
            properties: None,
        };

        encode_publish(&publish, &mut self.write_buf);
        self.last_packet_time = Instant::now();

        Ok(packet_id)
    }

    /// Poll for events with timeout.
    /// Returns true if there are events to process.
    pub fn poll(&mut self, timeout: Option<Duration>) -> Result<bool> {
        // First, try to flush any pending writes
        if !self.write_buf.is_empty() {
            self.flush_write_buffer()?;
        }

        // Check keep-alive
        if self.state == ConnectionState::Connected && self.config.keep_alive > 0 {
            let elapsed = self.last_packet_time.elapsed();
            let keep_alive_duration = Duration::from_secs(self.config.keep_alive as u64);

            if elapsed >= keep_alive_duration {
                if self.pending_pings >= 2 {
                    // No response to pings, connection dead
                    self.cleanup();
                    self.events.push_back(ClientEvent::Disconnected {
                        reason: Some("Keep-alive timeout".to_string()),
                    });
                    return Ok(!self.events.is_empty());
                }

                // Send PINGREQ
                encode_pingreq(&mut self.write_buf);
                self.pending_pings += 1;
                self.last_packet_time = Instant::now();
            }
        }

        // Poll for I/O events
        let mut events = Events::with_capacity(16);
        self.poll.poll(&mut events, timeout)?;

        for event in events.iter() {
            if event.token() == CLIENT {
                if event.is_readable() {
                    self.handle_read()?;
                }
                if event.is_writable() {
                    self.flush_write_buffer()?;
                }
            }
        }

        Ok(!self.events.is_empty())
    }

    /// Get the next event, if any.
    pub fn next_event(&mut self) -> Option<ClientEvent> {
        self.events.pop_front()
    }

    /// Check if connected.
    pub fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected
    }

    // === Internal methods ===

    fn next_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        id
    }

    fn handle_read(&mut self) -> Result<()> {
        let stream = match &mut self.stream {
            Some(s) => s,
            None => return Ok(()),
        };

        // Read into buffer
        let mut buf = [0u8; 4096];
        loop {
            match stream.read(&mut buf) {
                Ok(0) => {
                    // Connection closed
                    self.cleanup();
                    self.events.push_back(ClientEvent::Disconnected {
                        reason: Some("Connection closed by peer".to_string()),
                    });
                    return Ok(());
                }
                Ok(n) => {
                    self.read_buf.extend_from_slice(&buf[..n]);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(ClientError::Io(e)),
            }
        }

        // Parse packets
        self.parse_packets()
    }

    fn parse_packets(&mut self) -> Result<()> {
        loop {
            if self.read_buf.is_empty() {
                break;
            }

            match decode_packet(&self.read_buf, self.config.protocol_version, 0) {
                Ok(Some((packet, consumed))) => {
                    let _ = self.read_buf.split_to(consumed);
                    self.handle_packet(packet)?;
                }
                Ok(None) => break, // Need more data
                Err(e) => {
                    return Err(ClientError::Protocol(
                        mqlite_core::ProtocolError::MalformedPacket(e.to_string()),
                    ))
                }
            }
        }
        Ok(())
    }

    fn handle_packet(&mut self, packet: Packet) -> Result<()> {
        self.last_packet_time = Instant::now();

        match packet {
            Packet::Connack(connack) => self.handle_connack(connack),
            Packet::Publish(publish) => self.handle_publish(publish),
            Packet::Puback { packet_id } => {
                self.events
                    .push_back(ClientEvent::PubAck { packet_id });
                Ok(())
            }
            Packet::Pubrec { packet_id } => {
                // QoS 2: respond with PUBREL
                let pubrel = Packet::Pubrel { packet_id };
                encode_pubrel_packet(&pubrel, &mut self.write_buf);
                self.events
                    .push_back(ClientEvent::PubRec { packet_id });
                Ok(())
            }
            Packet::Pubcomp { packet_id } => {
                self.events
                    .push_back(ClientEvent::PubComp { packet_id });
                Ok(())
            }
            Packet::Suback(suback) => self.handle_suback(suback),
            Packet::Unsuback(unsuback) => self.handle_unsuback(unsuback),
            Packet::Pingresp => {
                self.pending_pings = 0;
                Ok(())
            }
            Packet::Disconnect { reason_code } => {
                let reason = Some(format!("Disconnect reason: {}", reason_code));
                self.cleanup();
                self.events
                    .push_back(ClientEvent::Disconnected { reason });
                Ok(())
            }
            _ => Ok(()), // Ignore unexpected packets
        }
    }

    fn handle_connack(&mut self, connack: Connack) -> Result<()> {
        if connack.code != ConnackCode::Accepted {
            let reason = format!("Connection refused: {:?}", connack.code);
            self.cleanup();
            return Err(ClientError::ConnectionRefused(reason));
        }

        self.state = ConnectionState::Connected;
        self.events.push_back(ClientEvent::Connected {
            session_present: connack.session_present,
        });
        Ok(())
    }

    fn handle_publish(&mut self, publish: Publish) -> Result<()> {
        // Send acknowledgments for QoS > 0
        match publish.qos {
            QoS::AtLeastOnce => {
                if let Some(packet_id) = publish.packet_id {
                    encode_puback_packet(packet_id, &mut self.write_buf);
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_id) = publish.packet_id {
                    encode_pubrec_packet(packet_id, &mut self.write_buf);
                }
            }
            QoS::AtMostOnce => {}
        }

        self.events.push_back(ClientEvent::Message {
            topic: publish.topic,
            payload: publish.payload,
            qos: publish.qos,
            retain: publish.retain,
            packet_id: publish.packet_id,
        });
        Ok(())
    }

    fn handle_suback(&mut self, suback: Suback) -> Result<()> {
        self.events.push_back(ClientEvent::SubAck {
            packet_id: suback.packet_id,
            return_codes: suback.return_codes,
        });
        Ok(())
    }

    fn handle_unsuback(&mut self, unsuback: Unsuback) -> Result<()> {
        self.events.push_back(ClientEvent::UnsubAck {
            packet_id: unsuback.packet_id,
        });
        Ok(())
    }

    fn flush_write_buffer(&mut self) -> Result<()> {
        if self.write_buf.is_empty() {
            return Ok(());
        }

        let stream = match &mut self.stream {
            Some(s) => s,
            None => return Ok(()),
        };

        let mut written = 0;
        loop {
            match stream.write(&self.write_buf[written..]) {
                Ok(0) => break,
                Ok(n) => {
                    written += n;
                    if written >= self.write_buf.len() {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(ClientError::Io(e)),
            }
        }

        if written > 0 {
            self.write_buf.drain(..written);
        }
        Ok(())
    }

    fn cleanup(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            let _ = self.poll.registry().deregister(&mut stream);
        }
        self.state = ConnectionState::Disconnected;
        self.read_buf.clear();
        self.write_buf.clear();
        self.pending_pings = 0;
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.disconnect();
    }
}

// Helper functions for encoding simple ACK packets
fn encode_puback_packet(packet_id: u16, buf: &mut Vec<u8>) {
    buf.push(0x40); // PUBACK fixed header
    buf.push(2); // Remaining length
    buf.extend_from_slice(&packet_id.to_be_bytes());
}

fn encode_pubrec_packet(packet_id: u16, buf: &mut Vec<u8>) {
    buf.push(0x50); // PUBREC fixed header
    buf.push(2); // Remaining length
    buf.extend_from_slice(&packet_id.to_be_bytes());
}

fn encode_pubrel_packet(_packet: &Packet, buf: &mut Vec<u8>) {
    if let Packet::Pubrel { packet_id } = _packet {
        buf.push(0x62); // PUBREL fixed header (flags = 0x02)
        buf.push(2); // Remaining length
        buf.extend_from_slice(&packet_id.to_be_bytes());
    }
}
