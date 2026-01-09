//! MQTT client implementation.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::{TcpStream as StdTcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};

use mqlite_core::packet::{
    decode_packet, encode_packet, Connack, ConnackCode, Connect, Packet, Publish, QoS, Suback,
    Subscribe, SubscriptionOptions, Unsuback, Unsubscribe, Will as CoreWill,
};

use crate::config::{ClientConfig, ConnectOptions};
use crate::error::{ClientError, Result};
use crate::events::{ClientEvent, ConnectionState};
use crate::packet_id::PacketIdAllocator;
use crate::session::{PendingPublish, Qos2OutState, Session};

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
    last_packet_time: Instant,
    pending_pings: u8,
    /// Session state tracking
    session: Session,
    /// Packet ID allocator
    packet_ids: PacketIdAllocator,
}

impl Client {
    /// Create a new MQTT client with the given configuration.
    pub fn new(config: ClientConfig) -> Result<Self> {
        let poll = Poll::new()?;
        let session = Session::new(config.client_id.clone(), config.clean_session);

        Ok(Self {
            config,
            state: ConnectionState::Disconnected,
            poll,
            stream: None,
            read_buf: BytesMut::with_capacity(DEFAULT_BUFFER_SIZE),
            write_buf: Vec::with_capacity(DEFAULT_BUFFER_SIZE),
            events: VecDeque::new(),
            last_packet_time: Instant::now(),
            pending_pings: 0,
            session,
            packet_ids: PacketIdAllocator::new(),
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
        self.poll.registry().register(
            &mut stream,
            CLIENT,
            Interest::READABLE | Interest::WRITABLE,
        )?;

        self.stream = Some(stream);
        self.state = ConnectionState::Connecting;

        // Build CONNECT packet
        let opts = options.unwrap_or_default();
        let client_id = opts
            .client_id
            .unwrap_or_else(|| self.config.client_id.clone());
        let clean_session = opts.clean_session.unwrap_or(self.config.clean_session);

        // Update session
        self.session.client_id = client_id.clone();
        self.session.clean_session = clean_session;

        // Clear session state if clean session
        if clean_session {
            self.session.clear();
            self.packet_ids.clear();
        }

        let protocol_name = "MQTT".to_string();

        // Build will config if present
        let will = self.config.will.as_ref().map(|w| CoreWill {
            topic: w.topic.clone(),
            message: w.payload.to_vec(),
            qos: w.qos,
            retain: w.retain,
            properties: None,
        });

        let connect = Connect {
            protocol_name,
            protocol_version: self.config.protocol_version,
            clean_session,
            keep_alive: self.config.keep_alive,
            client_id,
            username: self.config.username.clone(),
            password: self.config.password.clone(),
            will,
            properties: None,
        };

        // Encode and queue CONNECT
        encode_packet(&Packet::Connect(connect), &mut self.write_buf);
        self.last_packet_time = Instant::now();

        Ok(())
    }

    /// Reconnect to the broker.
    ///
    /// If the session is not clean, this will re-send any pending QoS 1/2 messages
    /// per [MQTT-4.4.0-1].
    pub fn reconnect(&mut self) -> Result<()> {
        if self.state != ConnectionState::Disconnected {
            self.cleanup();
        }

        // Connect with current session settings
        self.connect(Some(ConnectOptions {
            client_id: Some(self.session.client_id.clone()),
            clean_session: Some(self.session.clean_session),
        }))
    }

    /// Disconnect from the broker.
    pub fn disconnect(&mut self) -> Result<()> {
        if self.state == ConnectionState::Disconnected {
            return Ok(());
        }

        // Send DISCONNECT packet
        encode_packet(&Packet::Disconnect { reason_code: 0 }, &mut self.write_buf);

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
    pub fn subscribe_with_options(
        &mut self,
        topics: &[(&str, SubscriptionOptions)],
    ) -> Result<u16> {
        if self.state != ConnectionState::Connected {
            return Err(ClientError::NotConnected);
        }

        let packet_id = self.allocate_packet_id()?;
        let subscribe = Subscribe {
            packet_id,
            topics: topics
                .iter()
                .map(|(topic, opts)| (topic.to_string(), *opts))
                .collect(),
            subscription_id: None,
        };

        // Track subscription in session
        for (topic, opts) in topics {
            self.session.add_subscription(topic.to_string(), opts.qos);
        }

        encode_packet(&Packet::Subscribe(subscribe), &mut self.write_buf);
        self.last_packet_time = Instant::now();

        Ok(packet_id)
    }

    /// Unsubscribe from topics.
    pub fn unsubscribe(&mut self, topics: &[&str]) -> Result<u16> {
        if self.state != ConnectionState::Connected {
            return Err(ClientError::NotConnected);
        }

        let packet_id = self.allocate_packet_id()?;
        let unsubscribe = Unsubscribe {
            packet_id,
            topics: topics.iter().map(|t| t.to_string()).collect(),
        };

        // Remove from session
        for topic in topics {
            self.session.remove_subscription(topic);
        }

        encode_packet(&Packet::Unsubscribe(unsubscribe), &mut self.write_buf);
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

        // Check inflight limit
        if self.config.max_inflight > 0 && self.session.pending_count() >= self.config.max_inflight
        {
            return Err(ClientError::InvalidState(
                "Max inflight messages reached".to_string(),
            ));
        }

        let packet_id = if qos != QoS::AtMostOnce {
            Some(self.allocate_packet_id()?)
        } else {
            None
        };

        let topic_bytes = Bytes::from(topic.to_string());
        let payload_bytes = Bytes::copy_from_slice(payload);

        let publish = Publish {
            dup: false,
            qos,
            retain,
            topic: topic_bytes.clone(),
            packet_id,
            payload: payload_bytes.clone(),
            properties: None,
        };

        encode_packet(&Packet::Publish(publish), &mut self.write_buf);
        self.last_packet_time = Instant::now();

        // Track pending messages for QoS > 0
        if let Some(id) = packet_id {
            let pending = PendingPublish::new(id, topic_bytes, payload_bytes, qos, retain);
            match qos {
                QoS::AtLeastOnce => self.session.add_qos1_pending(pending),
                QoS::ExactlyOnce => self.session.add_qos2_pending(pending),
                QoS::AtMostOnce => {}
            }
        }

        Ok(packet_id)
    }

    /// Poll for events with timeout.
    /// Returns true if there are events to process.
    pub fn poll(&mut self, timeout: Option<Duration>) -> Result<bool> {
        // First, try to flush any pending writes
        if !self.write_buf.is_empty() {
            self.flush_write_buffer()?;
        }

        // Check for message retries
        self.check_retries()?;

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
                encode_packet(&Packet::Pingreq, &mut self.write_buf);
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

    /// Get the number of pending outbound messages.
    pub fn pending_count(&self) -> usize {
        self.session.pending_count()
    }

    /// Get a reference to the session state.
    pub fn session(&self) -> &Session {
        &self.session
    }

    // === Internal methods ===

    fn allocate_packet_id(&mut self) -> Result<u16> {
        self.packet_ids.allocate().ok_or_else(|| {
            ClientError::InvalidState("All packet IDs in use".to_string())
        })
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
            Packet::Puback { packet_id } => self.handle_puback(packet_id),
            Packet::Pubrec { packet_id } => self.handle_pubrec(packet_id),
            Packet::Pubrel { packet_id } => self.handle_pubrel(packet_id),
            Packet::Pubcomp { packet_id } => self.handle_pubcomp(packet_id),
            Packet::Suback(suback) => self.handle_suback(suback),
            Packet::Unsuback(unsuback) => self.handle_unsuback(unsuback),
            Packet::Pingresp => {
                self.pending_pings = 0;
                Ok(())
            }
            Packet::Disconnect { reason_code } => {
                let reason = Some(format!("Disconnect reason: {}", reason_code));
                self.cleanup();
                self.events.push_back(ClientEvent::Disconnected { reason });
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

        // If session not present and we expected one, clear local state
        if !connack.session_present && !self.session.clean_session {
            self.session.clear();
            self.packet_ids.clear();
        }

        // Re-send pending messages if session was restored
        // Per [MQTT-4.4.0-1]
        if connack.session_present {
            self.resend_pending_messages()?;
        }

        self.events.push_back(ClientEvent::Connected {
            session_present: connack.session_present,
        });
        Ok(())
    }

    fn handle_publish(&mut self, publish: Publish) -> Result<()> {
        // For QoS 2, check if this is a duplicate we've already processed
        if publish.qos == QoS::ExactlyOnce {
            if let Some(packet_id) = publish.packet_id {
                if self.session.has_qos2_inbound(packet_id) {
                    // Duplicate, just resend PUBREC
                    encode_packet(&Packet::Pubrec { packet_id }, &mut self.write_buf);
                    return Ok(());
                }
            }
        }

        // Send acknowledgments for QoS > 0
        match publish.qos {
            QoS::AtLeastOnce => {
                if let Some(packet_id) = publish.packet_id {
                    encode_packet(&Packet::Puback { packet_id }, &mut self.write_buf);
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_id) = publish.packet_id {
                    // Track that we received this message
                    self.session.add_qos2_inbound(packet_id);
                    encode_packet(&Packet::Pubrec { packet_id }, &mut self.write_buf);
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

    fn handle_puback(&mut self, packet_id: u16) -> Result<()> {
        // Complete QoS 1 flow
        if self.session.complete_qos1(packet_id).is_some() {
            self.packet_ids.release(packet_id);
            self.events.push_back(ClientEvent::PubAck { packet_id });
        }
        Ok(())
    }

    fn handle_pubrec(&mut self, packet_id: u16) -> Result<()> {
        // Transition QoS 2 flow: received PUBREC, send PUBREL
        if self.session.qos2_received_pubrec(packet_id) {
            encode_packet(&Packet::Pubrel { packet_id }, &mut self.write_buf);
            self.events.push_back(ClientEvent::PubRec { packet_id });
        }
        Ok(())
    }

    fn handle_pubrel(&mut self, packet_id: u16) -> Result<()> {
        // QoS 2 receiver flow: received PUBREL, send PUBCOMP
        if self.session.complete_qos2_in(packet_id) {
            encode_packet(&Packet::Pubcomp { packet_id }, &mut self.write_buf);
        }
        Ok(())
    }

    fn handle_pubcomp(&mut self, packet_id: u16) -> Result<()> {
        // Complete QoS 2 flow
        if self.session.complete_qos2_out(packet_id).is_some() {
            self.packet_ids.release(packet_id);
            self.events.push_back(ClientEvent::PubComp { packet_id });
        }
        Ok(())
    }

    fn handle_suback(&mut self, suback: Suback) -> Result<()> {
        self.packet_ids.release(suback.packet_id);
        self.events.push_back(ClientEvent::SubAck {
            packet_id: suback.packet_id,
            return_codes: suback.return_codes,
        });
        Ok(())
    }

    fn handle_unsuback(&mut self, unsuback: Unsuback) -> Result<()> {
        self.packet_ids.release(unsuback.packet_id);
        self.events.push_back(ClientEvent::UnsubAck {
            packet_id: unsuback.packet_id,
        });
        Ok(())
    }

    /// Check for messages that need to be retried.
    fn check_retries(&mut self) -> Result<()> {
        if self.state != ConnectionState::Connected {
            return Ok(());
        }

        let retry_interval = self.config.retry_interval;
        let now = Instant::now();

        // Check QoS 1 messages
        for pending in &mut self.session.pending_qos1 {
            if now.duration_since(pending.last_sent) >= retry_interval {
                // Re-send with DUP=1 per [MQTT-3.3.1-1]
                let publish = Publish {
                    dup: true,
                    qos: pending.qos,
                    retain: pending.retain,
                    topic: pending.topic.clone(),
                    packet_id: Some(pending.packet_id),
                    payload: pending.payload.clone(),
                    properties: None,
                };
                encode_packet(&Packet::Publish(publish), &mut self.write_buf);
                pending.mark_resent();
            }
        }

        // Check QoS 2 messages
        for pending in &mut self.session.pending_qos2_out {
            let last_sent = match pending.state {
                Qos2OutState::AwaitingPubrec => pending.publish.last_sent,
                Qos2OutState::AwaitingPubcomp => {
                    pending.pubrel_sent.unwrap_or(pending.publish.last_sent)
                }
            };

            if now.duration_since(last_sent) >= retry_interval {
                match pending.state {
                    Qos2OutState::AwaitingPubrec => {
                        // Re-send PUBLISH with DUP=1
                        let publish = Publish {
                            dup: true,
                            qos: pending.publish.qos,
                            retain: pending.publish.retain,
                            topic: pending.publish.topic.clone(),
                            packet_id: Some(pending.publish.packet_id),
                            payload: pending.publish.payload.clone(),
                            properties: None,
                        };
                        encode_packet(&Packet::Publish(publish), &mut self.write_buf);
                        pending.publish.mark_resent();
                    }
                    Qos2OutState::AwaitingPubcomp => {
                        // Re-send PUBREL
                        encode_packet(&Packet::Pubrel { packet_id: pending.publish.packet_id }, &mut self.write_buf);
                        pending.pubrel_sent = Some(now);
                    }
                }
            }
        }

        Ok(())
    }

    /// Re-send all pending messages after reconnect.
    /// Per [MQTT-4.4.0-1] and [MQTT-4.6.0-1].
    fn resend_pending_messages(&mut self) -> Result<()> {
        // Collect messages to resend (to avoid borrow issues)
        let messages: Vec<_> = self.session.messages_to_resend().collect();

        for msg in messages {
            match msg {
                crate::session::ResendMessage::Publish {
                    packet_id,
                    topic,
                    payload,
                    qos,
                    retain,
                    dup,
                } => {
                    let publish = Publish {
                        dup,
                        qos,
                        retain,
                        topic,
                        packet_id: Some(packet_id),
                        payload,
                        properties: None,
                    };
                    encode_packet(&Packet::Publish(publish), &mut self.write_buf);

                    // Update last_sent time
                    if let Some(p) = self.session.get_qos1_pending_mut(packet_id) {
                        p.mark_resent();
                    }
                    if let Some(p) = self.session.get_qos2_out_mut(packet_id) {
                        p.publish.mark_resent();
                    }
                }
                crate::session::ResendMessage::Pubrel { packet_id } => {
                    encode_packet(&Packet::Pubrel { packet_id }, &mut self.write_buf);
                    if let Some(p) = self.session.get_qos2_out_mut(packet_id) {
                        p.pubrel_sent = Some(Instant::now());
                    }
                }
            }
        }

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

