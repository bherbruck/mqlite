//! Async MQTT client implementation using tokio.
//!
//! Split architecture: `AsyncClient` (cloneable) + `EventLoop` (owns socket).
//!
//! ## Basic usage (raw events)
//!
//! ```ignore
//! let (client, mut eventloop) = AsyncClient::new(config, 10);
//! client.subscribe(&[("sensors/#", QoS::AtLeastOnce)]).await?;
//!
//! while let Ok(event) = eventloop.poll().await {
//!     if let Event::Message { topic, payload, .. } = event {
//!         println!("{}: {:?}", topic, payload);
//!     }
//! }
//! ```
//!
//! ## Per-subscription streams (recommended)
//!
//! ```ignore
//! let (client, mut eventloop) = AsyncClient::new(config, 10);
//!
//! // Each subscription gets its own stream
//! let mut sensors = client.subscribe_stream("sensors/#", QoS::AtLeastOnce).await?;
//! let mut commands = client.subscribe_stream("commands/#", QoS::AtLeastOnce).await?;
//!
//! // Handle in separate tasks
//! tokio::spawn(async move {
//!     while let Some(msg) = sensors.recv().await {
//!         println!("Sensor: {} = {:?}", msg.topic, msg.payload);
//!     }
//! });
//!
//! // Must still poll eventloop to drive I/O
//! while let Ok(_) = eventloop.poll().await {}
//! ```

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use rustls::pki_types::ServerName;

use crate::config::TlsConfig;

use mqlite_core::packet::{
    decode_packet, encode_packet, ConnackCode, Connect, Packet, Publish, QoS, Subscribe,
    SubscriptionOptions, Unsubscribe, Will as CoreWill,
};

/// Wrapper enum for async streams (plain TCP or TLS).
enum AsyncStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl AsyncRead for AsyncStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            AsyncStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            AsyncStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AsyncStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            AsyncStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            AsyncStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            AsyncStream::Plain(s) => Pin::new(s).poll_flush(cx),
            AsyncStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            AsyncStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            AsyncStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

use crate::config::ClientConfig;
use crate::error::{ClientError, Result};
use crate::packet_id::PacketIdAllocator;
use crate::session::{PendingPublish, Session};

const DEFAULT_BUFFER_SIZE: usize = 8192;
const DEFAULT_STREAM_CAPACITY: usize = 100;

/// A message received from a subscription stream.
#[derive(Debug, Clone)]
pub struct Message {
    /// Topic the message was published to.
    pub topic: String,
    /// Message payload.
    pub payload: Bytes,
    /// Quality of Service level.
    pub qos: QoS,
    /// Whether this is a retained message.
    pub retain: bool,
}

/// A stream of messages for a specific subscription.
///
/// Messages matching the subscription's topic filter are delivered here.
pub struct MessageStream {
    rx: mpsc::Receiver<Message>,
    filter: String,
}

impl MessageStream {
    /// Receive the next message.
    ///
    /// Returns `None` when the client disconnects or the stream is closed.
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    /// Try to receive a message without blocking.
    pub fn try_recv(&mut self) -> Option<Message> {
        self.rx.try_recv().ok()
    }

    /// Get the topic filter this stream is subscribed to.
    pub fn filter(&self) -> &str {
        &self.filter
    }
}

/// Events yielded by the EventLoop.
///
/// For advanced users who want direct access to all MQTT events.
/// Most users should prefer `subscribe_stream()` for message handling.
#[derive(Debug, Clone)]
pub enum Event {
    /// Connected to broker.
    Connected { session_present: bool },
    /// Received a message (only if no stream matched).
    Message {
        topic: String,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    },
    /// Publish acknowledged (QoS 1).
    PubAck { packet_id: u16 },
    /// Publish received (QoS 2 step 1).
    PubRec { packet_id: u16 },
    /// Publish complete (QoS 2 done).
    PubComp { packet_id: u16 },
    /// Subscribe acknowledged.
    SubAck {
        packet_id: u16,
        return_codes: Vec<u8>,
    },
    /// Unsubscribe acknowledged.
    UnsubAck { packet_id: u16 },
    /// Disconnected from broker.
    Disconnected,
    /// Attempting to reconnect (only when auto_reconnect is enabled).
    Reconnecting {
        /// Current reconnection attempt number (1-based).
        attempt: u32,
        /// Delay before this attempt.
        delay: Duration,
    },
}

/// Commands sent from AsyncClient to EventLoop.
enum Command {
    Publish {
        topic: String,
        payload: Bytes,
        qos: QoS,
        retain: bool,
        resp: oneshot::Sender<Result<Option<u16>>>,
    },
    Subscribe {
        topics: Vec<(String, QoS)>,
        resp: oneshot::Sender<Result<u16>>,
    },
    SubscribeStream {
        filter: String,
        qos: QoS,
        tx: mpsc::Sender<Message>,
        resp: oneshot::Sender<Result<u16>>,
    },
    Unsubscribe {
        topics: Vec<String>,
        resp: oneshot::Sender<Result<u16>>,
    },
    Disconnect,
}

/// Async MQTT client handle.
///
/// This is the user-facing API. It's `Clone` and can be shared across tasks.
/// Commands are sent to the `EventLoop` via a channel.
#[derive(Clone)]
pub struct AsyncClient {
    tx: mpsc::Sender<Command>,
}

impl AsyncClient {
    /// Create a new client and eventloop pair.
    ///
    /// `cap` is the command channel capacity (10 is usually fine).
    pub fn new(config: ClientConfig, cap: usize) -> (Self, EventLoop) {
        let (tx, rx) = mpsc::channel(cap);
        let client = Self { tx };
        let eventloop = EventLoop::new(config, rx);
        (client, eventloop)
    }

    /// Subscribe and get a dedicated message stream.
    ///
    /// This is the recommended way to handle messages. Each subscription
    /// gets its own stream, so you can handle different topics in different
    /// tasks without manual topic matching.
    ///
    /// ```ignore
    /// let mut sensors = client.subscribe_stream("sensors/#", QoS::AtLeastOnce).await?;
    ///
    /// while let Some(msg) = sensors.recv().await {
    ///     println!("{}: {:?}", msg.topic, msg.payload);
    /// }
    /// ```
    pub async fn subscribe_stream(&self, filter: &str, qos: QoS) -> Result<MessageStream> {
        let (msg_tx, msg_rx) = mpsc::channel(DEFAULT_STREAM_CAPACITY);
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx
            .send(Command::SubscribeStream {
                filter: filter.to_string(),
                qos,
                tx: msg_tx,
                resp: resp_tx,
            })
            .await
            .map_err(|_| ClientError::ConnectionClosed)?;

        resp_rx.await.map_err(|_| ClientError::ConnectionClosed)??;

        Ok(MessageStream {
            rx: msg_rx,
            filter: filter.to_string(),
        })
    }

    /// Publish a message.
    ///
    /// Returns the packet ID for QoS > 0, or None for QoS 0.
    pub async fn publish(
        &self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<Option<u16>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Publish {
                topic: topic.to_string(),
                payload: Bytes::copy_from_slice(payload),
                qos,
                retain,
                resp: resp_tx,
            })
            .await
            .map_err(|_| ClientError::ConnectionClosed)?;

        resp_rx.await.map_err(|_| ClientError::ConnectionClosed)?
    }

    /// Subscribe to topics (raw API).
    ///
    /// For most use cases, prefer `subscribe_stream()` which gives you a
    /// dedicated message stream for each subscription.
    pub async fn subscribe(&self, topics: &[(&str, QoS)]) -> Result<u16> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Subscribe {
                topics: topics
                    .iter()
                    .map(|(t, q)| (t.to_string(), *q))
                    .collect(),
                resp: resp_tx,
            })
            .await
            .map_err(|_| ClientError::ConnectionClosed)?;

        resp_rx.await.map_err(|_| ClientError::ConnectionClosed)?
    }

    /// Unsubscribe from topics.
    pub async fn unsubscribe(&self, topics: &[&str]) -> Result<u16> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Unsubscribe {
                topics: topics.iter().map(|t| t.to_string()).collect(),
                resp: resp_tx,
            })
            .await
            .map_err(|_| ClientError::ConnectionClosed)?;

        resp_rx.await.map_err(|_| ClientError::ConnectionClosed)?
    }

    /// Disconnect from the broker.
    pub async fn disconnect(&self) -> Result<()> {
        let _ = self.tx.send(Command::Disconnect).await;
        Ok(())
    }
}

/// Registered subscription stream.
struct StreamSubscription {
    filter: String,
    tx: mpsc::Sender<Message>,
}

/// The event loop that drives MQTT I/O.
///
/// You must call `poll()` repeatedly to process packets.
pub struct EventLoop {
    config: ClientConfig,
    rx: mpsc::Receiver<Command>,
    stream: Option<AsyncStream>,
    read_buf: BytesMut,
    write_buf: Vec<u8>,
    session: Session,
    packet_ids: PacketIdAllocator,
    last_packet_time: Instant,
    pending_pings: u8,
    connected: bool,
    /// Registered subscription streams for message routing.
    streams: Vec<StreamSubscription>,
    /// Cached TLS config for reconnection.
    tls_connector: Option<Arc<TlsConnector>>,
    /// Reconnection state for auto-reconnect.
    reconnect_attempt: u32,
    /// Current backoff delay for reconnection.
    reconnect_delay: Duration,
    /// Whether we should attempt to reconnect.
    should_reconnect: bool,
}

impl EventLoop {
    fn new(config: ClientConfig, rx: mpsc::Receiver<Command>) -> Self {
        let session = Session::new(config.client_id.clone(), config.clean_session);

        // Pre-build TLS connector if TLS is enabled
        let tls_connector = if config.tls.enabled {
            match build_tls_config(&config.tls) {
                Ok(tls_config) => Some(Arc::new(TlsConnector::from(Arc::new(tls_config)))),
                Err(e) => {
                    log::warn!("Failed to build TLS config: {}, will retry on connect", e);
                    None
                }
            }
        } else {
            None
        };

        let initial_delay = config.reconnect_backoff.initial_delay;

        Self {
            config,
            rx,
            stream: None,
            read_buf: BytesMut::with_capacity(DEFAULT_BUFFER_SIZE),
            write_buf: Vec::with_capacity(DEFAULT_BUFFER_SIZE),
            session,
            packet_ids: PacketIdAllocator::new(),
            last_packet_time: Instant::now(),
            pending_pings: 0,
            connected: false,
            streams: Vec::new(),
            tls_connector,
            reconnect_attempt: 0,
            reconnect_delay: initial_delay,
            should_reconnect: false,
        }
    }

    /// Poll for the next event.
    ///
    /// This drives all I/O. You must call this in a loop.
    ///
    /// If you're using `subscribe_stream()`, messages are automatically routed
    /// to their streams and won't appear as `Event::Message`. Only unmatched
    /// messages (from raw `subscribe()` calls) appear as events.
    ///
    /// When `auto_reconnect` is enabled, this will automatically attempt to
    /// reconnect on disconnection with exponential backoff. The `Reconnecting`
    /// event is emitted before each attempt.
    pub async fn poll(&mut self) -> Result<Event> {
        // Handle reconnection with backoff
        if self.stream.is_none() && self.should_reconnect {
            self.reconnect_attempt += 1;
            let delay = self.reconnect_delay;

            // Emit reconnecting event before delay
            let event = Event::Reconnecting {
                attempt: self.reconnect_attempt,
                delay,
            };

            // Wait for backoff delay
            tokio::time::sleep(delay).await;

            // Calculate next delay with exponential backoff
            let next_delay = Duration::from_secs_f64(
                delay.as_secs_f64() * self.config.reconnect_backoff.multiplier
            );
            self.reconnect_delay = next_delay.min(self.config.reconnect_backoff.max_delay);

            // Try to reconnect
            match self.connect().await {
                Ok(()) => {
                    // Reset backoff on success
                    self.reconnect_attempt = 0;
                    self.reconnect_delay = self.config.reconnect_backoff.initial_delay;
                    self.should_reconnect = false;
                    return Ok(Event::Connected {
                        session_present: !self.session.clean_session,
                    });
                }
                Err(e) => {
                    log::warn!("Reconnect attempt {} failed: {}", self.reconnect_attempt, e);
                    // Will retry on next poll
                    return Ok(event);
                }
            }
        }

        // Initial connection
        if self.stream.is_none() {
            self.connect().await?;
            // Reset reconnect state on successful initial connection
            self.reconnect_attempt = 0;
            self.reconnect_delay = self.config.reconnect_backoff.initial_delay;
            return Ok(Event::Connected {
                session_present: false,
            });
        }

        let keep_alive = if self.config.keep_alive > 0 {
            Duration::from_secs(self.config.keep_alive as u64)
        } else {
            Duration::from_secs(60)
        };

        loop {
            // Flush pending writes
            if !self.write_buf.is_empty() {
                if let Some(stream) = &mut self.stream {
                    stream
                        .write_all(&self.write_buf)
                        .await
                        .map_err(ClientError::Io)?;
                    self.write_buf.clear();
                }
            }

            // Try to parse buffered packets first
            if let Some(event) = self.try_parse_event().await? {
                return Ok(event);
            }

            // Select: socket read | channel recv | timeout
            let mut buf = [0u8; 4096];

            enum Action {
                Read(std::io::Result<usize>),
                Command(Option<Command>),
                Timeout,
            }

            let action = {
                let stream = self.stream.as_mut().unwrap();
                tokio::select! {
                    result = stream.read(&mut buf) => Action::Read(result),
                    cmd = self.rx.recv() => Action::Command(cmd),
                    _ = tokio::time::sleep(keep_alive) => Action::Timeout,
                }
            };

            match action {
                Action::Read(result) => {
                    let n = result.map_err(ClientError::Io)?;
                    if n == 0 {
                        // Connection closed by peer
                        self.handle_unexpected_disconnect();
                        return Ok(Event::Disconnected);
                    }
                    self.read_buf.extend_from_slice(&buf[..n]);
                    self.last_packet_time = Instant::now();
                }
                Action::Command(cmd) => match cmd {
                    Some(cmd) => self.handle_command(cmd)?,
                    None => {
                        // Client handle dropped - clean disconnect
                        self.disconnect_internal();
                        return Ok(Event::Disconnected);
                    }
                },
                Action::Timeout => {
                    if self.pending_pings >= 2 {
                        // Keep-alive timeout - unexpected disconnect
                        self.handle_unexpected_disconnect();
                        if self.config.auto_reconnect {
                            return Ok(Event::Disconnected);
                        }
                        return Err(ClientError::ConnectionClosed);
                    }
                    encode_packet(&Packet::Pingreq, &mut self.write_buf);
                    self.pending_pings += 1;
                }
            }
        }
    }

    async fn connect(&mut self) -> Result<()> {
        let tcp_stream = tokio::time::timeout(
            self.config.connect_timeout,
            TcpStream::connect(&self.config.address),
        )
        .await
        .map_err(|_| ClientError::ConnectionTimeout)?
        .map_err(ClientError::Io)?;

        tcp_stream.set_nodelay(true).map_err(ClientError::Io)?;

        // Wrap in TLS if enabled
        let stream = if self.config.tls.enabled {
            // Build TLS connector if not cached
            let connector = match &self.tls_connector {
                Some(c) => c.clone(),
                None => {
                    let tls_config = build_tls_config(&self.config.tls)?;
                    let connector = Arc::new(TlsConnector::from(Arc::new(tls_config)));
                    self.tls_connector = Some(connector.clone());
                    connector
                }
            };

            // Extract hostname for SNI
            let hostname = self.config.tls.server_name.as_deref()
                .unwrap_or_else(|| {
                    self.config.address.split(':').next().unwrap_or("localhost")
                });

            let server_name = ServerName::try_from(hostname.to_string())
                .map_err(|_| ClientError::Tls(format!("Invalid server name: {}", hostname)))?;

            let tls_stream = tokio::time::timeout(
                self.config.connect_timeout,
                connector.connect(server_name, tcp_stream),
            )
            .await
            .map_err(|_| ClientError::ConnectionTimeout)?
            .map_err(|e| ClientError::Tls(e.to_string()))?;

            AsyncStream::Tls(tls_stream)
        } else {
            AsyncStream::Plain(tcp_stream)
        };

        self.stream = Some(stream);

        // Send CONNECT
        self.send_connect().await?;

        // Wait for CONNACK
        self.wait_connack().await?;

        self.connected = true;
        self.last_packet_time = Instant::now();
        Ok(())
    }

    async fn send_connect(&mut self) -> Result<()> {
        let will = self.config.will.as_ref().map(|w| CoreWill {
            topic: w.topic.clone(),
            message: w.payload.to_vec(),
            qos: w.qos,
            retain: w.retain,
            properties: None,
        });

        let connect = Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: self.config.protocol_version,
            clean_session: self.config.clean_session,
            keep_alive: self.config.keep_alive,
            client_id: self.config.client_id.clone(),
            username: self.config.username.clone(),
            password: self.config.password.clone(),
            will,
            properties: None,
        };

        encode_packet(&Packet::Connect(connect), &mut self.write_buf);

        if let Some(stream) = &mut self.stream {
            stream
                .write_all(&self.write_buf)
                .await
                .map_err(ClientError::Io)?;
            self.write_buf.clear();
        }

        Ok(())
    }

    async fn wait_connack(&mut self) -> Result<()> {
        let stream = self.stream.as_mut().ok_or(ClientError::NotConnected)?;

        let result = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).await.map_err(ClientError::Io)?;
                if n == 0 {
                    return Err(ClientError::ConnectionClosed);
                }
                self.read_buf.extend_from_slice(&buf[..n]);

                if let Some((packet, consumed)) =
                    decode_packet(&self.read_buf, self.config.protocol_version, 0).map_err(
                        |e| {
                            ClientError::Protocol(mqlite_core::ProtocolError::MalformedPacket(
                                e.to_string(),
                            ))
                        },
                    )?
                {
                    let _ = self.read_buf.split_to(consumed);

                    if let Packet::Connack(connack) = packet {
                        if connack.code != ConnackCode::Accepted {
                            return Err(ClientError::ConnectionRefused(format!(
                                "{:?}",
                                connack.code
                            )));
                        }
                        return Ok(());
                    }
                }
            }
        })
        .await;

        result.map_err(|_| ClientError::ConnectionTimeout)?
    }

    fn handle_command(&mut self, cmd: Command) -> Result<()> {
        match cmd {
            Command::Publish {
                topic,
                payload,
                qos,
                retain,
                resp,
            } => {
                let result = self.do_publish(&topic, payload, qos, retain);
                let _ = resp.send(result);
            }
            Command::Subscribe { topics, resp } => {
                let result = self.do_subscribe(&topics);
                let _ = resp.send(result);
            }
            Command::SubscribeStream { filter, qos, tx, resp } => {
                let result = self.do_subscribe_stream(filter, qos, tx);
                let _ = resp.send(result);
            }
            Command::Unsubscribe { topics, resp } => {
                let result = self.do_unsubscribe(&topics);
                let _ = resp.send(result);
            }
            Command::Disconnect => {
                self.disconnect_internal();
            }
        }
        Ok(())
    }

    fn do_publish(
        &mut self,
        topic: &str,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<Option<u16>> {
        if !self.connected {
            return Err(ClientError::NotConnected);
        }

        let packet_id = if qos != QoS::AtMostOnce {
            Some(self.packet_ids.allocate().ok_or_else(|| {
                ClientError::InvalidState("All packet IDs in use".to_string())
            })?)
        } else {
            None
        };

        let topic_bytes = Bytes::from(topic.to_string());

        let publish = Publish {
            dup: false,
            qos,
            retain,
            topic: topic_bytes.clone(),
            packet_id,
            payload: payload.clone(),
            properties: None,
        };

        encode_packet(&Packet::Publish(publish), &mut self.write_buf);

        if let Some(id) = packet_id {
            let pending = PendingPublish::new(id, topic_bytes, payload, qos, retain);
            match qos {
                QoS::AtLeastOnce => self.session.add_qos1_pending(pending),
                QoS::ExactlyOnce => self.session.add_qos2_pending(pending),
                QoS::AtMostOnce => {}
            }
        }

        Ok(packet_id)
    }

    fn do_subscribe(&mut self, topics: &[(String, QoS)]) -> Result<u16> {
        if !self.connected {
            return Err(ClientError::NotConnected);
        }

        let packet_id = self.packet_ids.allocate().ok_or_else(|| {
            ClientError::InvalidState("All packet IDs in use".to_string())
        })?;

        let subscribe = Subscribe {
            packet_id,
            topics: topics
                .iter()
                .map(|(topic, qos)| {
                    (
                        topic.clone(),
                        SubscriptionOptions {
                            qos: *qos,
                            no_local: false,
                            retain_as_published: false,
                            retain_handling: 0,
                        },
                    )
                })
                .collect(),
            subscription_id: None,
        };

        for (topic, qos) in topics {
            self.session.add_subscription(topic.clone(), *qos);
        }

        encode_packet(&Packet::Subscribe(subscribe), &mut self.write_buf);
        Ok(packet_id)
    }

    fn do_subscribe_stream(
        &mut self,
        filter: String,
        qos: QoS,
        tx: mpsc::Sender<Message>,
    ) -> Result<u16> {
        // First, send the SUBSCRIBE to the broker
        let packet_id = self.do_subscribe(&[(filter.clone(), qos)])?;

        // Register the stream for routing
        self.streams.push(StreamSubscription { filter, tx });

        Ok(packet_id)
    }

    fn do_unsubscribe(&mut self, topics: &[String]) -> Result<u16> {
        if !self.connected {
            return Err(ClientError::NotConnected);
        }

        let packet_id = self.packet_ids.allocate().ok_or_else(|| {
            ClientError::InvalidState("All packet IDs in use".to_string())
        })?;

        let unsubscribe = Unsubscribe {
            packet_id,
            topics: topics.to_vec(),
        };

        for topic in topics {
            self.session.remove_subscription(topic);
            // Also remove from streams
            self.streams.retain(|s| s.filter != *topic);
        }

        encode_packet(&Packet::Unsubscribe(unsubscribe), &mut self.write_buf);
        Ok(packet_id)
    }

    fn disconnect_internal(&mut self) {
        if self.connected {
            encode_packet(&Packet::Disconnect { reason_code: 0 }, &mut self.write_buf);
        }
        self.connected = false;
        // Don't trigger auto-reconnect for clean disconnects
    }

    /// Handle an unexpected disconnection, triggering auto-reconnect if enabled.
    fn handle_unexpected_disconnect(&mut self) {
        self.connected = false;
        self.stream = None;
        self.read_buf.clear();
        self.pending_pings = 0;

        if self.config.auto_reconnect {
            self.should_reconnect = true;
        }
    }

    async fn try_parse_event(&mut self) -> Result<Option<Event>> {
        if self.read_buf.is_empty() {
            return Ok(None);
        }

        match decode_packet(&self.read_buf, self.config.protocol_version, 0) {
            Ok(Some((packet, consumed))) => {
                let _ = self.read_buf.split_to(consumed);
                self.last_packet_time = Instant::now();

                match packet {
                    Packet::Publish(publish) => {
                        self.handle_incoming_publish(&publish);

                        let topic = String::from_utf8_lossy(&publish.topic).to_string();
                        let msg = Message {
                            topic: topic.clone(),
                            payload: publish.payload,
                            qos: publish.qos,
                            retain: publish.retain,
                        };

                        // Try to route to a matching stream
                        if self.route_to_stream(&msg).await {
                            // Message was routed, don't emit as event
                            Ok(None)
                        } else {
                            // No stream matched, emit as event
                            Ok(Some(Event::Message {
                                topic: msg.topic,
                                payload: msg.payload,
                                qos: msg.qos,
                                retain: msg.retain,
                            }))
                        }
                    }
                    Packet::Puback { packet_id } => {
                        if self.session.complete_qos1(packet_id).is_some() {
                            self.packet_ids.release(packet_id);
                        }
                        Ok(Some(Event::PubAck { packet_id }))
                    }
                    Packet::Pubrec { packet_id } => {
                        if self.session.qos2_received_pubrec(packet_id) {
                            self.queue_pubrel(packet_id);
                        }
                        Ok(Some(Event::PubRec { packet_id }))
                    }
                    Packet::Pubrel { packet_id } => {
                        if self.session.complete_qos2_in(packet_id) {
                            self.queue_pubcomp(packet_id);
                        }
                        Ok(None)
                    }
                    Packet::Pubcomp { packet_id } => {
                        if self.session.complete_qos2_out(packet_id).is_some() {
                            self.packet_ids.release(packet_id);
                        }
                        Ok(Some(Event::PubComp { packet_id }))
                    }
                    Packet::Suback(suback) => {
                        self.packet_ids.release(suback.packet_id);
                        Ok(Some(Event::SubAck {
                            packet_id: suback.packet_id,
                            return_codes: suback.return_codes,
                        }))
                    }
                    Packet::Unsuback(unsuback) => {
                        self.packet_ids.release(unsuback.packet_id);
                        Ok(Some(Event::UnsubAck {
                            packet_id: unsuback.packet_id,
                        }))
                    }
                    Packet::Pingresp => {
                        self.pending_pings = 0;
                        Ok(None)
                    }
                    Packet::Disconnect { .. } => {
                        // Server-initiated disconnect - trigger reconnect if enabled
                        self.handle_unexpected_disconnect();
                        Ok(Some(Event::Disconnected))
                    }
                    _ => Ok(None),
                }
            }
            Ok(None) => Ok(None),
            Err(e) => {
                self.connected = false;
                Err(ClientError::Protocol(
                    mqlite_core::ProtocolError::MalformedPacket(e.to_string()),
                ))
            }
        }
    }

    /// Route a message to a matching stream.
    ///
    /// Returns true if the message was routed (matched a stream).
    async fn route_to_stream(&mut self, msg: &Message) -> bool {
        // Find first matching stream and send
        // We clone the message since we might need it for Event::Message
        for sub in &self.streams {
            if topic_matches_filter(&msg.topic, &sub.filter) {
                // Try to send, ignore if receiver dropped
                let _ = sub.tx.send(msg.clone()).await;
                return true;
            }
        }
        false
    }

    fn handle_incoming_publish(&mut self, publish: &Publish) {
        match publish.qos {
            QoS::AtLeastOnce => {
                if let Some(packet_id) = publish.packet_id {
                    self.queue_puback(packet_id);
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_id) = publish.packet_id {
                    if !self.session.has_qos2_inbound(packet_id) {
                        self.session.add_qos2_inbound(packet_id);
                    }
                    self.queue_pubrec(packet_id);
                }
            }
            QoS::AtMostOnce => {}
        }
    }

    fn queue_puback(&mut self, packet_id: u16) {
        self.write_buf.push(0x40);
        self.write_buf.push(2);
        self.write_buf.extend_from_slice(&packet_id.to_be_bytes());
    }

    fn queue_pubrec(&mut self, packet_id: u16) {
        self.write_buf.push(0x50);
        self.write_buf.push(2);
        self.write_buf.extend_from_slice(&packet_id.to_be_bytes());
    }

    fn queue_pubrel(&mut self, packet_id: u16) {
        self.write_buf.push(0x62);
        self.write_buf.push(2);
        self.write_buf.extend_from_slice(&packet_id.to_be_bytes());
    }

    fn queue_pubcomp(&mut self, packet_id: u16) {
        self.write_buf.push(0x70);
        self.write_buf.push(2);
        self.write_buf.extend_from_slice(&packet_id.to_be_bytes());
    }

    /// Check if connected to broker.
    pub fn is_connected(&self) -> bool {
        self.connected
    }
}

/// Check if a topic matches a filter pattern.
///
/// Supports MQTT wildcards:
/// - `+` matches a single level
/// - `#` matches multiple levels (must be last)
fn topic_matches_filter(topic: &str, filter: &str) -> bool {
    let topic_levels: Vec<&str> = topic.split('/').collect();
    let filter_levels: Vec<&str> = filter.split('/').collect();

    // MQTT-4.7.2-1: Topics starting with $ are not matched by wildcards at root level
    let topic_starts_with_dollar = topic_levels.first().is_some_and(|l| l.starts_with('$'));
    let filter_starts_with_wildcard = filter_levels
        .first()
        .is_some_and(|l| *l == "#" || *l == "+");

    if topic_starts_with_dollar && filter_starts_with_wildcard {
        return false;
    }

    let mut ti = 0;
    let mut fi = 0;

    while fi < filter_levels.len() {
        let filter_level = filter_levels[fi];

        if filter_level == "#" {
            // Multi-level wildcard matches everything from here
            return true;
        }

        if ti >= topic_levels.len() {
            // Topic has fewer levels than filter
            return false;
        }

        if filter_level == "+" {
            // Single-level wildcard matches any single level
            ti += 1;
            fi += 1;
        } else if filter_level == topic_levels[ti] {
            // Exact match
            ti += 1;
            fi += 1;
        } else {
            // No match
            return false;
        }
    }

    // Both must be fully consumed
    ti == topic_levels.len() && fi == filter_levels.len()
}

/// Build a rustls ClientConfig from TlsConfig.
fn build_tls_config(config: &TlsConfig) -> Result<rustls::ClientConfig> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls::RootCertStore;
    use std::fs::File;
    use std::io::BufReader;

    let mut root_store = RootCertStore::empty();

    // Load custom CA certificate if provided
    if let Some(ca_path) = &config.ca_cert {
        let file = File::open(ca_path)
            .map_err(|e| ClientError::Tls(format!("Failed to open CA cert: {}", e)))?;
        let mut reader = BufReader::new(file);

        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ClientError::Tls(format!("Failed to parse CA cert: {}", e)))?;

        for cert in certs {
            root_store.add(cert)
                .map_err(|e| ClientError::Tls(format!("Failed to add CA cert: {}", e)))?;
        }
    } else if !config.accept_invalid_certs {
        // Use system root certificates (unless we're accepting invalid certs)
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    // Handle insecure mode (accept any certificate)
    if config.accept_invalid_certs {
        let tls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertificateVerification))
            .with_no_client_auth();
        return Ok(tls_config);
    }

    let builder = rustls::ClientConfig::builder()
        .with_root_certificates(root_store);

    // Load client certificate for mutual TLS if provided
    let tls_config = if let (Some(cert_path), Some(key_path)) = (&config.client_cert, &config.client_key) {
        let cert_file = File::open(cert_path)
            .map_err(|e| ClientError::Tls(format!("Failed to open client cert: {}", e)))?;
        let mut cert_reader = BufReader::new(cert_file);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ClientError::Tls(format!("Failed to parse client cert: {}", e)))?;

        let key_file = File::open(key_path)
            .map_err(|e| ClientError::Tls(format!("Failed to open client key: {}", e)))?;
        let mut key_reader = BufReader::new(key_file);
        let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|e| ClientError::Tls(format!("Failed to parse client key: {}", e)))?
            .ok_or_else(|| ClientError::Tls("No private key found in file".to_string()))?;

        builder.with_client_auth_cert(certs, key)
            .map_err(|e| ClientError::Tls(format!("Failed to configure client auth: {}", e)))?
    } else {
        builder.with_no_client_auth()
    };

    Ok(tls_config)
}

/// Danger: A certificate verifier that accepts any certificate.
/// Only use for testing with self-signed certificates.
#[derive(Debug)]
struct NoCertificateVerification;

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_matching() {
        // Exact match
        assert!(topic_matches_filter("sensors/temp", "sensors/temp"));
        assert!(!topic_matches_filter("sensors/temp", "sensors/humidity"));

        // Single-level wildcard
        assert!(topic_matches_filter("sensors/temp", "sensors/+"));
        assert!(topic_matches_filter("sensors/humidity", "sensors/+"));
        assert!(!topic_matches_filter("sensors/room1/temp", "sensors/+"));

        // Multi-level wildcard
        assert!(topic_matches_filter("sensors/temp", "sensors/#"));
        assert!(topic_matches_filter("sensors/room1/temp", "sensors/#"));
        assert!(topic_matches_filter("sensors", "sensors/#"));

        // Combined
        assert!(topic_matches_filter("sensors/room1/temp", "sensors/+/temp"));
        assert!(topic_matches_filter("sensors/room2/temp", "sensors/+/temp"));
        assert!(!topic_matches_filter("sensors/room1/humidity", "sensors/+/temp"));

        // $ topics not matched by wildcards at root
        assert!(!topic_matches_filter("$SYS/broker/clients", "+/broker/clients"));
        assert!(!topic_matches_filter("$SYS/broker/clients", "#"));
        assert!(topic_matches_filter("$SYS/broker/clients", "$SYS/#"));
    }
}
