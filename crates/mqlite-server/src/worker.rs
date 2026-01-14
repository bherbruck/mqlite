//! Worker thread for handling MQTT client connections.
//!
//! Each worker owns:
//! - Its own mio Poll instance
//! - A HashMap of clients assigned to this worker
//! - A channel receiver for control messages (NewClient, Disconnect, Shutdown)
//!
//! Cross-thread publish delivery:
//! - Any thread can write directly to any client's buffer (mutex-protected)
//! - epoll_ctl is thread-safe, so we update poll interest directly
//! - No channels needed for publish delivery

use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::AHashMap;
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use mio::{Events, Interest, Poll, Token};

use mqlite_core::error::{ProtocolError, Result};
use mqlite_core::packet::{
    ConnackCode, Connect, Packet, Publish, Subscribe, Unsuback, Unsubscribe,
};

use crate::auth::AuthProvider;
use crate::cleanup;
use crate::client::{Client, ClientState, Transport};
use crate::client_handle::ClientWriteHandle;
use crate::config::Config;
use crate::fanout::{
    fanout_to_subscribers, fanout_will_to_subscribers, log_backpressure, log_hardlimit,
    write_cross_worker_pending, FanoutContext,
};
use crate::handlers::connect as connect_handler;
use crate::handlers::disconnect as disconnect_handler;
use crate::handlers::publish as publish_handler;
use crate::handlers::qos as qos_handler;
use crate::handlers::subscribe as subscribe_handler;
use crate::publish_encoder::PublishEncoder;
use crate::route_cache::RouteCache;
use crate::shared::{ClientLocation, Session, SharedStateHandle, StoredSubscription};
use crate::subscription::Subscriber;
use crate::util::RateLimitedCounter;
use crate::will::{self, WillManager};

/// Messages sent to workers via channels (control plane only).
/// Publish delivery uses direct writes, not channels.
#[allow(dead_code)] // Shutdown variant reserved for graceful shutdown
pub enum WorkerMsg {
    /// New connection from main thread.
    NewClient {
        transport: Transport,
        addr: SocketAddr,
        /// Bytes remaining after PROXY header (prepended to MQTT read buffer).
        preamble: Vec<u8>,
    },
    /// Disconnect a client (for client takeover).
    Disconnect { token: Token },
    /// Shutdown signal.
    Shutdown,
}

/// Starting token for client connections within this worker.
const CLIENT_START: usize = 1;

/// Worker thread that handles a subset of client connections.
pub struct Worker {
    /// Worker ID (0-indexed).
    pub id: usize,
    /// mio Poll instance for this worker.
    poll: Poll,
    /// The epoll fd (for passing to ClientWriteHandle).
    epoll_fd: i32,
    /// Clients owned by this worker.
    clients: AHashMap<Token, Client>,
    /// Maps Token to client handle for subscription management.
    token_to_handle: AHashMap<Token, Arc<ClientWriteHandle>>,
    /// Maps Token to ClientId for session lookup during cleanup.
    token_to_client_id: AHashMap<Token, String>,
    /// Next token for new clients.
    next_token: usize,
    /// Shared state across all workers.
    shared: SharedStateHandle,
    /// Channel receiver for control messages from other threads.
    rx: Receiver<WorkerMsg>,
    /// Senders to all workers for control messages (Disconnect, Shutdown).
    worker_senders: Vec<Sender<WorkerMsg>>,

    /// Reusable buffer for subscription matching results.
    subscriber_buf: Vec<Subscriber>,

    /// Route cache for topic-to-subscriber lookups.
    route_cache: RouteCache,

    /// Last seen subscription generation (for detecting stale cache entries).
    last_sub_generation: u64,

    /// Will message manager for delayed publication (MQTT v5 Will Delay Interval).
    will_manager: WillManager,

    /// Rate-limited subscriber backpressure logging (for cross-thread drops).
    subscriber_backpressure_log: RateLimitedCounter,

    /// Rate-limited hard limit logging (for 16MB buffer exceeded).
    hardlimit_log: RateLimitedCounter,

    /// Broker configuration.
    config: Arc<Config>,

    /// Authentication and authorization provider.
    auth: AuthProvider,

    /// Next packet ID for offline message delivery.
    next_offline_packet_id: u16,
}

impl Worker {
    /// Create a new worker.
    pub fn new(
        id: usize,
        shared: SharedStateHandle,
        rx: Receiver<WorkerMsg>,
        worker_senders: Vec<Sender<WorkerMsg>>,
        config: Arc<Config>,
    ) -> Result<Self> {
        let poll = Poll::new()?;
        let epoll_fd = poll.as_raw_fd();

        Ok(Self {
            id,
            poll,
            epoll_fd,
            clients: AHashMap::new(),
            token_to_handle: AHashMap::new(),
            token_to_client_id: AHashMap::new(),
            next_token: CLIENT_START,
            shared,
            rx,
            worker_senders,
            subscriber_buf: Vec::with_capacity(1024),
            route_cache: RouteCache::new(id),
            last_sub_generation: 0,
            will_manager: WillManager::new(),
            subscriber_backpressure_log: RateLimitedCounter::new(Duration::from_secs(10)),
            hardlimit_log: RateLimitedCounter::new(Duration::from_secs(10)),
            auth: AuthProvider::from_config(&config),
            config,
            next_offline_packet_id: 1,
        })
    }

    /// Run the worker event loop (blocking).
    #[allow(dead_code)]
    pub fn run(&mut self) -> Result<()> {
        loop {
            self.run_once()?;
        }
    }

    /// Run a single iteration of the worker event loop.
    pub fn run_once(&mut self) -> Result<()> {
        let mut events = Events::with_capacity(1024);

        // Determine poll timeout: use shorter of default and delayed will timeout
        let default_timeout = Duration::from_millis(10);
        let timeout = match self.will_manager.next_timeout() {
            Some(will_timeout) => std::cmp::min(default_timeout, will_timeout),
            None => default_timeout,
        };

        self.poll.poll(&mut events, Some(timeout))?;

        // Process mio events
        for event in events.iter() {
            let token = event.token();

            // Check for socket errors/closure first (dead connection detection)
            if event.is_error() || event.is_read_closed() || event.is_write_closed() {
                if let Some(client) = self.clients.get_mut(&token) {
                    client.state = ClientState::Disconnecting;
                }
                continue;
            }

            if event.is_readable() {
                self.handle_readable(token)?;
            }
            if event.is_writable() {
                self.handle_writable(token)?;
            }
        }

        // Process channel messages (non-blocking) - control plane only
        // Publish delivery uses direct writes via ClientWriteHandle
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                WorkerMsg::NewClient {
                    transport,
                    addr,
                    preamble,
                } => {
                    let token = self.accept_client(transport, addr, preamble)?;
                    // With edge-triggered epoll, data pending before registration won't
                    // fire an event. Do an initial read to handle WebSocket connections
                    // where MQTT CONNECT may arrive during the blocking handshake.
                    self.handle_readable(token)?;
                }
                WorkerMsg::Disconnect { token } => {
                    if let Some(client) = self.clients.get_mut(&token) {
                        // Immediately save pending messages to session before disconnect.
                        // This is critical for cross-worker takeover: the new client may
                        // already be waiting to drain these from the session.
                        if !client.clean_session {
                            if let Some(ref client_id) = client.client_id {
                                let mut sessions = self.shared.sessions.write();
                                let session = sessions.entry(client_id.clone()).or_default();
                                connect_handler::save_pending_to_session(client, session);
                                session.takeover_complete = true;
                            }
                        }
                        // Clear pending messages since we saved them
                        client.pending_qos1.clear();
                        client.pending_qos2.clear();
                        client.state = ClientState::Disconnecting;
                    }
                }
                WorkerMsg::Shutdown => {
                    return Ok(());
                }
            }
        }

        // Clean up disconnected clients
        let had_disconnects = self.cleanup_clients();

        // Release Arc<ClientWriteHandle> references when clients disconnect.
        // Clear route cache and subscriber_buf to free WriteBuffer memory.
        // Also check for generation changes from OTHER workers' disconnects.
        let current_gen = self.shared.subscriptions.read().generation();
        if had_disconnects || current_gen != self.last_sub_generation {
            self.last_sub_generation = current_gen;
            // Clear entire cache - generation-based purge wasn't reliable
            self.route_cache.clear();
            self.subscriber_buf.clear();
        }

        // Publish any delayed will messages that are ready
        self.publish_delayed_wills();

        // Check keep-alive timeouts and shrink idle read buffers
        let now = Instant::now();
        for (_token, client) in &mut self.clients {
            if client.keep_alive > 0 && client.state == ClientState::Connected {
                let timeout = Duration::from_secs((client.keep_alive as u64 * 3) / 2);
                if now.duration_since(client.last_packet_time) > timeout {
                    client.state = ClientState::Disconnecting;
                }
            }
            // Shrink oversized read buffers that have been idle
            client.maybe_shrink_read_buffer();
        }

        // Periodically shrink oversized worker buffers to release memory.
        const SHRINK_THRESHOLD: usize = 4096;
        if self.subscriber_buf.capacity() > SHRINK_THRESHOLD && self.subscriber_buf.is_empty() {
            self.subscriber_buf.shrink_to(1024);
        }
        self.route_cache.maybe_shrink();

        Ok(())
    }

    /// Accept a new client connection.
    /// Returns the token assigned to the new client.
    fn accept_client(
        &mut self,
        mut transport: Transport,
        addr: SocketAddr,
        preamble: Vec<u8>,
    ) -> Result<Token> {
        let token = Token(self.next_token);
        self.next_token += 1;

        self.poll
            .registry()
            .register(transport.tcp_stream_mut(), token, Interest::READABLE)?;

        let mut client = Client::new(token, transport, addr, self.id, self.epoll_fd);

        // Prepend any remaining bytes from PROXY protocol parsing
        if !preamble.is_empty() {
            client.prepend_to_read_buffer(&preamble);
        }

        // Store handle for subscription management
        let handle = client.handle.clone();
        self.token_to_handle.insert(token, handle);
        self.clients.insert(token, client);

        // Track socket opened for $SYS metrics
        self.shared.metrics.increment_sockets_opened();

        Ok(token)
    }

    /// Get subscribers for a topic, using cache if valid.
    /// Populates self.subscriber_buf with deduplicated subscribers.
    #[inline]
    fn get_subscribers_cached(&mut self, topic: &Bytes) {
        self.route_cache
            .get_subscribers(topic, &self.shared, &mut self.subscriber_buf);
    }

    fn handle_readable(&mut self, token: Token) -> Result<()> {
        // Read data from socket
        {
            let Some(client) = self.clients.get_mut(&token) else {
                return Ok(());
            };

            if client.read().is_err() {
                client.state = ClientState::Disconnecting;
                return Ok(());
            }
        }

        // Process packets
        loop {
            let packet = {
                let Some(client) = self.clients.get_mut(&token) else {
                    return Ok(());
                };

                // MQTT-3.1.4-5: Don't process data after rejecting CONNECT
                if client.state == ClientState::Disconnecting {
                    return Ok(());
                }

                match client.decode_packet(self.config.limits.max_packet_size) {
                    Ok(Some(packet)) => {
                        client.last_packet_time = Instant::now();
                        // Track message received for $SYS metrics
                        self.shared.metrics.add_msgs_received(1);
                        packet
                    }
                    Ok(None) => break,
                    Err(_) => {
                        client.state = ClientState::Disconnecting;
                        return Ok(());
                    }
                }
            };

            if self.handle_packet(token, packet).is_err() {
                if let Some(client) = self.clients.get_mut(&token) {
                    client.state = ClientState::Disconnecting;
                }
                return Ok(());
            }
        }

        // Note: queue_packet() already handles epoll interest via set_ready_for_writing()
        // No need to call update_interest/reregister here - it would conflict with raw epoll_ctl

        Ok(())
    }

    fn handle_writable(&mut self, token: Token) -> Result<()> {
        let Some(client) = self.clients.get_mut(&token) else {
            return Ok(());
        };

        if client.flush().is_err() {
            client.state = ClientState::Disconnecting;
            return Ok(());
        }

        // Note: flush() already handles epoll interest via set_ready_for_writing()
        // No need to call update_interest/reregister here

        Ok(())
    }

    fn handle_packet(&mut self, token: Token, packet: Packet) -> Result<()> {
        let client_state = self.clients.get(&token).map(|c| c.state);

        match packet {
            Packet::Connect(connect) => {
                if client_state != Some(ClientState::Connecting) {
                    if let Some(client) = self.clients.get_mut(&token) {
                        client.state = ClientState::Disconnecting;
                    }
                    return Ok(());
                }
                self.handle_connect(token, connect)?;
            }

            Packet::Publish(publish) => {
                if client_state != Some(ClientState::Connected) {
                    return Err(ProtocolError::FirstPacketNotConnect.into());
                }
                self.handle_publish(token, publish)?;
            }

            Packet::Puback { packet_id } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    qos_handler::handle_puback(client, packet_id);
                }
            }

            Packet::Pubrec { packet_id } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    qos_handler::handle_pubrec(client, packet_id);
                }
            }

            Packet::Pubrel { packet_id } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    qos_handler::handle_pubrel(client, packet_id);
                }
            }

            Packet::Pubcomp { packet_id } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    qos_handler::handle_pubcomp(client, packet_id);
                }
            }

            Packet::Subscribe(subscribe) => {
                if client_state != Some(ClientState::Connected) {
                    return Err(ProtocolError::FirstPacketNotConnect.into());
                }
                self.handle_subscribe(token, subscribe)?;
            }

            Packet::Unsubscribe(unsub) => {
                if client_state != Some(ClientState::Connected) {
                    return Err(ProtocolError::FirstPacketNotConnect.into());
                }
                self.handle_unsubscribe(token, unsub)?;
            }

            Packet::Pingreq => {
                if let Some(client) = self.clients.get_mut(&token) {
                    disconnect_handler::handle_pingreq(client);
                }
            }

            Packet::Disconnect { reason_code } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    disconnect_handler::handle_disconnect(client, reason_code);
                }
            }

            _ => {}
        }

        Ok(())
    }

    fn handle_connect(&mut self, token: Token, connect: Connect) -> Result<()> {
        let is_v5 = connect.protocol_version == 5;

        // Validate client ID
        if connect_handler::validate_client_id(&connect).is_err() {
            let client = self.clients.get_mut(&token).unwrap();
            client.protocol_version = connect.protocol_version;
            let connack = connect_handler::build_rejection_connack(
                ConnackCode::IdentifierRejected,
                0x85,
                is_v5,
            );
            let _ = client.queue_packet(&Packet::Connack(connack));
            client.state = ClientState::Disconnecting;
            return Ok(());
        }

        // Authentication
        let remote_addr = self.clients.get(&token).unwrap().remote_addr;
        let auth_result = connect_handler::authenticate_client(&connect, remote_addr, &self.auth);

        if !auth_result.allowed {
            let client = self.clients.get_mut(&token).unwrap();
            client.protocol_version = connect.protocol_version;
            let connack =
                connect_handler::build_auth_rejection_connack(&auth_result.auth_result, is_v5);
            let _ = client.queue_packet(&Packet::Connack(connack));
            client.state = ClientState::Disconnecting;
            log::debug!(
                "Authentication failed for client {:?} from {}: {:?}",
                connect.client_id,
                remote_addr,
                auth_result.auth_result
            );
            return Ok(());
        }

        // Store auth info
        {
            let client = self.clients.get_mut(&token).unwrap();
            client.username = connect.username.clone();
            client.role = auth_result.role;
            client.is_anonymous = connect.username.is_none() && connect.password.is_none();
        }

        // Handle client takeover (MQTT-3.1.4-2)
        let takeover_result = connect_handler::check_existing_client(
            &connect.client_id,
            token,
            self.id,
            &self.shared,
        );
        let old_location = takeover_result.as_ref().and_then(|r| r.old_location);

        if let Some(ref result) = takeover_result {
            if let Some(location) = result.old_location {
                if !result.is_cross_worker {
                    // Same worker - disconnect directly
                    if let Some(existing_client) = self.clients.get_mut(&location.token) {
                        if !existing_client.clean_session {
                            let mut sessions = self.shared.sessions.write();
                            let session = sessions.entry(connect.client_id.clone()).or_default();
                            connect_handler::save_pending_to_session(existing_client, session);
                            session.takeover_complete = true;
                        }
                        existing_client.state = ClientState::Disconnecting;
                    }
                } else {
                    // Cross-worker takeover
                    if !connect.clean_session {
                        let mut sessions = self.shared.sessions.write();
                        let session = sessions.entry(connect.client_id.clone()).or_default();
                        session.takeover_complete = false;
                    }
                    let _ = self.worker_senders[location.worker_id].send(WorkerMsg::Disconnect {
                        token: location.token,
                    });
                }
            }
        }

        connect_handler::register_client(&connect.client_id, token, self.id, &self.shared);

        // Handle session persistence
        let mut pending_to_resend: Vec<Publish> = Vec::new();
        let session_present = if !connect.clean_session && !connect.client_id.is_empty() {
            let is_cross_worker = old_location
                .map(|loc| loc.worker_id != self.id)
                .unwrap_or(false);

            if is_cross_worker {
                connect_handler::wait_for_takeover(&connect.client_id, &self.shared);
            }

            let mut sessions = self.shared.sessions.write();
            if let Some(session) = sessions.get_mut(&connect.client_id) {
                let session_last_connection = session.last_connection.take();
                session.takeover_complete = true;

                let local_old_tokens: Vec<Token> = self
                    .token_to_client_id
                    .iter()
                    .filter(|(_, cid)| *cid == &connect.client_id)
                    .map(|(t, _)| *t)
                    .collect();

                let subs_to_restore: Vec<StoredSubscription> = session.subscriptions.clone();
                pending_to_resend = connect_handler::collect_pending_messages(session);

                drop(sessions);

                // Restore subscriptions
                if let Some(handle) = self.token_to_handle.get(&token).cloned() {
                    let client_id: Arc<str> = Arc::from(connect.client_id.as_str());
                    let mut subs = self.shared.subscriptions.write();
                    connect_handler::restore_subscriptions(
                        &mut subs,
                        &subs_to_restore,
                        handle,
                        client_id,
                        old_location,
                        session_last_connection,
                        &local_old_tokens,
                        self.id,
                    );
                }

                for old_token in local_old_tokens {
                    self.token_to_client_id.remove(&old_token);
                }

                true
            } else {
                sessions.insert(connect.client_id.clone(), Session::default());
                false
            }
        } else {
            if !connect.client_id.is_empty() {
                self.shared.sessions.write().remove(&connect.client_id);
            }
            false
        };

        // Update client state
        let client = self.clients.get_mut(&token).unwrap();
        client.client_id = Some(connect.client_id.clone());
        client.keep_alive = if connect.keep_alive == 0 {
            self.config.session.default_keep_alive
        } else {
            connect.keep_alive.min(self.config.session.max_keep_alive)
        };
        client.clean_session = connect.clean_session;
        client.will = connect.will.clone();
        client.protocol_version = connect.protocol_version;
        client.handle.set_protocol_version(connect.protocol_version);
        client.state = ClientState::Connected;

        self.shared.metrics.client_connected();
        self.shared.metrics.increment_connections_total();

        if is_v5 {
            if let Some(ref props) = connect.properties {
                let client_recv_max = props.receive_maximum.unwrap_or(65535);
                let client_max_pkt = props.maximum_packet_size.unwrap_or(0);
                client.set_flow_control(client_recv_max, client_max_pkt);
            }
        }

        if !connect.clean_session && !connect.client_id.is_empty() {
            self.token_to_client_id
                .insert(token, connect.client_id.clone());
        }

        // Build and send CONNACK
        let assigned_client_id = if is_v5 && connect.client_id.is_empty() {
            let assigned_id = connect_handler::generate_client_id(token);
            client.client_id = Some(assigned_id.clone());
            self.shared.client_registry.write().insert(
                assigned_id.clone(),
                ClientLocation {
                    worker_id: self.id,
                    token,
                },
            );
            Some(assigned_id)
        } else {
            None
        };

        let connack = connect_handler::build_success_connack(
            session_present,
            assigned_client_id,
            &self.config,
            is_v5,
        );

        if let Err(e) = client.queue_control_packet(&Packet::Connack(connack)) {
            log::warn!(
                "Failed to queue CONNACK for client {:?}: {}",
                client.client_id,
                e
            );
        }

        // Resend pending messages
        connect_handler::resend_pending_messages(client, pending_to_resend);

        Ok(())
    }

    fn handle_subscribe(&mut self, token: Token, subscribe: Subscribe) -> Result<()> {
        let mut return_codes = Vec::with_capacity(subscribe.topics.len());
        let mut retained_to_send = Vec::new();

        let (client_id, clean_session, is_v5) = {
            let client = self.clients.get(&token);
            (
                client.and_then(|c| c.client_id.clone()),
                client.map(|c| c.clean_session).unwrap_or(true),
                client.map(|c| c.protocol_version == 5).unwrap_or(false),
            )
        };

        // Check subscription_identifiers support
        if subscribe.subscription_id.is_some() && !self.config.mqtt.subscription_identifiers {
            let error = subscribe_handler::SubscriptionError::SubscriptionIdNotSupported;
            for _ in &subscribe.topics {
                return_codes.push(error.to_return_code(is_v5));
            }
            if let Some(client) = self.clients.get_mut(&token) {
                subscribe_handler::send_suback(client, subscribe.packet_id, return_codes);
            }
            return Ok(());
        }

        let handle = self.token_to_handle.get(&token).cloned();
        let client_id_arc: Arc<str> = client_id
            .as_ref()
            .map(|s| Arc::from(s.as_str()))
            .unwrap_or_else(|| Arc::from(""));

        for (topic_filter, options) in &subscribe.topics {
            let (actual_filter, share_group) =
                subscribe_handler::parse_shared_subscription(topic_filter);

            // Validate subscription
            if let Err(e) = subscribe_handler::validate_subscription(
                &actual_filter,
                share_group.as_ref(),
                &self.config.mqtt,
                &self.config.limits,
            ) {
                return_codes.push(e.to_return_code(is_v5));
                continue;
            }

            // ACL check
            let acl_ok = {
                if let Some(client) = self.clients.get(&token) {
                    subscribe_handler::check_subscribe_acl(client, &actual_filter, &self.auth)
                        .is_ok()
                } else {
                    false
                }
            };

            if !acl_ok {
                return_codes.push(
                    subscribe_handler::SubscriptionError::NotAuthorized.to_return_code(is_v5),
                );
                log::debug!(
                    "ACL denied subscribe to '{}' for client {:?}",
                    actual_filter,
                    client_id
                );
                continue;
            }

            // Subscribe
            if let Some(ref h) = handle {
                self.shared.subscriptions.write().subscribe(
                    topic_filter,
                    Subscriber {
                        handle: h.clone(),
                        qos: options.qos,
                        client_id: client_id_arc.clone(),
                        options: *options,
                        subscription_id: subscribe.subscription_id,
                        share_group: share_group.clone(),
                    },
                );
            }
            return_codes.push(options.qos as u8);

            // Update session for persistent clients
            let mut skip_retained = options.retain_handling == 2;
            if !clean_session {
                if let Some(ref cid) = client_id {
                    let mut sessions = self.shared.sessions.write();
                    if let Some(session) = sessions.get_mut(cid) {
                        let result = subscribe_handler::update_session_subscription(
                            session,
                            topic_filter,
                            *options,
                            subscribe.subscription_id,
                        );
                        skip_retained = result.skip_retained;
                    }
                }
            }

            if skip_retained {
                continue;
            }

            // Collect retained messages
            let retained = self.shared.retained_messages.read();
            let matches = subscribe_handler::collect_retained_messages(
                &retained,
                &actual_filter,
                options.qos,
                options.retain_as_published,
                subscribe.subscription_id,
            );
            retained_to_send.extend(matches);
        }

        // Send SUBACK
        if let Some(client) = self.clients.get_mut(&token) {
            subscribe_handler::send_suback(client, subscribe.packet_id, return_codes);
        }

        // Send retained messages
        if let Some(client) = self.clients.get_mut(&token) {
            subscribe_handler::send_retained_messages(client, retained_to_send);
        }

        Ok(())
    }

    fn handle_unsubscribe(&mut self, token: Token, unsub: Unsubscribe) -> Result<()> {
        let (client_id, clean_session) = {
            let client = self.clients.get(&token);
            (
                client.and_then(|c| c.client_id.clone()),
                client.map(|c| c.clean_session).unwrap_or(true),
            )
        };

        for topic_filter in &unsub.topics {
            self.shared
                .subscriptions
                .write()
                .unsubscribe(topic_filter, self.id, token);

            if !clean_session {
                if let Some(ref cid) = client_id {
                    let mut sessions = self.shared.sessions.write();
                    if let Some(session) = sessions.get_mut(cid) {
                        session
                            .subscriptions
                            .retain(|s| s.topic_filter != *topic_filter);
                    }
                }
            }
        }

        if let Some(client) = self.clients.get_mut(&token) {
            // UNSUBACK: required response, drop if slow
            // For v5, include reason codes (0x00 = Success, 0x11 = No subscription existed)
            // Note: We always return Success as the broker removes the subscription if it exists
            let is_v5 = client.protocol_version == 5;
            let reason_codes = if is_v5 {
                // Return success (0x00) for each topic
                vec![0x00; unsub.topics.len()]
            } else {
                Vec::new()
            };
            // UNSUBACK is critical - client waiting for unsubscribe confirmation
            if let Err(e) = client.queue_control_packet(&Packet::Unsuback(Unsuback {
                packet_id: unsub.packet_id,
                reason_codes,
                is_v5,
            })) {
                log::warn!(
                    "Failed to queue UNSUBACK for client {:?}: {}",
                    client.client_id,
                    e
                );
            }
        }

        Ok(())
    }

    fn handle_publish(&mut self, from_token: Token, publish: Publish) -> Result<()> {
        // Track publish received for $SYS metrics
        self.shared.metrics.add_pub_msgs_received(1);
        self.shared
            .metrics
            .add_pub_bytes_received(publish.payload.len() as u64);

        // Validate publish against server constraints
        if publish_handler::validate_publish(
            &publish,
            &self.config.mqtt,
            self.config.limits.max_topic_length,
            self.config.limits.max_topic_levels,
        )
        .is_err()
        {
            if let Some(client) = self.clients.get_mut(&from_token) {
                client.state = ClientState::Disconnecting;
            }
            return Ok(());
        }

        // ACL check
        let acl_result = {
            if let Some(client) = self.clients.get(&from_token) {
                publish_handler::check_publish_acl(client, &publish.topic, &self.auth)
            } else {
                return Ok(());
            }
        };

        if !acl_result.allowed {
            log::debug!(
                "ACL denied publish to '{}' for client at token {:?}",
                String::from_utf8_lossy(&publish.topic),
                from_token
            );
            // MQTT v5: send ack even on denial; v3.1.1: silently drop
            if acl_result.is_v5 {
                if let Some(client) = self.clients.get_mut(&from_token) {
                    publish_handler::send_publisher_ack(client, &publish);
                }
            }
            return Ok(());
        }

        // Send PUBACK/PUBREC to publisher
        if let Some(client) = self.clients.get_mut(&from_token) {
            publish_handler::send_publisher_ack(client, &publish);
        }

        // Handle retained messages
        publish_handler::handle_retained(&publish, &self.shared);

        // Find matching subscribers (cached + shared)
        self.get_subscribers_cached(&publish.topic);
        {
            let topic_str = std::str::from_utf8(&publish.topic).unwrap_or("");
            let mut subscriptions = self.shared.subscriptions.write();
            subscriptions.match_shared_subscribers(topic_str, &mut self.subscriber_buf);
        }

        // Fanout to subscribers
        let mut factory = PublishEncoder::new(
            publish.topic.clone(),
            publish.payload.clone(),
            publish.properties.clone(),
        );

        let mut ctx = FanoutContext {
            worker_id: self.id,
            from_token: Some(from_token),
            clients: &mut self.clients,
            shared: &self.shared,
            factory: &mut factory,
            publish: &publish,
        };

        let (result, cross_worker_pending) = fanout_to_subscribers(&mut ctx, &self.subscriber_buf);

        // Log backpressure/hardlimit drops
        log_backpressure(
            &mut self.subscriber_backpressure_log,
            &result,
            "QoS 0 messages",
        );
        log_hardlimit(
            &mut self.hardlimit_log,
            &result,
            "messages (including QoS 1/2)",
        );

        // Write cross-worker pending messages
        write_cross_worker_pending(&self.shared, cross_worker_pending);

        // Deliver to offline persistent sessions
        self.next_offline_packet_id = publish_handler::deliver_to_offline_sessions(
            &publish,
            &self.shared,
            self.next_offline_packet_id,
        );

        Ok(())
    }

    /// Clean up disconnected clients. Returns true if any clients were cleaned up.
    fn cleanup_clients(&mut self) -> bool {
        let disconnected: Vec<Token> = self
            .clients
            .iter()
            .filter(|(_, c)| c.state == ClientState::Disconnecting)
            .map(|(t, _)| *t)
            .collect();

        if disconnected.is_empty() {
            return false;
        }

        let mut immediate_wills: Vec<Publish> = Vec::new();
        let mut delayed_wills: Vec<will::DelayedWill> = Vec::new();

        for &token in &disconnected {
            if let Some(mut client) = self.clients.remove(&token) {
                let _ = self
                    .poll
                    .registry()
                    .deregister(client.transport.tcp_stream_mut());

                // Track client disconnection for $SYS metrics
                // Only count if client was fully connected (has client_id assigned)
                if client.client_id.is_some() {
                    self.shared.metrics.client_disconnected();
                }

                // Clean up token maps to free memory (WriteBuffer, etc.)
                self.token_to_handle.remove(&token);
                self.token_to_client_id.remove(&token);

                // Always remove subscriptions from the trie on disconnect to free WriteBuffer memory.
                // For persistent sessions, subscriptions are saved in Session.subscriptions
                // and offline message delivery is handled during publish routing by checking
                // session subscriptions directly.
                self.shared
                    .subscriptions
                    .write()
                    .remove_client(self.id, token);

                // Remove this client from route cache to release Arc<ClientWriteHandle>.
                // Stale cache entries would otherwise hold onto dead client handles
                // until the same topic is published to again.
                self.route_cache.remove_client(self.id, token);

                if let Some(ref client_id) = client.client_id {
                    // Check if we're still the owner and remove from registry if so
                    let is_current_owner = cleanup::remove_from_registry_if_owner(
                        client_id,
                        token,
                        self.id,
                        &self.shared,
                    );

                    // Save pending messages for persistent sessions
                    if !client.clean_session {
                        log::debug!(
                            "cleanup_clients for {}: saving {} QoS1, {} QoS2 pending",
                            client_id,
                            client.pending_qos1.len(),
                            client.pending_qos2.len()
                        );
                        let (qos1, qos2) = cleanup::collect_pending_for_session(
                            &client.pending_qos1,
                            &client.pending_qos2,
                        );
                        cleanup::update_session_on_disconnect(
                            client_id,
                            token,
                            self.id,
                            is_current_owner,
                            &qos1,
                            &qos2,
                            &self.shared,
                        );
                    }
                }

                // Extract and process will message if non-graceful disconnect
                if !client.graceful_disconnect {
                    if let Some(client_will) = client.will.take() {
                        let (publish, delay) = will::extract_will(client_will);
                        if let Some(delay_duration) = delay {
                            delayed_wills.push(will::create_delayed_will(publish, delay_duration));
                        } else {
                            immediate_wills.push(publish);
                        }
                    }
                }
            }
        }

        // Publish immediate will messages using fanout module
        for will_publish in immediate_wills {
            self.publish_will_message(&will_publish);
        }

        // Schedule delayed wills for later publication
        self.will_manager.schedule_all(delayed_wills);

        true
    }

    /// Publish a single will message to subscribers.
    fn publish_will_message(&mut self, will_publish: &Publish) {
        // Store retained will message if applicable
        will::store_retained_will(will_publish, &self.shared);

        // Get subscribers for this topic
        self.get_subscribers_cached(&will_publish.topic);

        // Create encoder for zero-copy publish
        let mut factory = PublishEncoder::new(
            will_publish.topic.clone(),
            will_publish.payload.clone(),
            will_publish.properties.clone(),
        );

        // Fanout to all subscribers using the fanout module
        let (result, cross_worker_pending) = fanout_will_to_subscribers(
            self.id,
            &mut self.clients,
            &self.shared,
            &mut factory,
            will_publish,
            &self.subscriber_buf,
        );

        // Write cross-worker pending messages to sessions
        write_cross_worker_pending(&self.shared, cross_worker_pending);

        // Log any backpressure or hard limit drops
        log_backpressure(
            &mut self.subscriber_backpressure_log,
            &result,
            "will messages (QoS 0)",
        );
        log_hardlimit(
            &mut self.hardlimit_log,
            &result,
            "will messages (including QoS 1/2)",
        );
    }

    /// Publish any delayed will messages that are ready.
    fn publish_delayed_wills(&mut self) {
        // Take all ready wills from the manager
        let ready_wills = self.will_manager.take_ready();

        // Publish each ready will using the shared publish_will_message method
        for will_publish in ready_wills {
            self.publish_will_message(&will_publish);
        }
    }
}
