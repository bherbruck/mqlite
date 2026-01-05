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
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};

use crate::auth::{AuthContext, AuthProvider, AuthResult, ClientInfo};
use crate::client::{Client, ClientState, PendingPublish};
use crate::client_handle::ClientWriteHandle;
use crate::config::Config;
use crate::error::{ProtocolError, Result};
use crate::packet::{
    encode_variable_byte_integer, update_message_expiry, validate_topic, Connack, ConnackCode,
    ConnackProperties, Packet, Publish, QoS, Suback, Unsuback,
};
use crate::publish_encoder::PublishEncoder;
use crate::shared::{
    ClientLocation, RetainedMessage, Session, SharedStateHandle, StoredSubscription,
};
use crate::subscription::{topic_matches_filter, Subscriber};
use crate::util::RateLimitedCounter;

/// Messages sent to workers via channels (control plane only).
/// Publish delivery uses direct writes, not channels.
#[derive(Debug)]
#[allow(dead_code)] // Shutdown variant reserved for graceful shutdown
pub enum WorkerMsg {
    /// New connection from main thread.
    NewClient { socket: TcpStream, addr: SocketAddr },
    /// Disconnect a client (for client takeover).
    Disconnect { token: Token },
    /// Shutdown signal.
    Shutdown,
}

/// Starting token for client connections within this worker.
const CLIENT_START: usize = 1;

/// A will message scheduled for delayed publication.
#[derive(Debug)]
struct DelayedWill {
    /// When to publish the will message.
    publish_at: Instant,
    /// The will message to publish.
    publish: Publish,
}

/// Maximum number of cached routes per worker.
const ROUTE_CACHE_LIMIT: usize = 10_000;

/// Check cache effectiveness every N lookups.
const CACHE_EVAL_INTERVAL: u64 = 1000;

/// Minimum hit rate to keep cache enabled (50%).
const CACHE_MIN_HIT_RATE: f64 = 0.5;

/// Re-evaluate cache after this many lookups when disabled.
const CACHE_RETRY_INTERVAL: u64 = 10_000;

/// Cached subscription lookup result.
struct CachedRoute {
    /// The subscribers for this topic.
    subscribers: Vec<Subscriber>,
    /// Generation when this cache entry was computed.
    generation: u64,
}

/// Route cache statistics.
#[derive(Default)]
struct CacheStats {
    hits: u64,
    misses: u64,
    /// Whether caching is currently enabled.
    enabled: bool,
    /// Lookups since last evaluation.
    lookups_since_eval: u64,
    /// How many times cache has been disabled (for rate-limited logging).
    disabled_count: u64,
}

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

    // Reusable buffers to avoid allocation in hot paths
    /// Reusable buffer for subscription matching results.
    subscriber_buf: Vec<Subscriber>,
    /// Reusable buffer for subscriber deduplication.
    dedup_buf: AHashMap<(usize, Token), Subscriber>,

    /// Route cache: topic -> cached subscribers.
    /// Validated by generation counter on each lookup.
    route_cache: AHashMap<Bytes, CachedRoute>,

    /// Cache statistics for adaptive caching.
    cache_stats: CacheStats,

    /// Wills scheduled for delayed publication (MQTT v5 Will Delay Interval).
    delayed_wills: Vec<DelayedWill>,

    /// Rate-limited subscriber backpressure logging (for cross-thread drops).
    subscriber_backpressure_log: RateLimitedCounter,

    /// Broker configuration.
    config: Arc<Config>,

    /// Authentication and authorization provider.
    auth: AuthProvider,
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
            dedup_buf: AHashMap::with_capacity(1024),
            route_cache: AHashMap::with_capacity(1024),
            cache_stats: CacheStats {
                hits: 0,
                misses: 0,
                enabled: true, // Start with cache enabled
                lookups_since_eval: 0,
                disabled_count: 0,
            },
            delayed_wills: Vec::new(),
            subscriber_backpressure_log: RateLimitedCounter::new(Duration::from_secs(10)),
            auth: AuthProvider::from_config(&config),
            config,
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
        let timeout = match self.next_delayed_will_timeout() {
            Some(will_timeout) => std::cmp::min(default_timeout, will_timeout),
            None => default_timeout,
        };

        self.poll.poll(&mut events, Some(timeout))?;

        // Process mio events
        for event in events.iter() {
            let token = event.token();
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
                WorkerMsg::NewClient { socket, addr } => {
                    self.accept_client(socket, addr)?;
                }
                WorkerMsg::Disconnect { token } => {
                    if let Some(client) = self.clients.get_mut(&token) {
                        // Immediately save pending messages to session before disconnect.
                        // This is critical for cross-worker takeover: the new client may
                        // already be waiting to drain these from the session.
                        if !client.clean_session {
                            if let Some(ref client_id) = client.client_id {
                                let mut sessions = self.shared.sessions.write();
                                // Create session if it doesn't exist - handles race conditions
                                let session = sessions.entry(client_id.clone()).or_default();
                                for (pid, pending) in &client.pending_qos1 {
                                    session.pending_qos1.push((*pid, pending.publish.clone()));
                                }
                                for (pid, pending) in &client.pending_qos2 {
                                    session.pending_qos2.push((*pid, pending.publish.clone()));
                                }
                                // Signal that takeover is complete
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
        self.cleanup_clients();

        // Publish any delayed will messages that are ready
        self.publish_delayed_wills();

        // Check keep-alive timeouts
        let now = Instant::now();
        for (_token, client) in &mut self.clients {
            if client.keep_alive > 0 && client.state == ClientState::Connected {
                let timeout = Duration::from_secs((client.keep_alive as u64 * 3) / 2);
                if now.duration_since(client.last_packet_time) > timeout {
                    client.state = ClientState::Disconnecting;
                }
            }
        }

        Ok(())
    }

    /// Accept a new client connection.
    fn accept_client(&mut self, mut socket: TcpStream, addr: SocketAddr) -> Result<()> {
        let token = Token(self.next_token);
        self.next_token += 1;

        self.poll
            .registry()
            .register(&mut socket, token, Interest::READABLE)?;

        let client = Client::new(token, socket, addr, self.id, self.epoll_fd);
        // Store handle for subscription management
        let handle = client.handle.clone();
        self.token_to_handle.insert(token, handle);
        self.clients.insert(token, client);

        // Track socket opened for $SYS metrics
        self.shared.metrics.increment_sockets_opened();

        Ok(())
    }

    /// Get subscribers for a topic, using cache if valid.
    /// Populates self.subscriber_buf with deduplicated subscribers.
    /// Uses adaptive caching - disables cache if hit rate is too low.
    ///
    /// Deduplication is done at cache time, so the returned subscribers
    /// are ready for direct iteration - no further dedup needed.
    fn get_subscribers_cached(&mut self, topic: &Bytes) {
        self.cache_stats.lookups_since_eval += 1;

        // Periodically evaluate cache effectiveness
        if self.cache_stats.enabled {
            if self.cache_stats.lookups_since_eval >= CACHE_EVAL_INTERVAL {
                self.evaluate_cache_effectiveness();
            }
        } else {
            // When disabled, retry after more lookups
            if self.cache_stats.lookups_since_eval >= CACHE_RETRY_INTERVAL {
                self.cache_stats.enabled = true;
                self.cache_stats.lookups_since_eval = 0;
                self.cache_stats.hits = 0;
                self.cache_stats.misses = 0;
                log::debug!("Worker {}: Re-enabling route cache for evaluation", self.id);
            } else {
                // Cache disabled - go direct to trie, then dedup
                self.subscriber_buf.clear();
                {
                    let subscriptions = self.shared.subscriptions.read();
                    subscriptions.match_topic_bytes_into(topic, &mut self.subscriber_buf);
                }
                self.dedup_subscriber_buf();
                return;
            }
        }

        // Check if we have a valid cached entry
        let current_gen = self.shared.subscriptions.read().generation();
        if let Some(cached) = self.route_cache.get(topic) {
            if cached.generation == current_gen {
                // Cache hit - already deduplicated
                self.cache_stats.hits += 1;
                self.subscriber_buf.clear();
                self.subscriber_buf
                    .extend(cached.subscribers.iter().cloned());
                return;
            }
        }

        // Cache miss or stale - compute fresh
        self.cache_stats.misses += 1;
        self.subscriber_buf.clear();
        let generation;
        {
            let subscriptions = self.shared.subscriptions.read();
            subscriptions.match_topic_bytes_into(topic, &mut self.subscriber_buf);
            generation = subscriptions.generation();
        }

        // Dedup before caching
        self.dedup_subscriber_buf();

        // Evict if over limit (simple: just clear all)
        if self.route_cache.len() >= ROUTE_CACHE_LIMIT {
            self.route_cache.clear();
        }

        // Cache the deduplicated result
        self.route_cache.insert(
            topic.clone(),
            CachedRoute {
                subscribers: self.subscriber_buf.clone(),
                generation,
            },
        );
    }

    /// Deduplicate subscriber_buf in-place, keeping highest QoS per (worker_id, token).
    #[inline]
    fn dedup_subscriber_buf(&mut self) {
        if self.subscriber_buf.len() <= 1 {
            return; // Nothing to dedup
        }

        self.dedup_buf.clear();
        for sub in self.subscriber_buf.drain(..) {
            let key = (sub.worker_id(), sub.token());
            self.dedup_buf
                .entry(key)
                .and_modify(|existing| {
                    if (sub.qos as u8) > (existing.qos as u8) {
                        *existing = sub.clone();
                    }
                })
                .or_insert(sub);
        }

        // Move back to subscriber_buf
        self.subscriber_buf
            .extend(self.dedup_buf.drain().map(|(_, sub)| sub));
    }

    /// Evaluate cache effectiveness and disable if hit rate is too low.
    fn evaluate_cache_effectiveness(&mut self) {
        let total = self.cache_stats.hits + self.cache_stats.misses;
        if total == 0 {
            return;
        }

        let hit_rate = self.cache_stats.hits as f64 / total as f64;

        if hit_rate < CACHE_MIN_HIT_RATE {
            self.cache_stats.disabled_count += 1;
            // Only log first disable and every 100th after that
            if self.cache_stats.disabled_count == 1 || self.cache_stats.disabled_count.is_multiple_of(100) {
                log::info!(
                    "Worker {}: Route cache disabled (hit rate {:.1}% < {:.1}% threshold, {} hits / {} total, disabled {} times)",
                    self.id,
                    hit_rate * 100.0,
                    CACHE_MIN_HIT_RATE * 100.0,
                    self.cache_stats.hits,
                    total,
                    self.cache_stats.disabled_count
                );
            }
            self.cache_stats.enabled = false;
            // Keep cache entries for when re-enabled (avoids warmup penalty)
        } else {
            log::debug!(
                "Worker {}: Route cache healthy (hit rate {:.1}%, {} hits / {} total)",
                self.id,
                hit_rate * 100.0,
                self.cache_stats.hits,
                total
            );
        }

        // Reset counters for next evaluation period
        self.cache_stats.lookups_since_eval = 0;
        self.cache_stats.hits = 0;
        self.cache_stats.misses = 0;
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
                    client.pending_qos1.remove(&packet_id);
                    // MQTT 5: Restore outgoing quota when ACK received
                    client.restore_quota();
                }
            }

            Packet::Pubrec { packet_id } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    if let Err(e) = client.queue_packet(&Packet::Pubrel { packet_id }) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            client.record_backpressure_drop("PUBREL");
                        }
                    }
                }
            }

            Packet::Pubrel { packet_id } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    if let Err(e) = client.queue_packet(&Packet::Pubcomp { packet_id }) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            client.record_backpressure_drop("PUBCOMP");
                        }
                    }
                }
            }

            Packet::Pubcomp { packet_id } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    client.pending_qos2.remove(&packet_id);
                    // MQTT 5: Restore outgoing quota when QoS 2 flow completes
                    client.restore_quota();
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
                    // PINGRESP: not critical, drop if slow
                    let _ = client.queue_packet(&Packet::Pingresp);
                }
            }

            Packet::Disconnect { reason_code } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    // MQTT v5 reason code 0x04 = Disconnect with Will Message
                    if reason_code == 0x04 {
                        // Keep will and mark as non-graceful so will gets published
                        client.graceful_disconnect = false;
                    } else {
                        // Normal disconnect - clear will and mark as graceful
                        client.graceful_disconnect = true;
                        client.will = None;
                    }
                    client.state = ClientState::Disconnecting;
                }
            }

            _ => {}
        }

        Ok(())
    }

    fn handle_connect(&mut self, token: Token, connect: crate::packet::Connect) -> Result<()> {
        let is_v5 = connect.protocol_version == 5;

        // MQTT-3.1.3-7: Zero-length ClientId requires CleanSession=1
        if connect.client_id.is_empty() && !connect.clean_session {
            let client = self.clients.get_mut(&token).unwrap();
            client.protocol_version = connect.protocol_version;
            let connack = if is_v5 {
                Connack {
                    session_present: false,
                    code: ConnackCode::IdentifierRejected,
                    reason_code: Some(0x85), // Client Identifier not valid
                    properties: Some(ConnackProperties::default()),
                }
            } else {
                Connack {
                    session_present: false,
                    code: ConnackCode::IdentifierRejected,
                    reason_code: None,
                    properties: None,
                }
            };
            // CONNACK rejection: will disconnect anyway
            let _ = client.queue_packet(&Packet::Connack(connack));
            client.state = ClientState::Disconnecting;
            return Ok(());
        }

        // Authentication check
        let (auth_result, auth_role) = {
            let client = self.clients.get(&token).unwrap();
            let auth_ctx = AuthContext {
                client_id: &connect.client_id,
                username: connect.username.as_deref(),
                password: connect.password.as_deref(),
                remote_addr: client.remote_addr,
            };
            self.auth.authenticate(&auth_ctx)
        };

        if !auth_result.is_allowed() {
            let client = self.clients.get_mut(&token).unwrap();
            client.protocol_version = connect.protocol_version;
            let connack = if is_v5 {
                Connack {
                    session_present: false,
                    code: ConnackCode::NotAuthorized,
                    reason_code: Some(auth_result.to_reason_code_v5()),
                    properties: Some(ConnackProperties::default()),
                }
            } else {
                Connack {
                    session_present: false,
                    code: if matches!(auth_result, AuthResult::DenyBadCredentials) {
                        ConnackCode::BadUsernamePassword
                    } else {
                        ConnackCode::NotAuthorized
                    },
                    reason_code: None,
                    properties: None,
                }
            };
            let _ = client.queue_packet(&Packet::Connack(connack));
            client.state = ClientState::Disconnecting;
            log::debug!(
                "Authentication failed for client {:?} from {}: {:?}",
                connect.client_id,
                client.remote_addr,
                auth_result
            );
            return Ok(());
        }

        // Store auth info in client
        {
            let client = self.clients.get_mut(&token).unwrap();
            client.username = connect.username.clone();
            client.role = auth_role;
            // Determine if anonymous: no username and no password provided
            client.is_anonymous = connect.username.is_none() && connect.password.is_none();
        }

        // MQTT-3.1.4-2: If ClientId already exists, disconnect existing client
        // Save old location for subscription cleanup
        let mut old_location: Option<ClientLocation> = None;
        if !connect.client_id.is_empty() {
            let existing_location = self
                .shared
                .client_registry
                .read()
                .get(&connect.client_id)
                .cloned();
            if let Some(location) = existing_location {
                if location.token != token || location.worker_id != self.id {
                    // Save old location for subscription cleanup later
                    old_location = Some(location);

                    if location.worker_id == self.id {
                        // Same worker - disconnect directly and save pending messages
                        if let Some(existing_client) = self.clients.get_mut(&location.token) {
                            if !existing_client.clean_session {
                                let mut sessions = self.shared.sessions.write();
                                if let Some(session) = sessions.get_mut(&connect.client_id) {
                                    for (pid, pending) in &existing_client.pending_qos1 {
                                        session.pending_qos1.push((*pid, pending.publish.clone()));
                                    }
                                    for (pid, pending) in &existing_client.pending_qos2 {
                                        session.pending_qos2.push((*pid, pending.publish.clone()));
                                    }
                                    session.takeover_complete = true;
                                }
                            }
                            existing_client.state = ClientState::Disconnecting;
                        }
                    } else {
                        // Different worker - prepare for async takeover
                        // Create session if needed and set takeover_complete = false so we know to wait
                        if !connect.clean_session {
                            let mut sessions = self.shared.sessions.write();
                            let session = sessions.entry(connect.client_id.clone()).or_default();
                            session.takeover_complete = false;
                        }
                        // Send disconnect message - the other worker will set takeover_complete = true
                        let _ =
                            self.worker_senders[location.worker_id].send(WorkerMsg::Disconnect {
                                token: location.token,
                            });
                    }
                }
            }

            // Register new client
            self.shared.client_registry.write().insert(
                connect.client_id.clone(),
                ClientLocation {
                    worker_id: self.id,
                    token,
                },
            );
        }

        // Handle session persistence
        let mut pending_to_resend: Vec<Publish> = Vec::new();
        let session_present = if !connect.clean_session && !connect.client_id.is_empty() {
            // For cross-worker takeover, wait for the other worker to save pending messages
            let is_cross_worker_takeover = old_location
                .map(|loc| loc.worker_id != self.id)
                .unwrap_or(false);

            if is_cross_worker_takeover {
                // Wait for takeover_complete with timeout (max 100ms, 1ms intervals)
                for _ in 0..100 {
                    let complete = self
                        .shared
                        .sessions
                        .read()
                        .get(&connect.client_id)
                        .map(|s| s.takeover_complete)
                        .unwrap_or(true);
                    if complete {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }

            let mut sessions = self.shared.sessions.write();
            if let Some(session) = sessions.get_mut(&connect.client_id) {
                // Clear last_connection since we're taking over
                session.last_connection = None;
                // Reset takeover flag for next time
                session.takeover_complete = true;

                // Also check local token_to_client_id for same-worker reconnect
                let local_old_tokens: Vec<Token> = self
                    .token_to_client_id
                    .iter()
                    .filter(|(_, cid)| *cid == &connect.client_id)
                    .map(|(t, _)| *t)
                    .collect();

                // Clone subscriptions before releasing lock
                let subs_to_restore: Vec<StoredSubscription> = session.subscriptions.clone();

                // Collect pending messages, preserving original packet IDs [MQTT-4.5.0-1]
                for (pid, mut publish) in session.pending_qos1.drain(..) {
                    publish.dup = true;
                    publish.packet_id = Some(pid);
                    pending_to_resend.push(publish);
                }
                for (pid, mut publish) in session.pending_qos2.drain(..) {
                    publish.dup = true;
                    publish.packet_id = Some(pid);
                    pending_to_resend.push(publish);
                }

                drop(sessions);

                // Remove old subscriptions and restore with new token
                {
                    let mut subs = self.shared.subscriptions.write();

                    // Remove using old_location from client takeover (most reliable)
                    if let Some(loc) = old_location {
                        subs.remove_client(loc.worker_id, loc.token);
                    }

                    // Also remove any local old tokens (same-worker reconnect without takeover)
                    for old_token in &local_old_tokens {
                        subs.remove_client(self.id, *old_token);
                    }

                    // Restore subscriptions with new handle, preserving options and subscription_id
                    if let Some(handle) = self.token_to_handle.get(&token).cloned() {
                        let client_id: Arc<str> = Arc::from(connect.client_id.as_str());
                        for stored_sub in &subs_to_restore {
                            subs.subscribe(
                                &stored_sub.topic_filter,
                                Subscriber {
                                    handle: handle.clone(),
                                    qos: stored_sub.options.qos,
                                    client_id: client_id.clone(),
                                    options: stored_sub.options,
                                    subscription_id: stored_sub.subscription_id,
                                    share_group: None,
                                },
                            );
                        }
                    }
                }

                // Clean up local token_to_client_id
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

        let client = self.clients.get_mut(&token).unwrap();
        client.client_id = Some(connect.client_id.clone());
        client.keep_alive = connect.keep_alive;
        client.clean_session = connect.clean_session;
        client.will = connect.will.clone();
        client.protocol_version = connect.protocol_version;
        client.handle.set_protocol_version(connect.protocol_version);
        client.state = ClientState::Connected;

        // Track connection for $SYS metrics
        self.shared.metrics.client_connected();
        self.shared.metrics.increment_connections_total();

        // MQTT 5: Extract client's flow control values
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

        // Build CONNACK based on protocol version
        // For v5, handle assigned client ID before building CONNACK
        let assigned_client_id = if is_v5 && connect.client_id.is_empty() {
            use std::time::{SystemTime, UNIX_EPOCH};
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let assigned_id = format!("mqlite-{:016x}-{}", nanos, token.0);
            // Update client's ID
            client.client_id = Some(assigned_id.clone());
            // Register in client registry
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

        let connack = if is_v5 {
            // Build v5 CONNACK with properties
            let mut props = ConnackProperties::default();

            if let Some(id) = assigned_client_id {
                props.assigned_client_identifier = Some(id);
            }

            // Set server capabilities from config
            // Per MQTT 5 spec: retain_available defaults to true if absent
            if !self.config.mqtt.retain_available {
                props.retain_available = Some(false);
            }
            // Per MQTT 5 spec: wildcard_subscription_available defaults to true if absent
            if !self.config.mqtt.wildcard_subscriptions {
                props.wildcard_subscription_available = Some(false);
            }
            // Per MQTT 5 spec: subscription_identifiers_available defaults to true if absent
            if !self.config.mqtt.subscription_identifiers {
                props.subscription_identifiers_available = Some(false);
            }
            // Per MQTT 5 spec: shared_subscription_available defaults to true if absent
            if !self.config.mqtt.shared_subscriptions {
                props.shared_subscription_available = Some(false);
            }
            // Per MQTT 5 spec: Maximum QoS absent = QoS 2 supported
            // Only send if < 2 (0 or 1), never send value 2
            if self.config.mqtt.max_qos < 2 {
                props.maximum_qos = Some(self.config.mqtt.max_qos);
            }

            // Advertise server limits (MQTT 5 flow control)
            props.receive_maximum = Some(self.config.limits.receive_maximum);
            if self.config.limits.max_packet_size > 0 {
                props.maximum_packet_size = Some(self.config.limits.max_packet_size);
            }
            props.topic_alias_maximum = Some(self.config.limits.topic_alias_maximum);

            Connack {
                session_present,
                code: ConnackCode::Accepted,
                reason_code: Some(0x00), // Success
                properties: Some(props),
            }
        } else {
            Connack {
                session_present,
                code: ConnackCode::Accepted,
                reason_code: None,
                properties: None,
            }
        };
        // CONNACK: critical for connection, but client just connected so buffer should be empty
        let _ = client.queue_packet(&Packet::Connack(connack));

        // Re-send pending messages from previous session
        for publish in pending_to_resend {
            if let Some(pid) = publish.packet_id {
                let pending = PendingPublish {
                    publish: publish.clone(),
                    sent_at: Instant::now(),
                };
                match publish.qos {
                    QoS::AtLeastOnce => {
                        client.pending_qos1.insert(pid, pending);
                    }
                    QoS::ExactlyOnce => {
                        client.pending_qos2.insert(pid, pending);
                    }
                    _ => {}
                }
            }
            // Session restore: drop if slow (will retry on next reconnect)
            let _ = client.queue_packet(&Packet::Publish(publish));
        }

        Ok(())
    }

    fn handle_subscribe(
        &mut self,
        token: Token,
        subscribe: crate::packet::Subscribe,
    ) -> Result<()> {
        let mut return_codes = Vec::with_capacity(subscribe.topics.len());
        // (retained_msg, stored_at, sub_qos, retain_as_published, subscription_id)
        let mut retained_to_send: Vec<(Publish, Instant, QoS, bool, Option<u32>)> = Vec::new();

        let (client_id, clean_session, is_v5) = {
            let client = self.clients.get(&token);
            (
                client.and_then(|c| c.client_id.clone()),
                client.map(|c| c.clean_session).unwrap_or(true),
                client.map(|c| c.protocol_version == 5).unwrap_or(false),
            )
        };

        // Enforce subscription_identifiers: reject if subscription ID present but feature disabled
        if subscribe.subscription_id.is_some() && !self.config.mqtt.subscription_identifiers {
            // MQTT 5: 0xA1 = Subscription Identifiers not supported
            for _ in &subscribe.topics {
                return_codes.push(if is_v5 { 0xA1 } else { 0x80 });
            }
            if let Some(client) = self.clients.get_mut(&token) {
                let suback = Suback {
                    packet_id: subscribe.packet_id,
                    return_codes,
                    is_v5,
                };
                let _ = client.queue_packet(&Packet::Suback(suback));
            }
            return Ok(());
        }

        let handle = self.token_to_handle.get(&token).cloned();

        let client_id_arc: Arc<str> = client_id
            .as_ref()
            .map(|s| Arc::from(s.as_str()))
            .unwrap_or_else(|| Arc::from(""));

        for (topic_filter, options) in &subscribe.topics {
            // Parse shared subscription if applicable
            let (actual_filter, share_group) =
                if topic_filter.starts_with("$share/") && topic_filter.len() > 7 {
                    let rest = &topic_filter[7..];
                    if let Some(slash_pos) = rest.find('/') {
                        let group = &rest[..slash_pos];
                        let filter = &rest[slash_pos + 1..];
                        (filter.to_string(), Some(Arc::from(group)))
                    } else {
                        (topic_filter.clone(), None)
                    }
                } else {
                    (topic_filter.clone(), None)
                };

            // Enforce shared_subscriptions: reject if shared subscription but feature disabled
            if share_group.is_some() && !self.config.mqtt.shared_subscriptions {
                // MQTT 5: 0x9E = Shared Subscriptions not supported
                return_codes.push(if is_v5 { 0x9E } else { 0x80 });
                continue;
            }

            // Enforce wildcard_subscriptions: reject if wildcard in filter but feature disabled
            let has_wildcard = actual_filter.contains('+') || actual_filter.contains('#');
            if has_wildcard && !self.config.mqtt.wildcard_subscriptions {
                // MQTT 5: 0xA2 = Wildcard Subscriptions not supported
                return_codes.push(if is_v5 { 0xA2 } else { 0x80 });
                continue;
            }

            // Validate topic filter length and depth
            if validate_topic(
                actual_filter.as_bytes(),
                self.config.limits.max_topic_length,
                self.config.limits.max_topic_levels,
            )
            .is_err()
            {
                // Topic too long or too deep - return error code for this subscription
                // MQTT 5: 0x97 = Quota exceeded (closest match)
                // MQTT 3.1.1: 0x80 = Failure
                return_codes.push(if is_v5 { 0x97 } else { 0x80 });
                continue;
            }

            // ACL check for subscribe permission
            let acl_result = {
                let client = self.clients.get(&token);
                if let Some(c) = client {
                    let client_info = ClientInfo {
                        client_id: c.client_id.clone().unwrap_or_default(),
                        username: c.username.clone(),
                        role: c.role.clone(),
                        is_anonymous: c.is_anonymous,
                    };
                    self.auth.check_subscribe(&client_info, &actual_filter)
                } else {
                    AuthResult::DenyNotAuthorized
                }
            };

            if !acl_result.is_allowed() {
                // MQTT 5: 0x87 = Not authorized
                // MQTT 3.1.1: 0x80 = Failure
                return_codes.push(if is_v5 { 0x87 } else { 0x80 });
                log::debug!(
                    "ACL denied subscribe to '{}' for client {:?}",
                    actual_filter,
                    client_id
                );
                continue;
            }

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

            if !clean_session {
                if let Some(ref cid) = client_id {
                    let mut sessions = self.shared.sessions.write();
                    if let Some(session) = sessions.get_mut(cid) {
                        // Check if subscription already exists for RetainHandling
                        let subscription_exists = session
                            .subscriptions
                            .iter()
                            .any(|s| s.topic_filter == *topic_filter);
                        session
                            .subscriptions
                            .retain(|s| s.topic_filter != *topic_filter);
                        session.subscriptions.push(StoredSubscription {
                            topic_filter: topic_filter.clone(),
                            options: *options,
                            subscription_id: subscribe.subscription_id,
                        });

                        // RetainHandling: 0=always send, 1=only if new, 2=never send
                        if options.retain_handling == 2
                            || (options.retain_handling == 1 && subscription_exists)
                        {
                            continue; // Skip retained message delivery
                        }
                    }
                }
            }

            // RetainHandling check for clean session clients
            if options.retain_handling == 2 {
                continue; // Never send retained messages
            }

            let retained = self.shared.retained_messages.read();
            for (topic, retained_msg) in retained.iter() {
                if topic_matches_filter(topic, &actual_filter) {
                    retained_to_send.push((
                        retained_msg.publish.clone(),
                        retained_msg.stored_at,
                        options.qos,
                        options.retain_as_published,
                        subscribe.subscription_id,
                    ));
                }
            }
        }

        if let Some(client) = self.clients.get_mut(&token) {
            let suback = Suback {
                packet_id: subscribe.packet_id,
                return_codes,
                is_v5: client.protocol_version == 5,
            };
            // SUBACK: required response, drop if slow
            let _ = client.queue_packet(&Packet::Suback(suback));
        }

        // Send retained messages for matching topics
        if let Some(client) = self.clients.get_mut(&token) {
            let is_v5 = client.protocol_version == 5;
            for (retained, stored_at, sub_qos, _retain_as_published, subscription_id) in
                retained_to_send
            {
                // MQTT-3.3.2.3.3-2: Calculate elapsed time and update Message Expiry Interval
                let elapsed_secs = stored_at.elapsed().as_secs() as u32;

                // For v5 messages, check and update expiry
                let base_properties = if is_v5 {
                    match update_message_expiry(retained.properties.as_ref(), elapsed_secs) {
                        None => continue, // Message has expired, skip it
                        Some(props) => props,
                    }
                } else {
                    retained.properties.clone()
                };

                let effective_qos = std::cmp::min(retained.qos as u8, sub_qos as u8);
                let out_qos = QoS::try_from(effective_qos)?;

                let packet_id = if out_qos != QoS::AtMostOnce {
                    Some(client.allocate_packet_id())
                } else {
                    None
                };
                // MQTT-3.3.1-8: When sending a PUBLISH to a Client as a result of a new
                // subscription being made, the Server MUST set RETAIN to 1.
                // This applies to both v3.1.1 and v5 - RetainAsPublished only affects
                // normal publish forwarding, not retained message delivery to new subscribers.
                let out_retain = true;

                // MQTT-3.8.2.1.2: Add subscription identifier to properties if present
                // For v5 clients, we must always include properties (even if empty)
                let properties = if is_v5 {
                    if let Some(sub_id) = subscription_id {
                        // Build properties with subscription identifier prepended
                        let mut props = Vec::new();
                        props.push(0x0B); // Property ID for Subscription Identifier
                        encode_variable_byte_integer(sub_id, &mut props);
                        if let Some(ref existing) = base_properties {
                            props.extend_from_slice(existing);
                        }
                        Some(Bytes::from(props))
                    } else {
                        // No subscription ID, but still v5 - include base properties or empty
                        Some(base_properties.unwrap_or_else(Bytes::new))
                    }
                } else {
                    // v3.1.1 - no properties
                    None
                };

                let out_publish = Publish {
                    dup: false,
                    qos: out_qos,
                    retain: out_retain,
                    topic: retained.topic.clone(),
                    packet_id,
                    payload: retained.payload.clone(),
                    properties,
                };
                // Retained messages: drop if slow
                let _ = client.queue_packet(&Packet::Publish(out_publish));
            }
        }

        Ok(())
    }

    fn handle_unsubscribe(
        &mut self,
        token: Token,
        unsub: crate::packet::Unsubscribe,
    ) -> Result<()> {
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
            let _ = client.queue_packet(&Packet::Unsuback(Unsuback {
                packet_id: unsub.packet_id,
                reason_codes,
                is_v5,
            }));
        }

        Ok(())
    }

    fn handle_publish(&mut self, from_token: Token, publish: Publish) -> Result<()> {
        // Track publish received for $SYS metrics (before any validation)
        let payload_len = publish.payload.len() as u64;
        self.shared.metrics.add_pub_msgs_received(1);
        self.shared.metrics.add_pub_bytes_received(payload_len);

        // MQTT-3.3.2-2, MQTT-4.7.3-1: Topic Names MUST NOT contain wildcards
        if publish.topic.iter().any(|&b| b == b'+' || b == b'#') {
            // Protocol violation - disconnect client
            if let Some(client) = self.clients.get_mut(&from_token) {
                client.state = ClientState::Disconnecting;
            }
            return Ok(());
        }

        // Validate topic length and depth
        if let Err(_e) = validate_topic(
            &publish.topic,
            self.config.limits.max_topic_length,
            self.config.limits.max_topic_levels,
        ) {
            // Topic too long or too deep - disconnect client
            if let Some(client) = self.clients.get_mut(&from_token) {
                client.state = ClientState::Disconnecting;
            }
            return Ok(());
        }

        // Enforce max_qos: reject publish if QoS exceeds server maximum
        if publish.qos as u8 > self.config.mqtt.max_qos {
            if let Some(client) = self.clients.get_mut(&from_token) {
                // Protocol error - client violated server's advertised Maximum QoS
                // MQTT 5: 0x9B = QoS not supported
                client.state = ClientState::Disconnecting;
            }
            return Ok(());
        }

        // Enforce retain_available: reject retained publish if disabled
        if publish.retain && !self.config.mqtt.retain_available {
            if let Some(client) = self.clients.get_mut(&from_token) {
                // Protocol error - client violated server's advertised Retain Available
                // MQTT 5: 0x9A = Retain not supported
                client.state = ClientState::Disconnecting;
            }
            return Ok(());
        }

        // ACL check for publish permission
        let (acl_allowed, is_v5) = {
            let client = self.clients.get(&from_token);
            if let Some(c) = client {
                let topic_str = String::from_utf8_lossy(&publish.topic);
                let client_info = ClientInfo {
                    client_id: c.client_id.clone().unwrap_or_default(),
                    username: c.username.clone(),
                    role: c.role.clone(),
                    is_anonymous: c.is_anonymous,
                };
                let result = self.auth.check_publish(&client_info, &topic_str);
                (result.is_allowed(), c.protocol_version == 5)
            } else {
                (false, false)
            }
        };

        if !acl_allowed {
            // Log the ACL denial
            log::debug!(
                "ACL denied publish to '{}' for client at token {:?}",
                String::from_utf8_lossy(&publish.topic),
                from_token
            );

            // For MQTT v5: send PUBACK/PUBREC with 0x87 (Not authorized) reason code
            // For MQTT v3.1.1: silently drop (no error mechanism in PUBACK)
            if is_v5 {
                if publish.qos == QoS::AtLeastOnce {
                    if let Some(packet_id) = publish.packet_id {
                        if let Some(client) = self.clients.get_mut(&from_token) {
                            // TODO: Add reason_code support to Puback packet
                            // For now, just send regular PUBACK (client may retry)
                            let _ = client.queue_packet(&Packet::Puback { packet_id });
                        }
                    }
                }
                if publish.qos == QoS::ExactlyOnce {
                    if let Some(packet_id) = publish.packet_id {
                        if let Some(client) = self.clients.get_mut(&from_token) {
                            // TODO: Add reason_code support to Pubrec packet
                            let _ = client.queue_packet(&Packet::Pubrec { packet_id });
                        }
                    }
                }
            }
            // Don't forward the message - ACL denied
            return Ok(());
        }

        // Send PUBACK/PUBREC to publisher
        if publish.qos == QoS::AtLeastOnce {
            if let Some(packet_id) = publish.packet_id {
                if let Some(client) = self.clients.get_mut(&from_token) {
                    if let Err(e) = client.queue_packet(&Packet::Puback { packet_id }) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            client.record_backpressure_drop("PUBACK");
                        }
                    }
                }
            }
        }
        if publish.qos == QoS::ExactlyOnce {
            if let Some(packet_id) = publish.packet_id {
                if let Some(client) = self.clients.get_mut(&from_token) {
                    if let Err(e) = client.queue_packet(&Packet::Pubrec { packet_id }) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            client.record_backpressure_drop("PUBREC");
                        }
                    }
                }
            }
        }

        // Handle retained messages (only allocate String when needed)
        if publish.retain {
            let topic_str = String::from_utf8_lossy(&publish.topic).into_owned();
            let mut retained_msgs = self.shared.retained_messages.write();
            if publish.payload.is_empty() {
                retained_msgs.remove(&topic_str);
            } else {
                let retained_publish = Publish {
                    dup: false,
                    qos: publish.qos,
                    retain: true,
                    topic: publish.topic.clone(),
                    packet_id: None,
                    payload: publish.payload.clone(),
                    properties: publish.properties.clone(),
                };
                let retained = RetainedMessage {
                    publish: retained_publish,
                    stored_at: Instant::now(),
                };
                retained_msgs.insert(topic_str, retained);
            }
        }

        // Find matching subscribers - using cache for O(1) hot path
        // Returns pre-deduplicated subscribers, ready for direct iteration
        self.get_subscribers_cached(&publish.topic);

        // Add shared subscription subscribers (requires write lock, not cached)
        {
            let topic_str = std::str::from_utf8(&publish.topic).unwrap_or("");
            let mut subscriptions = self.shared.subscriptions.write();
            subscriptions.match_shared_subscribers(topic_str, &mut self.subscriber_buf);
        }

        let mut factory = PublishEncoder::new(
            publish.topic.clone(),
            publish.payload.clone(),
            publish.properties.clone(),
        );

        // Forward to subscribers using direct writes via handles
        // subscriber_buf is already deduplicated by get_subscribers_cached
        let mut backpressure_count: u32 = 0;
        let mut last_backpressure_sub: Option<(usize, Token)> = None;
        for sub in &self.subscriber_buf {
            // MQTT-3.8.3.1-2: NoLocal - don't deliver to publishing client
            if sub.options.no_local && sub.token() == from_token && sub.worker_id() == self.id {
                continue;
            }

            let worker_id = sub.worker_id();
            let sub_token = sub.token();
            let effective_qos = std::cmp::min(publish.qos as u8, sub.qos as u8);
            let out_qos = QoS::try_from(effective_qos)?;

            // Allocate packet ID from handle's atomic counter for QoS > 0
            let packet_id = if out_qos != QoS::AtMostOnce {
                Some(sub.handle.allocate_packet_id())
            } else {
                None
            };

            // For local clients, track pending messages for QoS 1/2 retransmission
            if worker_id == self.id {
                if !self.clients.contains_key(&sub_token) {
                    // Client disconnected - queue for offline delivery
                    // Use sub.client_id directly (token_to_client_id may already be cleaned up)
                    if !sub.client_id.is_empty() {
                        let mut sessions = self.shared.sessions.write();
                        if let Some(session) = sessions.get_mut(&*sub.client_id) {
                            if out_qos != QoS::AtMostOnce {
                                let pkt_id = packet_id.unwrap();
                                let queued_publish = Publish {
                                    dup: false,
                                    qos: out_qos,
                                    retain: false,
                                    topic: publish.topic.clone(),
                                    packet_id: Some(pkt_id),
                                    payload: publish.payload.clone(),
                                    properties: publish.properties.clone(),
                                };
                                match out_qos {
                                    QoS::AtLeastOnce => {
                                        session.pending_qos1.push((pkt_id, queued_publish));
                                    }
                                    QoS::ExactlyOnce => {
                                        session.pending_qos2.push((pkt_id, queued_publish));
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    continue;
                }

                // MQTT 5 flow control: check quota before sending QoS 1/2
                if out_qos != QoS::AtMostOnce {
                    if let Some(client) = self.clients.get_mut(&sub_token) {
                        if !client.consume_quota() {
                            // No quota available - skip this message (flow control)
                            backpressure_count += 1;
                            last_backpressure_sub = Some((worker_id, sub_token));
                            continue;
                        }
                    }
                }

                // Track pending for local clients
                if let Some(pid) = packet_id {
                    if let Some(client) = self.clients.get_mut(&sub_token) {
                        let pending_publish = Publish {
                            dup: false,
                            qos: out_qos,
                            retain: false,
                            topic: publish.topic.clone(),
                            packet_id: Some(pid),
                            payload: publish.payload.clone(),
                            properties: publish.properties.clone(),
                        };
                        let pending = PendingPublish {
                            publish: pending_publish,
                            sent_at: Instant::now(),
                        };
                        match out_qos {
                            QoS::AtLeastOnce => {
                                client.pending_qos1.insert(pid, pending);
                            }
                            QoS::ExactlyOnce => {
                                client.pending_qos2.insert(pid, pending);
                            }
                            _ => {}
                        }
                    }
                }
            } else {
                // Cross-worker: track pending messages in session for QoS 1/2
                // The client state is on another worker, so we track in the shared session
                if let Some(pid) = packet_id {
                    if !sub.client_id.is_empty() {
                        let mut sessions = self.shared.sessions.write();
                        if let Some(session) = sessions.get_mut(&*sub.client_id) {
                            let pending_publish = Publish {
                                dup: false,
                                qos: out_qos,
                                retain: false,
                                topic: publish.topic.clone(),
                                packet_id: Some(pid),
                                payload: publish.payload.clone(),
                                properties: publish.properties.clone(),
                            };
                            match out_qos {
                                QoS::AtLeastOnce => {
                                    session.pending_qos1.push((pid, pending_publish));
                                }
                                QoS::ExactlyOnce => {
                                    session.pending_qos2.push((pid, pending_publish));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }

            // Direct write to subscriber's buffer via handle (works for both local and cross-thread)
            // The handle updates epoll directly, so no need to mark_dirty
            //
            // Backpressure policy: if WouldBlock, the client is slow.
            // QoS 0: drop (at-most-once, loss is acceptable)
            // QoS 1/2: message is saved in session for retry on reconnect
            //
            // MQTT-3.8.3-4: If Retain As Published is 1, preserve the RETAIN flag
            let out_retain = if sub.options.retain_as_published {
                publish.retain
            } else {
                false
            };
            // MQTT-3.8.2.1.2: Include subscription identifier if subscriber has one
            if let Err(e) = sub.handle.queue_publish_with_sub_id(
                &mut factory,
                out_qos,
                packet_id,
                out_retain,
                sub.subscription_id,
            ) {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    // Track for rate-limited logging (can't call method due to borrow)
                    backpressure_count += 1;
                    last_backpressure_sub = Some((sub.handle.worker_id(), sub.handle.token()));
                    // Track dropped publish for $SYS metrics
                    self.shared.metrics.add_pub_msgs_dropped(1);
                }
            } else {
                // Track successful publish sent for $SYS metrics
                self.shared.metrics.add_pub_msgs_sent(1);
                self.shared.metrics.add_pub_bytes_sent(payload_len);
            }
        }

        // Log accumulated backpressure drops (rate limited to every 10s per worker)
        if backpressure_count > 0 {
            if let Some(count) = self
                .subscriber_backpressure_log
                .increment_by(backpressure_count as u64)
            {
                if let Some((worker_id, token)) = last_backpressure_sub {
                    log::warn!(
                        "Backpressure: dropped {} messages to slow subscribers (last: worker={}, token={:?})",
                        count,
                        worker_id,
                        token
                    );
                }
            }
        }

        Ok(())
    }

    fn cleanup_clients(&mut self) {
        let disconnected: Vec<Token> = self
            .clients
            .iter()
            .filter(|(_, c)| c.state == ClientState::Disconnecting)
            .map(|(t, _)| *t)
            .collect();

        let mut will_messages: Vec<Publish> = Vec::new();
        let mut delayed_wills: Vec<DelayedWill> = Vec::new();

        for token in disconnected {
            if let Some(mut client) = self.clients.remove(&token) {
                let _ = self.poll.registry().deregister(&mut client.socket);

                // Track client disconnection for $SYS metrics
                // Only count if client was fully connected (has client_id assigned)
                if client.client_id.is_some() {
                    self.shared.metrics.client_disconnected();
                }

                // Clean up token maps to free memory (WriteBuffer, etc.)
                self.token_to_handle.remove(&token);
                self.token_to_client_id.remove(&token);

                if client.clean_session {
                    self.shared
                        .subscriptions
                        .write()
                        .remove_client(self.id, token);
                }

                if let Some(ref client_id) = client.client_id {
                    // Check if we're still the owner of this client registration
                    let is_current_owner = {
                        let registry = self.shared.client_registry.read();
                        registry
                            .get(client_id)
                            .map(|loc| loc.token == token && loc.worker_id == self.id)
                            .unwrap_or(false)
                    };

                    if is_current_owner {
                        // We're still the owner - remove from registry
                        self.shared.client_registry.write().remove(client_id);
                    }

                    // Always save pending messages for persistent sessions, even after takeover.
                    // For cross-worker takeover, the new client may have already drained session
                    // pending messages, but we still need to save our client's pending messages
                    // (which were in flight when we got the Disconnect message).
                    if !client.clean_session {
                        let mut sessions = self.shared.sessions.write();
                        // Create session if it doesn't exist - handles race where cleanup
                        // runs before new client's CONNECT handler creates the session
                        let session = sessions.entry(client_id.clone()).or_default();
                        // Only set last_connection if we're still the owner
                        // (otherwise the new client has already taken over)
                        if is_current_owner {
                            session.last_connection = Some((self.id, token));
                        }
                        // Always save pending messages - they might not have been
                        // transferred to session yet if this was a takeover
                        for (pid, pending) in &client.pending_qos1 {
                            session.pending_qos1.push((*pid, pending.publish.clone()));
                        }
                        for (pid, pending) in &client.pending_qos2 {
                            session.pending_qos2.push((*pid, pending.publish.clone()));
                        }
                        // Signal takeover completion for any waiting worker
                        // (handles the case where cleanup runs before Disconnect msg is received)
                        session.takeover_complete = true;
                    }
                }

                if !client.graceful_disconnect {
                    if let Some(will) = client.will.take() {
                        let publish = Publish {
                            dup: false,
                            qos: will.qos,
                            retain: will.retain,
                            topic: Bytes::from(will.topic),
                            packet_id: None,
                            payload: Bytes::from(will.message),
                            properties: None, // TODO: convert WillProperties to raw bytes
                        };

                        // Check for MQTT v5 Will Delay Interval
                        let delay_secs = will
                            .properties
                            .as_ref()
                            .and_then(|p| p.will_delay_interval)
                            .unwrap_or(0);

                        if delay_secs > 0 {
                            // Schedule for delayed publication
                            let publish_at =
                                Instant::now() + Duration::from_secs(delay_secs as u64);
                            delayed_wills.push(DelayedWill {
                                publish_at,
                                publish,
                            });
                        } else {
                            // Immediate publication
                            will_messages.push(publish);
                        }
                    }
                }
            }
        }

        // Publish will messages using direct writes
        for will_publish in will_messages {
            if will_publish.retain {
                let topic_str = String::from_utf8_lossy(&will_publish.topic).into_owned();
                let mut retained = self.shared.retained_messages.write();
                if will_publish.payload.is_empty() {
                    retained.remove(&topic_str);
                } else {
                    retained.insert(
                        topic_str,
                        RetainedMessage {
                            publish: will_publish.clone(),
                            stored_at: Instant::now(),
                        },
                    );
                }
            }

            // Use cached route lookup - returns pre-deduplicated subscribers
            self.get_subscribers_cached(&will_publish.topic);

            let mut factory = PublishEncoder::new(
                will_publish.topic.clone(),
                will_publish.payload.clone(),
                will_publish.properties.clone(),
            );

            // subscriber_buf is already deduplicated by get_subscribers_cached
            let mut will_backpressure_count: u32 = 0;
            let mut last_will_backpressure_sub: Option<(usize, Token)> = None;
            for sub in &self.subscriber_buf {
                let worker_id = sub.worker_id();
                let sub_token = sub.token();
                let effective_qos = std::cmp::min(will_publish.qos as u8, sub.qos as u8);
                let out_qos = match QoS::try_from(effective_qos) {
                    Ok(q) => q,
                    Err(_) => continue,
                };

                let packet_id = if out_qos != QoS::AtMostOnce {
                    Some(sub.handle.allocate_packet_id())
                } else {
                    None
                };

                // Track pending for local clients
                if worker_id == self.id {
                    if let Some(pid) = packet_id {
                        if let Some(client) = self.clients.get_mut(&sub_token) {
                            let pending_publish = Publish {
                                dup: false,
                                qos: out_qos,
                                retain: false,
                                topic: will_publish.topic.clone(),
                                packet_id: Some(pid),
                                payload: will_publish.payload.clone(),
                                properties: will_publish.properties.clone(),
                            };
                            let pending = PendingPublish {
                                publish: pending_publish,
                                sent_at: Instant::now(),
                            };
                            match out_qos {
                                QoS::AtLeastOnce => {
                                    client.pending_qos1.insert(pid, pending);
                                }
                                QoS::ExactlyOnce => {
                                    client.pending_qos2.insert(pid, pending);
                                }
                                _ => {}
                            }
                        }
                    }
                } else {
                    // Cross-worker: track pending messages in session for QoS 1/2
                    if let Some(pid) = packet_id {
                        if !sub.client_id.is_empty() {
                            let mut sessions = self.shared.sessions.write();
                            if let Some(session) = sessions.get_mut(&*sub.client_id) {
                                let pending_publish = Publish {
                                    dup: false,
                                    qos: out_qos,
                                    retain: false,
                                    topic: will_publish.topic.clone(),
                                    packet_id: Some(pid),
                                    payload: will_publish.payload.clone(),
                                    properties: will_publish.properties.clone(),
                                };
                                match out_qos {
                                    QoS::AtLeastOnce => {
                                        session.pending_qos1.push((pid, pending_publish));
                                    }
                                    QoS::ExactlyOnce => {
                                        session.pending_qos2.push((pid, pending_publish));
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }

                // Direct write via handle (works for both local and cross-thread)
                // Backpressure: drop on slow client (will messages are best-effort)
                if let Err(e) = sub
                    .handle
                    .queue_publish(&mut factory, out_qos, packet_id, false)
                {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        will_backpressure_count += 1;
                        last_will_backpressure_sub =
                            Some((sub.handle.worker_id(), sub.handle.token()));
                    }
                }
            }

            // Log accumulated will backpressure drops (rate limited to every 10s per worker)
            if will_backpressure_count > 0 {
                if let Some(count) = self
                    .subscriber_backpressure_log
                    .increment_by(will_backpressure_count as u64)
                {
                    if let Some((worker_id, token)) = last_will_backpressure_sub {
                        log::warn!(
                            "Backpressure: dropped {} will messages to slow subscribers (last: worker={}, token={:?})",
                            count,
                            worker_id,
                            token
                        );
                    }
                }
            }
        }

        // Store delayed wills for later publication
        self.delayed_wills.extend(delayed_wills);
    }

    /// Publish any delayed will messages that are ready.
    fn publish_delayed_wills(&mut self) {
        let now = Instant::now();

        // Partition into ready and not-ready wills
        let mut ready_wills = Vec::new();
        self.delayed_wills.retain(|dw| {
            if dw.publish_at <= now {
                ready_wills.push(dw.publish.clone());
                false
            } else {
                true
            }
        });

        // Publish ready wills
        for will_publish in ready_wills {
            if will_publish.retain {
                let topic_str = String::from_utf8_lossy(&will_publish.topic).into_owned();
                let mut retained = self.shared.retained_messages.write();
                if will_publish.payload.is_empty() {
                    retained.remove(&topic_str);
                } else {
                    retained.insert(
                        topic_str,
                        RetainedMessage {
                            publish: will_publish.clone(),
                            stored_at: Instant::now(),
                        },
                    );
                }
            }

            // Use cached route lookup
            self.get_subscribers_cached(&will_publish.topic);

            let mut factory = PublishEncoder::new(
                will_publish.topic.clone(),
                will_publish.payload.clone(),
                will_publish.properties.clone(),
            );

            let mut delayed_backpressure_count: u32 = 0;
            let mut last_delayed_backpressure_sub: Option<(usize, Token)> = None;
            for sub in &self.subscriber_buf {
                let worker_id = sub.worker_id();
                let sub_token = sub.token();
                let effective_qos = std::cmp::min(will_publish.qos as u8, sub.qos as u8);
                let out_qos = match QoS::try_from(effective_qos) {
                    Ok(q) => q,
                    Err(_) => continue,
                };

                let packet_id = if out_qos != QoS::AtMostOnce {
                    Some(sub.handle.allocate_packet_id())
                } else {
                    None
                };

                // Track pending for local clients
                if worker_id == self.id {
                    if let Some(pid) = packet_id {
                        if let Some(client) = self.clients.get_mut(&sub_token) {
                            let pending_publish = Publish {
                                dup: false,
                                qos: out_qos,
                                retain: false,
                                topic: will_publish.topic.clone(),
                                packet_id: Some(pid),
                                payload: will_publish.payload.clone(),
                                properties: will_publish.properties.clone(),
                            };
                            let pending = PendingPublish {
                                publish: pending_publish,
                                sent_at: now,
                            };
                            match out_qos {
                                QoS::AtLeastOnce => {
                                    client.pending_qos1.insert(pid, pending);
                                }
                                QoS::ExactlyOnce => {
                                    client.pending_qos2.insert(pid, pending);
                                }
                                _ => {}
                            }
                        }
                    }
                }

                // Direct write via handle
                if let Err(e) = sub
                    .handle
                    .queue_publish(&mut factory, out_qos, packet_id, false)
                {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        delayed_backpressure_count += 1;
                        last_delayed_backpressure_sub =
                            Some((sub.handle.worker_id(), sub.handle.token()));
                    }
                }
            }

            // Log accumulated delayed will backpressure drops (rate limited to every 10s per worker)
            if delayed_backpressure_count > 0 {
                if let Some(count) = self
                    .subscriber_backpressure_log
                    .increment_by(delayed_backpressure_count as u64)
                {
                    if let Some((worker_id, token)) = last_delayed_backpressure_sub {
                        log::warn!(
                            "Backpressure: dropped {} delayed will messages to slow subscribers (last: worker={}, token={:?})",
                            count,
                            worker_id,
                            token
                        );
                    }
                }
            }
        }
    }

    /// Get the time until the next delayed will should be published.
    fn next_delayed_will_timeout(&self) -> Option<Duration> {
        if self.delayed_wills.is_empty() {
            return None;
        }

        let now = Instant::now();
        let next_publish = self.delayed_wills.iter().map(|dw| dw.publish_at).min()?;

        if next_publish <= now {
            Some(Duration::ZERO)
        } else {
            Some(next_publish - now)
        }
    }
}
