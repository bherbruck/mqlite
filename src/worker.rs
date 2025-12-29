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

use crate::client::{Client, ClientState, PendingPublish};
use crate::client_handle::ClientWriteHandle;
use crate::error::{ProtocolError, Result};
use crate::packet::{Connack, ConnackCode, Packet, Publish, QoS, Suback};
use crate::publish_encoder::PublishEncoder;
use crate::shared::{ClientLocation, Session, SharedStateHandle};
use crate::subscription::{topic_matches_filter, Subscriber};

/// Messages sent to workers via channels (control plane only).
/// Publish delivery uses direct writes, not channels.
#[derive(Debug)]
#[allow(dead_code)] // Shutdown variant reserved for graceful shutdown
pub enum WorkerMsg {
    /// New connection from main thread.
    NewClient {
        socket: TcpStream,
        addr: SocketAddr,
    },
    /// Disconnect a client (for client takeover).
    Disconnect { token: Token },
    /// Shutdown signal.
    Shutdown,
}

/// Starting token for client connections within this worker.
const CLIENT_START: usize = 1;

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
}

impl Worker {
    /// Create a new worker.
    pub fn new(
        id: usize,
        shared: SharedStateHandle,
        rx: Receiver<WorkerMsg>,
        worker_senders: Vec<Sender<WorkerMsg>>,
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
                enabled: true,  // Start with cache enabled
                lookups_since_eval: 0,
            },
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

        // Poll with a short timeout to check channel
        self.poll
            .poll(&mut events, Some(Duration::from_millis(10)))?;

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
                                let session = sessions
                                    .entry(client_id.clone())
                                    .or_default();
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
    fn accept_client(&mut self, mut socket: TcpStream, _addr: SocketAddr) -> Result<()> {
        let token = Token(self.next_token);
        self.next_token += 1;

        self.poll
            .registry()
            .register(&mut socket, token, Interest::READABLE)?;

        let client = Client::new(token, socket, self.id, self.epoll_fd);
        // Store handle for subscription management
        let handle = client.handle.clone();
        self.token_to_handle.insert(token, handle);
        self.clients.insert(token, client);

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
                log::debug!(
                    "Worker {}: Re-enabling route cache for evaluation",
                    self.id
                );
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
                self.subscriber_buf.extend(cached.subscribers.iter().cloned());
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
        self.subscriber_buf.extend(self.dedup_buf.drain().map(|(_, sub)| sub));
    }

    /// Evaluate cache effectiveness and disable if hit rate is too low.
    fn evaluate_cache_effectiveness(&mut self) {
        let total = self.cache_stats.hits + self.cache_stats.misses;
        if total == 0 {
            return;
        }

        let hit_rate = self.cache_stats.hits as f64 / total as f64;

        if hit_rate < CACHE_MIN_HIT_RATE {
            log::info!(
                "Worker {}: Route cache disabled (hit rate {:.1}% < {:.1}% threshold, {} hits / {} total)",
                self.id,
                hit_rate * 100.0,
                CACHE_MIN_HIT_RATE * 100.0,
                self.cache_stats.hits,
                total
            );
            self.cache_stats.enabled = false;
            self.route_cache.clear(); // Free memory
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

                match client.decode_packet() {
                    Ok(Some(packet)) => {
                        client.last_packet_time = Instant::now();
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
                }
            }

            Packet::Pubrec { packet_id } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    if let Err(e) = client.queue_packet(&Packet::Pubrel { packet_id }) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            log::warn!("Backpressure: dropping PUBREL to slow client (token={:?})", token);
                        }
                    }
                }
            }

            Packet::Pubrel { packet_id } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    if let Err(e) = client.queue_packet(&Packet::Pubcomp { packet_id }) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            log::warn!("Backpressure: dropping PUBCOMP to slow client (token={:?})", token);
                        }
                    }
                }
            }

            Packet::Pubcomp { packet_id } => {
                if let Some(client) = self.clients.get_mut(&token) {
                    client.pending_qos2.remove(&packet_id);
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

            Packet::Disconnect => {
                if let Some(client) = self.clients.get_mut(&token) {
                    client.graceful_disconnect = true;
                    client.will = None;
                    client.state = ClientState::Disconnecting;
                }
            }

            _ => {}
        }

        Ok(())
    }

    fn handle_connect(
        &mut self,
        token: Token,
        connect: crate::packet::Connect,
    ) -> Result<()> {
        // MQTT-3.1.3-7: Zero-length ClientId requires CleanSession=1
        if connect.client_id.is_empty() && !connect.clean_session {
            let client = self.clients.get_mut(&token).unwrap();
            let connack = Connack {
                session_present: false,
                code: ConnackCode::IdentifierRejected,
            };
            // CONNACK rejection: will disconnect anyway
            let _ = client.queue_packet(&Packet::Connack(connack));
            client.state = ClientState::Disconnecting;
            return Ok(());
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
                            let session = sessions
                                .entry(connect.client_id.clone())
                                .or_default();
                            session.takeover_complete = false;
                        }
                        // Send disconnect message - the other worker will set takeover_complete = true
                        let _ = self.worker_senders[location.worker_id]
                            .send(WorkerMsg::Disconnect { token: location.token });
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
                let subs_to_restore: Vec<(String, QoS)> = session.subscriptions.clone();

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

                    // Restore subscriptions with new handle
                    if let Some(handle) = self.token_to_handle.get(&token).cloned() {
                        let client_id: Arc<str> = Arc::from(connect.client_id.as_str());
                        for (filter, qos) in &subs_to_restore {
                            subs.subscribe(
                                filter,
                                Subscriber {
                                    handle: handle.clone(),
                                    qos: *qos,
                                    client_id: client_id.clone(),
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
        client.state = ClientState::Connected;

        if !connect.clean_session && !connect.client_id.is_empty() {
            self.token_to_client_id
                .insert(token, connect.client_id.clone());
        }

        let connack = Connack {
            session_present,
            code: ConnackCode::Accepted,
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
        let mut retained_to_send: Vec<(Publish, QoS)> = Vec::new();

        let (client_id, clean_session) = {
            let client = self.clients.get(&token);
            (
                client.and_then(|c| c.client_id.clone()),
                client.map(|c| c.clean_session).unwrap_or(true),
            )
        };

        let handle = self.token_to_handle.get(&token).cloned();

        let client_id_arc: Arc<str> = client_id
            .as_ref()
            .map(|s| Arc::from(s.as_str()))
            .unwrap_or_else(|| Arc::from(""));

        for (topic_filter, qos) in &subscribe.topics {
            if let Some(ref h) = handle {
                self.shared.subscriptions.write().subscribe(
                    topic_filter,
                    Subscriber {
                        handle: h.clone(),
                        qos: *qos,
                        client_id: client_id_arc.clone(),
                    },
                );
            }
            return_codes.push(*qos as u8);

            if !clean_session {
                if let Some(ref cid) = client_id {
                    let mut sessions = self.shared.sessions.write();
                    if let Some(session) = sessions.get_mut(cid) {
                        session.subscriptions.retain(|(f, _)| f != topic_filter);
                        session.subscriptions.push((topic_filter.clone(), *qos));
                    }
                }
            }

            let retained = self.shared.retained_messages.read();
            for (topic, retained_msg) in retained.iter() {
                if topic_matches_filter(topic, topic_filter) {
                    retained_to_send.push((retained_msg.clone(), *qos));
                }
            }
        }

        if let Some(client) = self.clients.get_mut(&token) {
            let suback = Suback {
                packet_id: subscribe.packet_id,
                return_codes,
            };
            // SUBACK: required response, drop if slow
            let _ = client.queue_packet(&Packet::Suback(suback));
        }

        // Send retained messages for matching topics
        if let Some(client) = self.clients.get_mut(&token) {
            for (retained, sub_qos) in retained_to_send {
                let effective_qos = std::cmp::min(retained.qos as u8, sub_qos as u8);
                let out_qos = QoS::try_from(effective_qos)?;

                let packet_id = if out_qos != QoS::AtMostOnce {
                    Some(client.allocate_packet_id())
                } else {
                    None
                };
                let out_publish = Publish {
                    dup: false,
                    qos: out_qos,
                    retain: true,
                    topic: retained.topic.clone(),
                    packet_id,
                    payload: retained.payload.clone(),
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
                        session.subscriptions.retain(|(f, _)| f != topic_filter);
                    }
                }
            }
        }

        if let Some(client) = self.clients.get_mut(&token) {
            // UNSUBACK: required response, drop if slow
            let _ = client.queue_packet(&Packet::Unsuback {
                packet_id: unsub.packet_id,
            });
        }

        Ok(())
    }

    fn handle_publish(&mut self, from_token: Token, publish: Publish) -> Result<()> {
        // MQTT-3.3.2-2, MQTT-4.7.3-1: Topic Names MUST NOT contain wildcards
        if publish.topic.iter().any(|&b| b == b'+' || b == b'#') {
            // Protocol violation - disconnect client
            if let Some(client) = self.clients.get_mut(&from_token) {
                client.state = ClientState::Disconnecting;
            }
            return Ok(());
        }

        // Send PUBACK/PUBREC to publisher
        if publish.qos == QoS::AtLeastOnce {
            if let Some(packet_id) = publish.packet_id {
                if let Some(client) = self.clients.get_mut(&from_token) {
                    if let Err(e) = client.queue_packet(&Packet::Puback { packet_id }) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            log::warn!("Backpressure: dropping PUBACK to slow publisher (token={:?})", from_token);
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
                            log::warn!("Backpressure: dropping PUBREC to slow publisher (token={:?})", from_token);
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
                let retained = Publish {
                    dup: false,
                    qos: publish.qos,
                    retain: true,
                    topic: publish.topic.clone(),
                    packet_id: None,
                    payload: publish.payload.clone(),
                };
                retained_msgs.insert(topic_str, retained);
            }
        }

        // Find matching subscribers - using cache for O(1) hot path
        // Returns pre-deduplicated subscribers, ready for direct iteration
        self.get_subscribers_cached(&publish.topic);

        let mut factory = PublishEncoder::new(
            publish.topic.clone(),
            publish.payload.clone(),
        );

        // Forward to subscribers using direct writes via handles
        // subscriber_buf is already deduplicated by get_subscribers_cached
        for sub in &self.subscriber_buf {
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
            if let Err(e) = sub.handle.queue_publish(&mut factory, out_qos, packet_id, false) {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    log::warn!(
                        "Backpressure: dropping publish to slow subscriber (worker={}, token={:?}, qos={:?})",
                        sub.handle.worker_id(),
                        sub.handle.token(),
                        out_qos
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

        for token in disconnected {
            if let Some(mut client) = self.clients.remove(&token) {
                let _ = self.poll.registry().deregister(&mut client.socket);

                // Clean up token maps to free memory (WriteBuffer, etc.)
                self.token_to_handle.remove(&token);
                self.token_to_client_id.remove(&token);

                if client.clean_session {
                    self.shared.subscriptions.write().remove_client(self.id, token);
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
                        let session = sessions
                            .entry(client_id.clone())
                            .or_default();
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
                        };
                        will_messages.push(publish);
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
                    retained.insert(topic_str, will_publish.clone());
                }
            }

            // Use cached route lookup - returns pre-deduplicated subscribers
            self.get_subscribers_cached(&will_publish.topic);

            let mut factory = PublishEncoder::new(
                will_publish.topic.clone(),
                will_publish.payload.clone(),
            );

            // subscriber_buf is already deduplicated by get_subscribers_cached
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
                if let Err(e) = sub.handle.queue_publish(&mut factory, out_qos, packet_id, false) {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        log::warn!(
                            "Backpressure: dropping will message to slow subscriber (worker={}, token={:?})",
                            sub.handle.worker_id(),
                            sub.handle.token()
                        );
                    }
                }
            }
        }
    }
}
