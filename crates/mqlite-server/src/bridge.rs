//! MQTT Bridge implementation.
//!
//! Bridges connect to remote MQTT brokers and sync messages bidirectionally:
//! - Subscribe topics: messages from remote → local broker
//! - Publish topics: messages from local → remote broker
//!
//! Each bridge publishes stats to $SYS/broker/bridge/<name>/...

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use log::{debug, error, info, warn};
use mqlite_client::{Client, ClientConfig, ClientEvent, QoS};

use crate::config::BridgeConfig;
use crate::shared::SharedStateHandle;

/// Bridge statistics for $SYS topics.
#[derive(Default)]
#[allow(dead_code)] // Fields are defined for future $SYS publishing
pub struct BridgeStats {
    /// Whether the bridge is connected.
    pub connected: AtomicBool,
    /// Messages received from remote.
    pub messages_received: AtomicU64,
    /// Messages sent to remote.
    pub messages_sent: AtomicU64,
    /// Bytes received from remote.
    pub bytes_received: AtomicU64,
    /// Bytes sent to remote.
    pub bytes_sent: AtomicU64,
    /// Number of reconnections.
    pub reconnects: AtomicU64,
}

impl BridgeStats {
    pub fn new() -> Self {
        Self::default()
    }
}

/// A bridge connection to a remote MQTT broker.
pub struct Bridge {
    config: BridgeConfig,
    shared: SharedStateHandle,
    stats: Arc<BridgeStats>,
    shutdown: Arc<AtomicBool>,
}

impl Bridge {
    /// Create a new bridge with the given configuration.
    pub fn new(config: BridgeConfig, shared: SharedStateHandle) -> Self {
        Self {
            config,
            shared,
            stats: Arc::new(BridgeStats::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get the bridge statistics.
    pub fn stats(&self) -> Arc<BridgeStats> {
        self.stats.clone()
    }

    /// Get the bridge name.
    #[allow(dead_code)] // Will be used for $SYS topic publishing
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Signal the bridge to shutdown.
    #[allow(dead_code)] // Will be used for graceful shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Start the bridge in a background thread.
    pub fn start(self) -> thread::JoinHandle<()> {
        thread::spawn(move || self.run())
    }

    /// Run the bridge (blocking).
    fn run(self) {
        let mut current_delay = self.config.reconnect_delay;
        let mut subscriber_buf = Vec::new();

        info!(
            "Bridge '{}' starting, connecting to {}",
            self.config.name, self.config.address
        );

        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                info!("Bridge '{}' shutting down", self.config.name);
                break;
            }

            // Create client config
            let client_config = ClientConfig {
                address: self.config.address.clone(),
                client_id: format!("{}-{}", self.config.clientid_prefix, self.config.name),
                username: self.config.remote_username.clone(),
                password: self.config.remote_password.as_ref().map(|p| p.as_bytes().to_vec()),
                keep_alive: self.config.keepalive,
                clean_session: self.config.clean_start,
                protocol_version: match self.config.protocol_version {
                    crate::config::BridgeProtocolVersion::Mqtt311 => 4,
                    crate::config::BridgeProtocolVersion::Mqtt5 => 5,
                },
                connect_timeout: Duration::from_secs(10),
            };

            // Try to connect
            match self.run_connection(client_config, &mut subscriber_buf) {
                Ok(()) => {
                    // Clean disconnect, reset delay
                    current_delay = self.config.reconnect_delay;
                }
                Err(e) => {
                    error!("Bridge '{}' error: {}", self.config.name, e);
                    self.stats.connected.store(false, Ordering::Relaxed);
                }
            }

            if self.shutdown.load(Ordering::SeqCst) {
                break;
            }

            // Reconnect with exponential backoff
            info!(
                "Bridge '{}' reconnecting in {}s",
                self.config.name, current_delay
            );
            thread::sleep(Duration::from_secs(current_delay));

            current_delay = (current_delay * 2).min(self.config.reconnect_max);
            self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Run a single connection session.
    fn run_connection(
        &self,
        config: ClientConfig,
        subscriber_buf: &mut Vec<crate::subscription::Subscriber>,
    ) -> Result<(), String> {
        let mut client = Client::new(config).map_err(|e| e.to_string())?;

        // Connect
        client.connect(None).map_err(|e| e.to_string())?;

        // Wait for connection
        let connect_start = Instant::now();
        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                return Ok(());
            }

            client
                .poll(Some(Duration::from_millis(100)))
                .map_err(|e| e.to_string())?;

            while let Some(event) = client.next_event() {
                match event {
                    ClientEvent::Connected { session_present } => {
                        info!(
                            "Bridge '{}' connected (session_present: {})",
                            self.config.name, session_present
                        );
                        self.stats.connected.store(true, Ordering::Relaxed);

                        // Subscribe to configured topics
                        if !self.config.subscribe.is_empty() {
                            let topics: Vec<(&str, QoS)> = self
                                .config
                                .subscribe
                                .iter()
                                .map(|t| {
                                    let qos = match t.qos {
                                        0 => QoS::AtMostOnce,
                                        1 => QoS::AtLeastOnce,
                                        _ => QoS::ExactlyOnce,
                                    };
                                    (t.topic.as_str(), qos)
                                })
                                .collect();

                            if let Err(e) = client.subscribe(&topics) {
                                warn!(
                                    "Bridge '{}' subscribe failed: {}",
                                    self.config.name, e
                                );
                            }
                        }

                        // Start main loop
                        return self.run_main_loop(&mut client, subscriber_buf);
                    }
                    ClientEvent::Disconnected { reason } => {
                        return Err(reason.unwrap_or_else(|| "Disconnected".to_string()));
                    }
                    _ => {}
                }
            }

            if connect_start.elapsed() > Duration::from_secs(30) {
                return Err("Connection timeout".to_string());
            }
        }
    }

    /// Main event loop for an established connection.
    fn run_main_loop(
        &self,
        client: &mut Client,
        subscriber_buf: &mut Vec<crate::subscription::Subscriber>,
    ) -> Result<(), String> {
        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                let _ = client.disconnect();
                return Ok(());
            }

            // Poll for events from remote broker
            client
                .poll(Some(Duration::from_millis(100)))
                .map_err(|e| e.to_string())?;

            // Handle events
            while let Some(event) = client.next_event() {
                match event {
                    ClientEvent::Message {
                        topic,
                        payload,
                        qos,
                        retain,
                        ..
                    } => {
                        self.handle_remote_message(
                            topic,
                            payload,
                            qos,
                            retain,
                            subscriber_buf,
                        );
                    }
                    ClientEvent::Disconnected { reason } => {
                        self.stats.connected.store(false, Ordering::Relaxed);
                        return Err(reason.unwrap_or_else(|| "Disconnected".to_string()));
                    }
                    ClientEvent::SubAck { packet_id, return_codes } => {
                        debug!(
                            "Bridge '{}' suback packet_id={} codes={:?}",
                            self.config.name, packet_id, return_codes
                        );
                    }
                    _ => {}
                }
            }

            // TODO: Handle local→remote forwarding
            // This requires registering as a subscriber to local topics
            // and forwarding matching publishes to the remote broker
        }
    }

    /// Handle a message received from the remote broker.
    fn handle_remote_message(
        &self,
        mut topic: Bytes,
        payload: Bytes,
        qos: QoS,
        retain: bool,
        subscriber_buf: &mut Vec<crate::subscription::Subscriber>,
    ) {
        // Update stats
        self.stats.messages_received.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_received
            .fetch_add(payload.len() as u64, Ordering::Relaxed);

        // Apply local prefix if configured
        if let Some(ref prefix) = self.config.local_prefix {
            let mut new_topic = prefix.as_bytes().to_vec();
            new_topic.extend_from_slice(&topic);
            topic = Bytes::from(new_topic);
        }

        // Create publish packet for local broker
        let publish = mqlite_core::packet::Publish {
            dup: false,
            qos,
            retain,
            topic,
            packet_id: None, // Will be assigned by internal_publish if needed
            payload,
            properties: None,
        };

        // Publish to local broker
        self.shared.internal_publish(publish, subscriber_buf);

        debug!(
            "Bridge '{}' forwarded message from remote",
            self.config.name
        );
    }
}

/// Manager for all bridge connections.
pub struct BridgeManager {
    bridges: Vec<(Arc<BridgeStats>, Arc<AtomicBool>)>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl BridgeManager {
    /// Create a new bridge manager.
    pub fn new() -> Self {
        Self {
            bridges: Vec::new(),
            handles: Vec::new(),
        }
    }

    /// Start bridges from configuration.
    pub fn start_bridges(&mut self, configs: Vec<BridgeConfig>, shared: SharedStateHandle) {
        for config in configs {
            let name = config.name.clone();
            let bridge = Bridge::new(config, shared.clone());
            let stats = bridge.stats();
            let shutdown = bridge.shutdown.clone();

            info!("Starting bridge '{}'", name);
            let handle = bridge.start();

            self.bridges.push((stats, shutdown));
            self.handles.push(handle);
        }
    }

    /// Get statistics for all bridges.
    #[allow(dead_code)] // Will be used for $SYS topic publishing
    pub fn stats(&self) -> Vec<Arc<BridgeStats>> {
        self.bridges.iter().map(|(s, _)| s.clone()).collect()
    }

    /// Shutdown all bridges.
    pub fn shutdown(&mut self) {
        for (_, shutdown) in &self.bridges {
            shutdown.store(true, Ordering::SeqCst);
        }

        // Wait for all threads to finish
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Drop for BridgeManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}
