//! $SYS topic broker statistics implementation.
//!
//! Implements zero-copy, zero-allocation $SYS topics with:
//! - Atomic counters for metrics collection (updated by workers)
//! - Timer-based publishing with change detection
//! - Static topic strings and stack-allocated formatting

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;

use crate::packet::{Publish, QoS};
use crate::publish_encoder::PublishEncoder;
use crate::shared::{RetainedMessage, SharedStateHandle};
use crate::subscription::Subscriber;

/// Static topic string constants - zero allocation via Bytes::from_static().
pub mod topics {
    pub const VERSION: &str = "$SYS/broker/version";
    pub const UPTIME: &str = "$SYS/broker/uptime";

    // Client statistics
    pub const CLIENTS_CONNECTED: &str = "$SYS/broker/clients/connected";
    pub const CLIENTS_DISCONNECTED: &str = "$SYS/broker/clients/disconnected";
    pub const CLIENTS_TOTAL: &str = "$SYS/broker/clients/total";
    pub const CLIENTS_MAXIMUM: &str = "$SYS/broker/clients/maximum";
    pub const CLIENTS_EXPIRED: &str = "$SYS/broker/clients/expired";

    // Message counters
    pub const MESSAGES_RECEIVED: &str = "$SYS/broker/messages/received";
    pub const MESSAGES_SENT: &str = "$SYS/broker/messages/sent";
    pub const STORE_MESSAGES_COUNT: &str = "$SYS/broker/store/messages/count";
    pub const STORE_MESSAGES_BYTES: &str = "$SYS/broker/store/messages/bytes";

    // Publish statistics
    pub const PUBLISH_MESSAGES_RECEIVED: &str = "$SYS/broker/publish/messages/received";
    pub const PUBLISH_MESSAGES_SENT: &str = "$SYS/broker/publish/messages/sent";
    pub const PUBLISH_MESSAGES_DROPPED: &str = "$SYS/broker/publish/messages/dropped";
    pub const PUBLISH_BYTES_RECEIVED: &str = "$SYS/broker/publish/bytes/received";
    pub const PUBLISH_BYTES_SENT: &str = "$SYS/broker/publish/bytes/sent";

    // Byte counters
    pub const BYTES_RECEIVED: &str = "$SYS/broker/bytes/received";
    pub const BYTES_SENT: &str = "$SYS/broker/bytes/sent";

    // Subscription/Retained counts
    pub const SUBSCRIPTIONS_COUNT: &str = "$SYS/broker/subscriptions/count";
    pub const RETAINED_MESSAGES_COUNT: &str = "$SYS/broker/retained messages/count";

    // Load averages - messages received
    pub const LOAD_MESSAGES_RECEIVED_1MIN: &str = "$SYS/broker/load/messages/received/1min";
    pub const LOAD_MESSAGES_RECEIVED_5MIN: &str = "$SYS/broker/load/messages/received/5min";
    pub const LOAD_MESSAGES_RECEIVED_15MIN: &str = "$SYS/broker/load/messages/received/15min";

    // Load averages - messages sent
    pub const LOAD_MESSAGES_SENT_1MIN: &str = "$SYS/broker/load/messages/sent/1min";
    pub const LOAD_MESSAGES_SENT_5MIN: &str = "$SYS/broker/load/messages/sent/5min";
    pub const LOAD_MESSAGES_SENT_15MIN: &str = "$SYS/broker/load/messages/sent/15min";

    // Load averages - publish dropped
    pub const LOAD_PUBLISH_DROPPED_1MIN: &str = "$SYS/broker/load/publish/dropped/1min";
    pub const LOAD_PUBLISH_DROPPED_5MIN: &str = "$SYS/broker/load/publish/dropped/5min";
    pub const LOAD_PUBLISH_DROPPED_15MIN: &str = "$SYS/broker/load/publish/dropped/15min";

    // Load averages - publish received
    pub const LOAD_PUBLISH_RECEIVED_1MIN: &str = "$SYS/broker/load/publish/received/1min";
    pub const LOAD_PUBLISH_RECEIVED_5MIN: &str = "$SYS/broker/load/publish/received/5min";
    pub const LOAD_PUBLISH_RECEIVED_15MIN: &str = "$SYS/broker/load/publish/received/15min";

    // Load averages - publish sent
    pub const LOAD_PUBLISH_SENT_1MIN: &str = "$SYS/broker/load/publish/sent/1min";
    pub const LOAD_PUBLISH_SENT_5MIN: &str = "$SYS/broker/load/publish/sent/5min";
    pub const LOAD_PUBLISH_SENT_15MIN: &str = "$SYS/broker/load/publish/sent/15min";

    // Load averages - bytes received
    pub const LOAD_BYTES_RECEIVED_1MIN: &str = "$SYS/broker/load/bytes/received/1min";
    pub const LOAD_BYTES_RECEIVED_5MIN: &str = "$SYS/broker/load/bytes/received/5min";
    pub const LOAD_BYTES_RECEIVED_15MIN: &str = "$SYS/broker/load/bytes/received/15min";

    // Load averages - bytes sent
    pub const LOAD_BYTES_SENT_1MIN: &str = "$SYS/broker/load/bytes/sent/1min";
    pub const LOAD_BYTES_SENT_5MIN: &str = "$SYS/broker/load/bytes/sent/5min";
    pub const LOAD_BYTES_SENT_15MIN: &str = "$SYS/broker/load/bytes/sent/15min";

    // Load averages - sockets
    pub const LOAD_SOCKETS_1MIN: &str = "$SYS/broker/load/sockets/1min";
    pub const LOAD_SOCKETS_5MIN: &str = "$SYS/broker/load/sockets/5min";
    pub const LOAD_SOCKETS_15MIN: &str = "$SYS/broker/load/sockets/15min";

    // Load averages - connections
    pub const LOAD_CONNECTIONS_1MIN: &str = "$SYS/broker/load/connections/1min";
    pub const LOAD_CONNECTIONS_5MIN: &str = "$SYS/broker/load/connections/5min";
    pub const LOAD_CONNECTIONS_15MIN: &str = "$SYS/broker/load/connections/15min";
}

/// Global broker metrics using atomic counters.
/// Updated by workers on the hot path, read by timer thread.
pub struct BrokerMetrics {
    // Byte counters
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,

    // Message counters (all MQTT packets)
    pub msgs_received: AtomicU64,
    pub msgs_sent: AtomicU64,

    // Publish statistics
    pub pub_msgs_received: AtomicU64,
    pub pub_msgs_sent: AtomicU64,
    pub pub_msgs_dropped: AtomicU64,
    pub pub_bytes_received: AtomicU64,
    pub pub_bytes_sent: AtomicU64,

    // Connection statistics
    pub connections_total: AtomicU64,
    pub clients_connected: AtomicU64,
    pub clients_maximum: AtomicU64,
    pub clients_expired: AtomicU64,
    pub sockets_opened: AtomicU64,
}

impl BrokerMetrics {
    pub const fn new() -> Self {
        Self {
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            msgs_received: AtomicU64::new(0),
            msgs_sent: AtomicU64::new(0),
            pub_msgs_received: AtomicU64::new(0),
            pub_msgs_sent: AtomicU64::new(0),
            pub_msgs_dropped: AtomicU64::new(0),
            pub_bytes_received: AtomicU64::new(0),
            pub_bytes_sent: AtomicU64::new(0),
            connections_total: AtomicU64::new(0),
            clients_connected: AtomicU64::new(0),
            clients_maximum: AtomicU64::new(0),
            clients_expired: AtomicU64::new(0),
            sockets_opened: AtomicU64::new(0),
        }
    }

    // Inline increment methods for hot path performance

    #[inline]
    pub fn add_bytes_received(&self, n: u64) {
        self.bytes_received.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_bytes_sent(&self, n: u64) {
        self.bytes_sent.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_msgs_received(&self, n: u64) {
        self.msgs_received.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_msgs_sent(&self, n: u64) {
        self.msgs_sent.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_pub_msgs_received(&self, n: u64) {
        self.pub_msgs_received.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_pub_msgs_sent(&self, n: u64) {
        self.pub_msgs_sent.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_pub_msgs_dropped(&self, n: u64) {
        self.pub_msgs_dropped.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_pub_bytes_received(&self, n: u64) {
        self.pub_bytes_received.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_pub_bytes_sent(&self, n: u64) {
        self.pub_bytes_sent.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn increment_connections_total(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn increment_sockets_opened(&self) {
        self.sockets_opened.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn client_connected(&self) {
        let current = self.clients_connected.fetch_add(1, Ordering::Relaxed) + 1;
        // Update maximum if needed
        let mut max = self.clients_maximum.load(Ordering::Relaxed);
        while current > max {
            match self.clients_maximum.compare_exchange_weak(
                max,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => max = actual,
            }
        }
    }

    #[inline]
    pub fn client_disconnected(&self) {
        self.clients_connected.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn increment_clients_expired(&self) {
        self.clients_expired.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for BrokerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Previous values for change detection.
#[derive(Default)]
struct PreviousValues {
    bytes_received: u64,
    bytes_sent: u64,
    msgs_received: u64,
    msgs_sent: u64,
    pub_msgs_received: u64,
    pub_msgs_sent: u64,
    pub_msgs_dropped: u64,
    pub_bytes_received: u64,
    pub_bytes_sent: u64,
    connections_total: u64,
    clients_connected: u64,
    clients_maximum: u64,
    clients_expired: u64,
    sockets_opened: u64,
    uptime: u64,
    subscriptions_count: u64,
    retained_count: u64,
    clients_disconnected: u64,
    clients_total: u64,
    store_messages_count: u64,
    store_messages_bytes: u64,
}

/// Exponential moving average state for load calculations.
struct LoadAverage {
    /// EMA value
    value: f64,
    /// Previous counter value for delta calculation
    prev_counter: u64,
}

impl LoadAverage {
    fn new() -> Self {
        Self {
            value: 0.0,
            prev_counter: 0,
        }
    }

    /// Update with new counter value and return the load average.
    /// Alpha is the smoothing factor (e.g., 1 - e^(-interval/period)).
    fn update(&mut self, current: u64, alpha: f64) -> f64 {
        let delta = current.saturating_sub(self.prev_counter);
        self.prev_counter = current;
        self.value = alpha * delta as f64 + (1.0 - alpha) * self.value;
        self.value
    }
}

/// State for all load average calculations.
struct LoadAverageState {
    // Alpha values for 1min, 5min, 15min with typical 10s interval
    // alpha = 1 - e^(-interval/period)
    alpha_1min: f64,
    alpha_5min: f64,
    alpha_15min: f64,

    // Load averages for each metric
    msgs_received: [LoadAverage; 3],
    msgs_sent: [LoadAverage; 3],
    pub_dropped: [LoadAverage; 3],
    pub_received: [LoadAverage; 3],
    pub_sent: [LoadAverage; 3],
    bytes_received: [LoadAverage; 3],
    bytes_sent: [LoadAverage; 3],
    sockets: [LoadAverage; 3],
    connections: [LoadAverage; 3],
}

impl LoadAverageState {
    fn new(interval_secs: u64) -> Self {
        let interval = interval_secs as f64;
        Self {
            alpha_1min: 1.0 - (-interval / 60.0_f64).exp(),
            alpha_5min: 1.0 - (-interval / 300.0_f64).exp(),
            alpha_15min: 1.0 - (-interval / 900.0_f64).exp(),
            msgs_received: [LoadAverage::new(), LoadAverage::new(), LoadAverage::new()],
            msgs_sent: [LoadAverage::new(), LoadAverage::new(), LoadAverage::new()],
            pub_dropped: [LoadAverage::new(), LoadAverage::new(), LoadAverage::new()],
            pub_received: [LoadAverage::new(), LoadAverage::new(), LoadAverage::new()],
            pub_sent: [LoadAverage::new(), LoadAverage::new(), LoadAverage::new()],
            bytes_received: [LoadAverage::new(), LoadAverage::new(), LoadAverage::new()],
            bytes_sent: [LoadAverage::new(), LoadAverage::new(), LoadAverage::new()],
            sockets: [LoadAverage::new(), LoadAverage::new(), LoadAverage::new()],
            connections: [LoadAverage::new(), LoadAverage::new(), LoadAverage::new()],
        }
    }

    /// Update all load averages and return the values.
    fn update(
        &mut self,
        msgs_received: u64,
        msgs_sent: u64,
        pub_dropped: u64,
        pub_received: u64,
        pub_sent: u64,
        bytes_received: u64,
        bytes_sent: u64,
        sockets: u64,
        connections: u64,
    ) -> LoadAverageValues {
        LoadAverageValues {
            msgs_received: [
                self.msgs_received[0].update(msgs_received, self.alpha_1min),
                self.msgs_received[1].update(msgs_received, self.alpha_5min),
                self.msgs_received[2].update(msgs_received, self.alpha_15min),
            ],
            msgs_sent: [
                self.msgs_sent[0].update(msgs_sent, self.alpha_1min),
                self.msgs_sent[1].update(msgs_sent, self.alpha_5min),
                self.msgs_sent[2].update(msgs_sent, self.alpha_15min),
            ],
            pub_dropped: [
                self.pub_dropped[0].update(pub_dropped, self.alpha_1min),
                self.pub_dropped[1].update(pub_dropped, self.alpha_5min),
                self.pub_dropped[2].update(pub_dropped, self.alpha_15min),
            ],
            pub_received: [
                self.pub_received[0].update(pub_received, self.alpha_1min),
                self.pub_received[1].update(pub_received, self.alpha_5min),
                self.pub_received[2].update(pub_received, self.alpha_15min),
            ],
            pub_sent: [
                self.pub_sent[0].update(pub_sent, self.alpha_1min),
                self.pub_sent[1].update(pub_sent, self.alpha_5min),
                self.pub_sent[2].update(pub_sent, self.alpha_15min),
            ],
            bytes_received: [
                self.bytes_received[0].update(bytes_received, self.alpha_1min),
                self.bytes_received[1].update(bytes_received, self.alpha_5min),
                self.bytes_received[2].update(bytes_received, self.alpha_15min),
            ],
            bytes_sent: [
                self.bytes_sent[0].update(bytes_sent, self.alpha_1min),
                self.bytes_sent[1].update(bytes_sent, self.alpha_5min),
                self.bytes_sent[2].update(bytes_sent, self.alpha_15min),
            ],
            sockets: [
                self.sockets[0].update(sockets, self.alpha_1min),
                self.sockets[1].update(sockets, self.alpha_5min),
                self.sockets[2].update(sockets, self.alpha_15min),
            ],
            connections: [
                self.connections[0].update(connections, self.alpha_1min),
                self.connections[1].update(connections, self.alpha_5min),
                self.connections[2].update(connections, self.alpha_15min),
            ],
        }
    }
}

/// Computed load average values.
struct LoadAverageValues {
    msgs_received: [f64; 3],
    msgs_sent: [f64; 3],
    pub_dropped: [f64; 3],
    pub_received: [f64; 3],
    pub_sent: [f64; 3],
    bytes_received: [f64; 3],
    bytes_sent: [f64; 3],
    sockets: [f64; 3],
    connections: [f64; 3],
}

/// Previous load average values for change detection.
#[derive(Default)]
struct PreviousLoadAverages {
    msgs_received: [u64; 3],
    msgs_sent: [u64; 3],
    pub_dropped: [u64; 3],
    pub_received: [u64; 3],
    pub_sent: [u64; 3],
    bytes_received: [u64; 3],
    bytes_sent: [u64; 3],
    sockets: [u64; 3],
    connections: [u64; 3],
}

/// Publisher for $SYS topics that runs on a timer.
pub struct SysTreePublisher {
    shared: SharedStateHandle,
    start_time: Instant,
    prev: PreviousValues,
    load_state: LoadAverageState,
    prev_load: PreviousLoadAverages,
    subscriber_buf: Vec<Subscriber>,
}

impl SysTreePublisher {
    /// Create a new $SYS tree publisher.
    pub fn new(shared: SharedStateHandle, interval_secs: u64) -> Self {
        // Publish static values on creation
        let publisher = Self {
            shared,
            start_time: Instant::now(),
            prev: PreviousValues::default(),
            load_state: LoadAverageState::new(interval_secs),
            prev_load: PreviousLoadAverages::default(),
            subscriber_buf: Vec::with_capacity(64),
        };

        // Publish version immediately (static, never changes)
        publisher.publish_static(
            topics::VERSION,
            format!("mqlite {}", env!("CARGO_PKG_VERSION")),
        );

        publisher
    }

    /// Publish a static value (no change detection).
    fn publish_static(&self, topic: &'static str, value: String) {
        let topic_bytes = Bytes::from_static(topic.as_bytes());
        let payload = Bytes::from(value);

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: topic_bytes,
            packet_id: None,
            payload,
            properties: None,
        };

        // Store as retained message
        self.shared.retained_messages.write().insert(
            topic.to_string(),
            RetainedMessage {
                publish,
                stored_at: Instant::now(),
            },
        );
    }

    /// Publish metrics if changed since last call.
    pub fn publish_if_changed(&mut self) {
        let metrics = &self.shared.metrics;

        // Read all metrics with Relaxed ordering (eventual consistency OK)
        let bytes_received = metrics.bytes_received.load(Ordering::Relaxed);
        let bytes_sent = metrics.bytes_sent.load(Ordering::Relaxed);
        let msgs_received = metrics.msgs_received.load(Ordering::Relaxed);
        let msgs_sent = metrics.msgs_sent.load(Ordering::Relaxed);
        let pub_msgs_received = metrics.pub_msgs_received.load(Ordering::Relaxed);
        let pub_msgs_sent = metrics.pub_msgs_sent.load(Ordering::Relaxed);
        let pub_msgs_dropped = metrics.pub_msgs_dropped.load(Ordering::Relaxed);
        let pub_bytes_received = metrics.pub_bytes_received.load(Ordering::Relaxed);
        let pub_bytes_sent = metrics.pub_bytes_sent.load(Ordering::Relaxed);
        let connections_total = metrics.connections_total.load(Ordering::Relaxed);
        let clients_connected = metrics.clients_connected.load(Ordering::Relaxed);
        let clients_maximum = metrics.clients_maximum.load(Ordering::Relaxed);
        let clients_expired = metrics.clients_expired.load(Ordering::Relaxed);
        let sockets_opened = metrics.sockets_opened.load(Ordering::Relaxed);

        // Derived metrics
        let uptime = self.start_time.elapsed().as_secs();
        let subscriptions_count = self.shared.subscriptions.read().subscription_count() as u64;
        let retained_count = self.shared.retained_messages.read().len() as u64;
        let sessions_count = self.shared.sessions.read().len() as u64;
        let clients_disconnected = sessions_count.saturating_sub(clients_connected);
        let clients_total = clients_connected + clients_disconnected;

        // Store message count and bytes (from retained messages)
        let (store_messages_count, store_messages_bytes) = {
            let retained = self.shared.retained_messages.read();
            let count = retained.len() as u64;
            let bytes: u64 = retained
                .values()
                .map(|r| r.publish.payload.len() as u64)
                .sum();
            (count, bytes)
        };

        // Publish changed counters (inline change detection to avoid borrow issues)
        if bytes_received != self.prev.bytes_received {
            self.prev.bytes_received = bytes_received;
            self.publish_u64(topics::BYTES_RECEIVED, bytes_received);
        }
        if bytes_sent != self.prev.bytes_sent {
            self.prev.bytes_sent = bytes_sent;
            self.publish_u64(topics::BYTES_SENT, bytes_sent);
        }
        if msgs_received != self.prev.msgs_received {
            self.prev.msgs_received = msgs_received;
            self.publish_u64(topics::MESSAGES_RECEIVED, msgs_received);
        }
        if msgs_sent != self.prev.msgs_sent {
            self.prev.msgs_sent = msgs_sent;
            self.publish_u64(topics::MESSAGES_SENT, msgs_sent);
        }
        if pub_msgs_received != self.prev.pub_msgs_received {
            self.prev.pub_msgs_received = pub_msgs_received;
            self.publish_u64(topics::PUBLISH_MESSAGES_RECEIVED, pub_msgs_received);
        }
        if pub_msgs_sent != self.prev.pub_msgs_sent {
            self.prev.pub_msgs_sent = pub_msgs_sent;
            self.publish_u64(topics::PUBLISH_MESSAGES_SENT, pub_msgs_sent);
        }
        if pub_msgs_dropped != self.prev.pub_msgs_dropped {
            self.prev.pub_msgs_dropped = pub_msgs_dropped;
            self.publish_u64(topics::PUBLISH_MESSAGES_DROPPED, pub_msgs_dropped);
        }
        if pub_bytes_received != self.prev.pub_bytes_received {
            self.prev.pub_bytes_received = pub_bytes_received;
            self.publish_u64(topics::PUBLISH_BYTES_RECEIVED, pub_bytes_received);
        }
        if pub_bytes_sent != self.prev.pub_bytes_sent {
            self.prev.pub_bytes_sent = pub_bytes_sent;
            self.publish_u64(topics::PUBLISH_BYTES_SENT, pub_bytes_sent);
        }
        if clients_connected != self.prev.clients_connected {
            self.prev.clients_connected = clients_connected;
            self.publish_u64(topics::CLIENTS_CONNECTED, clients_connected);
        }
        if clients_maximum != self.prev.clients_maximum {
            self.prev.clients_maximum = clients_maximum;
            self.publish_u64(topics::CLIENTS_MAXIMUM, clients_maximum);
        }
        if clients_expired != self.prev.clients_expired {
            self.prev.clients_expired = clients_expired;
            self.publish_u64(topics::CLIENTS_EXPIRED, clients_expired);
        }
        if clients_disconnected != self.prev.clients_disconnected {
            self.prev.clients_disconnected = clients_disconnected;
            self.publish_u64(topics::CLIENTS_DISCONNECTED, clients_disconnected);
        }
        if clients_total != self.prev.clients_total {
            self.prev.clients_total = clients_total;
            self.publish_u64(topics::CLIENTS_TOTAL, clients_total);
        }
        if subscriptions_count != self.prev.subscriptions_count {
            self.prev.subscriptions_count = subscriptions_count;
            self.publish_u64(topics::SUBSCRIPTIONS_COUNT, subscriptions_count);
        }
        if retained_count != self.prev.retained_count {
            self.prev.retained_count = retained_count;
            self.publish_u64(topics::RETAINED_MESSAGES_COUNT, retained_count);
        }
        if store_messages_count != self.prev.store_messages_count {
            self.prev.store_messages_count = store_messages_count;
            self.publish_u64(topics::STORE_MESSAGES_COUNT, store_messages_count);
        }
        if store_messages_bytes != self.prev.store_messages_bytes {
            self.prev.store_messages_bytes = store_messages_bytes;
            self.publish_u64(topics::STORE_MESSAGES_BYTES, store_messages_bytes);
        }

        // Uptime always changes, format as "N seconds"
        if uptime != self.prev.uptime {
            self.prev.uptime = uptime;
            self.publish_uptime(uptime);
        }

        // Update and publish load averages
        let loads = self.load_state.update(
            msgs_received,
            msgs_sent,
            pub_msgs_dropped,
            pub_msgs_received,
            pub_msgs_sent,
            bytes_received,
            bytes_sent,
            sockets_opened,
            connections_total,
        );

        self.publish_load_averages(&loads);
    }

    /// Publish a u64 metric value.
    fn publish_u64(&mut self, topic: &'static str, value: u64) {
        // Stack-allocated formatting using itoa::Buffer
        let mut buf = itoa::Buffer::new();
        let formatted = buf.format(value);
        let payload = Bytes::copy_from_slice(formatted.as_bytes());
        let topic_bytes = Bytes::from_static(topic.as_bytes());

        self.publish_retained_and_fanout(topic, topic_bytes, payload);
    }

    /// Publish uptime in "N seconds" format.
    fn publish_uptime(&mut self, seconds: u64) {
        let mut itoa_buf = itoa::Buffer::new();
        let formatted = itoa_buf.format(seconds);
        let mut buf = [0u8; 48];
        let n = formatted.len();
        buf[..n].copy_from_slice(formatted.as_bytes());
        let suffix = b" seconds";
        buf[n..n + suffix.len()].copy_from_slice(suffix);
        let total_len = n + suffix.len();
        let payload = Bytes::copy_from_slice(&buf[..total_len]);
        let topic_bytes = Bytes::from_static(topics::UPTIME.as_bytes());

        self.publish_retained_and_fanout(topics::UPTIME, topic_bytes, payload);
    }

    /// Format and publish a load average value (as float with 2 decimal places).
    fn format_and_publish_load(&mut self, topic: &'static str, value: f64) {
        // Convert to fixed-point (2 decimal places)
        let value_fixed = (value * 100.0) as u64;

        // Format as "X.XX" using itoa::Buffer
        let mut buf = [0u8; 32];
        let integer_part = value_fixed / 100;
        let decimal_part = value_fixed % 100;

        let mut itoa_buf = itoa::Buffer::new();
        let int_str = itoa_buf.format(integer_part);
        let n = int_str.len();
        buf[..n].copy_from_slice(int_str.as_bytes());
        buf[n] = b'.';
        let pos = n + 1;

        // Pad decimal with leading zero if needed
        let total = if decimal_part < 10 {
            buf[pos] = b'0';
            let mut itoa_buf2 = itoa::Buffer::new();
            let dec_str = itoa_buf2.format(decimal_part);
            buf[pos + 1..pos + 1 + dec_str.len()].copy_from_slice(dec_str.as_bytes());
            pos + 1 + dec_str.len()
        } else {
            let mut itoa_buf2 = itoa::Buffer::new();
            let dec_str = itoa_buf2.format(decimal_part);
            buf[pos..pos + dec_str.len()].copy_from_slice(dec_str.as_bytes());
            pos + dec_str.len()
        };

        let payload = Bytes::copy_from_slice(&buf[..total]);
        let topic_bytes = Bytes::from_static(topic.as_bytes());
        self.publish_retained_and_fanout(topic, topic_bytes, payload);
    }

    /// Check if load value changed (standalone function to avoid borrow issues).
    #[inline]
    fn load_changed(value: f64, prev: &mut u64) -> bool {
        let value_fixed = (value * 100.0) as u64;
        if value_fixed != *prev {
            *prev = value_fixed;
            true
        } else {
            false
        }
    }

    /// Publish all load averages.
    fn publish_load_averages(&mut self, loads: &LoadAverageValues) {
        // Messages received
        if Self::load_changed(loads.msgs_received[0], &mut self.prev_load.msgs_received[0]) {
            self.format_and_publish_load(topics::LOAD_MESSAGES_RECEIVED_1MIN, loads.msgs_received[0]);
        }
        if Self::load_changed(loads.msgs_received[1], &mut self.prev_load.msgs_received[1]) {
            self.format_and_publish_load(topics::LOAD_MESSAGES_RECEIVED_5MIN, loads.msgs_received[1]);
        }
        if Self::load_changed(loads.msgs_received[2], &mut self.prev_load.msgs_received[2]) {
            self.format_and_publish_load(topics::LOAD_MESSAGES_RECEIVED_15MIN, loads.msgs_received[2]);
        }

        // Messages sent
        if Self::load_changed(loads.msgs_sent[0], &mut self.prev_load.msgs_sent[0]) {
            self.format_and_publish_load(topics::LOAD_MESSAGES_SENT_1MIN, loads.msgs_sent[0]);
        }
        if Self::load_changed(loads.msgs_sent[1], &mut self.prev_load.msgs_sent[1]) {
            self.format_and_publish_load(topics::LOAD_MESSAGES_SENT_5MIN, loads.msgs_sent[1]);
        }
        if Self::load_changed(loads.msgs_sent[2], &mut self.prev_load.msgs_sent[2]) {
            self.format_and_publish_load(topics::LOAD_MESSAGES_SENT_15MIN, loads.msgs_sent[2]);
        }

        // Publish dropped
        if Self::load_changed(loads.pub_dropped[0], &mut self.prev_load.pub_dropped[0]) {
            self.format_and_publish_load(topics::LOAD_PUBLISH_DROPPED_1MIN, loads.pub_dropped[0]);
        }
        if Self::load_changed(loads.pub_dropped[1], &mut self.prev_load.pub_dropped[1]) {
            self.format_and_publish_load(topics::LOAD_PUBLISH_DROPPED_5MIN, loads.pub_dropped[1]);
        }
        if Self::load_changed(loads.pub_dropped[2], &mut self.prev_load.pub_dropped[2]) {
            self.format_and_publish_load(topics::LOAD_PUBLISH_DROPPED_15MIN, loads.pub_dropped[2]);
        }

        // Publish received
        if Self::load_changed(loads.pub_received[0], &mut self.prev_load.pub_received[0]) {
            self.format_and_publish_load(topics::LOAD_PUBLISH_RECEIVED_1MIN, loads.pub_received[0]);
        }
        if Self::load_changed(loads.pub_received[1], &mut self.prev_load.pub_received[1]) {
            self.format_and_publish_load(topics::LOAD_PUBLISH_RECEIVED_5MIN, loads.pub_received[1]);
        }
        if Self::load_changed(loads.pub_received[2], &mut self.prev_load.pub_received[2]) {
            self.format_and_publish_load(topics::LOAD_PUBLISH_RECEIVED_15MIN, loads.pub_received[2]);
        }

        // Publish sent
        if Self::load_changed(loads.pub_sent[0], &mut self.prev_load.pub_sent[0]) {
            self.format_and_publish_load(topics::LOAD_PUBLISH_SENT_1MIN, loads.pub_sent[0]);
        }
        if Self::load_changed(loads.pub_sent[1], &mut self.prev_load.pub_sent[1]) {
            self.format_and_publish_load(topics::LOAD_PUBLISH_SENT_5MIN, loads.pub_sent[1]);
        }
        if Self::load_changed(loads.pub_sent[2], &mut self.prev_load.pub_sent[2]) {
            self.format_and_publish_load(topics::LOAD_PUBLISH_SENT_15MIN, loads.pub_sent[2]);
        }

        // Bytes received
        if Self::load_changed(loads.bytes_received[0], &mut self.prev_load.bytes_received[0]) {
            self.format_and_publish_load(topics::LOAD_BYTES_RECEIVED_1MIN, loads.bytes_received[0]);
        }
        if Self::load_changed(loads.bytes_received[1], &mut self.prev_load.bytes_received[1]) {
            self.format_and_publish_load(topics::LOAD_BYTES_RECEIVED_5MIN, loads.bytes_received[1]);
        }
        if Self::load_changed(loads.bytes_received[2], &mut self.prev_load.bytes_received[2]) {
            self.format_and_publish_load(topics::LOAD_BYTES_RECEIVED_15MIN, loads.bytes_received[2]);
        }

        // Bytes sent
        if Self::load_changed(loads.bytes_sent[0], &mut self.prev_load.bytes_sent[0]) {
            self.format_and_publish_load(topics::LOAD_BYTES_SENT_1MIN, loads.bytes_sent[0]);
        }
        if Self::load_changed(loads.bytes_sent[1], &mut self.prev_load.bytes_sent[1]) {
            self.format_and_publish_load(topics::LOAD_BYTES_SENT_5MIN, loads.bytes_sent[1]);
        }
        if Self::load_changed(loads.bytes_sent[2], &mut self.prev_load.bytes_sent[2]) {
            self.format_and_publish_load(topics::LOAD_BYTES_SENT_15MIN, loads.bytes_sent[2]);
        }

        // Sockets
        if Self::load_changed(loads.sockets[0], &mut self.prev_load.sockets[0]) {
            self.format_and_publish_load(topics::LOAD_SOCKETS_1MIN, loads.sockets[0]);
        }
        if Self::load_changed(loads.sockets[1], &mut self.prev_load.sockets[1]) {
            self.format_and_publish_load(topics::LOAD_SOCKETS_5MIN, loads.sockets[1]);
        }
        if Self::load_changed(loads.sockets[2], &mut self.prev_load.sockets[2]) {
            self.format_and_publish_load(topics::LOAD_SOCKETS_15MIN, loads.sockets[2]);
        }

        // Connections
        if Self::load_changed(loads.connections[0], &mut self.prev_load.connections[0]) {
            self.format_and_publish_load(topics::LOAD_CONNECTIONS_1MIN, loads.connections[0]);
        }
        if Self::load_changed(loads.connections[1], &mut self.prev_load.connections[1]) {
            self.format_and_publish_load(topics::LOAD_CONNECTIONS_5MIN, loads.connections[1]);
        }
        if Self::load_changed(loads.connections[2], &mut self.prev_load.connections[2]) {
            self.format_and_publish_load(topics::LOAD_CONNECTIONS_15MIN, loads.connections[2]);
        }
    }

    /// Publish as retained and fan out to $SYS/# subscribers.
    fn publish_retained_and_fanout(
        &mut self,
        topic_str: &str,
        topic_bytes: Bytes,
        payload: Bytes,
    ) {
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: topic_bytes.clone(),
            packet_id: None,
            payload: payload.clone(),
            properties: None,
        };

        // Store as retained message
        self.shared.retained_messages.write().insert(
            topic_str.to_string(),
            RetainedMessage {
                publish: publish.clone(),
                stored_at: Instant::now(),
            },
        );

        // Fan out to subscribers matching $SYS/#
        self.subscriber_buf.clear();
        {
            let subscriptions = self.shared.subscriptions.read();
            subscriptions.match_topic_bytes_into(&topic_bytes, &mut self.subscriber_buf);
        }

        if self.subscriber_buf.is_empty() {
            return;
        }

        let mut factory = PublishEncoder::new(topic_bytes, payload, None);

        for sub in &self.subscriber_buf {
            // $SYS topics are always QoS 0
            let _ = sub.handle.queue_publish(&mut factory, QoS::AtMostOnce, None, false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_metrics_new() {
        let metrics = BrokerMetrics::new();
        assert_eq!(metrics.bytes_received.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.bytes_sent.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_broker_metrics_add() {
        let metrics = BrokerMetrics::new();
        metrics.add_bytes_received(100);
        metrics.add_bytes_received(50);
        assert_eq!(metrics.bytes_received.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn test_client_connected_updates_maximum() {
        let metrics = BrokerMetrics::new();
        metrics.client_connected();
        metrics.client_connected();
        metrics.client_connected();
        assert_eq!(metrics.clients_connected.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.clients_maximum.load(Ordering::Relaxed), 3);

        metrics.client_disconnected();
        assert_eq!(metrics.clients_connected.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.clients_maximum.load(Ordering::Relaxed), 3);

        metrics.client_connected();
        metrics.client_connected();
        assert_eq!(metrics.clients_connected.load(Ordering::Relaxed), 4);
        assert_eq!(metrics.clients_maximum.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn test_load_average_calculation() {
        let mut load = LoadAverage::new();

        // With alpha = 0.5, each update blends 50% new value
        let alpha = 0.5;

        // First update: delta = 100
        let v1 = load.update(100, alpha);
        assert!((v1 - 50.0).abs() < 0.001); // 0.5 * 100 + 0.5 * 0 = 50

        // Second update: delta = 100, prev = 100
        let v2 = load.update(200, alpha);
        assert!((v2 - 75.0).abs() < 0.001); // 0.5 * 100 + 0.5 * 50 = 75
    }
}
