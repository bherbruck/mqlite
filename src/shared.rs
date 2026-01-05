//! Shared state for multi-threaded broker.
//!
//! This module contains state that is shared across all worker threads:
//! - SubscriptionStore: topic trie (read-heavy, write on sub/unsub)
//! - Sessions: persistent sessions (read on connect, write on disconnect)
//! - RetainedMessages: retained message store
//! - ClientRegistry: maps ClientId → (worker_id, Token) for routing
//! - BrokerMetrics: atomic counters for $SYS topics

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use mio::Token;

use crate::packet::{Publish, QoS, SubscriptionOptions};
use crate::publish_encoder::PublishEncoder;
use crate::subscription::{Subscriber, SubscriptionStore};
use crate::sys_tree::BrokerMetrics;

/// A retained message with timestamp for expiry countdown.
#[derive(Debug, Clone)]
pub struct RetainedMessage {
    pub publish: Publish,
    /// When the message was stored (for Message Expiry Interval countdown).
    pub stored_at: Instant,
}

/// Stored subscription info for session persistence.
#[derive(Debug, Clone)]
pub struct StoredSubscription {
    pub topic_filter: String,
    pub options: SubscriptionOptions,
    pub subscription_id: Option<u32>,
}

/// Stored session state for CleanSession=0 clients.
#[derive(Debug, Default, Clone)]
pub struct Session {
    /// Subscribed topic filters with options and subscription IDs.
    pub subscriptions: Vec<StoredSubscription>,
    /// Pending QoS 1 messages not yet acknowledged (packet_id, Publish).
    pub pending_qos1: Vec<(u16, Publish)>,
    /// Pending QoS 2 messages not yet completed (packet_id, Publish).
    pub pending_qos2: Vec<(u16, Publish)>,
    /// Last connection info (worker_id, token) for cleanup on reconnect.
    pub last_connection: Option<(usize, Token)>,
    /// Set to true when pending messages have been saved after a takeover.
    /// Used for synchronization between workers during client takeover.
    pub takeover_complete: bool,
}

/// Client location info for cross-thread routing.
#[derive(Debug, Clone, Copy)]
pub struct ClientLocation {
    pub worker_id: usize,
    pub token: Token,
}

/// Shared state protected by RwLock for multi-threaded access.
///
/// In Phase 1, we use a single worker (worker_id=0) but structure the code
/// to support multiple workers in later phases.
pub struct SharedState {
    /// Topic subscription trie.
    pub subscriptions: RwLock<SubscriptionStore>,
    /// Persistent sessions for CleanSession=0 clients.
    pub sessions: RwLock<HashMap<String, Session>>,
    /// Retained messages stored by topic (with timestamp for expiry countdown).
    pub retained_messages: RwLock<HashMap<String, RetainedMessage>>,
    /// Maps ClientId → ClientLocation for cross-thread routing and duplicate detection.
    pub client_registry: RwLock<HashMap<String, ClientLocation>>,
    /// Broker metrics for $SYS topics (NOT behind RwLock - uses atomics).
    pub metrics: BrokerMetrics,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(SubscriptionStore::new()),
            sessions: RwLock::new(HashMap::new()),
            retained_messages: RwLock::new(HashMap::new()),
            client_registry: RwLock::new(HashMap::new()),
            metrics: BrokerMetrics::new(),
        }
    }

    /// Publish a message internally (for $SYS topics, bridges, etc.).
    ///
    /// This method handles:
    /// - Storing retained messages if `publish.retain` is true
    /// - Fanout to all matching subscribers
    /// - Proper retain flag handling per MQTT spec
    ///
    /// Used by internal broker components that need to publish messages
    /// through the same path as regular client publishes.
    pub fn internal_publish(&self, publish: Publish, subscriber_buf: &mut Vec<Subscriber>) {
        // 1. Handle retained message storage
        if publish.retain {
            let topic_str = String::from_utf8_lossy(&publish.topic).into_owned();
            if publish.payload.is_empty() {
                // Empty payload = delete retained message
                self.retained_messages.write().remove(&topic_str);
            } else {
                self.retained_messages.write().insert(
                    topic_str,
                    RetainedMessage {
                        publish: publish.clone(),
                        stored_at: Instant::now(),
                    },
                );
            }
        }

        // 2. Find all matching subscribers
        subscriber_buf.clear();
        self.subscriptions
            .read()
            .match_topic_bytes_into(&publish.topic, subscriber_buf);

        if subscriber_buf.is_empty() {
            return;
        }

        // 3. Fanout to each subscriber
        let mut factory = PublishEncoder::new(
            publish.topic.clone(),
            publish.payload.clone(),
            publish.properties.clone(),
        );

        for sub in subscriber_buf.iter() {
            // For internal publishes (like $SYS), preserve the retain flag as-is.
            // This is broker-originated, not client-forwarding, so retain_as_published
            // doesn't apply.
            let out_retain = publish.retain;

            // Downgrade QoS to subscriber's max
            let effective_qos = std::cmp::min(publish.qos as u8, sub.qos as u8);
            let out_qos = match effective_qos {
                0 => QoS::AtMostOnce,
                1 => QoS::AtLeastOnce,
                _ => QoS::ExactlyOnce,
            };

            let packet_id = if out_qos != QoS::AtMostOnce {
                Some(sub.handle.allocate_packet_id())
            } else {
                None
            };

            // Queue to subscriber (ignore slow client errors)
            let _ = sub
                .handle
                .queue_publish(&mut factory, out_qos, packet_id, out_retain);
        }
    }

    /// Convenience method to publish with a fresh subscriber buffer.
    /// For frequent publishing, prefer `internal_publish` with a reusable buffer.
    #[allow(dead_code)]
    pub fn internal_publish_alloc(&self, publish: Publish) {
        let mut buf = Vec::with_capacity(64);
        self.internal_publish(publish, &mut buf);
    }
}

impl Default for SharedState {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience type alias for shared state handle.
pub type SharedStateHandle = Arc<SharedState>;
