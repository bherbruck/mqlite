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

use crate::packet::{Publish, SubscriptionOptions};
use crate::subscription::SubscriptionStore;
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
}

impl Default for SharedState {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience type alias for shared state handle.
pub type SharedStateHandle = Arc<SharedState>;
