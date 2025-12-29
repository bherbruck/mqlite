//! Subscription store with trie-based topic matching.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use mio::Token;

use crate::client_handle::ClientWriteHandle;
use crate::packet::QoS;

/// Subscriber info stored in the trie.
///
/// Contains both identification (token) and direct write access (handle).
/// The handle allows any thread to write directly to the client's buffer
/// without channel-based message passing.
#[derive(Clone)]
pub struct Subscriber {
    /// Direct write access to the client's buffer.
    pub handle: Arc<ClientWriteHandle>,
    /// Subscription QoS.
    pub qos: QoS,
    /// Client ID for session lookup (needed for cross-worker QoS tracking).
    /// Arc<str> for zero-copy cloning in route cache.
    pub client_id: Arc<str>,
}

impl Subscriber {
    /// Get the client's token (for identification/removal).
    pub fn token(&self) -> Token {
        self.handle.token()
    }

    /// Get the worker ID (for identification/removal).
    pub fn worker_id(&self) -> usize {
        self.handle.worker_id()
    }
}

/// A node in the subscription trie.
#[derive(Default)]
struct TrieNode {
    /// Direct subscribers at this node.
    subscribers: Vec<Subscriber>,
    /// Child nodes by topic level.
    children: HashMap<String, TrieNode>,
    /// Single-level wildcard (+) subscribers.
    single_wildcard: Option<Box<TrieNode>>,
    /// Multi-level wildcard (#) subscribers.
    multi_wildcard: Vec<Subscriber>,
}

impl TrieNode {
    fn new() -> Self {
        Self::default()
    }

    fn insert(&mut self, levels: &[&str], subscriber: Subscriber) {
        if levels.is_empty() {
            // Add subscriber at this node
            // Remove existing subscription from same client (matched by worker_id + token)
            let wid = subscriber.worker_id();
            let tok = subscriber.token();
            self.subscribers
                .retain(|s| !(s.worker_id() == wid && s.token() == tok));
            self.subscribers.push(subscriber);
            return;
        }

        let level = levels[0];
        let remaining = &levels[1..];

        match level {
            "#" => {
                // Multi-level wildcard - must be last
                let wid = subscriber.worker_id();
                let tok = subscriber.token();
                self.multi_wildcard
                    .retain(|s| !(s.worker_id() == wid && s.token() == tok));
                self.multi_wildcard.push(subscriber);
            }
            "+" => {
                // Single-level wildcard
                let child = self
                    .single_wildcard
                    .get_or_insert_with(|| Box::new(TrieNode::new()));
                child.insert(remaining, subscriber);
            }
            _ => {
                // Normal level
                let child = self.children.entry(level.to_string()).or_default();
                child.insert(remaining, subscriber);
            }
        }
    }

    fn remove(&mut self, levels: &[&str], worker_id: usize, token: Token) {
        if levels.is_empty() {
            self.subscribers
                .retain(|s| !(s.worker_id() == worker_id && s.token() == token));
            return;
        }

        let level = levels[0];
        let remaining = &levels[1..];

        match level {
            "#" => {
                self.multi_wildcard
                    .retain(|s| !(s.worker_id() == worker_id && s.token() == token));
            }
            "+" => {
                if let Some(child) = &mut self.single_wildcard {
                    child.remove(remaining, worker_id, token);
                }
            }
            _ => {
                if let Some(child) = self.children.get_mut(level) {
                    child.remove(remaining, worker_id, token);
                }
            }
        }
    }

    fn remove_client(&mut self, worker_id: usize, token: Token) {
        self.subscribers
            .retain(|s| !(s.worker_id() == worker_id && s.token() == token));
        self.multi_wildcard
            .retain(|s| !(s.worker_id() == worker_id && s.token() == token));

        if let Some(child) = &mut self.single_wildcard {
            child.remove_client(worker_id, token);
        }

        for child in self.children.values_mut() {
            child.remove_client(worker_id, token);
        }
    }

    fn collect_subscribers(&self, levels: &[&str], is_root: bool, result: &mut Vec<Subscriber>) {
        // MQTT-4.7.2-1: Topics starting with $ are not matched by wildcards at root level
        let skip_wildcards = is_root && levels.first().is_some_and(|l| l.starts_with('$'));

        if !skip_wildcards {
            // Multi-level wildcard matches everything from here
            result.extend(self.multi_wildcard.iter().cloned());
        }

        if levels.is_empty() {
            // End of topic - collect direct subscribers
            result.extend(self.subscribers.iter().cloned());
            return;
        }

        let level = levels[0];
        let remaining = &levels[1..];

        // Check single-level wildcard (skip for $ topics at root)
        if !skip_wildcards {
            if let Some(child) = &self.single_wildcard {
                child.collect_subscribers(remaining, false, result);
            }
        }

        // Check exact match
        if let Some(child) = self.children.get(level) {
            child.collect_subscribers(remaining, false, result);
        }
    }
}

/// Subscription store using a trie for efficient topic matching.
pub struct SubscriptionStore {
    root: TrieNode,
    /// Generation counter for cache invalidation.
    /// Incremented on any subscription change.
    generation: AtomicU64,
}

impl SubscriptionStore {
    pub fn new() -> Self {
        Self {
            root: TrieNode::new(),
            generation: AtomicU64::new(0),
        }
    }

    /// Get current generation (for cache validation).
    /// Can be called with just a read lock.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Bump generation to invalidate all route caches.
    #[inline]
    fn invalidate_caches(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Subscribe to a topic filter.
    pub fn subscribe(&mut self, topic_filter: &str, subscriber: Subscriber) {
        let levels: Vec<&str> = topic_filter.split('/').collect();
        self.root.insert(&levels, subscriber);
        self.invalidate_caches();
    }

    /// Unsubscribe from a topic filter.
    pub fn unsubscribe(&mut self, topic_filter: &str, worker_id: usize, token: Token) {
        let levels: Vec<&str> = topic_filter.split('/').collect();
        self.root.remove(&levels, worker_id, token);
        self.invalidate_caches();
    }

    /// Remove all subscriptions for a client.
    pub fn remove_client(&mut self, worker_id: usize, token: Token) {
        self.root.remove_client(worker_id, token);
        self.invalidate_caches();
    }

    /// Find all subscribers matching a topic.
    #[allow(dead_code)]
    pub fn match_topic(&self, topic: &str) -> Vec<Subscriber> {
        let mut subscribers = Vec::new();
        self.match_topic_into(topic, &mut subscribers);
        subscribers
    }

    /// Find all subscribers matching a topic, reusing a buffer to avoid allocation.
    #[inline]
    fn match_topic_into(&self, topic: &str, out: &mut Vec<Subscriber>) {
        out.clear();
        let levels: Vec<&str> = topic.split('/').collect();
        self.root.collect_subscribers(&levels, true, out);
    }

    /// Find all subscribers matching a topic (Bytes variant), reusing a buffer.
    #[inline]
    pub fn match_topic_bytes_into(&self, topic: &[u8], out: &mut Vec<Subscriber>) {
        let topic_str = std::str::from_utf8(topic).unwrap_or("");
        self.match_topic_into(topic_str, out);
    }
}

impl Default for SubscriptionStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a topic matches a topic filter (which may contain wildcards).
/// This is used for delivering retained messages to new subscribers.
pub fn topic_matches_filter(topic: &str, filter: &str) -> bool {
    let topic_levels: Vec<&str> = topic.split('/').collect();
    let filter_levels: Vec<&str> = filter.split('/').collect();

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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handle(worker_id: usize, id: usize) -> Arc<ClientWriteHandle> {
        // For tests, use dummy fds (won't actually work for I/O)
        Arc::new(ClientWriteHandle::new(worker_id, -1, id as i32, Token(id)))
    }

    fn sub(handle: Arc<ClientWriteHandle>, qos: QoS) -> Subscriber {
        Subscriber {
            handle,
            qos,
            client_id: Arc::from(""),
        }
    }

    #[test]
    fn test_exact_match() {
        let mut store = SubscriptionStore::new();
        let h1 = make_handle(0, 1);
        store.subscribe("sensors/temp", sub(h1.clone(), QoS::AtMostOnce));

        let subs = store.match_topic("sensors/temp");
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].token(), Token(1));

        let subs = store.match_topic("sensors/humidity");
        assert!(subs.is_empty());
    }

    #[test]
    fn test_single_wildcard() {
        let mut store = SubscriptionStore::new();
        let h1 = make_handle(0, 1);
        store.subscribe("sensors/+/temp", sub(h1.clone(), QoS::AtMostOnce));

        let subs = store.match_topic("sensors/room1/temp");
        assert_eq!(subs.len(), 1);

        let subs = store.match_topic("sensors/room2/temp");
        assert_eq!(subs.len(), 1);

        let subs = store.match_topic("sensors/room1/humidity");
        assert!(subs.is_empty());
    }

    #[test]
    fn test_multi_wildcard() {
        let mut store = SubscriptionStore::new();
        let h1 = make_handle(0, 1);
        store.subscribe("sensors/#", sub(h1.clone(), QoS::AtMostOnce));

        let subs = store.match_topic("sensors/temp");
        assert_eq!(subs.len(), 1);

        let subs = store.match_topic("sensors/room1/temp");
        assert_eq!(subs.len(), 1);

        let subs = store.match_topic("sensors/room1/floor2/temp");
        assert_eq!(subs.len(), 1);

        let subs = store.match_topic("actuators/light");
        assert!(subs.is_empty());
    }

    #[test]
    fn test_unsubscribe() {
        let mut store = SubscriptionStore::new();
        let h1 = make_handle(0, 1);
        let h2 = make_handle(0, 2);
        store.subscribe("sensors/temp", sub(h1.clone(), QoS::AtMostOnce));
        store.subscribe("sensors/temp", sub(h2.clone(), QoS::AtMostOnce));

        let subs = store.match_topic("sensors/temp");
        assert_eq!(subs.len(), 2);

        store.unsubscribe("sensors/temp", 0, Token(1));

        let subs = store.match_topic("sensors/temp");
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].token(), Token(2));
    }
}
