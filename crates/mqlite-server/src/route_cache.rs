//! Route cache for topic-to-subscriber lookups.
//!
//! This module provides a generation-based cache for topic routing,
//! with adaptive enabling/disabling based on hit rate.

use ahash::AHashMap;
use bytes::Bytes;
use mio::Token;

use crate::shared::SharedStateHandle;
use crate::subscription::Subscriber;

/// Maximum number of cached routes per worker.
/// Set to 0 to disable caching.
const ROUTE_CACHE_LIMIT: usize = 0;

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

/// Route cache statistics for adaptive caching.
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

/// Route cache with adaptive caching based on hit rate.
///
/// The cache stores topic->subscriber mappings and validates them using
/// a generation counter. When the subscription store changes, the generation
/// increments, invalidating stale cache entries.
pub struct RouteCache {
    /// Worker ID for logging.
    worker_id: usize,
    /// Route cache: topic -> cached subscribers.
    cache: AHashMap<Bytes, CachedRoute>,
    /// Cache statistics for adaptive caching.
    stats: CacheStats,
    /// Reusable buffer for subscriber deduplication.
    dedup_buf: AHashMap<(usize, Token), Subscriber>,
}

impl RouteCache {
    /// Create a new route cache for the given worker.
    pub fn new(worker_id: usize) -> Self {
        Self {
            worker_id,
            cache: AHashMap::with_capacity(1024),
            stats: CacheStats {
                hits: 0,
                misses: 0,
                enabled: true,
                lookups_since_eval: 0,
                disabled_count: 0,
            },
            dedup_buf: AHashMap::with_capacity(1024),
        }
    }

    /// Get subscribers for a topic, using cache if valid.
    /// Populates the provided buffer with deduplicated subscribers.
    ///
    /// Uses adaptive caching - disables cache if hit rate is too low.
    /// Deduplication is done at cache time, so returned subscribers
    /// are ready for direct iteration.
    pub fn get_subscribers(
        &mut self,
        topic: &Bytes,
        shared: &SharedStateHandle,
        subscriber_buf: &mut Vec<Subscriber>,
    ) {
        // Cache disabled - go direct to trie
        #[allow(clippy::absurd_extreme_comparisons)]
        if ROUTE_CACHE_LIMIT == 0 {
            subscriber_buf.clear();
            let subscriptions = shared.subscriptions.read();
            subscriptions.match_topic_bytes_into(topic, subscriber_buf);
            drop(subscriptions);
            self.dedup_subscriber_buf(subscriber_buf);
            return;
        }

        self.stats.lookups_since_eval += 1;

        // Periodically evaluate cache effectiveness
        if self.stats.enabled {
            if self.stats.lookups_since_eval >= CACHE_EVAL_INTERVAL {
                self.evaluate_effectiveness();
            }
        } else {
            // When disabled, retry after more lookups
            if self.stats.lookups_since_eval >= CACHE_RETRY_INTERVAL {
                self.stats.enabled = true;
                self.stats.lookups_since_eval = 0;
                self.stats.hits = 0;
                self.stats.misses = 0;
                log::debug!(
                    "Worker {}: Re-enabling route cache for evaluation",
                    self.worker_id
                );
            } else {
                // Cache disabled - go direct to trie, then dedup
                subscriber_buf.clear();
                {
                    let subscriptions = shared.subscriptions.read();
                    subscriptions.match_topic_bytes_into(topic, subscriber_buf);
                }
                self.dedup_subscriber_buf(subscriber_buf);
                return;
            }
        }

        // Check if we have a valid cached entry
        let current_gen = shared.subscriptions.read().generation();
        if let Some(cached) = self.cache.get(topic) {
            if cached.generation == current_gen {
                // Cache hit - already deduplicated
                self.stats.hits += 1;
                subscriber_buf.clear();
                subscriber_buf.extend(cached.subscribers.iter().cloned());
                return;
            }
            // Stale entry - remove it to release Arc<ClientWriteHandle>
            self.cache.remove(topic);
        }

        // Cache miss or stale - compute fresh
        self.stats.misses += 1;
        subscriber_buf.clear();
        let generation;
        {
            let subscriptions = shared.subscriptions.read();
            subscriptions.match_topic_bytes_into(topic, subscriber_buf);
            generation = subscriptions.generation();
        }

        // Dedup before caching
        self.dedup_subscriber_buf(subscriber_buf);

        // Evict if over limit (simple: just clear all)
        #[allow(clippy::absurd_extreme_comparisons)]
        if ROUTE_CACHE_LIMIT > 0 && self.cache.len() >= ROUTE_CACHE_LIMIT {
            self.cache.clear();
            // Release memory but keep reasonable capacity for reuse
            self.cache.shrink_to(1024);
        }

        // Cache the deduplicated result
        self.cache.insert(
            topic.clone(),
            CachedRoute {
                subscribers: subscriber_buf.clone(),
                generation,
            },
        );
    }

    /// Deduplicate subscriber buffer in-place, keeping highest QoS per (worker_id, token).
    fn dedup_subscriber_buf(&mut self, subscriber_buf: &mut Vec<Subscriber>) {
        if subscriber_buf.len() <= 1 {
            return;
        }

        self.dedup_buf.clear();
        for sub in subscriber_buf.drain(..) {
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
        subscriber_buf.extend(self.dedup_buf.drain().map(|(_, sub)| sub));
    }

    /// Evaluate cache effectiveness and disable if hit rate is too low.
    fn evaluate_effectiveness(&mut self) {
        let total = self.stats.hits + self.stats.misses;
        if total == 0 {
            return;
        }

        let hit_rate = self.stats.hits as f64 / total as f64;

        if hit_rate < CACHE_MIN_HIT_RATE {
            self.stats.disabled_count += 1;
            // Only log first disable and every 100th after that
            if self.stats.disabled_count == 1 || self.stats.disabled_count.is_multiple_of(100) {
                log::info!(
                    "Worker {}: Route cache disabled (hit rate {:.1}% < {:.1}% threshold, {} hits / {} total, disabled {} times)",
                    self.worker_id,
                    hit_rate * 100.0,
                    CACHE_MIN_HIT_RATE * 100.0,
                    self.stats.hits,
                    total,
                    self.stats.disabled_count
                );
            }
            self.stats.enabled = false;
            // Keep cache entries for when re-enabled (avoids warmup penalty)
        } else {
            log::debug!(
                "Worker {}: Route cache healthy (hit rate {:.1}%, {} hits / {} total)",
                self.worker_id,
                hit_rate * 100.0,
                self.stats.hits,
                total
            );
        }

        // Reset counters for next evaluation period
        self.stats.lookups_since_eval = 0;
        self.stats.hits = 0;
        self.stats.misses = 0;
    }

    /// Remove a client from all cached routes.
    /// Call this when a client disconnects to prevent stale handles.
    pub fn remove_client(&mut self, worker_id: usize, token: Token) {
        for cached in self.cache.values_mut() {
            cached
                .subscribers
                .retain(|s| !(s.worker_id() == worker_id && s.token() == token));
        }
        // Remove empty cache entries to release memory
        self.cache
            .retain(|_, cached| !cached.subscribers.is_empty());
    }

    /// Shrink internal buffers if oversized and empty.
    /// Call periodically to release memory.
    pub fn maybe_shrink(&mut self) {
        const SHRINK_THRESHOLD: usize = 4096;
        if self.dedup_buf.capacity() > SHRINK_THRESHOLD && self.dedup_buf.is_empty() {
            self.dedup_buf.shrink_to(1024);
        }
    }

    /// Remove all stale cache entries (generation mismatch).
    /// Call this periodically to release Arc<ClientWriteHandle> for disconnected clients.
    #[allow(dead_code)]
    pub fn purge_stale(&mut self, current_generation: u64) {
        self.cache
            .retain(|_, cached| cached.generation == current_generation);
    }

    /// Clear all cache entries, releasing all Arc<ClientWriteHandle> references.
    pub fn clear(&mut self) {
        self.cache.clear();
    }
}
