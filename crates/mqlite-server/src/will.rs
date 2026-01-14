//! Will message handling for MQTT.
//!
//! This module handles:
//! - Extracting will messages from disconnecting clients
//! - Scheduling delayed will messages (MQTT v5 Will Delay Interval)
//! - Managing the delayed wills queue
//! - Retained will message storage
//!
//! The actual fanout to subscribers is handled by the `fanout` module.

use std::time::{Duration, Instant};

use bytes::Bytes;

use mqlite_core::packet::{Publish, Will};

use crate::shared::{RetainedMessage, SharedStateHandle};

/// A will message scheduled for delayed publication.
#[derive(Debug)]
pub struct DelayedWill {
    /// When to publish the will message.
    pub publish_at: Instant,
    /// The will message to publish.
    pub publish: Publish,
}

/// Manages delayed will messages for a worker.
///
/// Each worker maintains its own `WillManager` to handle will messages
/// from clients that disconnect with a Will Delay Interval.
#[derive(Default)]
pub struct WillManager {
    /// Wills scheduled for delayed publication.
    delayed_wills: Vec<DelayedWill>,
}

impl WillManager {
    /// Create a new WillManager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Schedule a will for delayed publication.
    #[allow(dead_code)]
    pub fn schedule(&mut self, will: DelayedWill) {
        self.delayed_wills.push(will);
    }

    /// Schedule multiple wills for delayed publication.
    pub fn schedule_all(&mut self, wills: Vec<DelayedWill>) {
        self.delayed_wills.extend(wills);
    }

    /// Take all wills that are ready for publication.
    ///
    /// Returns the ready wills and removes them from the queue.
    pub fn take_ready(&mut self) -> Vec<Publish> {
        let now = Instant::now();
        let mut ready = Vec::new();

        self.delayed_wills.retain(|dw| {
            if dw.publish_at <= now {
                ready.push(dw.publish.clone());
                false
            } else {
                true
            }
        });

        ready
    }

    /// Get the time until the next delayed will should be published.
    ///
    /// Returns `None` if there are no delayed wills.
    pub fn next_timeout(&self) -> Option<Duration> {
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

    /// Check if there are any delayed wills pending.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.delayed_wills.is_empty()
    }

    /// Get the number of delayed wills pending.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.delayed_wills.len()
    }
}

/// Extract a will from a client and convert it to a Publish message.
///
/// Returns `None` if the client has no will or had a graceful disconnect.
/// Returns `Some((publish, delay))` where `delay` is `Some(Duration)` if
/// the will should be delayed (MQTT v5 Will Delay Interval), or `None`
/// for immediate publication.
pub fn extract_will(will: Will) -> (Publish, Option<Duration>) {
    let publish = Publish {
        dup: false,
        qos: will.qos,
        retain: will.retain,
        topic: Bytes::from(will.topic),
        packet_id: None,
        payload: Bytes::from(will.message),
        properties: None, // TODO: convert WillProperties to raw bytes if needed
    };

    // Check for MQTT v5 Will Delay Interval
    let delay = will
        .properties
        .as_ref()
        .and_then(|p| p.will_delay_interval)
        .filter(|&d| d > 0)
        .map(|d| Duration::from_secs(d as u64));

    (publish, delay)
}

/// Create a DelayedWill from a Publish and delay duration.
pub fn create_delayed_will(publish: Publish, delay: Duration) -> DelayedWill {
    DelayedWill {
        publish_at: Instant::now() + delay,
        publish,
    }
}

/// Store or delete a retained will message.
///
/// If the payload is empty, the retained message is deleted.
/// Otherwise, it is stored (or replaced if one already exists).
pub fn store_retained_will(publish: &Publish, shared: &SharedStateHandle) {
    if !publish.retain {
        return;
    }

    let topic_str = String::from_utf8_lossy(&publish.topic).into_owned();
    let mut retained = shared.retained_messages.write();

    if publish.payload.is_empty() {
        retained.remove(&topic_str);
        // Remove from persistence
        #[cfg(feature = "persistence")]
        if let Err(e) = shared.remove_persisted_retained(&topic_str) {
            log::warn!(
                "Failed to remove persisted retained will for '{}': {}",
                topic_str,
                e
            );
        }
    } else {
        retained.insert(
            topic_str.clone(),
            RetainedMessage {
                publish: publish.clone(),
                stored_at: Instant::now(),
            },
        );
        // Persist to disk
        #[cfg(feature = "persistence")]
        if let Err(e) = shared.persist_retained(&topic_str, publish) {
            log::warn!("Failed to persist retained will for '{}': {}", topic_str, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqlite_core::packet::QoS;

    fn make_test_publish(topic: &str, payload: &[u8]) -> Publish {
        Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: Bytes::from(topic.to_string()),
            packet_id: None,
            payload: Bytes::from(payload.to_vec()),
            properties: None,
        }
    }

    #[test]
    fn test_will_manager_empty() {
        let manager = WillManager::new();
        assert!(manager.is_empty());
        assert_eq!(manager.len(), 0);
        assert!(manager.next_timeout().is_none());
    }

    #[test]
    fn test_will_manager_schedule_and_take() {
        let mut manager = WillManager::new();

        // Schedule a will for immediate publication (delay = 0)
        let publish = make_test_publish("test/topic", b"hello");
        let delayed = DelayedWill {
            publish_at: Instant::now(),
            publish,
        };
        manager.schedule(delayed);

        assert!(!manager.is_empty());
        assert_eq!(manager.len(), 1);

        // Take ready wills
        let ready = manager.take_ready();
        assert_eq!(ready.len(), 1);
        assert!(manager.is_empty());
    }

    #[test]
    fn test_will_manager_future_will() {
        let mut manager = WillManager::new();

        // Schedule a will for future publication
        let publish = make_test_publish("test/topic", b"hello");
        let delayed = DelayedWill {
            publish_at: Instant::now() + Duration::from_secs(60),
            publish,
        };
        manager.schedule(delayed);

        // Should not be ready yet
        let ready = manager.take_ready();
        assert!(ready.is_empty());
        assert!(!manager.is_empty());

        // Should have a timeout
        let timeout = manager.next_timeout();
        assert!(timeout.is_some());
        assert!(timeout.unwrap() > Duration::ZERO);
    }

    #[test]
    fn test_extract_will_no_delay() {
        let will = Will {
            topic: "test/will".to_string(),
            message: b"goodbye".to_vec(),
            qos: QoS::AtLeastOnce,
            retain: true,
            properties: None,
        };

        let (publish, delay) = extract_will(will);

        assert_eq!(publish.topic.as_ref(), b"test/will");
        assert_eq!(publish.payload.as_ref(), b"goodbye");
        assert_eq!(publish.qos, QoS::AtLeastOnce);
        assert!(publish.retain);
        assert!(delay.is_none());
    }

    #[test]
    fn test_extract_will_with_delay() {
        use mqlite_core::packet::WillProperties;

        let will = Will {
            topic: "test/will".to_string(),
            message: b"goodbye".to_vec(),
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Some(WillProperties {
                will_delay_interval: Some(30),
                payload_format_indicator: None,
                message_expiry_interval: None,
                content_type: None,
                response_topic: None,
                correlation_data: None,
                user_properties: Vec::new(),
            }),
        };

        let (publish, delay) = extract_will(will);

        assert!(delay.is_some());
        assert_eq!(delay.unwrap(), Duration::from_secs(30));
        assert_eq!(publish.topic.as_ref(), b"test/will");
    }
}
