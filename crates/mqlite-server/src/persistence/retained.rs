//! Retained message data structures for persistence.
//!
//! This module defines the serializable data structure for retained messages.
//! The format is designed to be compact and forward-compatible.

use serde::{Deserialize, Serialize};

use super::current_unix_timestamp;

/// Retained message data for persistence.
///
/// This structure is serialized to disk using bincode. The topic is stored
/// as the key, not in this structure, enabling prefix compression in fjall.
///
/// # Schema Version
///
/// The first byte of the serialized data indicates the schema version.
/// Current version: 1
///
/// # Fields
///
/// All fields are designed to be minimal while supporting both MQTT v3.1.1
/// and v5 retained message semantics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetainedData {
    /// QoS level (0, 1, or 2).
    ///
    /// MQTT-3.3.1-5: "the Server MUST store the Application Message and its QoS"
    pub qos: u8,

    /// Message payload bytes.
    ///
    /// Note: Empty payloads should never be stored (they mean "delete").
    pub payload: Vec<u8>,

    /// MQTT v5 properties (raw bytes).
    ///
    /// Stored as raw bytes to avoid parsing overhead and preserve all
    /// properties including User Properties, Content Type, etc.
    ///
    /// For v3.1.1 clients, this is None.
    pub properties: Option<Vec<u8>>,

    /// Unix timestamp when the message was stored.
    ///
    /// Used for:
    /// - Message Expiry Interval countdown (v5)
    /// - Calculating remaining TTL on delivery
    pub stored_at: u64,

    /// Message Expiry Interval in seconds (v5 only).
    ///
    /// If present, the message expires `message_expiry_interval` seconds
    /// after `stored_at`. On delivery, the remaining time is sent to
    /// the subscriber.
    ///
    /// Extracted from properties for efficient expiry checking on load.
    pub message_expiry_interval: Option<u32>,
}

impl RetainedData {
    /// Create a new RetainedData from raw components.
    ///
    /// # Arguments
    ///
    /// * `qos` - QoS level (0, 1, or 2)
    /// * `payload` - Message payload bytes
    /// * `properties` - Raw MQTT v5 properties bytes (None for v3.1.1)
    /// * `message_expiry_interval` - Extracted expiry interval if present
    pub fn new(
        qos: u8,
        payload: Vec<u8>,
        properties: Option<Vec<u8>>,
        message_expiry_interval: Option<u32>,
    ) -> Self {
        Self {
            qos,
            payload,
            properties,
            stored_at: current_unix_timestamp(),
            message_expiry_interval,
        }
    }

    /// Calculate the remaining Message Expiry Interval.
    ///
    /// Returns None if:
    /// - No expiry was set
    /// - The message has expired
    ///
    /// Returns Some(remaining_seconds) if the message is still valid.
    #[allow(dead_code)] // Used in tests and intended for delivery-time calculation
    pub fn remaining_expiry(&self) -> Option<u32> {
        let expiry = self.message_expiry_interval?;
        let now = current_unix_timestamp();
        let elapsed = now.saturating_sub(self.stored_at);

        if elapsed >= expiry as u64 {
            None // Expired
        } else {
            Some((expiry as u64 - elapsed) as u32)
        }
    }

    /// Check if the message has expired.
    ///
    /// Returns true if Message Expiry Interval was set and has elapsed.
    #[allow(dead_code)] // Used in tests and intended for expiry checking
    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.message_expiry_interval {
            let now = current_unix_timestamp();
            let elapsed = now.saturating_sub(self.stored_at);
            elapsed >= expiry as u64
        } else {
            false // No expiry = never expires
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn current_unix_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    #[test]
    fn test_new_retained_data() {
        let data = RetainedData::new(1, b"hello".to_vec(), None, None);
        assert_eq!(data.qos, 1);
        assert_eq!(data.payload, b"hello");
        assert!(data.properties.is_none());
        assert!(data.message_expiry_interval.is_none());
        assert!(data.stored_at > 0);
    }

    #[test]
    fn test_remaining_expiry_no_expiry() {
        let data = RetainedData::new(0, b"test".to_vec(), None, None);
        assert!(data.remaining_expiry().is_none());
        assert!(!data.is_expired());
    }

    #[test]
    fn test_remaining_expiry_valid() {
        let mut data = RetainedData::new(0, b"test".to_vec(), None, Some(3600));
        // Just created, should have ~3600 seconds remaining
        let remaining = data.remaining_expiry().unwrap();
        assert!(remaining > 3590 && remaining <= 3600);
        assert!(!data.is_expired());

        // Simulate stored 100 seconds ago
        data.stored_at = current_unix_timestamp().saturating_sub(100);
        let remaining = data.remaining_expiry().unwrap();
        assert!(remaining > 3490 && remaining <= 3500);
        assert!(!data.is_expired());
    }

    #[test]
    fn test_remaining_expiry_expired() {
        let mut data = RetainedData::new(0, b"test".to_vec(), None, Some(60));
        // Simulate stored 100 seconds ago with 60 second expiry
        data.stored_at = current_unix_timestamp().saturating_sub(100);
        assert!(data.remaining_expiry().is_none());
        assert!(data.is_expired());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let original = RetainedData {
            qos: 2,
            payload: b"test payload".to_vec(),
            properties: Some(vec![0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x3C]),
            stored_at: 1234567890,
            message_expiry_interval: Some(300),
        };

        let serialized = bincode::serialize(&original).unwrap();
        let deserialized: RetainedData = bincode::deserialize(&serialized).unwrap();

        assert_eq!(original, deserialized);
    }
}
