//! Persistence layer for retained messages.
//!
//! This module provides optional disk persistence for retained messages using
//! fjall, an LSM-tree based embedded database. Retained messages are stored
//! so they survive broker restarts.
//!
//! # MQTT Spec Compliance
//!
//! - v3.1.1 MQTT-3.3.1-5: Retained messages MUST be stored
//! - v3.1.1 MQTT-3.3.1-10: Empty payload removes retained message
//! - v3.1.1 MQTT-3.3.1-11: Zero byte retained message MUST NOT be stored
//! - v5 Message Expiry: Expired messages are not loaded on startup
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      SharedState                            │
//! │  ┌──────────────────────┐    ┌────────────────────────┐    │
//! │  │  retained_messages   │◄───│     Persistence        │    │
//! │  │    (in-memory)       │    │  ┌──────────────────┐  │    │
//! │  └──────────────────────┘    │  │      fjall       │  │    │
//! │                              │  │   (on disk)      │  │    │
//! │                              │  └──────────────────┘  │    │
//! │                              └────────────────────────┘    │
//! └─────────────────────────────────────────────────────────────┘
//!
//! On startup: Load from fjall → populate in-memory HashMap
//! On PUBLISH retain=1: Write to in-memory + write to fjall
//! On PUBLISH retain=1 empty: Remove from in-memory + remove from fjall
//! ```

#[cfg(feature = "persistence")]
mod retained;

#[cfg(feature = "persistence")]
pub use retained::*;

#[cfg(feature = "persistence")]
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
#[cfg(feature = "persistence")]
use std::path::Path;
#[cfg(feature = "persistence")]
use std::time::{SystemTime, UNIX_EPOCH};

/// Persistence handle for retained message storage.
///
/// Wraps a fjall database with a keyspace for retained messages.
/// All operations are designed to be non-blocking for the hot path.
#[cfg(feature = "persistence")]
pub struct Persistence {
    #[allow(dead_code)] // Kept alive to maintain database handle
    db: Database,
    retained: Keyspace,
}

#[cfg(feature = "persistence")]
impl Persistence {
    /// Open or create a persistence database at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - Directory path for the database files
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or created.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, PersistenceError> {
        let db = Database::builder(path)
            .open()
            .map_err(|e| PersistenceError::Open(e.to_string()))?;

        let retained = db
            .keyspace("retained", KeyspaceCreateOptions::default)
            .map_err(|e| PersistenceError::Keyspace(e.to_string()))?;

        log::info!("Persistence layer opened successfully");
        Ok(Self { db, retained })
    }

    /// Save a retained message to disk.
    ///
    /// The topic is used as the key, and the message data is serialized
    /// as the value. This enables prefix compression for similar topics.
    ///
    /// # MQTT Spec
    ///
    /// - MQTT-3.3.1-5: Store the message and its QoS
    /// - MQTT-3.3.1-7 (v5): Zero-length payload removes retained message
    pub fn save_retained(&self, topic: &str, data: &RetainedData) -> Result<(), PersistenceError> {
        let value =
            bincode::serialize(data).map_err(|e| PersistenceError::Serialize(e.to_string()))?;

        self.retained
            .insert(topic, value)
            .map_err(|e| PersistenceError::Write(e.to_string()))?;

        Ok(())
    }

    /// Remove a retained message from disk.
    ///
    /// # MQTT Spec
    ///
    /// - MQTT-3.3.1-10: Empty payload removes retained message
    pub fn remove_retained(&self, topic: &str) -> Result<(), PersistenceError> {
        self.retained
            .remove(topic)
            .map_err(|e| PersistenceError::Write(e.to_string()))?;

        Ok(())
    }

    /// Load all retained messages from disk.
    ///
    /// Returns an iterator over (topic, data) pairs. Expired messages
    /// (based on Message Expiry Interval) are automatically skipped.
    ///
    /// # MQTT v5 Message Expiry
    ///
    /// Messages with a Message Expiry Interval that has elapsed since
    /// `stored_at` are not returned.
    pub fn load_retained(&self) -> Result<Vec<(String, RetainedData)>, PersistenceError> {
        let now = current_unix_timestamp();
        let mut messages = Vec::new();

        // Collect all keys first (Guard consumes self on key()/value())
        let keys: Vec<_> = self
            .retained
            .iter()
            .filter_map(|kv| kv.key().ok())
            .collect();

        for key_bytes in keys {
            let topic = String::from_utf8_lossy(&key_bytes).into_owned();

            // Fetch value by key
            let value = match self.retained.get(&topic) {
                Ok(Some(v)) => v,
                Ok(None) => continue, // Key was deleted between iter and get
                Err(e) => {
                    log::warn!("Failed to read retained message for {}: {}", topic, e);
                    continue;
                }
            };

            let data: RetainedData = match bincode::deserialize(&value) {
                Ok(d) => d,
                Err(e) => {
                    log::warn!(
                        "Failed to deserialize retained message for {}: {}",
                        topic,
                        e
                    );
                    continue;
                }
            };

            // Check Message Expiry Interval (MQTT v5)
            if let Some(expiry) = data.message_expiry_interval {
                let elapsed = now.saturating_sub(data.stored_at);
                if elapsed >= expiry as u64 {
                    // Message has expired, skip it and remove from storage
                    log::debug!("Skipping expired retained message on topic: {}", topic);
                    let _ = self.retained.remove(&topic);
                    continue;
                }
            }

            messages.push((topic, data));
        }

        log::info!("Loaded {} retained messages from disk", messages.len());
        Ok(messages)
    }

    /// Flush all pending writes to disk.
    ///
    /// This ensures durability of all retained messages written so far.
    /// Called on graceful shutdown.
    #[allow(dead_code)] // Used in tests and intended for graceful shutdown
    pub fn sync(&self) -> Result<(), PersistenceError> {
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|e| PersistenceError::Sync(e.to_string()))?;
        Ok(())
    }

    /// Get the count of retained messages in the database.
    #[allow(dead_code)]
    pub fn retained_count(&self) -> usize {
        self.retained.len().unwrap_or(0)
    }
}

/// Get current Unix timestamp in seconds.
#[cfg(feature = "persistence")]
pub fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Errors that can occur during persistence operations.
#[derive(Debug)]
#[allow(dead_code)] // Some variants reserved for future use
pub enum PersistenceError {
    /// Failed to open the database
    Open(String),
    /// Failed to create or open a keyspace
    Keyspace(String),
    /// Failed to serialize data
    Serialize(String),
    /// Failed to deserialize data
    Deserialize(String),
    /// Failed to write to the database
    Write(String),
    /// Failed to read from the database
    Read(String),
    /// Failed to sync to disk
    Sync(String),
}

impl std::fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(e) => write!(f, "failed to open persistence database: {}", e),
            Self::Keyspace(e) => write!(f, "failed to open keyspace: {}", e),
            Self::Serialize(e) => write!(f, "serialization error: {}", e),
            Self::Deserialize(e) => write!(f, "deserialization error: {}", e),
            Self::Write(e) => write!(f, "write error: {}", e),
            Self::Read(e) => write!(f, "read error: {}", e),
            Self::Sync(e) => write!(f, "sync error: {}", e),
        }
    }
}

impl std::error::Error for PersistenceError {}

#[cfg(test)]
#[cfg(feature = "persistence")]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_open_and_close() {
        let dir = tempdir().unwrap();
        let persistence = Persistence::open(dir.path()).unwrap();
        persistence.sync().unwrap();
    }

    #[test]
    fn test_save_and_load_retained() {
        let dir = tempdir().unwrap();
        let persistence = Persistence::open(dir.path()).unwrap();

        let data = RetainedData {
            qos: 1,
            payload: b"hello world".to_vec(),
            properties: None,
            stored_at: current_unix_timestamp(),
            message_expiry_interval: None,
        };

        persistence.save_retained("test/topic", &data).unwrap();
        persistence.sync().unwrap();

        let loaded = persistence.load_retained().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, "test/topic");
        assert_eq!(loaded[0].1.payload, b"hello world");
        assert_eq!(loaded[0].1.qos, 1);
    }

    #[test]
    fn test_remove_retained() {
        let dir = tempdir().unwrap();
        let persistence = Persistence::open(dir.path()).unwrap();

        let data = RetainedData {
            qos: 0,
            payload: b"test".to_vec(),
            properties: None,
            stored_at: current_unix_timestamp(),
            message_expiry_interval: None,
        };

        persistence.save_retained("topic/to/remove", &data).unwrap();
        assert_eq!(persistence.load_retained().unwrap().len(), 1);

        persistence.remove_retained("topic/to/remove").unwrap();
        assert_eq!(persistence.load_retained().unwrap().len(), 0);
    }

    #[test]
    fn test_message_expiry_on_load() {
        let dir = tempdir().unwrap();
        let persistence = Persistence::open(dir.path()).unwrap();

        // Message that expired 10 seconds ago
        let expired_data = RetainedData {
            qos: 1,
            payload: b"expired".to_vec(),
            properties: None,
            stored_at: current_unix_timestamp().saturating_sub(100),
            message_expiry_interval: Some(50), // 50 second expiry, stored 100 seconds ago
        };

        // Message that won't expire for a while
        let valid_data = RetainedData {
            qos: 1,
            payload: b"valid".to_vec(),
            properties: None,
            stored_at: current_unix_timestamp(),
            message_expiry_interval: Some(3600), // 1 hour expiry
        };

        persistence
            .save_retained("expired/topic", &expired_data)
            .unwrap();
        persistence
            .save_retained("valid/topic", &valid_data)
            .unwrap();

        let loaded = persistence.load_retained().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, "valid/topic");
    }

    #[test]
    fn test_prefix_grouping() {
        let dir = tempdir().unwrap();
        let persistence = Persistence::open(dir.path()).unwrap();

        // These topics should benefit from prefix compression
        let topics = [
            "home/living/temp",
            "home/living/humidity",
            "home/kitchen/temp",
            "home/kitchen/humidity",
            "sensors/outdoor/wind",
        ];

        for topic in &topics {
            let data = RetainedData {
                qos: 0,
                payload: topic.as_bytes().to_vec(),
                properties: None,
                stored_at: current_unix_timestamp(),
                message_expiry_interval: None,
            };
            persistence.save_retained(topic, &data).unwrap();
        }

        let loaded = persistence.load_retained().unwrap();
        assert_eq!(loaded.len(), topics.len());
    }
}
