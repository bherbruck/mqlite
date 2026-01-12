//! Limits configuration.

use serde::Deserialize;

/// Default maximum packet size (1MB).
pub const DEFAULT_MAX_PACKET_SIZE: u32 = 1024 * 1024;

/// Default maximum topic length in bytes.
pub const DEFAULT_MAX_TOPIC_LENGTH: usize = 4096;

/// Default maximum topic levels (depth).
pub const DEFAULT_MAX_TOPIC_LEVELS: usize = 32;

/// Default receive maximum (max inflight QoS 1/2 from clients).
pub const DEFAULT_RECEIVE_MAXIMUM: u16 = 65535;

/// Default topic alias maximum.
pub const DEFAULT_TOPIC_ALIAS_MAXIMUM: u16 = 0; // Disabled by default

/// Default client write buffer soft limit (1MB).
pub const DEFAULT_CLIENT_WRITE_BUFFER_SIZE: usize = 1024 * 1024;

/// Default max inflight messages per client (broker -> client).
pub const DEFAULT_MAX_INFLIGHT: u16 = 32;

/// Default maximum connections.
pub const DEFAULT_MAX_CONNECTIONS: usize = 100_000;

/// Limits configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
#[allow(dead_code)]
pub struct LimitsConfig {
    /// Maximum MQTT packet size in bytes.
    /// Packets exceeding this are rejected. Advertised in CONNACK for MQTT 5.
    #[serde(default = "default_max_packet_size")]
    pub max_packet_size: u32,

    /// Maximum topic name length in bytes.
    #[serde(default = "default_max_topic_length")]
    pub max_topic_length: usize,

    /// Maximum topic levels (segments separated by '/').
    /// Prevents subscription trie explosion from deeply nested wildcards.
    #[serde(default = "default_max_topic_levels")]
    pub max_topic_levels: usize,

    /// Receive Maximum: max unacked QoS 1/2 messages we accept from a client.
    /// Advertised in CONNACK. Client exceeding this gets disconnected.
    #[serde(default = "default_receive_maximum")]
    pub receive_maximum: u16,

    /// Topic Alias Maximum: max topic aliases we support per client.
    /// Advertised in CONNACK. 0 = topic aliases disabled.
    #[serde(default = "default_topic_alias_maximum")]
    pub topic_alias_maximum: u16,

    /// Per-client write buffer soft limit in bytes.
    /// When exceeded, QoS 0 messages are dropped (backpressure).
    #[serde(default = "default_client_write_buffer_size")]
    pub client_write_buffer_size: usize,

    /// Maximum inflight QoS 1/2 messages per client (broker -> client).
    /// Respects client's Receive Maximum if lower.
    #[serde(default = "default_max_inflight")]
    pub max_inflight: u16,

    /// Maximum concurrent connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

fn default_max_packet_size() -> u32 {
    DEFAULT_MAX_PACKET_SIZE
}
fn default_max_topic_length() -> usize {
    DEFAULT_MAX_TOPIC_LENGTH
}
fn default_max_topic_levels() -> usize {
    DEFAULT_MAX_TOPIC_LEVELS
}
fn default_receive_maximum() -> u16 {
    DEFAULT_RECEIVE_MAXIMUM
}
fn default_topic_alias_maximum() -> u16 {
    DEFAULT_TOPIC_ALIAS_MAXIMUM
}
fn default_client_write_buffer_size() -> usize {
    DEFAULT_CLIENT_WRITE_BUFFER_SIZE
}
fn default_max_inflight() -> u16 {
    DEFAULT_MAX_INFLIGHT
}
fn default_max_connections() -> usize {
    DEFAULT_MAX_CONNECTIONS
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_packet_size: DEFAULT_MAX_PACKET_SIZE,
            max_topic_length: DEFAULT_MAX_TOPIC_LENGTH,
            max_topic_levels: DEFAULT_MAX_TOPIC_LEVELS,
            receive_maximum: DEFAULT_RECEIVE_MAXIMUM,
            topic_alias_maximum: DEFAULT_TOPIC_ALIAS_MAXIMUM,
            client_write_buffer_size: DEFAULT_CLIENT_WRITE_BUFFER_SIZE,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

impl LimitsConfig {
    /// Validate the limits configuration.
    pub fn validate(&self) -> Result<(), String> {
        // MQTT protocol maximum is 268,435,455 bytes
        if self.max_packet_size > 268_435_455 {
            return Err("max_packet_size cannot exceed MQTT protocol maximum (268,435,455)".into());
        }

        // Receive Maximum of 0 is a protocol error
        if self.receive_maximum == 0 {
            return Err("receive_maximum must be at least 1".into());
        }

        // Max topic length cannot exceed protocol limit
        if self.max_topic_length > 65535 {
            return Err("max_topic_length cannot exceed 65535".into());
        }

        // Note: max_inflight = 0 means unbounded (uses 65535)
        // Note: max_connections = 0 means unbounded
        // Note: max_packet_size = 0 means unbounded (uses protocol max)

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_valid() {
        assert!(LimitsConfig::default().validate().is_ok());
    }

    #[test]
    fn test_invalid_receive_maximum() {
        let config = LimitsConfig {
            receive_maximum: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_max_packet_size() {
        let config = LimitsConfig {
            max_packet_size: 300_000_000,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_max_topic_length() {
        let config = LimitsConfig {
            max_topic_length: 70000,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
