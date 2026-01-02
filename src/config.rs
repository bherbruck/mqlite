//! Broker configuration and limits.

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

/// Broker configuration.
#[derive(Debug, Clone)]
pub struct Config {
    // === Protocol Limits ===
    /// Maximum MQTT packet size in bytes.
    /// Packets exceeding this are rejected. Advertised in CONNACK for MQTT 5.
    pub max_packet_size: u32,

    /// Maximum topic name length in bytes.
    pub max_topic_length: usize,

    /// Maximum topic levels (segments separated by '/').
    /// Prevents subscription trie explosion from deeply nested wildcards.
    pub max_topic_levels: usize,

    // === Flow Control (MQTT 5) ===
    /// Receive Maximum: max unacked QoS 1/2 messages we accept from a client.
    /// Advertised in CONNACK. Client exceeding this gets disconnected.
    pub receive_maximum: u16,

    /// Topic Alias Maximum: max topic aliases we support per client.
    /// Advertised in CONNACK. 0 = topic aliases disabled.
    pub topic_alias_maximum: u16,

    // === Backpressure ===
    /// Per-client write buffer soft limit in bytes.
    /// When exceeded, QoS 0 messages are dropped (backpressure).
    pub client_write_buffer_size: usize,

    /// Maximum inflight QoS 1/2 messages per client (broker -> client).
    /// Respects client's Receive Maximum if lower.
    pub max_inflight: u16,

    // === Connection Limits ===
    /// Maximum concurrent connections.
    pub max_connections: usize,
}

impl Default for Config {
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

impl Config {
    /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // MQTT protocol maximum is 268,435,455 bytes
        if self.max_packet_size > 268_435_455 {
            return Err(ConfigError::InvalidValue(
                "max_packet_size cannot exceed MQTT protocol maximum (268,435,455)".into(),
            ));
        }

        // Receive Maximum of 0 is a protocol error
        if self.receive_maximum == 0 {
            return Err(ConfigError::InvalidValue(
                "receive_maximum must be at least 1".into(),
            ));
        }

        // Max topic length cannot exceed protocol limit
        if self.max_topic_length > 65535 {
            return Err(ConfigError::InvalidValue(
                "max_topic_length cannot exceed 65535".into(),
            ));
        }

        // Sanity check on max inflight
        if self.max_inflight == 0 {
            return Err(ConfigError::InvalidValue(
                "max_inflight must be at least 1".into(),
            ));
        }

        Ok(())
    }
}

/// Configuration error.
#[derive(Debug, Clone)]
pub enum ConfigError {
    /// Invalid configuration value.
    InvalidValue(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::InvalidValue(msg) => write!(f, "invalid config: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_receive_maximum() {
        let mut config = Config::default();
        config.receive_maximum = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_max_packet_size() {
        let mut config = Config::default();
        config.max_packet_size = 300_000_000; // Exceeds MQTT max
        assert!(config.validate().is_err());
    }
}
