//! Bridge configuration for connecting to remote MQTT brokers.

use std::path::PathBuf;

use serde::Deserialize;

/// MQTT protocol version for bridge connections.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BridgeProtocolVersion {
    /// MQTT 3.1.1
    #[default]
    #[serde(alias = "mqtt3.1.1", alias = "3.1.1", alias = "v3")]
    Mqtt311,
    /// MQTT 5.0
    #[serde(alias = "mqtt5", alias = "5", alias = "v5")]
    Mqtt5,
}

/// Bridge topic configuration with QoS.
#[derive(Debug, Clone, Deserialize)]
pub struct BridgeTopic {
    /// Topic filter pattern.
    pub topic: String,
    /// QoS level for this topic (0, 1, or 2).
    #[serde(default)]
    pub qos: u8,
}

/// Bridge configuration for connecting to a remote MQTT broker.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // Fields for future bridge features (TLS, publish routing)
pub struct BridgeConfig {
    /// Bridge name (used in $SYS/broker/bridge/<name>/...).
    pub name: String,
    /// Remote broker address (host:port).
    pub address: String,

    // === Authentication ===
    /// Username for remote broker authentication.
    #[serde(default)]
    pub remote_username: Option<String>,
    /// Password for remote broker authentication.
    #[serde(default)]
    pub remote_password: Option<String>,
    /// Local username identity (for ACL evaluation of bridged messages).
    #[serde(default)]
    pub local_username: Option<String>,

    // === Protocol ===
    /// MQTT protocol version (mqtt311 or mqtt5).
    #[serde(default)]
    pub protocol_version: BridgeProtocolVersion,
    /// Keep-alive interval in seconds.
    #[serde(default = "default_bridge_keepalive")]
    pub keepalive: u16,
    /// Clean start / clean session.
    #[serde(default = "default_true")]
    pub clean_start: bool,
    /// Client ID prefix for bridge connection.
    #[serde(default = "default_bridge_clientid_prefix")]
    pub clientid_prefix: String,

    // === TLS ===
    /// Enable TLS for bridge connection.
    #[serde(default)]
    pub tls: bool,
    /// Path to CA certificate file for TLS verification.
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    /// Skip TLS certificate verification (insecure).
    #[serde(default)]
    pub tls_insecure: bool,

    // === Topic Routing ===
    /// Topics to subscribe on remote broker (remote -> local).
    #[serde(default)]
    pub subscribe: Vec<BridgeTopic>,
    /// Topics to forward to remote broker (local -> remote).
    #[serde(default)]
    pub publish: Vec<BridgeTopic>,
    /// Prefix to add to incoming messages (from remote).
    #[serde(default)]
    pub local_prefix: Option<String>,
    /// Prefix to add to outgoing messages (to remote).
    #[serde(default)]
    pub remote_prefix: Option<String>,

    // === Reconnection ===
    /// Initial reconnect delay in seconds.
    #[serde(default = "default_bridge_reconnect_delay")]
    pub reconnect_delay: u64,
    /// Maximum reconnect delay in seconds (exponential backoff cap).
    #[serde(default = "default_bridge_reconnect_max")]
    pub reconnect_max: u64,
}

fn default_bridge_keepalive() -> u16 {
    60
}

fn default_true() -> bool {
    true
}

fn default_bridge_clientid_prefix() -> String {
    "mqlite-bridge".to_string()
}

fn default_bridge_reconnect_delay() -> u64 {
    5
}

fn default_bridge_reconnect_max() -> u64 {
    300
}
