//! Broker configuration and limits.
//!
//! Supports configuration from:
//! - TOML file (default: `mqlite.toml`)
//! - Environment variables with `MQLITE__` prefix (double underscore for nesting)
//! - In-file variable substitution: `${VAR}` or `${VAR:-default}`
//!
//! Environment variable examples:
//! - `MQLITE__SERVER__BIND=0.0.0.0:1884`
//! - `MQLITE__LIMITS__MAX_PACKET_SIZE=2097152`
//! - `MQLITE__MQTT__MAX_QOS=1`
//!
//! In-file substitution examples:
//! ```toml
//! [server]
//! bind = "${MQTT_HOST:-0.0.0.0}:${MQTT_PORT:-1883}"
//! ```

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use config::{Environment, File, FileFormat};
use regex::Regex;
use serde::Deserialize;

// === Default Constants ===

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

/// Default keep alive in seconds.
pub const DEFAULT_KEEP_ALIVE: u16 = 60;

/// Default maximum keep alive in seconds.
pub const DEFAULT_MAX_KEEP_ALIVE: u16 = 65535;

// === Environment Variable Substitution ===

/// Substitute environment variables in a string.
/// Supports `${VAR}` and `${VAR:-default}` syntax.
fn substitute_env_vars(content: &str) -> String {
    let re = Regex::new(r"\$\{([^}:]+)(?::-([^}]*))?\}").unwrap();
    re.replace_all(content, |caps: &regex::Captures| {
        let var_name = &caps[1];
        let default = caps.get(2).map(|m| m.as_str()).unwrap_or("");
        std::env::var(var_name).unwrap_or_else(|_| default.to_string())
    })
    .to_string()
}

// === Configuration Structures ===

/// Root configuration structure.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Logging configuration.
    pub log: LogConfig,
    /// Server configuration.
    pub server: ServerConfig,
    /// Limits configuration.
    pub limits: LimitsConfig,
    /// Session configuration.
    pub session: SessionConfig,
    /// MQTT feature configuration.
    pub mqtt: MqttConfig,
    /// Authentication configuration.
    pub auth: AuthConfig,
    /// Access control list configuration.
    pub acl: AclConfig,
    /// Prometheus metrics configuration.
    pub prometheus: PrometheusConfig,
    /// TLS configuration.
    pub tls: TlsConfig,
}


/// Logging configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    /// Log level: error, warn, info, debug, trace.
    #[serde(default = "default_log_level")]
    pub level: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

/// Default $SYS topic publish interval in seconds.
pub const DEFAULT_SYS_INTERVAL: u64 = 10;

/// Default Prometheus metrics bind address.
pub const DEFAULT_PROMETHEUS_BIND: &str = "127.0.0.1:9090";

/// Default TLS bind address.
pub const DEFAULT_TLS_BIND: &str = "0.0.0.0:8883";

/// Server configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// TCP bind address.
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
    /// Number of worker threads (0 = auto based on CPU count).
    #[serde(default)]
    pub workers: usize,
    /// $SYS topic publish interval in seconds (0 = disabled).
    #[serde(default = "default_sys_interval")]
    pub sys_interval: u64,
}

fn default_sys_interval() -> u64 {
    DEFAULT_SYS_INTERVAL
}

fn default_bind() -> SocketAddr {
    "0.0.0.0:1883".parse().unwrap()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            workers: 0,
            sys_interval: DEFAULT_SYS_INTERVAL,
        }
    }
}

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

/// Session configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
#[allow(dead_code)]
pub struct SessionConfig {
    /// Default keep alive in seconds (used when client sends 0).
    #[serde(default = "default_keep_alive")]
    pub default_keep_alive: u16,

    /// Maximum keep alive in seconds (client value capped to this).
    #[serde(default = "default_max_keep_alive")]
    pub max_keep_alive: u16,

    /// Maximum topic aliases per client (MQTT v5).
    #[serde(default = "default_topic_alias_maximum")]
    pub max_topic_aliases: u16,
}

fn default_keep_alive() -> u16 {
    DEFAULT_KEEP_ALIVE
}
fn default_max_keep_alive() -> u16 {
    DEFAULT_MAX_KEEP_ALIVE
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            default_keep_alive: DEFAULT_KEEP_ALIVE,
            max_keep_alive: DEFAULT_MAX_KEEP_ALIVE,
            max_topic_aliases: DEFAULT_TOPIC_ALIAS_MAXIMUM,
        }
    }
}

/// MQTT feature configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MqttConfig {
    /// Maximum QoS level (0, 1, or 2).
    #[serde(default = "default_max_qos")]
    pub max_qos: u8,

    /// Whether retained messages are available.
    #[serde(default = "default_true")]
    pub retain_available: bool,

    /// Whether wildcard subscriptions are available.
    #[serde(default = "default_true")]
    pub wildcard_subscriptions: bool,

    /// Whether subscription identifiers are available (MQTT v5).
    #[serde(default = "default_true")]
    pub subscription_identifiers: bool,

    /// Whether shared subscriptions are available.
    #[serde(default = "default_true")]
    pub shared_subscriptions: bool,
}

fn default_max_qos() -> u8 {
    2
}
fn default_true() -> bool {
    true
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            max_qos: 2,
            retain_available: true,
            wildcard_subscriptions: true,
            subscription_identifiers: true,
            shared_subscriptions: true,
        }
    }
}

// === Authentication Configuration ===

/// Authentication configuration.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AuthConfig {
    /// Enable authentication.
    pub enabled: bool,
    /// Allow anonymous connections when auth is enabled.
    pub allow_anonymous: bool,
    /// Static user list.
    #[serde(default)]
    pub users: Vec<UserConfig>,
}

/// User configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct UserConfig {
    /// Username.
    pub username: String,
    /// Plaintext password (use password_hash for production).
    #[serde(default)]
    pub password: Option<String>,
    /// Argon2 password hash.
    #[serde(default)]
    pub password_hash: Option<String>,
    /// ACL role reference.
    #[serde(default)]
    pub role: Option<String>,
}

// === ACL Configuration ===

/// ACL (Access Control List) configuration.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AclConfig {
    /// Enable ACL.
    pub enabled: bool,
    /// Role definitions.
    #[serde(default)]
    pub roles: Vec<RoleConfig>,
    /// Default permissions for authenticated users without an explicit role.
    #[serde(default)]
    pub default: DefaultPermissions,
    /// Permissions for anonymous (unauthenticated) connections.
    #[serde(default)]
    pub anonymous: DefaultPermissions,
}

/// Role configuration with publish/subscribe patterns.
#[derive(Debug, Clone, Deserialize)]
pub struct RoleConfig {
    /// Role name.
    pub name: String,
    /// Topics this role can publish to.
    /// Supports %c (client_id) and %u (username) substitution.
    #[serde(default)]
    pub publish: Vec<String>,
    /// Topic filters this role can subscribe to.
    /// Supports %c (client_id) and %u (username) substitution.
    #[serde(default)]
    pub subscribe: Vec<String>,
}

/// Default permissions for users without an explicit role.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DefaultPermissions {
    /// Topics that can be published to.
    pub publish: Vec<String>,
    /// Topic filters that can be subscribed to.
    pub subscribe: Vec<String>,
}

// === Prometheus Configuration ===

/// Prometheus metrics configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PrometheusConfig {
    /// Enable Prometheus metrics endpoint.
    pub enabled: bool,
    /// HTTP bind address for metrics endpoint.
    #[serde(default = "default_prometheus_bind")]
    pub bind: SocketAddr,
}

fn default_prometheus_bind() -> SocketAddr {
    DEFAULT_PROMETHEUS_BIND.parse().unwrap()
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_prometheus_bind(),
        }
    }
}

// === TLS Configuration ===

/// TLS/SSL configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    /// Enable TLS listener.
    pub enabled: bool,
    /// TLS bind address (default: 0.0.0.0:8883).
    #[serde(default = "default_tls_bind")]
    pub bind: SocketAddr,
    /// Path to PEM-encoded certificate file.
    #[serde(default)]
    pub cert: PathBuf,
    /// Path to PEM-encoded private key file.
    #[serde(default)]
    pub key: PathBuf,
}

fn default_tls_bind() -> SocketAddr {
    DEFAULT_TLS_BIND.parse().unwrap()
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_tls_bind(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
        }
    }
}

// === Configuration Error ===

/// Configuration error.
#[derive(Debug)]
pub enum ConfigError {
    /// IO error reading config file.
    Io(std::io::Error),
    /// Config parsing/loading error.
    Config(config::ConfigError),
    /// Invalid configuration value.
    Validation(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "IO error: {}", e),
            ConfigError::Config(e) => write!(f, "Config error: {}", e),
            ConfigError::Validation(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self {
        ConfigError::Io(e)
    }
}

impl From<config::ConfigError> for ConfigError {
    fn from(e: config::ConfigError) -> Self {
        ConfigError::Config(e)
    }
}

// === Configuration Implementation ===

impl Config {
    /// Load configuration from a TOML file with environment variable overrides.
    ///
    /// Supports two forms of environment variable usage:
    /// 1. In-file substitution: `${VAR}` or `${VAR:-default}` syntax in the TOML file
    /// 2. Override via env vars: `MQLITE__` prefix with double underscores for nesting:
    ///    - `MQLITE__SERVER__BIND=0.0.0.0:1884`
    ///    - `MQLITE__LIMITS__MAX_PACKET_SIZE=2097152`
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let mut builder = config::Config::builder()
            // Start with defaults
            .set_default("log.level", "info")?
            .set_default("server.bind", "0.0.0.0:1883")?
            .set_default("server.workers", 0)?
            .set_default("server.sys_interval", DEFAULT_SYS_INTERVAL as i64)?
            .set_default("limits.max_packet_size", DEFAULT_MAX_PACKET_SIZE as i64)?
            .set_default("limits.max_topic_length", DEFAULT_MAX_TOPIC_LENGTH as i64)?
            .set_default("limits.max_topic_levels", DEFAULT_MAX_TOPIC_LEVELS as i64)?
            .set_default("limits.receive_maximum", DEFAULT_RECEIVE_MAXIMUM as i64)?
            .set_default(
                "limits.topic_alias_maximum",
                DEFAULT_TOPIC_ALIAS_MAXIMUM as i64,
            )?
            .set_default(
                "limits.client_write_buffer_size",
                DEFAULT_CLIENT_WRITE_BUFFER_SIZE as i64,
            )?
            .set_default("limits.max_inflight", DEFAULT_MAX_INFLIGHT as i64)?
            .set_default("limits.max_connections", DEFAULT_MAX_CONNECTIONS as i64)?
            .set_default("session.default_keep_alive", DEFAULT_KEEP_ALIVE as i64)?
            .set_default("session.max_keep_alive", DEFAULT_MAX_KEEP_ALIVE as i64)?
            .set_default(
                "session.max_topic_aliases",
                DEFAULT_TOPIC_ALIAS_MAXIMUM as i64,
            )?
            .set_default("mqtt.max_qos", 2)?
            .set_default("mqtt.retain_available", true)?
            .set_default("mqtt.wildcard_subscriptions", true)?
            .set_default("mqtt.subscription_identifiers", true)?
            .set_default("mqtt.shared_subscriptions", true)?
            // Auth defaults (disabled by default)
            .set_default("auth.enabled", false)?
            .set_default("auth.allow_anonymous", true)?
            // ACL defaults (disabled by default)
            .set_default("acl.enabled", false)?
            // Prometheus defaults (disabled by default)
            .set_default("prometheus.enabled", false)?
            .set_default("prometheus.bind", DEFAULT_PROMETHEUS_BIND)?
            // TLS defaults (disabled by default)
            .set_default("tls.enabled", false)?
            .set_default("tls.bind", DEFAULT_TLS_BIND)?;

        // Load from file with env var substitution
        let path = path.as_ref();
        if path.exists() {
            match std::fs::read_to_string(path) {
                Ok(content) => {
                    let substituted = substitute_env_vars(&content);
                    builder = builder.add_source(File::from_str(&substituted, FileFormat::Toml));
                }
                Err(e) => return Err(ConfigError::Io(e)),
            }
        }

        // Override with environment variables (MQLITE__SERVER__BIND, etc.)
        let cfg = builder
            .add_source(
                Environment::with_prefix("MQLITE")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?;

        let config: Config = cfg.try_deserialize()?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from environment variables only (no file).
    #[allow(dead_code)]
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::load(Path::new(""))
    }

    /// Parse configuration from a TOML string (for testing).
    #[allow(dead_code)]
    pub fn parse(content: &str) -> Result<Self, ConfigError> {
        let substituted = substitute_env_vars(content);
        let config: Config = toml::from_str(&substituted)
            .map_err(|e| ConfigError::Validation(format!("TOML parse error: {}", e)))?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // MQTT protocol maximum is 268,435,455 bytes
        if self.limits.max_packet_size > 268_435_455 {
            return Err(ConfigError::Validation(
                "max_packet_size cannot exceed MQTT protocol maximum (268,435,455)".into(),
            ));
        }

        // Receive Maximum of 0 is a protocol error
        if self.limits.receive_maximum == 0 {
            return Err(ConfigError::Validation(
                "receive_maximum must be at least 1".into(),
            ));
        }

        // Max topic length cannot exceed protocol limit
        if self.limits.max_topic_length > 65535 {
            return Err(ConfigError::Validation(
                "max_topic_length cannot exceed 65535".into(),
            ));
        }

        // Note: max_inflight = 0 means unbounded (uses 65535)
        // Note: max_connections = 0 means unbounded
        // Note: max_packet_size = 0 means unbounded (uses protocol max)

        // Validate max_qos
        if self.mqtt.max_qos > 2 {
            return Err(ConfigError::Validation("max_qos must be 0, 1, or 2".into()));
        }

        // Validate TLS config
        if self.tls.enabled {
            if self.tls.cert.as_os_str().is_empty() {
                return Err(ConfigError::Validation(
                    "tls.cert is required when TLS is enabled".into(),
                ));
            }
            if self.tls.key.as_os_str().is_empty() {
                return Err(ConfigError::Validation(
                    "tls.key is required when TLS is enabled".into(),
                ));
            }
        }

        Ok(())
    }
}

// === Tests ===

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
        config.limits.receive_maximum = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_max_packet_size() {
        let mut config = Config::default();
        config.limits.max_packet_size = 300_000_000; // Exceeds MQTT max
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_max_qos() {
        let mut config = Config::default();
        config.mqtt.max_qos = 3;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_parse_toml() {
        let toml = r#"
[log]
level = "debug"

[server]
bind = "127.0.0.1:1884"
workers = 4

[limits]
max_packet_size = 2097152
max_topic_levels = 64

[session]
default_keep_alive = 120
max_keep_alive = 300

[mqtt]
max_qos = 1
retain_available = false
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.log.level, "debug");
        assert_eq!(config.server.bind.port(), 1884);
        assert_eq!(config.server.workers, 4);
        assert_eq!(config.limits.max_packet_size, 2097152);
        assert_eq!(config.limits.max_topic_levels, 64);
        assert_eq!(config.session.default_keep_alive, 120);
        assert_eq!(config.session.max_keep_alive, 300);
        assert_eq!(config.mqtt.max_qos, 1);
        assert!(!config.mqtt.retain_available);
    }

    #[test]
    fn test_parse_partial_toml() {
        // Only override some values, rest should use defaults
        let toml = r#"
[limits]
max_packet_size = 512000
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.limits.max_packet_size, 512000);
        assert_eq!(config.limits.max_topic_levels, DEFAULT_MAX_TOPIC_LEVELS);
        assert_eq!(config.server.bind, default_bind());
        assert_eq!(config.mqtt.max_qos, 2);
    }

    #[test]
    fn test_env_var_substitution() {
        std::env::set_var("TEST_PORT", "1885");
        let content = r#"
[server]
bind = "0.0.0.0:${TEST_PORT}"
"#;
        let substituted = substitute_env_vars(content);
        assert!(substituted.contains("0.0.0.0:1885"));
        std::env::remove_var("TEST_PORT");
    }

    #[test]
    fn test_env_var_substitution_with_default() {
        // Ensure var is not set
        std::env::remove_var("NONEXISTENT_VAR");
        let content = r#"bind = "${NONEXISTENT_VAR:-0.0.0.0:1883}""#;
        let substituted = substitute_env_vars(content);
        assert!(substituted.contains("0.0.0.0:1883"));
    }

    #[test]
    fn test_tls_disabled_is_valid() {
        let config = Config::default();
        assert!(!config.tls.enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_tls_enabled_without_cert_fails() {
        let mut config = Config::default();
        config.tls.enabled = true;
        config.tls.key = PathBuf::from("/path/to/key.pem");
        // cert is empty
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tls_enabled_without_key_fails() {
        let mut config = Config::default();
        config.tls.enabled = true;
        config.tls.cert = PathBuf::from("/path/to/cert.pem");
        // key is empty
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tls_enabled_with_cert_and_key_is_valid() {
        let mut config = Config::default();
        config.tls.enabled = true;
        config.tls.cert = PathBuf::from("/path/to/cert.pem");
        config.tls.key = PathBuf::from("/path/to/key.pem");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_parse_tls_config() {
        let toml = r#"
[tls]
enabled = true
bind = "0.0.0.0:8884"
cert = "/etc/mqlite/cert.pem"
key = "/etc/mqlite/key.pem"
"#;
        let config = Config::parse(toml).unwrap();
        assert!(config.tls.enabled);
        assert_eq!(config.tls.bind.port(), 8884);
        assert_eq!(config.tls.cert, PathBuf::from("/etc/mqlite/cert.pem"));
        assert_eq!(config.tls.key, PathBuf::from("/etc/mqlite/key.pem"));
    }

    #[test]
    fn test_tls_default_bind() {
        let config = Config::default();
        assert_eq!(config.tls.bind.port(), 8883);
    }
}
