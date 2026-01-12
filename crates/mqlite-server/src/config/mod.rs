//! Broker configuration and limits.
#![allow(unused_imports)] // Re-exports are used by other modules in the crate
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

mod acl;
mod auth;
mod bridge;
mod limits;
mod log;
mod mqtt;
mod prometheus;
mod server;
mod session;
mod tls;
mod websocket;

use std::path::Path;

use config::{Environment, File, FileFormat};
use regex::Regex;
use serde::Deserialize;

// Re-export all config types for use throughout the crate
pub use acl::{AclConfig, DefaultPermissions, RoleConfig};
pub use auth::{AuthConfig, UserConfig};
pub use bridge::{BridgeConfig, BridgeProtocolVersion, BridgeTopic};
pub use limits::{
    LimitsConfig, DEFAULT_CLIENT_WRITE_BUFFER_SIZE, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_INFLIGHT,
    DEFAULT_MAX_PACKET_SIZE, DEFAULT_MAX_TOPIC_LENGTH, DEFAULT_MAX_TOPIC_LEVELS,
    DEFAULT_RECEIVE_MAXIMUM, DEFAULT_TOPIC_ALIAS_MAXIMUM,
};
pub use log::LogConfig;
pub use mqtt::MqttConfig;
pub use prometheus::{PrometheusConfig, DEFAULT_PROMETHEUS_BIND};
pub use server::{
    ProxyProtocolConfig, ServerConfig, DEFAULT_PROXY_TIMEOUT_SECS, DEFAULT_SYS_INTERVAL,
};
pub use session::{SessionConfig, DEFAULT_KEEP_ALIVE, DEFAULT_MAX_KEEP_ALIVE};
pub use tls::{TlsConfig, DEFAULT_TLS_BIND};
pub use websocket::{WebSocketConfig, WebSocketTlsConfig, DEFAULT_WSS_BIND, DEFAULT_WS_BIND};

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
    /// WebSocket configuration.
    pub websocket: WebSocketConfig,
    /// Secure WebSocket (WSS) configuration.
    pub websocket_tls: WebSocketTlsConfig,
    /// Bridge configurations.
    #[serde(default)]
    pub bridge: Vec<BridgeConfig>,
}

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
            .set_default("tls.bind", DEFAULT_TLS_BIND)?
            // WebSocket defaults (disabled by default)
            .set_default("websocket.enabled", false)?
            .set_default("websocket.bind", DEFAULT_WS_BIND)?
            .set_default("websocket_tls.enabled", false)?
            .set_default("websocket_tls.bind", DEFAULT_WSS_BIND)?;

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
        self.limits.validate().map_err(ConfigError::Validation)?;
        self.mqtt.validate().map_err(ConfigError::Validation)?;
        self.tls.validate().map_err(ConfigError::Validation)?;
        self.websocket_tls
            .validate(&self.tls)
            .map_err(ConfigError::Validation)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let config = Config::default();
        assert!(config.validate().is_ok());
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
        assert_eq!(config.server.bind.port(), 1883);
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
}
