//! Server configuration.

use std::net::SocketAddr;

use serde::Deserialize;

/// Default $SYS topic publish interval in seconds.
pub const DEFAULT_SYS_INTERVAL: u64 = 10;

/// Default PROXY protocol header timeout in seconds.
pub const DEFAULT_PROXY_TIMEOUT_SECS: u64 = 5;

/// PROXY protocol configuration for a listener.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ProxyProtocolConfig {
    /// Enable PROXY protocol parsing on this listener.
    pub enabled: bool,
    /// Timeout in seconds for reading PROXY header.
    #[serde(default = "default_proxy_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_proxy_timeout_secs() -> u64 {
    DEFAULT_PROXY_TIMEOUT_SECS
}

impl Default for ProxyProtocolConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            timeout_secs: DEFAULT_PROXY_TIMEOUT_SECS,
        }
    }
}

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
    /// PROXY protocol configuration.
    #[serde(default)]
    pub proxy_protocol: ProxyProtocolConfig,
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
            proxy_protocol: ProxyProtocolConfig::default(),
        }
    }
}
