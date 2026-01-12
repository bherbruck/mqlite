//! Prometheus metrics configuration.

use std::net::SocketAddr;

use serde::Deserialize;

/// Default Prometheus metrics bind address.
pub const DEFAULT_PROMETHEUS_BIND: &str = "127.0.0.1:9090";

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
