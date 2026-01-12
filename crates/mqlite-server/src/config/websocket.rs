//! WebSocket configuration.

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

use super::server::ProxyProtocolConfig;

/// Default WebSocket bind address.
pub const DEFAULT_WS_BIND: &str = "0.0.0.0:8083";

/// Default secure WebSocket bind address.
pub const DEFAULT_WSS_BIND: &str = "0.0.0.0:8084";

/// WebSocket configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WebSocketConfig {
    /// Enable WebSocket listener.
    pub enabled: bool,
    /// WebSocket bind address (default: 0.0.0.0:8083).
    #[serde(default = "default_ws_bind")]
    pub bind: SocketAddr,
    /// WebSocket path (default: /mqtt). Clients must connect to this path.
    #[serde(default = "default_ws_path")]
    pub path: String,
    /// PROXY protocol configuration.
    #[serde(default)]
    #[allow(dead_code)]
    pub proxy_protocol: ProxyProtocolConfig,
}

fn default_ws_path() -> String {
    String::new() // Empty = accept any path
}

fn default_ws_bind() -> SocketAddr {
    DEFAULT_WS_BIND.parse().unwrap()
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_ws_bind(),
            path: default_ws_path(),
            proxy_protocol: ProxyProtocolConfig::default(),
        }
    }
}

/// Secure WebSocket (WSS) configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WebSocketTlsConfig {
    /// Enable secure WebSocket listener.
    pub enabled: bool,
    /// Secure WebSocket bind address (default: 0.0.0.0:9002).
    #[serde(default = "default_wss_bind")]
    pub bind: SocketAddr,
    /// Path to PEM-encoded certificate file (uses tls.cert if not specified).
    #[serde(default)]
    pub cert: Option<PathBuf>,
    /// Path to PEM-encoded private key file (uses tls.key if not specified).
    #[serde(default)]
    pub key: Option<PathBuf>,
    /// PROXY protocol configuration.
    #[serde(default)]
    #[allow(dead_code)]
    pub proxy_protocol: ProxyProtocolConfig,
}

fn default_wss_bind() -> SocketAddr {
    DEFAULT_WSS_BIND.parse().unwrap()
}

impl Default for WebSocketTlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_wss_bind(),
            cert: None,
            key: None,
            proxy_protocol: ProxyProtocolConfig::default(),
        }
    }
}

impl WebSocketTlsConfig {
    /// Validate the secure WebSocket configuration.
    ///
    /// Requires a reference to TlsConfig to check for fallback cert/key.
    pub fn validate(&self, tls: &super::tls::TlsConfig) -> Result<(), String> {
        if self.enabled {
            let has_own_cert = self
                .cert
                .as_ref()
                .is_some_and(|p| !p.as_os_str().is_empty());
            let has_own_key = self.key.as_ref().is_some_and(|p| !p.as_os_str().is_empty());

            if !has_own_cert && !tls.has_cert() {
                return Err(
                    "websocket_tls requires cert (set websocket_tls.cert or tls.cert)".into(),
                );
            }
            if !has_own_key && !tls.has_key() {
                return Err("websocket_tls requires key (set websocket_tls.key or tls.key)".into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tls::TlsConfig;
    use super::*;

    #[test]
    fn test_disabled_is_valid() {
        let wss = WebSocketTlsConfig::default();
        let tls = TlsConfig::default();
        assert!(wss.validate(&tls).is_ok());
    }

    #[test]
    fn test_enabled_without_any_cert_fails() {
        let wss = WebSocketTlsConfig {
            enabled: true,
            ..Default::default()
        };
        let tls = TlsConfig::default();
        assert!(wss.validate(&tls).is_err());
    }

    #[test]
    fn test_enabled_with_own_cert_and_key() {
        let wss = WebSocketTlsConfig {
            enabled: true,
            cert: Some(PathBuf::from("/path/to/cert.pem")),
            key: Some(PathBuf::from("/path/to/key.pem")),
            ..Default::default()
        };
        let tls = TlsConfig::default();
        assert!(wss.validate(&tls).is_ok());
    }

    #[test]
    fn test_enabled_with_tls_fallback() {
        let wss = WebSocketTlsConfig {
            enabled: true,
            ..Default::default()
        };
        let tls = TlsConfig {
            cert: PathBuf::from("/path/to/cert.pem"),
            key: PathBuf::from("/path/to/key.pem"),
            ..Default::default()
        };
        assert!(wss.validate(&tls).is_ok());
    }
}
