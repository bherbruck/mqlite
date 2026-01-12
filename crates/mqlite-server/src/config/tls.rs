//! TLS/SSL configuration.

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

use super::server::ProxyProtocolConfig;

/// Default TLS bind address.
pub const DEFAULT_TLS_BIND: &str = "0.0.0.0:8883";

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
    /// PROXY protocol configuration.
    #[serde(default)]
    pub proxy_protocol: ProxyProtocolConfig,
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
            proxy_protocol: ProxyProtocolConfig::default(),
        }
    }
}

impl TlsConfig {
    /// Validate the TLS configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled {
            if self.cert.as_os_str().is_empty() {
                return Err("tls.cert is required when TLS is enabled".into());
            }
            if self.key.as_os_str().is_empty() {
                return Err("tls.key is required when TLS is enabled".into());
            }
        }
        Ok(())
    }

    /// Check if this TLS config has a certificate.
    pub fn has_cert(&self) -> bool {
        !self.cert.as_os_str().is_empty()
    }

    /// Check if this TLS config has a key.
    pub fn has_key(&self) -> bool {
        !self.key.as_os_str().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_is_valid() {
        assert!(TlsConfig::default().validate().is_ok());
    }

    #[test]
    fn test_enabled_without_cert_fails() {
        let config = TlsConfig {
            enabled: true,
            key: PathBuf::from("/path/to/key.pem"),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_enabled_without_key_fails() {
        let config = TlsConfig {
            enabled: true,
            cert: PathBuf::from("/path/to/cert.pem"),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_enabled_with_cert_and_key_is_valid() {
        let config = TlsConfig {
            enabled: true,
            cert: PathBuf::from("/path/to/cert.pem"),
            key: PathBuf::from("/path/to/key.pem"),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }
}
