//! Client configuration types.

use std::time::Duration;

use crate::will::Will;

/// Reconnection backoff configuration.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial delay before first reconnect attempt.
    pub initial_delay: Duration,
    /// Maximum delay between reconnect attempts.
    pub max_delay: Duration,
    /// Multiplier applied to delay after each failed attempt.
    pub multiplier: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        }
    }
}

/// Client configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Remote broker address (host:port).
    pub address: String,
    /// Client identifier.
    pub client_id: String,
    /// Username for authentication.
    pub username: Option<String>,
    /// Password for authentication.
    pub password: Option<Vec<u8>>,
    /// Keep-alive interval in seconds (0 = disabled).
    pub keep_alive: u16,
    /// Clean session flag.
    pub clean_session: bool,
    /// MQTT protocol version (4 = 3.1.1, 5 = 5.0).
    pub protocol_version: u8,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Last Will and Testament message.
    pub will: Option<Will>,
    /// Retry interval for unacknowledged QoS 1/2 messages.
    pub retry_interval: Duration,
    /// Maximum number of in-flight messages (0 = unlimited).
    pub max_inflight: usize,
    /// Enable automatic reconnection.
    pub auto_reconnect: bool,
    /// Reconnection backoff configuration.
    pub reconnect_backoff: BackoffConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            address: "localhost:1883".to_string(),
            client_id: String::new(),
            username: None,
            password: None,
            keep_alive: 60,
            clean_session: true,
            protocol_version: 4, // MQTT 3.1.1
            connect_timeout: Duration::from_secs(10),
            will: None,
            retry_interval: Duration::from_secs(20),
            max_inflight: 65535,
            auto_reconnect: false,
            reconnect_backoff: BackoffConfig::default(),
        }
    }
}

impl ClientConfig {
    /// Create a new config with the given address.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            ..Default::default()
        }
    }

    /// Set the client ID.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }

    /// Set username and password.
    pub fn credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<Vec<u8>>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    /// Set keep-alive interval in seconds.
    pub fn keep_alive(mut self, seconds: u16) -> Self {
        self.keep_alive = seconds;
        self
    }

    /// Set clean session flag.
    pub fn clean_session(mut self, clean: bool) -> Self {
        self.clean_session = clean;
        self
    }

    /// Use MQTT 5.0 protocol.
    pub fn mqtt5(mut self) -> Self {
        self.protocol_version = 5;
        self
    }

    /// Set connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the Last Will and Testament message.
    pub fn will(mut self, will: Will) -> Self {
        self.will = Some(will);
        self
    }

    /// Set retry interval for unacknowledged messages.
    pub fn retry_interval(mut self, interval: Duration) -> Self {
        self.retry_interval = interval;
        self
    }

    /// Set maximum number of in-flight messages.
    pub fn max_inflight(mut self, max: usize) -> Self {
        self.max_inflight = max;
        self
    }

    /// Enable automatic reconnection.
    pub fn auto_reconnect(mut self, enabled: bool) -> Self {
        self.auto_reconnect = enabled;
        self
    }

    /// Set reconnection backoff configuration.
    pub fn reconnect_backoff(mut self, backoff: BackoffConfig) -> Self {
        self.reconnect_backoff = backoff;
        self
    }
}

/// Options for connect operation (for reconnection with different settings).
#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    /// Override client_id.
    pub client_id: Option<String>,
    /// Override clean_session.
    pub clean_session: Option<bool>,
}
