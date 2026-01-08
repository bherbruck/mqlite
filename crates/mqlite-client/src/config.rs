//! Client configuration types.

use std::time::Duration;

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
}

/// Options for connect operation (for reconnection with different settings).
#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    /// Override client_id.
    pub client_id: Option<String>,
    /// Override clean_session.
    pub clean_session: Option<bool>,
}
