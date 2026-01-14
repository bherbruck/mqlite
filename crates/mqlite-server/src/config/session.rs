//! Session configuration.

use serde::Deserialize;

use super::limits::DEFAULT_TOPIC_ALIAS_MAXIMUM;

/// Default keep alive in seconds.
pub const DEFAULT_KEEP_ALIVE: u16 = 60;

/// Default maximum keep alive in seconds.
pub const DEFAULT_MAX_KEEP_ALIVE: u16 = 65535;

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
fn default_topic_alias_maximum() -> u16 {
    DEFAULT_TOPIC_ALIAS_MAXIMUM
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
