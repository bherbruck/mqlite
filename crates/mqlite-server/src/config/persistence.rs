//! Persistence configuration.
//!
//! Controls retained message persistence using fjall.

use std::path::PathBuf;

use serde::Deserialize;

/// Default persistence data directory.
pub const DEFAULT_PERSISTENCE_PATH: &str = "./data";

/// Persistence configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PersistenceConfig {
    /// Enable persistence for retained messages.
    ///
    /// When enabled, retained messages are stored to disk and survive
    /// broker restarts. Requires the `persistence` feature.
    pub enabled: bool,

    /// Directory path for persistence data files.
    ///
    /// Default: `./data`
    pub path: PathBuf,

    /// Sync interval in seconds for periodic durability.
    ///
    /// Retained messages are flushed to disk at this interval.
    /// Set to 0 to disable periodic sync (data is synced on shutdown).
    ///
    /// Default: 30 seconds
    pub sync_interval_secs: u64,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: PathBuf::from(DEFAULT_PERSISTENCE_PATH),
            sync_interval_secs: 30,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_persistence_config() {
        let config = PersistenceConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.path, PathBuf::from("./data"));
        assert_eq!(config.sync_interval_secs, 30);
    }
}
