//! MQTT feature configuration.

use serde::Deserialize;

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

impl MqttConfig {
    /// Validate the MQTT configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_qos > 2 {
            return Err("max_qos must be 0, 1, or 2".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_valid() {
        assert!(MqttConfig::default().validate().is_ok());
    }

    #[test]
    fn test_invalid_max_qos() {
        let config = MqttConfig {
            max_qos: 3,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
