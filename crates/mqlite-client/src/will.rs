//! Will message (Last Will and Testament) support.
//!
//! Implements requirements:
//! - [MQTT-3.1.2-8] If Will Flag is set, Will Message MUST be stored and published on abnormal disconnect
//! - [MQTT-3.1.2-9] Will Topic and Will Message fields MUST be present if Will Flag is set

use bytes::Bytes;
use mqlite_core::packet::QoS;

/// Last Will and Testament message.
///
/// The Will Message is published by the broker if the client disconnects
/// unexpectedly (without sending DISCONNECT).
#[derive(Debug, Clone)]
pub struct Will {
    /// Topic to publish the will message to.
    pub topic: String,
    /// Will message payload.
    pub payload: Bytes,
    /// QoS level for will message delivery.
    pub qos: QoS,
    /// Whether the will message should be retained.
    pub retain: bool,
}

impl Will {
    /// Create a new will message with QoS 0 and no retain.
    pub fn new(topic: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            topic: topic.into(),
            payload: payload.into(),
            qos: QoS::AtMostOnce,
            retain: false,
        }
    }

    /// Set the QoS level for the will message.
    pub fn qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    /// Set whether the will message should be retained.
    pub fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_will_builder() {
        let will = Will::new("client/status", "offline")
            .qos(QoS::AtLeastOnce)
            .retain(true);

        assert_eq!(will.topic, "client/status");
        assert_eq!(will.payload.as_ref(), b"offline");
        assert_eq!(will.qos, QoS::AtLeastOnce);
        assert!(will.retain);
    }
}
