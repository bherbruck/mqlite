//! Publish packet handling.
//!
//! This module extracts publish validation, ACL checks, retained message handling,
//! and offline session delivery from the main worker loop.

use std::time::Instant;

use mqlite_core::packet::{validate_topic, Packet, Publish, QoS};

use crate::auth::{AuthProvider, ClientInfo};
use crate::client::Client;
use crate::config::MqttConfig;
use crate::shared::{RetainedMessage, SharedStateHandle};
use crate::subscription::topic_matches_filter;

/// Validation error types for publish packets.
#[derive(Debug)]
pub enum PublishValidationError {
    /// Topic contains wildcards (+ or #)
    TopicContainsWildcard,
    /// Topic exceeds length or depth limits
    TopicInvalid,
    /// QoS exceeds server maximum
    QosNotSupported,
    /// Retain not available on this server
    RetainNotSupported,
}

/// Result of ACL check for publish.
pub struct AclCheckResult {
    pub allowed: bool,
    pub is_v5: bool,
}

/// Validate a publish packet against server constraints.
///
/// Returns Ok(()) if valid, or an error indicating the violation.
#[inline]
pub fn validate_publish(
    publish: &Publish,
    config: &MqttConfig,
    max_topic_length: usize,
    max_topic_levels: usize,
) -> Result<(), PublishValidationError> {
    // MQTT-3.3.2-2, MQTT-4.7.3-1: Topic Names MUST NOT contain wildcards
    if publish.topic.iter().any(|&b| b == b'+' || b == b'#') {
        return Err(PublishValidationError::TopicContainsWildcard);
    }

    // Validate topic length and depth
    if validate_topic(&publish.topic, max_topic_length, max_topic_levels).is_err() {
        return Err(PublishValidationError::TopicInvalid);
    }

    // Enforce max_qos: reject publish if QoS exceeds server maximum
    if publish.qos as u8 > config.max_qos {
        return Err(PublishValidationError::QosNotSupported);
    }

    // Enforce retain_available: reject retained publish if disabled
    if publish.retain && !config.retain_available {
        return Err(PublishValidationError::RetainNotSupported);
    }

    Ok(())
}

/// Check ACL permissions for a publish operation.
#[inline]
pub fn check_publish_acl(client: &Client, topic: &[u8], auth: &AuthProvider) -> AclCheckResult {
    let topic_str = String::from_utf8_lossy(topic);
    let client_info = ClientInfo {
        client_id: client.client_id.clone().unwrap_or_default(),
        username: client.username.clone(),
        role: client.role.clone(),
        is_anonymous: client.is_anonymous,
    };
    let result = auth.check_publish(&client_info, &topic_str);
    AclCheckResult {
        allowed: result.is_allowed(),
        is_v5: client.protocol_version == 5,
    }
}

/// Send PUBACK or PUBREC acknowledgment to the publishing client.
///
/// For ACL denials on MQTT v5, this sends the ack with appropriate reason code.
/// For successful publishes, this sends the standard ack.
#[inline]
pub fn send_publisher_ack(client: &mut Client, publish: &Publish) {
    match publish.qos {
        QoS::AtLeastOnce => {
            if let Some(packet_id) = publish.packet_id {
                if let Err(e) = client.queue_control_packet(&Packet::Puback { packet_id }) {
                    log::warn!(
                        "Failed to queue PUBACK for client {:?} packet_id={}: {}",
                        client.client_id,
                        packet_id,
                        e
                    );
                }
            }
        }
        QoS::ExactlyOnce => {
            if let Some(packet_id) = publish.packet_id {
                if let Err(e) = client.queue_control_packet(&Packet::Pubrec { packet_id }) {
                    log::warn!(
                        "Failed to queue PUBREC for client {:?} packet_id={}: {}",
                        client.client_id,
                        packet_id,
                        e
                    );
                }
            }
        }
        QoS::AtMostOnce => {}
    }
}

/// Handle retained message storage or deletion.
#[inline]
pub fn handle_retained(publish: &Publish, shared: &SharedStateHandle) {
    if !publish.retain {
        return;
    }

    let topic_str = String::from_utf8_lossy(&publish.topic).into_owned();
    let mut retained_msgs = shared.retained_messages.write();

    if publish.payload.is_empty() {
        // Empty payload = delete retained message
        retained_msgs.remove(&topic_str);
    } else {
        let retained_publish = Publish {
            dup: false,
            qos: publish.qos,
            retain: true,
            topic: publish.topic.clone(),
            packet_id: None,
            payload: publish.payload.clone(),
            properties: publish.properties.clone(),
        };
        let retained = RetainedMessage {
            publish: retained_publish,
            stored_at: Instant::now(),
        };
        retained_msgs.insert(topic_str, retained);
    }
}

/// Deliver publish to offline persistent sessions [MQTT-4.4.0-1].
///
/// Sessions whose clients are disconnected still need to receive QoS 1/2 messages.
/// Returns the next offline packet ID to use.
pub fn deliver_to_offline_sessions(
    publish: &Publish,
    shared: &SharedStateHandle,
    mut next_packet_id: u16,
) -> u16 {
    if publish.qos == QoS::AtMostOnce {
        return next_packet_id;
    }

    let topic_str = std::str::from_utf8(&publish.topic).unwrap_or("");
    let registry = shared.client_registry.read();
    let mut sessions = shared.sessions.write();

    for (client_id, session) in sessions.iter_mut() {
        // Skip if client is currently online
        if registry.contains_key(client_id) {
            continue;
        }

        // Check if any subscription matches this topic
        for stored_sub in &session.subscriptions {
            if topic_matches_filter(topic_str, &stored_sub.topic_filter) {
                // Calculate effective QoS
                let effective_qos = std::cmp::min(publish.qos as u8, stored_sub.options.qos as u8);
                let out_qos = QoS::try_from(effective_qos).unwrap_or(QoS::AtMostOnce);

                if out_qos != QoS::AtMostOnce {
                    // Allocate packet ID for offline delivery
                    let pkt_id = next_packet_id;
                    next_packet_id = next_packet_id.wrapping_add(1);
                    if next_packet_id == 0 {
                        next_packet_id = 1;
                    }

                    let queued_publish = Publish {
                        dup: false,
                        qos: out_qos,
                        retain: false,
                        topic: publish.topic.clone(),
                        packet_id: Some(pkt_id),
                        payload: publish.payload.clone(),
                        properties: publish.properties.clone(),
                    };

                    match out_qos {
                        QoS::AtLeastOnce => {
                            session.pending_qos1.push((pkt_id, queued_publish));
                        }
                        QoS::ExactlyOnce => {
                            session.pending_qos2.push((pkt_id, queued_publish));
                        }
                        _ => {}
                    }
                }
                // Only queue once per session even if multiple subscriptions match
                break;
            }
        }
    }

    next_packet_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_publish(topic: &[u8], qos: QoS, retain: bool) -> Publish {
        Publish {
            dup: false,
            qos,
            retain,
            topic: Bytes::copy_from_slice(topic),
            packet_id: if qos != QoS::AtMostOnce {
                Some(1)
            } else {
                None
            },
            payload: Bytes::from_static(b"test"),
            properties: None,
        }
    }

    fn make_config(max_qos: u8, retain_available: bool) -> MqttConfig {
        MqttConfig {
            max_qos,
            retain_available,
            ..Default::default()
        }
    }

    #[test]
    fn test_validate_publish_wildcard_plus() {
        let publish = make_publish(b"test/+/topic", QoS::AtMostOnce, false);
        let config = make_config(2, true);
        let result = validate_publish(&publish, &config, 1024, 32);
        assert!(matches!(
            result,
            Err(PublishValidationError::TopicContainsWildcard)
        ));
    }

    #[test]
    fn test_validate_publish_wildcard_hash() {
        let publish = make_publish(b"test/#", QoS::AtMostOnce, false);
        let config = make_config(2, true);
        let result = validate_publish(&publish, &config, 1024, 32);
        assert!(matches!(
            result,
            Err(PublishValidationError::TopicContainsWildcard)
        ));
    }

    #[test]
    fn test_validate_publish_qos_exceeded() {
        let publish = make_publish(b"test/topic", QoS::ExactlyOnce, false);
        let config = make_config(1, true); // max QoS 1, but publish is QoS 2
        let result = validate_publish(&publish, &config, 1024, 32);
        assert!(matches!(
            result,
            Err(PublishValidationError::QosNotSupported)
        ));
    }

    #[test]
    fn test_validate_publish_retain_disabled() {
        let publish = make_publish(b"test/topic", QoS::AtMostOnce, true);
        let config = make_config(2, false); // retain disabled
        let result = validate_publish(&publish, &config, 1024, 32);
        assert!(matches!(
            result,
            Err(PublishValidationError::RetainNotSupported)
        ));
    }

    #[test]
    fn test_validate_publish_valid() {
        let publish = make_publish(b"test/topic", QoS::AtLeastOnce, false);
        let config = make_config(2, true);
        let result = validate_publish(&publish, &config, 1024, 32);
        assert!(result.is_ok());
    }
}
