//! SUBSCRIBE packet handling.
//!
//! This module extracts subscription validation, ACL checks,
//! and retained message delivery from the main worker loop.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;

use mqlite_core::packet::{
    encode_variable_byte_integer, update_message_expiry, validate_topic, Packet, Publish, QoS,
    Suback,
};

use crate::auth::{AuthProvider, ClientInfo};
use crate::client::Client;
use crate::config::{LimitsConfig, MqttConfig};
use crate::shared::RetainedMessage;
use crate::subscription::topic_matches_filter;

/// Validation error for subscriptions.
#[derive(Debug, Clone, Copy)]
pub enum SubscriptionError {
    /// Subscription identifiers not supported
    SubscriptionIdNotSupported,
    /// Shared subscriptions not supported
    SharedNotSupported,
    /// Wildcard subscriptions not supported
    WildcardNotSupported,
    /// Topic filter too long or too deep
    TopicInvalid,
    /// ACL denied
    NotAuthorized,
}

impl SubscriptionError {
    /// Get the MQTT v5 reason code for this error.
    #[inline]
    pub fn to_reason_code_v5(self) -> u8 {
        match self {
            Self::SubscriptionIdNotSupported => 0xA1,
            Self::SharedNotSupported => 0x9E,
            Self::WildcardNotSupported => 0xA2,
            Self::TopicInvalid => 0x97,
            Self::NotAuthorized => 0x87,
        }
    }

    /// Get the return code for this error (0x80 for v3.1.1).
    #[inline]
    pub fn to_return_code(self, is_v5: bool) -> u8 {
        if is_v5 {
            self.to_reason_code_v5()
        } else {
            0x80
        }
    }
}

/// Parse a shared subscription topic filter.
///
/// Returns (actual_filter, share_group) if this is a shared subscription,
/// or (filter, None) for a normal subscription.
#[inline]
pub fn parse_shared_subscription(topic_filter: &str) -> (String, Option<Arc<str>>) {
    if topic_filter.starts_with("$share/") && topic_filter.len() > 7 {
        let rest = &topic_filter[7..];
        if let Some(slash_pos) = rest.find('/') {
            let group = &rest[..slash_pos];
            let filter = &rest[slash_pos + 1..];
            return (filter.to_string(), Some(Arc::from(group)));
        }
    }
    (topic_filter.to_string(), None)
}

/// Validate a subscription topic filter against server configuration.
#[inline]
pub fn validate_subscription(
    actual_filter: &str,
    share_group: Option<&Arc<str>>,
    mqtt_config: &MqttConfig,
    limits_config: &LimitsConfig,
) -> Result<(), SubscriptionError> {
    // Shared subscription check
    if share_group.is_some() && !mqtt_config.shared_subscriptions {
        return Err(SubscriptionError::SharedNotSupported);
    }

    // Wildcard check
    let has_wildcard = actual_filter.contains('+') || actual_filter.contains('#');
    if has_wildcard && !mqtt_config.wildcard_subscriptions {
        return Err(SubscriptionError::WildcardNotSupported);
    }

    // Topic filter length and depth
    if validate_topic(
        actual_filter.as_bytes(),
        limits_config.max_topic_length,
        limits_config.max_topic_levels,
    )
    .is_err()
    {
        return Err(SubscriptionError::TopicInvalid);
    }

    Ok(())
}

/// Check ACL permissions for a subscribe operation.
#[inline]
pub fn check_subscribe_acl(
    client: &Client,
    topic_filter: &str,
    auth: &AuthProvider,
) -> Result<(), SubscriptionError> {
    let client_info = ClientInfo {
        client_id: client.client_id.clone().unwrap_or_default(),
        username: client.username.clone(),
        role: client.role.clone(),
        is_anonymous: client.is_anonymous,
    };
    let result = auth.check_subscribe(&client_info, topic_filter);
    if result.is_allowed() {
        Ok(())
    } else {
        Err(SubscriptionError::NotAuthorized)
    }
}

/// Retained message with subscription context.
#[allow(dead_code)]
pub struct RetainedToSend {
    pub publish: Publish,
    pub stored_at: Instant,
    pub sub_qos: QoS,
    pub retain_as_published: bool,
    pub subscription_id: Option<u32>,
}

/// Collect retained messages matching a topic filter.
pub fn collect_retained_messages(
    retained_messages: &std::collections::HashMap<String, RetainedMessage>,
    actual_filter: &str,
    sub_qos: QoS,
    retain_as_published: bool,
    subscription_id: Option<u32>,
) -> Vec<RetainedToSend> {
    let mut result = Vec::new();
    for (topic, retained_msg) in retained_messages.iter() {
        if topic_matches_filter(topic, actual_filter) {
            result.push(RetainedToSend {
                publish: retained_msg.publish.clone(),
                stored_at: retained_msg.stored_at,
                sub_qos,
                retain_as_published,
                subscription_id,
            });
        }
    }
    result
}

/// Send retained messages to a client.
pub fn send_retained_messages(client: &mut Client, retained_to_send: Vec<RetainedToSend>) {
    let is_v5 = client.protocol_version == 5;

    for retained in retained_to_send {
        let elapsed_secs = retained.stored_at.elapsed().as_secs() as u32;

        // For v5, check and update message expiry
        let base_properties = if is_v5 {
            match update_message_expiry(retained.publish.properties.as_ref(), elapsed_secs) {
                None => continue, // Message expired
                Some(props) => props,
            }
        } else {
            retained.publish.properties.clone()
        };

        let effective_qos = std::cmp::min(retained.publish.qos as u8, retained.sub_qos as u8);
        let out_qos = match QoS::try_from(effective_qos) {
            Ok(q) => q,
            Err(_) => continue,
        };

        let packet_id = if out_qos != QoS::AtMostOnce {
            Some(client.allocate_packet_id())
        } else {
            None
        };

        // MQTT-3.3.1-8: Retained message delivery to new subscriber must have RETAIN=1
        let out_retain = true;

        // Build properties with subscription ID for v5
        let properties = if is_v5 {
            if let Some(sub_id) = retained.subscription_id {
                let mut props = Vec::new();
                props.push(0x0B); // Subscription Identifier property
                encode_variable_byte_integer(sub_id, &mut props);
                if let Some(ref existing) = base_properties {
                    props.extend_from_slice(existing);
                }
                Some(Bytes::from(props))
            } else {
                Some(base_properties.unwrap_or_else(Bytes::new))
            }
        } else {
            None
        };

        let out_publish = Publish {
            dup: false,
            qos: out_qos,
            retain: out_retain,
            topic: retained.publish.topic.clone(),
            packet_id,
            payload: retained.publish.payload.clone(),
            properties,
        };

        // Drop if slow
        let _ = client.queue_packet(&Packet::Publish(out_publish));
    }
}

/// Queue a SUBACK packet to the client.
#[inline]
pub fn send_suback(client: &mut Client, packet_id: u16, return_codes: Vec<u8>) {
    let suback = Suback {
        packet_id,
        return_codes,
        is_v5: client.protocol_version == 5,
    };
    if let Err(e) = client.queue_control_packet(&Packet::Suback(suback)) {
        log::warn!(
            "Failed to queue SUBACK for client {:?}: {}",
            client.client_id,
            e
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_shared_subscription_normal() {
        let (filter, group) = parse_shared_subscription("test/topic");
        assert_eq!(filter, "test/topic");
        assert!(group.is_none());
    }

    #[test]
    fn test_parse_shared_subscription_shared() {
        let (filter, group) = parse_shared_subscription("$share/mygroup/test/topic");
        assert_eq!(filter, "test/topic");
        assert_eq!(group.as_deref(), Some("mygroup"));
    }

    #[test]
    fn test_parse_shared_subscription_invalid() {
        let (filter, group) = parse_shared_subscription("$share/nofilter");
        assert_eq!(filter, "$share/nofilter");
        assert!(group.is_none());
    }

    #[test]
    fn test_subscription_error_codes_v5() {
        assert_eq!(
            SubscriptionError::SubscriptionIdNotSupported.to_reason_code_v5(),
            0xA1
        );
        assert_eq!(
            SubscriptionError::SharedNotSupported.to_reason_code_v5(),
            0x9E
        );
        assert_eq!(
            SubscriptionError::WildcardNotSupported.to_reason_code_v5(),
            0xA2
        );
        assert_eq!(SubscriptionError::TopicInvalid.to_reason_code_v5(), 0x97);
        assert_eq!(SubscriptionError::NotAuthorized.to_reason_code_v5(), 0x87);
    }

    #[test]
    fn test_subscription_error_codes_v3() {
        assert_eq!(
            SubscriptionError::SubscriptionIdNotSupported.to_return_code(false),
            0x80
        );
        assert_eq!(
            SubscriptionError::SharedNotSupported.to_return_code(false),
            0x80
        );
    }
}
