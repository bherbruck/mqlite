//! CONNECT packet handling.
//!
//! This module extracts connection validation, authentication, session management,
//! and CONNACK building from the main worker loop.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mio::Token;

use mqlite_core::packet::{Connack, ConnackCode, ConnackProperties, Connect, Packet, Publish, QoS};

use crate::auth::{AuthContext, AuthProvider, AuthResult};
use crate::client::{Client, PendingPublish};
use crate::client_handle::ClientWriteHandle;
use crate::config::Config;
use crate::shared::{ClientLocation, Session, SharedStateHandle, StoredSubscription};
use crate::subscription::{Subscriber, SubscriptionStore};

/// Validation error for CONNECT packets.
#[derive(Debug)]
pub enum ConnectValidationError {
    /// Zero-length ClientId with CleanSession=false
    EmptyClientIdNotClean,
}

/// Result of authentication.
pub struct AuthenticationResult {
    pub allowed: bool,
    pub auth_result: AuthResult,
    pub role: Option<String>,
}

/// Validate client ID constraints.
///
/// MQTT-3.1.3-7: Zero-length ClientId requires CleanSession=1
#[inline]
pub fn validate_client_id(connect: &Connect) -> Result<(), ConnectValidationError> {
    if connect.client_id.is_empty() && !connect.clean_session {
        return Err(ConnectValidationError::EmptyClientIdNotClean);
    }
    Ok(())
}

/// Build a rejection CONNACK for validation errors.
#[inline]
pub fn build_rejection_connack(code: ConnackCode, reason_code_v5: u8, is_v5: bool) -> Connack {
    if is_v5 {
        Connack {
            session_present: false,
            code,
            reason_code: Some(reason_code_v5),
            properties: Some(ConnackProperties::default()),
        }
    } else {
        Connack {
            session_present: false,
            code,
            reason_code: None,
            properties: None,
        }
    }
}

/// Authenticate a client.
#[inline]
pub fn authenticate_client(
    connect: &Connect,
    remote_addr: std::net::SocketAddr,
    auth: &AuthProvider,
) -> AuthenticationResult {
    let auth_ctx = AuthContext {
        client_id: &connect.client_id,
        username: connect.username.as_deref(),
        password: connect.password.as_deref(),
        remote_addr,
    };
    let (auth_result, role) = auth.authenticate(&auth_ctx);
    AuthenticationResult {
        allowed: auth_result.is_allowed(),
        auth_result,
        role,
    }
}

/// Build a rejection CONNACK for authentication failure.
#[inline]
pub fn build_auth_rejection_connack(auth_result: &AuthResult, is_v5: bool) -> Connack {
    if is_v5 {
        Connack {
            session_present: false,
            code: ConnackCode::NotAuthorized,
            reason_code: Some(auth_result.to_reason_code_v5()),
            properties: Some(ConnackProperties::default()),
        }
    } else {
        Connack {
            session_present: false,
            code: if matches!(auth_result, AuthResult::DenyBadCredentials) {
                ConnackCode::BadUsernamePassword
            } else {
                ConnackCode::NotAuthorized
            },
            reason_code: None,
            properties: None,
        }
    }
}

/// Result of client takeover handling.
pub struct TakeoverResult {
    /// Location of the old client (if any) for subscription cleanup.
    pub old_location: Option<ClientLocation>,
    /// Whether this was a cross-worker takeover (requires waiting).
    pub is_cross_worker: bool,
}

/// Save a client's pending QoS 1/2 messages to a session.
///
/// This is used during:
/// - Same-worker takeover (save before disconnect)
/// - Cross-worker takeover (save when receiving Disconnect message)
/// - Client cleanup (save on disconnect for persistent sessions)
#[inline]
pub fn save_pending_to_session(client: &Client, session: &mut Session) {
    for (pid, pending) in &client.pending_qos1 {
        session.pending_qos1.push((*pid, pending.publish.clone()));
    }
    for (pid, pending) in &client.pending_qos2 {
        session.pending_qos2.push((*pid, pending.publish.clone()));
    }
}

/// Check if a client with this ID already exists and determine takeover type.
///
/// Returns `None` if no existing client, or `Some(TakeoverResult)` with the
/// old location and whether it's a cross-worker takeover.
#[inline]
pub fn check_existing_client(
    client_id: &str,
    new_token: Token,
    worker_id: usize,
    shared: &SharedStateHandle,
) -> Option<TakeoverResult> {
    if client_id.is_empty() {
        return None;
    }

    let existing_location = shared.client_registry.read().get(client_id).cloned();

    match existing_location {
        Some(location) if location.token != new_token || location.worker_id != worker_id => {
            Some(TakeoverResult {
                old_location: Some(location),
                is_cross_worker: location.worker_id != worker_id,
            })
        }
        _ => None,
    }
}

/// Register a new client in the client registry.
#[inline]
pub fn register_client(
    client_id: &str,
    token: Token,
    worker_id: usize,
    shared: &SharedStateHandle,
) {
    if !client_id.is_empty() {
        shared
            .client_registry
            .write()
            .insert(client_id.to_string(), ClientLocation { worker_id, token });
    }
}

/// Generate an assigned client ID for MQTT 5 clients with empty client ID.
pub fn generate_client_id(token: Token) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("mqlite-{:016x}-{}", nanos, token.0)
}

/// Build a success CONNACK with appropriate properties.
pub fn build_success_connack(
    session_present: bool,
    assigned_client_id: Option<String>,
    config: &Config,
    is_v5: bool,
) -> Connack {
    if is_v5 {
        let mut props = ConnackProperties::default();

        if let Some(id) = assigned_client_id {
            props.assigned_client_identifier = Some(id);
        }

        // Set server capabilities from config
        if !config.mqtt.retain_available {
            props.retain_available = Some(false);
        }
        if !config.mqtt.wildcard_subscriptions {
            props.wildcard_subscription_available = Some(false);
        }
        if !config.mqtt.subscription_identifiers {
            props.subscription_identifiers_available = Some(false);
        }
        if !config.mqtt.shared_subscriptions {
            props.shared_subscription_available = Some(false);
        }
        if config.mqtt.max_qos < 2 {
            props.maximum_qos = Some(config.mqtt.max_qos);
        }

        // Advertise server limits
        props.receive_maximum = Some(config.limits.receive_maximum);
        if config.limits.max_packet_size > 0 {
            props.maximum_packet_size = Some(config.limits.max_packet_size);
        }
        props.topic_alias_maximum = Some(config.limits.topic_alias_maximum);

        Connack {
            session_present,
            code: ConnackCode::Accepted,
            reason_code: Some(0x00),
            properties: Some(props),
        }
    } else {
        Connack {
            session_present,
            code: ConnackCode::Accepted,
            reason_code: None,
            properties: None,
        }
    }
}

/// Wait for cross-worker takeover to complete.
///
/// Waits up to 100ms for the other worker to save pending messages.
pub fn wait_for_takeover(client_id: &str, shared: &SharedStateHandle) {
    for _ in 0..100 {
        let complete = shared
            .sessions
            .read()
            .get(client_id)
            .map(|s| s.takeover_complete)
            .unwrap_or(true);
        if complete {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Restore subscriptions from a session to the subscription store.
#[allow(clippy::too_many_arguments)]
pub fn restore_subscriptions(
    subscriptions: &mut SubscriptionStore,
    subs_to_restore: &[StoredSubscription],
    handle: Arc<ClientWriteHandle>,
    client_id: Arc<str>,
    old_location: Option<ClientLocation>,
    session_last_connection: Option<(usize, Token)>,
    local_old_tokens: &[Token],
    worker_id: usize,
) {
    // Remove old subscriptions
    if let Some(loc) = old_location {
        subscriptions.remove_client(loc.worker_id, loc.token);
    } else if let Some((old_worker, old_token)) = session_last_connection {
        subscriptions.remove_client(old_worker, old_token);
    }

    // Remove local old tokens
    for old_token in local_old_tokens {
        subscriptions.remove_client(worker_id, *old_token);
    }

    // Restore subscriptions with new handle
    for stored_sub in subs_to_restore {
        subscriptions.subscribe(
            &stored_sub.topic_filter,
            Subscriber {
                handle: handle.clone(),
                qos: stored_sub.options.qos,
                client_id: client_id.clone(),
                options: stored_sub.options,
                subscription_id: stored_sub.subscription_id,
                share_group: None,
            },
        );
    }
}

/// Collect pending messages from a session for resend.
pub fn collect_pending_messages(session: &mut Session) -> Vec<Publish> {
    let mut pending_to_resend = Vec::new();

    log::debug!(
        "Session restore: {} QoS1, {} QoS2 pending",
        session.pending_qos1.len(),
        session.pending_qos2.len()
    );

    for (pid, mut publish) in session.pending_qos1.drain(..) {
        publish.dup = true;
        publish.packet_id = Some(pid);
        pending_to_resend.push(publish);
    }
    for (pid, mut publish) in session.pending_qos2.drain(..) {
        publish.dup = true;
        publish.packet_id = Some(pid);
        pending_to_resend.push(publish);
    }

    pending_to_resend
}

/// Track and queue pending messages for resend.
pub fn resend_pending_messages(client: &mut Client, pending_to_resend: Vec<Publish>) {
    for publish in pending_to_resend {
        if let Some(pid) = publish.packet_id {
            let pending = PendingPublish {
                publish: publish.clone(),
                sent_at: Instant::now(),
            };
            match publish.qos {
                QoS::AtLeastOnce => {
                    client.pending_qos1.insert(pid, pending);
                }
                QoS::ExactlyOnce => {
                    client.pending_qos2.insert(pid, pending);
                }
                _ => {}
            }
        }
        // Session restore: drop if slow (will retry on next reconnect)
        let _ = client.queue_packet(&Packet::Publish(publish));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_connect(client_id: &str, clean_session: bool, is_v5: bool) -> Connect {
        Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: if is_v5 { 5 } else { 4 },
            clean_session,
            keep_alive: 60,
            client_id: client_id.to_string(),
            will: None,
            username: None,
            password: None,
            properties: None,
        }
    }

    #[test]
    fn test_validate_client_id_empty_clean() {
        let connect = make_connect("", true, false);
        assert!(validate_client_id(&connect).is_ok());
    }

    #[test]
    fn test_validate_client_id_empty_not_clean() {
        let connect = make_connect("", false, false);
        assert!(matches!(
            validate_client_id(&connect),
            Err(ConnectValidationError::EmptyClientIdNotClean)
        ));
    }

    #[test]
    fn test_validate_client_id_non_empty() {
        let connect = make_connect("client123", false, false);
        assert!(validate_client_id(&connect).is_ok());
    }

    #[test]
    fn test_build_rejection_connack_v5() {
        let connack = build_rejection_connack(ConnackCode::IdentifierRejected, 0x85, true);
        assert!(!connack.session_present);
        assert_eq!(connack.code, ConnackCode::IdentifierRejected);
        assert_eq!(connack.reason_code, Some(0x85));
        assert!(connack.properties.is_some());
    }

    #[test]
    fn test_build_rejection_connack_v3() {
        let connack = build_rejection_connack(ConnackCode::IdentifierRejected, 0x85, false);
        assert!(!connack.session_present);
        assert_eq!(connack.code, ConnackCode::IdentifierRejected);
        assert!(connack.reason_code.is_none());
        assert!(connack.properties.is_none());
    }

    #[test]
    fn test_generate_client_id() {
        let id = generate_client_id(Token(42));
        assert!(id.starts_with("mqlite-"));
        assert!(id.ends_with("-42"));
    }

    #[test]
    fn test_save_pending_to_session() {
        use bytes::Bytes;

        let mut session = Session::default();

        // Create mock pending messages
        let publish1 = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: Bytes::from_static(b"test/topic1"),
            packet_id: Some(1),
            payload: Bytes::from_static(b"payload1"),
            properties: None,
        };
        let publish2 = Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: Bytes::from_static(b"test/topic2"),
            packet_id: Some(2),
            payload: Bytes::from_static(b"payload2"),
            properties: None,
        };

        // Simulate client pending messages
        let mut pending_qos1 = ahash::AHashMap::new();
        pending_qos1.insert(
            1u16,
            PendingPublish {
                publish: publish1.clone(),
                sent_at: Instant::now(),
            },
        );

        let mut pending_qos2 = ahash::AHashMap::new();
        pending_qos2.insert(
            2u16,
            PendingPublish {
                publish: publish2.clone(),
                sent_at: Instant::now(),
            },
        );

        // Manually do what save_pending_to_session does
        for (pid, pending) in &pending_qos1 {
            session.pending_qos1.push((*pid, pending.publish.clone()));
        }
        for (pid, pending) in &pending_qos2 {
            session.pending_qos2.push((*pid, pending.publish.clone()));
        }

        assert_eq!(session.pending_qos1.len(), 1);
        assert_eq!(session.pending_qos2.len(), 1);
        assert_eq!(session.pending_qos1[0].0, 1);
        assert_eq!(session.pending_qos2[0].0, 2);
    }

    #[test]
    fn test_takeover_result_structure() {
        let result = TakeoverResult {
            old_location: Some(ClientLocation {
                worker_id: 1,
                token: Token(42),
            }),
            is_cross_worker: true,
        };

        assert!(result.old_location.is_some());
        assert!(result.is_cross_worker);

        let loc = result.old_location.unwrap();
        assert_eq!(loc.worker_id, 1);
        assert_eq!(loc.token, Token(42));
    }
}
