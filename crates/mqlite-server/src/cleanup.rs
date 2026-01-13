//! Client cleanup utilities.
//!
//! This module provides helpers for cleaning up disconnected clients:
//! - Deregistering from the client registry
//! - Saving pending messages to sessions
//! - Checking client ownership

use mio::Token;

use crate::shared::SharedStateHandle;

/// Check if the current worker is the owner of a client registration.
///
/// Returns `true` if the given token and worker_id match the registered location.
#[inline]
pub fn is_client_owner(
    client_id: &str,
    token: Token,
    worker_id: usize,
    shared: &SharedStateHandle,
) -> bool {
    shared
        .client_registry
        .read()
        .get(client_id)
        .map(|loc| loc.token == token && loc.worker_id == worker_id)
        .unwrap_or(false)
}

/// Remove a client from the registry if we're the current owner.
///
/// Returns `true` if the client was removed, `false` if we weren't the owner.
#[inline]
pub fn remove_from_registry_if_owner(
    client_id: &str,
    token: Token,
    worker_id: usize,
    shared: &SharedStateHandle,
) -> bool {
    let is_owner = is_client_owner(client_id, token, worker_id, shared);
    if is_owner {
        shared.client_registry.write().remove(client_id);
    }
    is_owner
}

/// Update session on client disconnect.
///
/// For persistent sessions (clean_session=false):
/// - Sets last_connection if we're the owner
/// - Saves pending QoS 1/2 messages to the session
/// - Marks takeover as complete
///
/// Returns whether the session was updated.
pub fn update_session_on_disconnect(
    client_id: &str,
    token: Token,
    worker_id: usize,
    is_current_owner: bool,
    pending_qos1: &[(u16, mqlite_core::packet::Publish)],
    pending_qos2: &[(u16, mqlite_core::packet::Publish)],
    shared: &SharedStateHandle,
) {
    let mut sessions = shared.sessions.write();
    // Create session if it doesn't exist - handles race where cleanup
    // runs before new client's CONNECT handler creates the session
    let session = sessions.entry(client_id.to_string()).or_default();

    // Only set last_connection if we're still the owner
    // (otherwise the new client has already taken over)
    if is_current_owner {
        session.last_connection = Some((worker_id, token));
    }

    // Always save pending messages - they might not have been
    // transferred to session yet if this was a takeover
    for (pid, publish) in pending_qos1 {
        session.pending_qos1.push((*pid, publish.clone()));
    }
    for (pid, publish) in pending_qos2 {
        session.pending_qos2.push((*pid, publish.clone()));
    }

    // Signal takeover completion for any waiting worker
    session.takeover_complete = true;
}

/// Prepare pending messages for session storage.
///
/// Converts the client's pending publish maps to vectors suitable for
/// `update_session_on_disconnect`.
pub fn collect_pending_for_session(
    pending_qos1: &ahash::AHashMap<u16, crate::client::PendingPublish>,
    pending_qos2: &ahash::AHashMap<u16, crate::client::PendingPublish>,
) -> (
    Vec<(u16, mqlite_core::packet::Publish)>,
    Vec<(u16, mqlite_core::packet::Publish)>,
) {
    let qos1: Vec<_> = pending_qos1
        .iter()
        .map(|(pid, p)| (*pid, p.publish.clone()))
        .collect();
    let qos2: Vec<_> = pending_qos2
        .iter()
        .map(|(pid, p)| (*pid, p.publish.clone()))
        .collect();
    (qos1, qos2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mqlite_core::packet::{Publish, QoS};

    #[test]
    fn test_collect_pending_for_session_empty() {
        let pending_qos1 = ahash::AHashMap::new();
        let pending_qos2 = ahash::AHashMap::new();

        let (qos1, qos2) = collect_pending_for_session(&pending_qos1, &pending_qos2);

        assert!(qos1.is_empty());
        assert!(qos2.is_empty());
    }

    #[test]
    fn test_collect_pending_for_session_with_messages() {
        use crate::client::PendingPublish;
        use std::time::Instant;

        let mut pending_qos1 = ahash::AHashMap::new();
        let mut pending_qos2 = ahash::AHashMap::new();

        pending_qos1.insert(
            1u16,
            PendingPublish {
                publish: Publish {
                    dup: false,
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    topic: Bytes::from_static(b"test/topic1"),
                    packet_id: Some(1),
                    payload: Bytes::from_static(b"payload1"),
                    properties: None,
                },
                sent_at: Instant::now(),
            },
        );

        pending_qos2.insert(
            2u16,
            PendingPublish {
                publish: Publish {
                    dup: false,
                    qos: QoS::ExactlyOnce,
                    retain: false,
                    topic: Bytes::from_static(b"test/topic2"),
                    packet_id: Some(2),
                    payload: Bytes::from_static(b"payload2"),
                    properties: None,
                },
                sent_at: Instant::now(),
            },
        );

        let (qos1, qos2) = collect_pending_for_session(&pending_qos1, &pending_qos2);

        assert_eq!(qos1.len(), 1);
        assert_eq!(qos2.len(), 1);
        assert_eq!(qos1[0].0, 1);
        assert_eq!(qos2[0].0, 2);
    }
}
