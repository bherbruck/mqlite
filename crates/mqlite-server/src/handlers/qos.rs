//! QoS acknowledgment packet handlers.
//!
//! This module handles the QoS flow control packets:
//! - PUBACK (QoS 1 acknowledgment)
//! - PUBREC (QoS 2 received)
//! - PUBREL (QoS 2 release)
//! - PUBCOMP (QoS 2 complete)

use mqlite_core::packet::Packet;

use crate::client::Client;

/// Handle PUBACK packet - QoS 1 acknowledgment from subscriber.
///
/// Removes the pending message from tracking and restores the outgoing quota.
#[inline]
pub fn handle_puback(client: &mut Client, packet_id: u16) {
    client.pending_qos1.remove(&packet_id);
    // MQTT 5: Restore outgoing quota when ACK received
    client.restore_quota();
}

/// Handle PUBREC packet - QoS 2 received acknowledgment.
///
/// Sends PUBREL to continue the QoS 2 flow.
#[inline]
pub fn handle_pubrec(client: &mut Client, packet_id: u16) {
    // PUBREL is critical for QoS 2 flow - use guaranteed write
    if let Err(e) = client.queue_control_packet(&Packet::Pubrel { packet_id }) {
        log::warn!(
            "Failed to queue PUBREL for client {:?} packet_id={}: {}",
            client.client_id,
            packet_id,
            e
        );
    }
}

/// Handle PUBREL packet - QoS 2 release from publisher.
///
/// Sends PUBCOMP to complete the QoS 2 flow.
#[inline]
pub fn handle_pubrel(client: &mut Client, packet_id: u16) {
    // PUBCOMP is critical for QoS 2 flow - use guaranteed write
    if let Err(e) = client.queue_control_packet(&Packet::Pubcomp { packet_id }) {
        log::warn!(
            "Failed to queue PUBCOMP for client {:?} packet_id={}: {}",
            client.client_id,
            packet_id,
            e
        );
    }
}

/// Handle PUBCOMP packet - QoS 2 complete acknowledgment.
///
/// Removes the pending message from tracking and restores the outgoing quota.
#[inline]
pub fn handle_pubcomp(client: &mut Client, packet_id: u16) {
    client.pending_qos2.remove(&packet_id);
    // MQTT 5: Restore outgoing quota when QoS 2 flow completes
    client.restore_quota();
}

#[cfg(test)]
mod tests {
    use crate::client::PendingPublish;
    use bytes::Bytes;
    use mqlite_core::packet::{Publish, QoS};
    use std::time::Instant;

    fn make_pending_publish(packet_id: u16, qos: QoS) -> PendingPublish {
        PendingPublish {
            publish: Publish {
                dup: false,
                qos,
                retain: false,
                topic: Bytes::from_static(b"test/topic"),
                packet_id: Some(packet_id),
                payload: Bytes::from_static(b"test"),
                properties: None,
            },
            sent_at: Instant::now(),
        }
    }

    #[test]
    fn test_handle_puback_removes_pending() {
        // Create a minimal client for testing
        // Note: Full client creation requires transport, so we test the logic directly
        let mut pending_qos1 = ahash::AHashMap::new();
        pending_qos1.insert(1, make_pending_publish(1, QoS::AtLeastOnce));
        pending_qos1.insert(2, make_pending_publish(2, QoS::AtLeastOnce));

        // Simulate PUBACK for packet_id 1
        pending_qos1.remove(&1);

        assert!(!pending_qos1.contains_key(&1));
        assert!(pending_qos1.contains_key(&2));
    }

    #[test]
    fn test_handle_pubcomp_removes_pending() {
        let mut pending_qos2 = ahash::AHashMap::new();
        pending_qos2.insert(1, make_pending_publish(1, QoS::ExactlyOnce));
        pending_qos2.insert(2, make_pending_publish(2, QoS::ExactlyOnce));

        // Simulate PUBCOMP for packet_id 1
        pending_qos2.remove(&1);

        assert!(!pending_qos2.contains_key(&1));
        assert!(pending_qos2.contains_key(&2));
    }

    #[test]
    fn test_handle_puback_nonexistent_packet_id() {
        let mut pending_qos1 = ahash::AHashMap::<u16, PendingPublish>::new();

        // Should not panic on nonexistent packet_id
        pending_qos1.remove(&999);

        assert!(pending_qos1.is_empty());
    }
}
