//! Session state tracking for MQTT client.
//!
//! Implements client-side session state per MQTT spec Section 6.1:
//! - QoS 1 and QoS 2 messages sent but not completely acknowledged
//! - QoS 2 messages received but not completely acknowledged
//!
//! Key requirements:
//! - [MQTT-4.4.0-1] On reconnect with CleanSession=0, re-send unacknowledged messages
//! - [MQTT-4.6.0-1] Re-send in the order originally sent

use std::collections::VecDeque;
use std::time::Instant;

use bytes::Bytes;
use mqlite_core::packet::QoS;

/// A pending outbound QoS 1 or QoS 2 publish awaiting acknowledgment.
#[derive(Debug, Clone)]
pub struct PendingPublish {
    /// Packet identifier
    pub packet_id: u16,
    /// Topic name
    pub topic: Bytes,
    /// Message payload
    pub payload: Bytes,
    /// Quality of service level
    pub qos: QoS,
    /// Retain flag
    pub retain: bool,
    /// When the message was first sent
    pub first_sent: Instant,
    /// When the message was last sent (for retry timing)
    pub last_sent: Instant,
    /// Number of times this message has been sent
    pub send_count: u32,
}

impl PendingPublish {
    /// Create a new pending publish record.
    pub fn new(packet_id: u16, topic: Bytes, payload: Bytes, qos: QoS, retain: bool) -> Self {
        let now = Instant::now();
        Self {
            packet_id,
            topic,
            payload,
            qos,
            retain,
            first_sent: now,
            last_sent: now,
            send_count: 1,
        }
    }

    /// Mark as re-sent.
    pub fn mark_resent(&mut self) {
        self.last_sent = Instant::now();
        self.send_count += 1;
    }
}

/// State of a QoS 2 outbound message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Qos2OutState {
    /// PUBLISH sent, awaiting PUBREC
    AwaitingPubrec,
    /// PUBREC received, PUBREL sent, awaiting PUBCOMP
    AwaitingPubcomp,
}

/// A pending outbound QoS 2 publish with state tracking.
#[derive(Debug, Clone)]
pub struct PendingQos2Out {
    /// The publish details
    pub publish: PendingPublish,
    /// Current state in QoS 2 flow
    pub state: Qos2OutState,
    /// When PUBREL was sent (if in AwaitingPubcomp state)
    pub pubrel_sent: Option<Instant>,
}

/// A pending inbound QoS 2 message (we received PUBLISH, sent PUBREC, awaiting PUBREL).
#[derive(Debug, Clone)]
pub struct PendingQos2In {
    /// Packet identifier
    pub packet_id: u16,
    /// When we sent PUBREC
    pub pubrec_sent: Instant,
}

/// Subscription record for session restoration.
#[derive(Debug, Clone)]
pub struct Subscription {
    /// Topic filter
    pub topic_filter: String,
    /// Maximum QoS
    pub qos: QoS,
}

/// Client session state.
///
/// Tracks all state needed for reliable message delivery and session persistence.
#[derive(Debug)]
pub struct Session {
    /// Client identifier
    pub client_id: String,

    /// Whether this is a clean session
    pub clean_session: bool,

    /// Pending outbound QoS 1 messages (awaiting PUBACK).
    /// Ordered by send time for [MQTT-4.6.0-1] compliance.
    pub pending_qos1: VecDeque<PendingPublish>,

    /// Pending outbound QoS 2 messages.
    /// Ordered by send time for [MQTT-4.6.0-1] compliance.
    pub pending_qos2_out: VecDeque<PendingQos2Out>,

    /// Pending inbound QoS 2 messages (awaiting PUBREL).
    /// We've received PUBLISH and sent PUBREC.
    pub pending_qos2_in: VecDeque<PendingQos2In>,

    /// Current subscriptions (for session restore info).
    pub subscriptions: Vec<Subscription>,
}

impl Session {
    /// Create a new session.
    pub fn new(client_id: String, clean_session: bool) -> Self {
        Self {
            client_id,
            clean_session,
            pending_qos1: VecDeque::new(),
            pending_qos2_out: VecDeque::new(),
            pending_qos2_in: VecDeque::new(),
            subscriptions: Vec::new(),
        }
    }

    /// Clear all pending state (used on clean session connect).
    pub fn clear(&mut self) {
        self.pending_qos1.clear();
        self.pending_qos2_out.clear();
        self.pending_qos2_in.clear();
        self.subscriptions.clear();
    }

    /// Add a QoS 1 pending publish.
    pub fn add_qos1_pending(&mut self, pending: PendingPublish) {
        debug_assert!(pending.qos == QoS::AtLeastOnce);
        self.pending_qos1.push_back(pending);
    }

    /// Remove a QoS 1 pending publish by packet ID (on PUBACK received).
    pub fn complete_qos1(&mut self, packet_id: u16) -> Option<PendingPublish> {
        if let Some(pos) = self
            .pending_qos1
            .iter()
            .position(|p| p.packet_id == packet_id)
        {
            self.pending_qos1.remove(pos)
        } else {
            None
        }
    }

    /// Get a mutable reference to a QoS 1 pending publish.
    pub fn get_qos1_pending_mut(&mut self, packet_id: u16) -> Option<&mut PendingPublish> {
        self.pending_qos1
            .iter_mut()
            .find(|p| p.packet_id == packet_id)
    }

    /// Add a QoS 2 pending publish.
    pub fn add_qos2_pending(&mut self, pending: PendingPublish) {
        debug_assert!(pending.qos == QoS::ExactlyOnce);
        self.pending_qos2_out.push_back(PendingQos2Out {
            publish: pending,
            state: Qos2OutState::AwaitingPubrec,
            pubrel_sent: None,
        });
    }

    /// Transition QoS 2 outbound from AwaitingPubrec to AwaitingPubcomp (on PUBREC received).
    pub fn qos2_received_pubrec(&mut self, packet_id: u16) -> bool {
        if let Some(pending) = self
            .pending_qos2_out
            .iter_mut()
            .find(|p| p.publish.packet_id == packet_id)
        {
            if pending.state == Qos2OutState::AwaitingPubrec {
                pending.state = Qos2OutState::AwaitingPubcomp;
                pending.pubrel_sent = Some(Instant::now());
                return true;
            }
        }
        false
    }

    /// Complete QoS 2 outbound (on PUBCOMP received).
    pub fn complete_qos2_out(&mut self, packet_id: u16) -> Option<PendingQos2Out> {
        if let Some(pos) = self
            .pending_qos2_out
            .iter()
            .position(|p| p.publish.packet_id == packet_id)
        {
            self.pending_qos2_out.remove(pos)
        } else {
            None
        }
    }

    /// Get a QoS 2 outbound pending by packet ID.
    pub fn get_qos2_out(&self, packet_id: u16) -> Option<&PendingQos2Out> {
        self.pending_qos2_out
            .iter()
            .find(|p| p.publish.packet_id == packet_id)
    }

    /// Get a mutable reference to QoS 2 outbound pending.
    pub fn get_qos2_out_mut(&mut self, packet_id: u16) -> Option<&mut PendingQos2Out> {
        self.pending_qos2_out
            .iter_mut()
            .find(|p| p.publish.packet_id == packet_id)
    }

    /// Record that we received a QoS 2 PUBLISH and sent PUBREC.
    pub fn add_qos2_inbound(&mut self, packet_id: u16) {
        // Check if already exists (duplicate PUBLISH)
        if self
            .pending_qos2_in
            .iter()
            .any(|p| p.packet_id == packet_id)
        {
            return;
        }
        self.pending_qos2_in.push_back(PendingQos2In {
            packet_id,
            pubrec_sent: Instant::now(),
        });
    }

    /// Check if we have a pending inbound QoS 2 for this packet ID.
    pub fn has_qos2_inbound(&self, packet_id: u16) -> bool {
        self.pending_qos2_in
            .iter()
            .any(|p| p.packet_id == packet_id)
    }

    /// Complete QoS 2 inbound (on PUBREL received, after we send PUBCOMP).
    pub fn complete_qos2_in(&mut self, packet_id: u16) -> bool {
        if let Some(pos) = self
            .pending_qos2_in
            .iter()
            .position(|p| p.packet_id == packet_id)
        {
            self.pending_qos2_in.remove(pos);
            true
        } else {
            false
        }
    }

    /// Get total count of pending outbound messages.
    pub fn pending_count(&self) -> usize {
        self.pending_qos1.len() + self.pending_qos2_out.len()
    }

    /// Check if there are any pending messages.
    pub fn has_pending(&self) -> bool {
        !self.pending_qos1.is_empty() || !self.pending_qos2_out.is_empty()
    }

    /// Add a subscription record.
    pub fn add_subscription(&mut self, topic_filter: String, qos: QoS) {
        // Replace if exists
        if let Some(sub) = self
            .subscriptions
            .iter_mut()
            .find(|s| s.topic_filter == topic_filter)
        {
            sub.qos = qos;
        } else {
            self.subscriptions.push(Subscription { topic_filter, qos });
        }
    }

    /// Remove a subscription record.
    pub fn remove_subscription(&mut self, topic_filter: &str) {
        self.subscriptions
            .retain(|s| s.topic_filter != topic_filter);
    }

    /// Iterate over all messages that need to be re-sent on reconnect.
    /// Per [MQTT-4.4.0-1], these should be re-sent with their original packet IDs.
    /// Per [MQTT-4.6.0-1], they should be re-sent in original order.
    pub fn messages_to_resend(&self) -> impl Iterator<Item = ResendMessage> + '_ {
        // First all QoS 1 messages
        let qos1 = self.pending_qos1.iter().map(|p| ResendMessage::Publish {
            packet_id: p.packet_id,
            topic: p.topic.clone(),
            payload: p.payload.clone(),
            qos: p.qos,
            retain: p.retain,
            dup: true,
        });

        // Then QoS 2 messages based on their state
        let qos2 = self.pending_qos2_out.iter().map(|p| match p.state {
            Qos2OutState::AwaitingPubrec => ResendMessage::Publish {
                packet_id: p.publish.packet_id,
                topic: p.publish.topic.clone(),
                payload: p.publish.payload.clone(),
                qos: p.publish.qos,
                retain: p.publish.retain,
                dup: true,
            },
            Qos2OutState::AwaitingPubcomp => ResendMessage::Pubrel {
                packet_id: p.publish.packet_id,
            },
        });

        qos1.chain(qos2)
    }
}

/// A message that needs to be re-sent on reconnect.
#[derive(Debug, Clone)]
pub enum ResendMessage {
    /// Re-send a PUBLISH packet (with DUP=1)
    Publish {
        packet_id: u16,
        topic: Bytes,
        payload: Bytes,
        qos: QoS,
        retain: bool,
        dup: bool,
    },
    /// Re-send a PUBREL packet
    Pubrel { packet_id: u16 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qos1_lifecycle() {
        let mut session = Session::new("test".to_string(), true);

        let pending = PendingPublish::new(
            1,
            Bytes::from_static(b"test/topic"),
            Bytes::from_static(b"payload"),
            QoS::AtLeastOnce,
            false,
        );
        session.add_qos1_pending(pending);

        assert_eq!(session.pending_count(), 1);
        assert!(session.get_qos1_pending_mut(1).is_some());

        let completed = session.complete_qos1(1);
        assert!(completed.is_some());
        assert_eq!(session.pending_count(), 0);
    }

    #[test]
    fn test_qos2_lifecycle() {
        let mut session = Session::new("test".to_string(), true);

        let pending = PendingPublish::new(
            1,
            Bytes::from_static(b"test/topic"),
            Bytes::from_static(b"payload"),
            QoS::ExactlyOnce,
            false,
        );
        session.add_qos2_pending(pending);

        assert_eq!(session.pending_count(), 1);
        assert_eq!(
            session.get_qos2_out(1).unwrap().state,
            Qos2OutState::AwaitingPubrec
        );

        // Receive PUBREC
        assert!(session.qos2_received_pubrec(1));
        assert_eq!(
            session.get_qos2_out(1).unwrap().state,
            Qos2OutState::AwaitingPubcomp
        );

        // Receive PUBCOMP
        let completed = session.complete_qos2_out(1);
        assert!(completed.is_some());
        assert_eq!(session.pending_count(), 0);
    }

    #[test]
    fn test_qos2_inbound() {
        let mut session = Session::new("test".to_string(), true);

        session.add_qos2_inbound(100);
        assert!(session.has_qos2_inbound(100));
        assert!(!session.has_qos2_inbound(101));

        // Duplicate should not add again
        session.add_qos2_inbound(100);
        assert_eq!(session.pending_qos2_in.len(), 1);

        assert!(session.complete_qos2_in(100));
        assert!(!session.has_qos2_inbound(100));
    }

    #[test]
    fn test_resend_order() {
        let mut session = Session::new("test".to_string(), false);

        // Add QoS 1 message
        session.add_qos1_pending(PendingPublish::new(
            1,
            Bytes::from_static(b"topic1"),
            Bytes::from_static(b"p1"),
            QoS::AtLeastOnce,
            false,
        ));

        // Add QoS 2 message awaiting PUBREC
        session.add_qos2_pending(PendingPublish::new(
            2,
            Bytes::from_static(b"topic2"),
            Bytes::from_static(b"p2"),
            QoS::ExactlyOnce,
            false,
        ));

        // Add QoS 2 message that received PUBREC (awaiting PUBCOMP)
        session.add_qos2_pending(PendingPublish::new(
            3,
            Bytes::from_static(b"topic3"),
            Bytes::from_static(b"p3"),
            QoS::ExactlyOnce,
            false,
        ));
        session.qos2_received_pubrec(3);

        let resend: Vec<_> = session.messages_to_resend().collect();
        assert_eq!(resend.len(), 3);

        // First should be QoS 1 PUBLISH
        assert!(matches!(&resend[0], ResendMessage::Publish { packet_id: 1, dup: true, .. }));
        // Second should be QoS 2 PUBLISH (awaiting PUBREC)
        assert!(matches!(&resend[1], ResendMessage::Publish { packet_id: 2, dup: true, .. }));
        // Third should be PUBREL (awaiting PUBCOMP)
        assert!(matches!(&resend[2], ResendMessage::Pubrel { packet_id: 3 }));
    }
}
