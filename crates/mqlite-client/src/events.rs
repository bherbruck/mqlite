//! Client events and state types.

use std::time::Duration;

use bytes::Bytes;
use mqlite_core::packet::QoS;

/// Events returned by the client.
#[derive(Debug)]
pub enum ClientEvent {
    /// Connected to broker.
    Connected {
        /// Whether a previous session was restored.
        session_present: bool,
    },
    /// Disconnected from broker.
    Disconnected {
        /// Reason for disconnection, if known.
        reason: Option<String>,
    },
    /// Attempting to reconnect (only when auto_reconnect is enabled).
    Reconnecting {
        /// Current reconnection attempt number (1-based).
        attempt: u32,
        /// Delay before this attempt.
        delay: Duration,
    },
    /// Received a publish message.
    Message {
        /// Topic the message was published to.
        topic: Bytes,
        /// Message payload.
        payload: Bytes,
        /// Quality of Service level.
        qos: QoS,
        /// Whether this is a retained message.
        retain: bool,
        /// Packet ID (for QoS 1/2).
        packet_id: Option<u16>,
    },
    /// Subscribe acknowledgment.
    SubAck {
        /// Packet ID of the SUBSCRIBE.
        packet_id: u16,
        /// Return codes for each topic (0x00-0x02 = granted QoS, 0x80 = failure).
        return_codes: Vec<u8>,
    },
    /// Unsubscribe acknowledgment.
    UnsubAck {
        /// Packet ID of the UNSUBSCRIBE.
        packet_id: u16,
    },
    /// Publish acknowledgment (QoS 1).
    PubAck {
        /// Packet ID of the acknowledged PUBLISH.
        packet_id: u16,
    },
    /// Publish received (QoS 2 step 1).
    PubRec {
        /// Packet ID.
        packet_id: u16,
    },
    /// Publish complete (QoS 2 step 3).
    PubComp {
        /// Packet ID.
        packet_id: u16,
    },
}

/// Connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    /// Waiting for backoff before reconnecting.
    Reconnecting,
}
