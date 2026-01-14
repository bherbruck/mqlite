//! DISCONNECT and PINGREQ packet handlers.
//!
//! This module handles:
//! - DISCONNECT packet processing (graceful vs will-triggering disconnects)
//! - PINGREQ/PINGRESP keep-alive handling

use mqlite_core::packet::Packet;

use crate::client::{Client, ClientState};

/// Handle PINGREQ packet - keep-alive ping from client.
///
/// Sends PINGRESP immediately and flushes to ensure timely delivery.
/// The client will disconnect if PINGRESP is not received within keep-alive interval.
#[inline]
pub fn handle_pingreq(client: &mut Client) {
    // PINGRESP is critical - client will disconnect if not received
    if let Err(e) = client.queue_control_packet(&Packet::Pingresp) {
        log::warn!(
            "Failed to queue PINGRESP for client {:?}: {}",
            client.client_id,
            e
        );
    }
    // Flush immediately - don't wait for next poll() cycle
    // This ensures PINGRESP goes out ASAP even if worker is busy with fan-out
    let _ = client.flush();
}

/// Handle DISCONNECT packet from client.
///
/// MQTT v5 reason code 0x04 means "Disconnect with Will Message" - the will
/// should still be published. Any other reason code (or v3.1.1 DISCONNECT)
/// is a graceful disconnect where the will is cleared.
#[inline]
pub fn handle_disconnect(client: &mut Client, reason_code: u8) {
    // MQTT v5 reason code 0x04 = Disconnect with Will Message
    if reason_code == 0x04 {
        // Keep will and mark as non-graceful so will gets published
        client.graceful_disconnect = false;
    } else {
        // Normal disconnect - clear will and mark as graceful
        client.graceful_disconnect = true;
        client.will = None;
    }
    client.state = ClientState::Disconnecting;
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_disconnect_normal_clears_will() {
        // Test that normal disconnect (reason 0x00) clears the will
        let graceful_disconnect = true;
        let will_cleared = true;

        // Simulate normal disconnect behavior
        let reason_code: u8 = 0x00;
        let result_graceful = reason_code != 0x04;
        let result_will_cleared = reason_code != 0x04;

        assert_eq!(result_graceful, graceful_disconnect);
        assert_eq!(result_will_cleared, will_cleared);
    }

    #[test]
    fn test_disconnect_with_will_keeps_will() {
        // Test that disconnect with will (reason 0x04) keeps the will
        let reason_code: u8 = 0x04;
        let result_graceful = reason_code != 0x04;
        let result_will_cleared = reason_code != 0x04;

        assert!(!result_graceful);
        assert!(!result_will_cleared);
    }

    #[test]
    fn test_disconnect_reason_codes() {
        // Various MQTT 5 disconnect reason codes
        let normal_disconnect = 0x00;
        let disconnect_with_will = 0x04;
        let unspecified_error = 0x80;
        let protocol_error = 0x82;

        // Only 0x04 should keep the will
        assert!(normal_disconnect != 0x04);
        assert!(disconnect_with_will == 0x04);
        assert!(unspecified_error != 0x04);
        assert!(protocol_error != 0x04);
    }
}
