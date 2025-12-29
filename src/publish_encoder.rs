//! Zero-copy publish packet factory.
//!
//! Caches encoded packets by QoS level to avoid re-encoding for each subscriber.

use bytes::Bytes;

use crate::packet::{encode_remaining_length, PacketType, QoS};

/// Pre-encoded publish packet data for efficient fan-out.
///
/// For QoS 0: The entire packet is cached and can be written directly.
/// For QoS 1/2: The header (up to packet_id) is cached, then packet_id
/// and payload are appended per-subscriber.
pub struct PublishEncoder {
    /// Original topic (shared reference).
    topic: Bytes,
    /// Original payload (shared reference).
    payload: Bytes,
    /// Cached QoS 0 packet (complete, ready to send).
    cache_qos0: Option<Bytes>,
}

impl PublishEncoder {
    /// Create a new factory from topic and payload.
    /// The topic and payload should be Bytes for zero-copy sharing.
    #[inline]
    pub fn new(topic: Bytes, payload: Bytes) -> Self {
        Self {
            topic,
            payload,
            cache_qos0: None,
        }
    }

    /// Write a publish packet for a subscriber with the given effective QoS and packet_id.
    ///
    /// This method minimizes allocations by:
    /// - Caching the encoded packet for QoS 0 (all subscribers get same bytes)
    /// - Caching the header for QoS 1/2 (only packet_id differs per subscriber)
    /// - Using Bytes for zero-copy payload sharing
    #[inline]
    pub fn write_to<W: std::io::Write>(
        &mut self,
        writer: &mut W,
        effective_qos: QoS,
        packet_id: Option<u16>,
        retain: bool,
    ) -> std::io::Result<()> {
        match effective_qos {
            QoS::AtMostOnce => {
                // QoS 0: Use cached complete packet (single write)
                let packet = self.get_or_build_qos0(retain);
                writer.write_all(&packet)?;
            }
            QoS::AtLeastOnce | QoS::ExactlyOnce => {
                // QoS 1/2: Build complete packet and write once
                // This reduces 3 write_all calls to 1, minimizing buffer operations
                self.write_qos12_packet(writer, effective_qos, packet_id.unwrap(), retain)?;
            }
        }
        Ok(())
    }

    /// Write QoS 1/2 packet with single write operation.
    #[inline]
    fn write_qos12_packet<W: std::io::Write>(
        &mut self,
        writer: &mut W,
        qos: QoS,
        packet_id: u16,
        retain: bool,
    ) -> std::io::Result<()> {
        // Build complete packet on stack to minimize allocations
        let topic_len = self.topic.len();
        let payload_len = self.payload.len();
        let remaining = 2 + topic_len + 2 + payload_len; // topic_len_bytes + topic + packet_id + payload

        // Calculate total size: fixed_header(1) + remaining_length(1-4) + remaining
        let remaining_len_bytes = if remaining < 128 { 1 } else if remaining < 16384 { 2 } else if remaining < 2097152 { 3 } else { 4 };
        let total_size = 1 + remaining_len_bytes + remaining;

        // For small packets (most common), use stack buffer
        if total_size <= 256 {
            let mut buf = [0u8; 256];
            let written = self.encode_qos12_to_slice(&mut buf, qos, packet_id, retain, remaining);
            writer.write_all(&buf[..written])
        } else {
            // Large packets: allocate
            let mut buf = vec![0u8; total_size];
            let written = self.encode_qos12_to_slice(&mut buf, qos, packet_id, retain, remaining);
            writer.write_all(&buf[..written])
        }
    }

    /// Encode QoS 1/2 packet into provided buffer. Returns bytes written.
    #[inline]
    fn encode_qos12_to_slice(
        &self,
        buf: &mut [u8],
        qos: QoS,
        packet_id: u16,
        retain: bool,
        remaining: usize,
    ) -> usize {
        let mut pos = 0;

        // Fixed header
        let mut fixed_header = (PacketType::Publish as u8) << 4;
        fixed_header |= (qos as u8) << 1;
        if retain {
            fixed_header |= 0x01;
        }
        buf[pos] = fixed_header;
        pos += 1;

        // Remaining length (variable-length encoding)
        let mut len_buf = [0u8; 4];
        let len_bytes = encode_remaining_length(remaining, &mut len_buf);
        buf[pos..pos + len_bytes].copy_from_slice(&len_buf[..len_bytes]);
        pos += len_bytes;

        // Topic length + topic
        let topic_len = self.topic.len() as u16;
        buf[pos..pos + 2].copy_from_slice(&topic_len.to_be_bytes());
        pos += 2;
        buf[pos..pos + self.topic.len()].copy_from_slice(&self.topic);
        pos += self.topic.len();

        // Packet ID
        buf[pos..pos + 2].copy_from_slice(&packet_id.to_be_bytes());
        pos += 2;

        // Payload
        buf[pos..pos + self.payload.len()].copy_from_slice(&self.payload);
        pos += self.payload.len();

        pos
    }

    /// Get or build the complete QoS 0 packet.
    fn get_or_build_qos0(&mut self, retain: bool) -> Bytes {
        // For QoS 0, we can cache the entire packet if retain matches
        // (retain flag is in the fixed header, so different retain = different packet)
        if let Some(ref cached) = self.cache_qos0 {
            // Check if retain matches (it's bit 0 of byte 0)
            if (cached[0] & 0x01 != 0) == retain {
                return cached.clone();
            }
        }

        // Build the complete packet
        let topic_len = self.topic.len();
        let remaining = 2 + topic_len + self.payload.len();

        let mut buf = Vec::with_capacity(1 + 4 + remaining); // fixed + max_remaining_len + payload

        // Fixed header
        let mut fixed_header = (PacketType::Publish as u8) << 4;
        if retain {
            fixed_header |= 0x01;
        }
        // QoS 0 = bits 1-2 are 0, no DUP flag for QoS 0
        buf.push(fixed_header);

        // Remaining length
        let mut len_buf = [0u8; 4];
        let len_bytes = encode_remaining_length(remaining, &mut len_buf);
        buf.extend_from_slice(&len_buf[..len_bytes]);

        // Topic
        buf.extend_from_slice(&(topic_len as u16).to_be_bytes());
        buf.extend_from_slice(&self.topic);

        // Payload
        buf.extend_from_slice(&self.payload);

        let packet = Bytes::from(buf);
        self.cache_qos0 = Some(packet.clone());
        packet
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qos0_caching() {
        let topic = Bytes::from_static(b"test/topic");
        let payload = Bytes::from_static(b"hello");

        let mut factory = PublishEncoder::new(topic, payload);

        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();

        factory.write_to(&mut buf1, QoS::AtMostOnce, None, false).unwrap();
        factory.write_to(&mut buf2, QoS::AtMostOnce, None, false).unwrap();

        // Should be identical
        assert_eq!(buf1, buf2);
        assert!(!buf1.is_empty());
    }

    #[test]
    fn test_qos1_different_packet_ids() {
        let topic = Bytes::from_static(b"test/topic");
        let payload = Bytes::from_static(b"hello");

        let mut factory = PublishEncoder::new(topic, payload);

        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();

        factory.write_to(&mut buf1, QoS::AtLeastOnce, Some(1), false).unwrap();
        factory.write_to(&mut buf2, QoS::AtLeastOnce, Some(2), false).unwrap();

        // Should differ only in packet_id bytes
        assert_ne!(buf1, buf2);
        // But should have same length
        assert_eq!(buf1.len(), buf2.len());
    }
}
