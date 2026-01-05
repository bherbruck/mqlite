//! Zero-copy publish packet factory.
//!
//! Caches encoded packets by QoS level to avoid re-encoding for each subscriber.

use bytes::Bytes;

use mqlite_core::packet::{encode_remaining_length, PacketType, QoS};

/// Calculate the encoded length of a variable byte integer.
#[inline]
fn varint_len(value: u32) -> usize {
    if value < 128 {
        1
    } else if value < 16384 {
        2
    } else if value < 2097152 {
        3
    } else {
        4
    }
}

/// Encode a variable byte integer into a buffer. Returns bytes written.
#[inline]
fn encode_varint(mut value: u32, buf: &mut [u8]) -> usize {
    let mut pos = 0;
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value > 0 {
            byte |= 0x80;
        }
        buf[pos] = byte;
        pos += 1;
        if value == 0 {
            break;
        }
    }
    pos
}

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
    /// MQTT v5 properties to forward (raw bytes, without length prefix).
    properties: Option<Bytes>,
    /// Cached QoS 0 packet for MQTT 3.1.1 (complete, ready to send).
    cache_qos0: Option<Bytes>,
    /// Cached QoS 0 packet for MQTT 5.0 (includes properties).
    cache_qos0_v5: Option<Bytes>,
}

impl PublishEncoder {
    /// Create a new factory from topic, payload, and optional v5 properties.
    /// The topic, payload, and properties should be Bytes for zero-copy sharing.
    #[inline]
    pub fn new(topic: Bytes, payload: Bytes, properties: Option<Bytes>) -> Self {
        Self {
            topic,
            payload,
            properties,
            cache_qos0: None,
            cache_qos0_v5: None,
        }
    }

    /// Write a publish packet for a subscriber with the given effective QoS and packet_id.
    ///
    /// This method minimizes allocations by:
    /// - Caching the encoded packet for QoS 0 (all subscribers get same bytes)
    /// - Caching the header for QoS 1/2 (only packet_id differs per subscriber)
    /// - Using Bytes for zero-copy payload sharing
    #[inline]
    #[allow(dead_code)]
    pub fn write_to<W: std::io::Write>(
        &mut self,
        writer: &mut W,
        effective_qos: QoS,
        packet_id: Option<u16>,
        retain: bool,
        is_v5: bool,
    ) -> std::io::Result<()> {
        self.write_to_with_sub_id(writer, effective_qos, packet_id, retain, is_v5, None)
    }

    /// Write a publish packet with optional subscription identifier.
    /// Used for MQTT v5 when subscriber has a subscription_id.
    #[inline]
    pub fn write_to_with_sub_id<W: std::io::Write>(
        &mut self,
        writer: &mut W,
        effective_qos: QoS,
        packet_id: Option<u16>,
        retain: bool,
        is_v5: bool,
        subscription_id: Option<u32>,
    ) -> std::io::Result<()> {
        match effective_qos {
            QoS::AtMostOnce => {
                if subscription_id.is_some() && is_v5 {
                    // Can't use cache when adding subscription ID
                    self.write_qos0_with_sub_id(writer, retain, subscription_id)?;
                } else {
                    // QoS 0: Use cached complete packet (single write)
                    let packet = self.get_or_build_qos0(retain, is_v5);
                    writer.write_all(&packet)?;
                }
            }
            QoS::AtLeastOnce | QoS::ExactlyOnce => {
                // QoS 1/2: Build complete packet and write once
                // This reduces 3 write_all calls to 1, minimizing buffer operations
                self.write_qos12_packet(
                    writer,
                    effective_qos,
                    packet_id.unwrap(),
                    retain,
                    is_v5,
                    subscription_id,
                )?;
            }
        }
        Ok(())
    }

    /// Write QoS 0 packet with subscription identifier (can't use cache).
    #[inline]
    fn write_qos0_with_sub_id<W: std::io::Write>(
        &self,
        writer: &mut W,
        retain: bool,
        subscription_id: Option<u32>,
    ) -> std::io::Result<()> {
        let topic_len = self.topic.len();

        // Calculate subscription ID size
        let sub_id_len = subscription_id.map(|id| 1 + varint_len(id)).unwrap_or(0);
        let existing_props_len = self.properties.as_ref().map(|p| p.len()).unwrap_or(0);
        let props_content_len = existing_props_len + sub_id_len;
        let props_header_len = if props_content_len < 128 {
            1
        } else if props_content_len < 16384 {
            2
        } else {
            3
        };

        let remaining = 2 + topic_len + props_header_len + props_content_len + self.payload.len();

        let mut buf = Vec::with_capacity(1 + 4 + remaining);

        // Fixed header
        let mut fixed_header = (PacketType::Publish as u8) << 4;
        if retain {
            fixed_header |= 0x01;
        }
        buf.push(fixed_header);

        // Remaining length
        let mut len_buf = [0u8; 4];
        let len_bytes = encode_remaining_length(remaining, &mut len_buf);
        buf.extend_from_slice(&len_buf[..len_bytes]);

        // Topic
        buf.extend_from_slice(&(topic_len as u16).to_be_bytes());
        buf.extend_from_slice(&self.topic);

        // Properties length
        let mut props_len_buf = [0u8; 4];
        let props_len_bytes = encode_remaining_length(props_content_len, &mut props_len_buf);
        buf.extend_from_slice(&props_len_buf[..props_len_bytes]);

        // Subscription identifier property (if present)
        if let Some(id) = subscription_id {
            buf.push(0x0B); // Property ID for Subscription Identifier
            let mut varint_buf = [0u8; 4];
            let varint_bytes = encode_varint(id, &mut varint_buf);
            buf.extend_from_slice(&varint_buf[..varint_bytes]);
        }

        // Existing properties
        if let Some(ref props) = self.properties {
            buf.extend_from_slice(props);
        }

        // Payload
        buf.extend_from_slice(&self.payload);

        writer.write_all(&buf)
    }

    /// Write QoS 1/2 packet with single write operation.
    #[inline]
    fn write_qos12_packet<W: std::io::Write>(
        &mut self,
        writer: &mut W,
        qos: QoS,
        packet_id: u16,
        retain: bool,
        is_v5: bool,
        subscription_id: Option<u32>,
    ) -> std::io::Result<()> {
        // Build complete packet on stack to minimize allocations
        let topic_len = self.topic.len();
        let payload_len = self.payload.len();

        // Calculate subscription ID encoding length (property ID 0x0B + variable byte integer)
        let sub_id_len = if is_v5 {
            subscription_id.map(|id| 1 + varint_len(id)).unwrap_or(0)
        } else {
            0
        };

        // For v5: calculate properties section size
        let (props_content_len, props_header_len) = if is_v5 {
            let content_len = self.properties.as_ref().map(|p| p.len()).unwrap_or(0) + sub_id_len;
            // Variable byte integer length for properties length
            let header_len = if content_len < 128 {
                1
            } else if content_len < 16384 {
                2
            } else {
                3
            };
            (content_len, header_len)
        } else {
            (0, 0)
        };

        let remaining = 2 + topic_len + 2 + props_header_len + props_content_len + payload_len;

        // Calculate total size: fixed_header(1) + remaining_length(1-4) + remaining
        let remaining_len_bytes = if remaining < 128 {
            1
        } else if remaining < 16384 {
            2
        } else if remaining < 2097152 {
            3
        } else {
            4
        };
        let total_size = 1 + remaining_len_bytes + remaining;

        // For small packets (most common), use stack buffer
        if total_size <= 256 {
            let mut buf = [0u8; 256];
            let written = self.encode_qos12_to_slice(
                &mut buf,
                qos,
                packet_id,
                retain,
                remaining,
                is_v5,
                subscription_id,
            );
            writer.write_all(&buf[..written])
        } else {
            // Large packets: allocate
            let mut buf = vec![0u8; total_size];
            let written = self.encode_qos12_to_slice(
                &mut buf,
                qos,
                packet_id,
                retain,
                remaining,
                is_v5,
                subscription_id,
            );
            writer.write_all(&buf[..written])
        }
    }

    /// Encode QoS 1/2 packet into provided buffer. Returns bytes written.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn encode_qos12_to_slice(
        &self,
        buf: &mut [u8],
        qos: QoS,
        packet_id: u16,
        retain: bool,
        remaining: usize,
        is_v5: bool,
        subscription_id: Option<u32>,
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

        // MQTT v5: properties
        if is_v5 {
            // Calculate subscription ID size
            let sub_id_len = subscription_id.map(|id| 1 + varint_len(id)).unwrap_or(0);
            let existing_props_len = self.properties.as_ref().map(|p| p.len()).unwrap_or(0);
            let props_len = existing_props_len + sub_id_len;

            // Write properties length as variable byte integer
            let mut len_buf = [0u8; 4];
            let len_bytes = encode_remaining_length(props_len, &mut len_buf);
            buf[pos..pos + len_bytes].copy_from_slice(&len_buf[..len_bytes]);
            pos += len_bytes;

            // Write subscription identifier property first (if present)
            if let Some(id) = subscription_id {
                buf[pos] = 0x0B; // Property ID for Subscription Identifier
                pos += 1;
                pos += encode_varint(id, &mut buf[pos..]);
            }

            // Write existing properties content
            if let Some(ref props) = self.properties {
                buf[pos..pos + props.len()].copy_from_slice(props);
                pos += props.len();
            }
        }

        // Payload
        buf[pos..pos + self.payload.len()].copy_from_slice(&self.payload);
        pos += self.payload.len();

        pos
    }

    /// Get or build the complete QoS 0 packet.
    fn get_or_build_qos0(&mut self, retain: bool, is_v5: bool) -> Bytes {
        // Only use cache if no properties (properties vary per message)
        let can_cache = self.properties.is_none();

        // Check appropriate cache
        if can_cache {
            let cache = if is_v5 {
                &self.cache_qos0_v5
            } else {
                &self.cache_qos0
            };
            if let Some(ref cached) = cache {
                // Check if retain matches (it's bit 0 of byte 0)
                if (cached[0] & 0x01 != 0) == retain {
                    return cached.clone();
                }
            }
        }

        // Build the complete packet
        let topic_len = self.topic.len();

        // For v5: calculate properties section size
        let (props_content_len, props_header_len) = if is_v5 {
            let content_len = self.properties.as_ref().map(|p| p.len()).unwrap_or(0);
            let header_len = if content_len < 128 {
                1
            } else if content_len < 16384 {
                2
            } else {
                3
            };
            (content_len, header_len)
        } else {
            (0, 0)
        };

        let remaining = 2 + topic_len + props_header_len + props_content_len + self.payload.len();

        let mut buf = Vec::with_capacity(1 + 4 + remaining);

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

        // MQTT v5: properties
        if is_v5 {
            let props_len = self.properties.as_ref().map(|p| p.len()).unwrap_or(0);
            let mut len_buf = [0u8; 4];
            let len_bytes = encode_remaining_length(props_len, &mut len_buf);
            buf.extend_from_slice(&len_buf[..len_bytes]);
            if let Some(ref props) = self.properties {
                buf.extend_from_slice(props);
            }
        }

        // Payload
        buf.extend_from_slice(&self.payload);

        let packet = Bytes::from(buf);
        if can_cache {
            if is_v5 {
                self.cache_qos0_v5 = Some(packet.clone());
            } else {
                self.cache_qos0 = Some(packet.clone());
            }
        }
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

        let mut factory = PublishEncoder::new(topic, payload, None);

        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();

        factory
            .write_to(&mut buf1, QoS::AtMostOnce, None, false, false)
            .unwrap();
        factory
            .write_to(&mut buf2, QoS::AtMostOnce, None, false, false)
            .unwrap();

        // Should be identical
        assert_eq!(buf1, buf2);
        assert!(!buf1.is_empty());
    }

    #[test]
    fn test_qos1_different_packet_ids() {
        let topic = Bytes::from_static(b"test/topic");
        let payload = Bytes::from_static(b"hello");

        let mut factory = PublishEncoder::new(topic, payload, None);

        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();

        factory
            .write_to(&mut buf1, QoS::AtLeastOnce, Some(1), false, false)
            .unwrap();
        factory
            .write_to(&mut buf2, QoS::AtLeastOnce, Some(2), false, false)
            .unwrap();

        // Should differ only in packet_id bytes
        assert_ne!(buf1, buf2);
        // But should have same length
        assert_eq!(buf1.len(), buf2.len());
    }

    #[test]
    fn test_v5_includes_properties() {
        let topic = Bytes::from_static(b"test/topic");
        let payload = Bytes::from_static(b"hello");

        let mut factory = PublishEncoder::new(topic, payload, None);

        let mut buf_v3 = Vec::new();
        let mut buf_v5 = Vec::new();

        factory
            .write_to(&mut buf_v3, QoS::AtMostOnce, None, false, false)
            .unwrap();
        factory
            .write_to(&mut buf_v5, QoS::AtMostOnce, None, false, true)
            .unwrap();

        // v5 should be 1 byte longer (empty properties = 0x00)
        assert_eq!(buf_v5.len(), buf_v3.len() + 1);
    }
}
