//! MQTT packet types and codec for MQTT 3.1.1.

use bytes::Bytes;

use crate::error::{ProtocolError, Result};

/// MQTT Control Packet Types (4 bits).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
}

impl TryFrom<u8> for PacketType {
    type Error = ProtocolError;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(PacketType::Connect),
            2 => Ok(PacketType::Connack),
            3 => Ok(PacketType::Publish),
            4 => Ok(PacketType::Puback),
            5 => Ok(PacketType::Pubrec),
            6 => Ok(PacketType::Pubrel),
            7 => Ok(PacketType::Pubcomp),
            8 => Ok(PacketType::Subscribe),
            9 => Ok(PacketType::Suback),
            10 => Ok(PacketType::Unsubscribe),
            11 => Ok(PacketType::Unsuback),
            12 => Ok(PacketType::Pingreq),
            13 => Ok(PacketType::Pingresp),
            14 => Ok(PacketType::Disconnect),
            _ => Err(ProtocolError::InvalidPacketType(value)),
        }
    }
}

/// Quality of Service levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
#[allow(clippy::enum_variant_names)] // MQTT spec names
pub enum QoS {
    #[default]
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QoS {
    type Error = ProtocolError;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            _ => Err(ProtocolError::MalformedPacket(format!(
                "Invalid QoS: {}",
                value
            ))),
        }
    }
}

/// CONNACK return codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[allow(dead_code)] // MQTT spec requires all variants
pub enum ConnackCode {
    Accepted = 0,
    UnacceptableProtocolVersion = 1,
    IdentifierRejected = 2,
    ServerUnavailable = 3,
    BadUsernamePassword = 4,
    NotAuthorized = 5,
}

/// MQTT Packets.
#[derive(Debug, Clone)]
pub enum Packet {
    Connect(Connect),
    Connack(Connack),
    Publish(Publish),
    Puback { packet_id: u16 },
    Pubrec { packet_id: u16 },
    Pubrel { packet_id: u16 },
    Pubcomp { packet_id: u16 },
    Subscribe(Subscribe),
    Suback(Suback),
    Unsubscribe(Unsubscribe),
    Unsuback { packet_id: u16 },
    Pingreq,
    Pingresp,
    Disconnect,
}

/// CONNECT packet data.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields required by MQTT spec
pub struct Connect {
    pub protocol_name: String,
    pub protocol_version: u8,
    pub clean_session: bool,
    pub keep_alive: u16,
    pub client_id: String,
    pub will: Option<Will>,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
}

/// Will message configuration.
#[derive(Debug, Clone)]
pub struct Will {
    pub topic: String,
    pub message: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

/// CONNACK packet data.
#[derive(Debug, Clone)]
pub struct Connack {
    pub session_present: bool,
    pub code: ConnackCode,
}

/// PUBLISH packet data.
#[derive(Debug, Clone)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: Bytes,
    pub packet_id: Option<u16>,
    pub payload: Bytes,
}

/// SUBSCRIBE packet data.
#[derive(Debug, Clone)]
pub struct Subscribe {
    pub packet_id: u16,
    pub topics: Vec<(String, QoS)>,
}

/// SUBACK packet data.
#[derive(Debug, Clone)]
pub struct Suback {
    pub packet_id: u16,
    pub return_codes: Vec<u8>,
}

/// UNSUBSCRIBE packet data.
#[derive(Debug, Clone)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub topics: Vec<String>,
}

/// Decoder for MQTT packets.
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn read_u8(&mut self) -> Result<u8> {
        if self.pos >= self.buf.len() {
            return Err(ProtocolError::IncompletePacket {
                needed: 1,
                have: 0,
            }
            .into());
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_u16(&mut self) -> Result<u16> {
        if self.remaining() < 2 {
            return Err(ProtocolError::IncompletePacket {
                needed: 2,
                have: self.remaining(),
            }
            .into());
        }
        let val = u16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(val)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.remaining() < len {
            return Err(ProtocolError::IncompletePacket {
                needed: len,
                have: self.remaining(),
            }
            .into());
        }
        let bytes = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(bytes)
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u16()? as usize;
        let bytes = self.read_bytes(len)?;
        // MQTT-1.5.3-2: UTF-8 string MUST NOT contain null character U+0000
        if bytes.contains(&0u8) {
            return Err(ProtocolError::MalformedPacket(
                "UTF-8 string must not contain null character".into(),
            )
            .into());
        }
        String::from_utf8(bytes.to_vec()).map_err(|_| ProtocolError::InvalidUtf8.into())
    }

    fn read_binary(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u16()? as usize;
        let bytes = self.read_bytes(len)?;
        Ok(bytes.to_vec())
    }
}

/// Decode the remaining length field (variable length encoding).
/// Returns (length, bytes_consumed) or None if incomplete.
pub fn decode_remaining_length(buf: &[u8]) -> Result<Option<(usize, usize)>> {
    let mut multiplier = 1usize;
    let mut value = 0usize;

    for (i, &byte) in buf.iter().enumerate() {
        value += ((byte & 0x7F) as usize) * multiplier;

        if multiplier > 128 * 128 * 128 {
            return Err(ProtocolError::InvalidRemainingLength.into());
        }

        if (byte & 0x80) == 0 {
            return Ok(Some((value, i + 1)));
        }

        multiplier *= 128;
    }

    // Need more bytes
    Ok(None)
}

/// Encode remaining length into buffer. Returns bytes written.
pub fn encode_remaining_length(mut len: usize, buf: &mut [u8]) -> usize {
    let mut i = 0;
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        buf[i] = byte;
        i += 1;
        if len == 0 {
            break;
        }
    }
    i
}

/// Try to decode a complete packet from the buffer.
/// Returns Ok(Some((packet, bytes_consumed))) if successful,
/// Ok(None) if more data is needed, or Err on protocol errors.
pub fn decode_packet(buf: &[u8]) -> Result<Option<(Packet, usize)>> {
    if buf.is_empty() {
        return Ok(None);
    }

    let fixed_header = buf[0];
    let packet_type_raw = fixed_header >> 4;
    let flags = fixed_header & 0x0F;

    // Decode remaining length
    let Some((remaining_len, len_bytes)) = decode_remaining_length(&buf[1..])? else {
        return Ok(None);
    };

    let header_len = 1 + len_bytes;
    let total_len = header_len + remaining_len;

    if buf.len() < total_len {
        return Ok(None);
    }

    let packet_type = PacketType::try_from(packet_type_raw)?;
    let payload = &buf[header_len..total_len];

    // Validate fixed header flags for specific packet types
    // MQTT-3.8.1-1: SUBSCRIBE fixed header flags MUST be 0010
    // MQTT-3.10.1-1: UNSUBSCRIBE fixed header flags MUST be 0010
    // MQTT-3.6.1-1: PUBREL fixed header flags MUST be 0010
    match packet_type {
        PacketType::Subscribe | PacketType::Unsubscribe | PacketType::Pubrel => {
            if flags != 0x02 {
                return Err(ProtocolError::MalformedPacket(format!(
                    "{:?} fixed header flags must be 0x02, got {:#04x}",
                    packet_type, flags
                ))
                .into());
            }
        }
        _ => {}
    }

    let packet = match packet_type {
        PacketType::Connect => decode_connect(payload)?,
        PacketType::Publish => decode_publish(flags, payload)?,
        PacketType::Puback => decode_puback(payload)?,
        PacketType::Pubrec => decode_pubrec(payload)?,
        PacketType::Pubrel => decode_pubrel(payload)?,
        PacketType::Pubcomp => decode_pubcomp(payload)?,
        PacketType::Subscribe => decode_subscribe(payload)?,
        PacketType::Unsubscribe => decode_unsubscribe(payload)?,
        PacketType::Pingreq => Packet::Pingreq,
        PacketType::Disconnect => Packet::Disconnect,
        _ => {
            return Err(ProtocolError::MalformedPacket(format!(
                "Unexpected packet type from client: {:?}",
                packet_type
            ))
            .into())
        }
    };

    Ok(Some((packet, total_len)))
}

fn decode_connect(payload: &[u8]) -> Result<Packet> {
    let mut dec = Decoder::new(payload);

    // Protocol name
    let protocol_name = dec.read_string()?;
    if protocol_name != "MQTT" && protocol_name != "MQIsdp" {
        return Err(ProtocolError::InvalidProtocolName(protocol_name).into());
    }

    // Protocol version
    let protocol_version = dec.read_u8()?;
    if protocol_version != 4 && protocol_version != 3 {
        // 4 = MQTT 3.1.1, 3 = MQTT 3.1
        return Err(ProtocolError::UnsupportedProtocolVersion(protocol_version).into());
    }

    // Connect flags
    let flags = dec.read_u8()?;
    let clean_session = (flags & 0x02) != 0;
    let will_flag = (flags & 0x04) != 0;
    let will_qos = QoS::try_from((flags >> 3) & 0x03)?;
    let will_retain = (flags & 0x20) != 0;
    let password_flag = (flags & 0x40) != 0;
    let username_flag = (flags & 0x80) != 0;

    // Reserved bit must be 0
    if (flags & 0x01) != 0 {
        return Err(ProtocolError::InvalidConnectFlags(flags).into());
    }

    // MQTT-3.1.2-11/13: If Will Flag is 0, Will QoS MUST be 0
    if !will_flag && will_qos != QoS::AtMostOnce {
        return Err(ProtocolError::MalformedPacket(
            "Will QoS must be 0 when Will Flag is 0".into(),
        )
        .into());
    }

    // MQTT-3.1.2-15: If Will Flag is 0, Will Retain MUST be 0
    if !will_flag && will_retain {
        return Err(ProtocolError::MalformedPacket(
            "Will Retain must be 0 when Will Flag is 0".into(),
        )
        .into());
    }

    // MQTT-3.1.2-22: If Username Flag is 0, Password Flag MUST be 0
    if !username_flag && password_flag {
        return Err(ProtocolError::MalformedPacket(
            "Password Flag must be 0 when Username Flag is 0".into(),
        )
        .into());
    }

    // Keep alive
    let keep_alive = dec.read_u16()?;

    // Client ID
    let client_id = dec.read_string()?;

    // Will
    let will = if will_flag {
        let topic = dec.read_string()?;
        let message = dec.read_binary()?;
        Some(Will {
            topic,
            message,
            qos: will_qos,
            retain: will_retain,
        })
    } else {
        None
    };

    // Username
    let username = if username_flag {
        Some(dec.read_string()?)
    } else {
        None
    };

    // Password
    let password = if password_flag {
        Some(dec.read_binary()?)
    } else {
        None
    };

    Ok(Packet::Connect(Connect {
        protocol_name,
        protocol_version,
        clean_session,
        keep_alive,
        client_id,
        will,
        username,
        password,
    }))
}

fn decode_publish(flags: u8, payload: &[u8]) -> Result<Packet> {
    let dup = (flags & 0x08) != 0;
    let qos = QoS::try_from((flags >> 1) & 0x03)?;
    let retain = (flags & 0x01) != 0;

    let mut dec = Decoder::new(payload);

    let topic = dec.read_string()?;

    let packet_id = if qos != QoS::AtMostOnce {
        Some(dec.read_u16()?)
    } else {
        None
    };

    let payload_data = dec.read_bytes(dec.remaining())?;

    Ok(Packet::Publish(Publish {
        dup,
        qos,
        retain,
        topic: Bytes::copy_from_slice(topic.as_bytes()),
        packet_id,
        payload: Bytes::copy_from_slice(payload_data),
    }))
}

fn decode_puback(payload: &[u8]) -> Result<Packet> {
    let mut dec = Decoder::new(payload);
    let packet_id = dec.read_u16()?;
    Ok(Packet::Puback { packet_id })
}

fn decode_pubrec(payload: &[u8]) -> Result<Packet> {
    let mut dec = Decoder::new(payload);
    let packet_id = dec.read_u16()?;
    Ok(Packet::Pubrec { packet_id })
}

fn decode_pubrel(payload: &[u8]) -> Result<Packet> {
    let mut dec = Decoder::new(payload);
    let packet_id = dec.read_u16()?;
    Ok(Packet::Pubrel { packet_id })
}

fn decode_pubcomp(payload: &[u8]) -> Result<Packet> {
    let mut dec = Decoder::new(payload);
    let packet_id = dec.read_u16()?;
    Ok(Packet::Pubcomp { packet_id })
}

fn decode_subscribe(payload: &[u8]) -> Result<Packet> {
    let mut dec = Decoder::new(payload);
    let packet_id = dec.read_u16()?;

    let mut topics = Vec::new();
    while dec.remaining() > 0 {
        let topic = dec.read_string()?;

        // MQTT-4.7.0-1: Topic Filter must be at least 1 character
        if topic.is_empty() {
            return Err(
                ProtocolError::MalformedPacket("Topic filter must be at least 1 character".into())
                    .into(),
            );
        }

        // MQTT-3.8.3-4: Topic Filter must not contain embedded null
        if topic.contains('\0') {
            return Err(ProtocolError::MalformedPacket(
                "Topic filter must not contain embedded null character".into(),
            )
            .into());
        }

        let qos = QoS::try_from(dec.read_u8()? & 0x03)?;
        topics.push((topic, qos));
    }

    if topics.is_empty() {
        return Err(ProtocolError::MalformedPacket("SUBSCRIBE with no topics".into()).into());
    }

    Ok(Packet::Subscribe(Subscribe { packet_id, topics }))
}

fn decode_unsubscribe(payload: &[u8]) -> Result<Packet> {
    let mut dec = Decoder::new(payload);
    let packet_id = dec.read_u16()?;

    let mut topics = Vec::new();
    while dec.remaining() > 0 {
        let topic = dec.read_string()?;

        // MQTT-4.7.0-1: Topic Filter must be at least 1 character
        if topic.is_empty() {
            return Err(
                ProtocolError::MalformedPacket("Topic filter must be at least 1 character".into())
                    .into(),
            );
        }

        // Topic Filter must not contain embedded null
        if topic.contains('\0') {
            return Err(ProtocolError::MalformedPacket(
                "Topic filter must not contain embedded null character".into(),
            )
            .into());
        }

        topics.push(topic);
    }

    if topics.is_empty() {
        return Err(ProtocolError::MalformedPacket("UNSUBSCRIBE with no topics".into()).into());
    }

    Ok(Packet::Unsubscribe(Unsubscribe { packet_id, topics }))
}

/// Encode a packet into the provided buffer.
/// Returns the number of bytes written.
pub fn encode_packet(packet: &Packet, buf: &mut Vec<u8>) {
    match packet {
        Packet::Connack(connack) => encode_connack(connack, buf),
        Packet::Publish(publish) => encode_publish(publish, buf),
        Packet::Puback { packet_id } => encode_simple_ack(PacketType::Puback, *packet_id, buf),
        Packet::Pubrec { packet_id } => encode_simple_ack(PacketType::Pubrec, *packet_id, buf),
        Packet::Pubrel { packet_id } => encode_pubrel(*packet_id, buf),
        Packet::Pubcomp { packet_id } => encode_simple_ack(PacketType::Pubcomp, *packet_id, buf),
        Packet::Suback(suback) => encode_suback(suback, buf),
        Packet::Unsuback { packet_id } => encode_simple_ack(PacketType::Unsuback, *packet_id, buf),
        Packet::Pingresp => encode_pingresp(buf),
        _ => {} // Client-only packets, don't encode
    }
}

fn encode_connack(connack: &Connack, buf: &mut Vec<u8>) {
    buf.push((PacketType::Connack as u8) << 4);
    buf.push(2); // Remaining length
    buf.push(if connack.session_present { 1 } else { 0 });
    buf.push(connack.code as u8);
}

fn encode_publish(publish: &Publish, buf: &mut Vec<u8>) {
    let mut fixed_header = (PacketType::Publish as u8) << 4;
    if publish.dup {
        fixed_header |= 0x08;
    }
    fixed_header |= (publish.qos as u8) << 1;
    if publish.retain {
        fixed_header |= 0x01;
    }
    buf.push(fixed_header);

    // Calculate remaining length
    let topic_len = 2 + publish.topic.len();
    let packet_id_len = if publish.qos != QoS::AtMostOnce { 2 } else { 0 };
    let remaining = topic_len + packet_id_len + publish.payload.len();

    // Encode remaining length
    let mut len_buf = [0u8; 4];
    let len_bytes = encode_remaining_length(remaining, &mut len_buf);
    buf.extend_from_slice(&len_buf[..len_bytes]);

    // Topic
    buf.extend_from_slice(&(publish.topic.len() as u16).to_be_bytes());
    buf.extend_from_slice(&publish.topic);

    // Packet ID (if QoS > 0)
    if let Some(id) = publish.packet_id {
        buf.extend_from_slice(&id.to_be_bytes());
    }

    // Payload
    buf.extend_from_slice(&publish.payload);
}

fn encode_simple_ack(packet_type: PacketType, packet_id: u16, buf: &mut Vec<u8>) {
    buf.push((packet_type as u8) << 4);
    buf.push(2); // Remaining length
    buf.extend_from_slice(&packet_id.to_be_bytes());
}

fn encode_pubrel(packet_id: u16, buf: &mut Vec<u8>) {
    // PUBREL has fixed header flags of 0x02
    buf.push(((PacketType::Pubrel as u8) << 4) | 0x02);
    buf.push(2); // Remaining length
    buf.extend_from_slice(&packet_id.to_be_bytes());
}

fn encode_suback(suback: &Suback, buf: &mut Vec<u8>) {
    buf.push((PacketType::Suback as u8) << 4);

    let remaining = 2 + suback.return_codes.len();
    let mut len_buf = [0u8; 4];
    let len_bytes = encode_remaining_length(remaining, &mut len_buf);
    buf.extend_from_slice(&len_buf[..len_bytes]);

    buf.extend_from_slice(&suback.packet_id.to_be_bytes());
    buf.extend_from_slice(&suback.return_codes);
}

fn encode_pingresp(buf: &mut Vec<u8>) {
    buf.push((PacketType::Pingresp as u8) << 4);
    buf.push(0); // Remaining length
}
