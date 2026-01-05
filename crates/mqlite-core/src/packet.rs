//! MQTT packet types and codec for MQTT 3.1.1 and MQTT 5.0.

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
    Auth = 15,
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
            15 => Ok(PacketType::Auth),
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

/// CONNACK return codes (MQTT 3.1.1).
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

/// MQTT v5 Reason Codes (used in CONNACK, PUBACK, SUBACK, etc.).
/// These are constants rather than enum values since some codes have the same numeric value
/// with different semantic meanings depending on context.
#[allow(dead_code)]
pub mod reason_code {
    pub const SUCCESS: u8 = 0x00;
    pub const NORMAL_DISCONNECTION: u8 = 0x00;
    pub const GRANTED_QOS_0: u8 = 0x00;
    pub const GRANTED_QOS_1: u8 = 0x01;
    pub const GRANTED_QOS_2: u8 = 0x02;
    pub const DISCONNECT_WITH_WILL: u8 = 0x04;
    pub const NO_MATCHING_SUBSCRIBERS: u8 = 0x10;
    pub const NO_SUBSCRIPTION_EXISTED: u8 = 0x11;
    pub const CONTINUE_AUTHENTICATION: u8 = 0x18;
    pub const RE_AUTHENTICATE: u8 = 0x19;
    pub const UNSPECIFIED_ERROR: u8 = 0x80;
    pub const MALFORMED_PACKET: u8 = 0x81;
    pub const PROTOCOL_ERROR: u8 = 0x82;
    pub const IMPLEMENTATION_SPECIFIC_ERROR: u8 = 0x83;
    pub const UNSUPPORTED_PROTOCOL_VERSION: u8 = 0x84;
    pub const CLIENT_IDENTIFIER_NOT_VALID: u8 = 0x85;
    pub const BAD_USER_NAME_OR_PASSWORD: u8 = 0x86;
    pub const NOT_AUTHORIZED: u8 = 0x87;
    pub const SERVER_UNAVAILABLE: u8 = 0x88;
    pub const SERVER_BUSY: u8 = 0x89;
    pub const BANNED: u8 = 0x8A;
    pub const SERVER_SHUTTING_DOWN: u8 = 0x8B;
    pub const BAD_AUTHENTICATION_METHOD: u8 = 0x8C;
    pub const KEEP_ALIVE_TIMEOUT: u8 = 0x8D;
    pub const SESSION_TAKEN_OVER: u8 = 0x8E;
    pub const TOPIC_FILTER_INVALID: u8 = 0x8F;
    pub const TOPIC_NAME_INVALID: u8 = 0x90;
    pub const PACKET_IDENTIFIER_IN_USE: u8 = 0x91;
    pub const PACKET_IDENTIFIER_NOT_FOUND: u8 = 0x92;
    pub const RECEIVE_MAXIMUM_EXCEEDED: u8 = 0x93;
    pub const TOPIC_ALIAS_INVALID: u8 = 0x94;
    pub const PACKET_TOO_LARGE: u8 = 0x95;
    pub const MESSAGE_RATE_TOO_HIGH: u8 = 0x96;
    pub const QUOTA_EXCEEDED: u8 = 0x97;
    pub const ADMINISTRATIVE_ACTION: u8 = 0x98;
    pub const PAYLOAD_FORMAT_INVALID: u8 = 0x99;
    pub const RETAIN_NOT_SUPPORTED: u8 = 0x9A;
    pub const QOS_NOT_SUPPORTED: u8 = 0x9B;
    pub const USE_ANOTHER_SERVER: u8 = 0x9C;
    pub const SERVER_MOVED: u8 = 0x9D;
    pub const SHARED_SUBSCRIPTIONS_NOT_SUPPORTED: u8 = 0x9E;
    pub const CONNECTION_RATE_EXCEEDED: u8 = 0x9F;
    pub const MAXIMUM_CONNECT_TIME: u8 = 0xA0;
    pub const SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED: u8 = 0xA1;
    pub const WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED: u8 = 0xA2;
}

/// MQTT v5 CONNECT properties.
#[derive(Debug, Clone, Default)]
pub struct ConnectProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_packet_size: Option<u32>,
    pub topic_alias_maximum: Option<u16>,
    pub request_response_information: bool,
    pub request_problem_information: bool,
    pub user_properties: Vec<(String, String)>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
}

/// MQTT v5 Will properties.
#[derive(Debug, Clone, Default)]
pub struct WillProperties {
    pub will_delay_interval: Option<u32>,
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub user_properties: Vec<(String, String)>,
}

/// MQTT v5 CONNACK properties.
#[derive(Debug, Clone, Default)]
pub struct ConnackProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_qos: Option<u8>,
    pub retain_available: Option<bool>,
    pub maximum_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub topic_alias_maximum: Option<u16>,
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
    pub wildcard_subscription_available: Option<bool>,
    pub subscription_identifiers_available: Option<bool>,
    pub shared_subscription_available: Option<bool>,
    pub server_keep_alive: Option<u16>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
}

/// MQTT v5 PUBLISH properties.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)] // Reserved for future PUBLISH properties support
pub struct PublishProperties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub user_properties: Vec<(String, String)>,
    pub subscription_identifier: Option<u32>,
    pub content_type: Option<String>,
}

/// MQTT v5 subscription options.
#[derive(Debug, Clone, Copy, Default)]
pub struct SubscriptionOptions {
    pub qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8,
}

impl SubscriptionOptions {
    /// Parse from SUBSCRIBE options byte.
    pub fn from_byte(byte: u8) -> Result<Self> {
        let qos = QoS::try_from(byte & 0x03)?;
        let no_local = (byte & 0x04) != 0;
        let retain_as_published = (byte & 0x08) != 0;
        let retain_handling = (byte >> 4) & 0x03;
        Ok(Self {
            qos,
            no_local,
            retain_as_published,
            retain_handling,
        })
    }
}

/// MQTT Packets.
#[derive(Debug, Clone)]
pub enum Packet {
    Connect(Connect),
    Connack(Connack),
    Publish(Publish),
    Puback {
        packet_id: u16,
    },
    Pubrec {
        packet_id: u16,
    },
    Pubrel {
        packet_id: u16,
    },
    Pubcomp {
        packet_id: u16,
    },
    Subscribe(Subscribe),
    Suback(Suback),
    Unsubscribe(Unsubscribe),
    Unsuback(Unsuback),
    Pingreq,
    Pingresp,
    /// DISCONNECT packet with reason code (0x00 = normal, 0x04 = with will)
    Disconnect {
        reason_code: u8,
    },
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
    /// MQTT v5 properties (None for v3.1.1)
    pub properties: Option<ConnectProperties>,
}

/// Will message configuration.
#[derive(Debug, Clone)]
pub struct Will {
    pub topic: String,
    pub message: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
    /// MQTT v5 will properties (None for v3.1.1)
    pub properties: Option<WillProperties>,
}

/// CONNACK packet data.
#[derive(Debug, Clone)]
pub struct Connack {
    pub session_present: bool,
    pub code: ConnackCode,
    /// MQTT v5 reason code (used instead of code for v5)
    pub reason_code: Option<u8>,
    /// MQTT v5 properties
    pub properties: Option<ConnackProperties>,
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
    /// MQTT v5 properties (raw bytes to forward as-is)
    pub properties: Option<Bytes>,
}

/// SUBSCRIBE packet data.
#[derive(Debug, Clone)]
pub struct Subscribe {
    pub packet_id: u16,
    /// Topic filters with their subscription options (v5) or just QoS (v3.1.1).
    pub topics: Vec<(String, SubscriptionOptions)>,
    /// MQTT v5 subscription identifier (applies to all topics in this SUBSCRIBE).
    pub subscription_id: Option<u32>,
}

/// SUBACK packet data.
#[derive(Debug, Clone)]
pub struct Suback {
    pub packet_id: u16,
    pub return_codes: Vec<u8>,
    /// If true, encode with v5 format (includes property length)
    pub is_v5: bool,
}

/// UNSUBACK packet data.
#[derive(Debug, Clone)]
pub struct Unsuback {
    pub packet_id: u16,
    /// Reason codes (one per topic filter) - v5 only, v3.1.1 has no payload
    pub reason_codes: Vec<u8>,
    /// If true, encode with v5 format (includes reason codes)
    pub is_v5: bool,
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
            return Err(ProtocolError::IncompletePacket { needed: 1, have: 0 }.into());
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

    fn read_u32(&mut self) -> Result<u32> {
        if self.remaining() < 4 {
            return Err(ProtocolError::IncompletePacket {
                needed: 4,
                have: self.remaining(),
            }
            .into());
        }
        let val = u32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(val)
    }

    /// Read Variable Byte Integer (MQTT 5.0).
    fn read_variable_byte_integer(&mut self) -> Result<u32> {
        let mut multiplier = 1u32;
        let mut value = 0u32;

        loop {
            if self.pos >= self.buf.len() {
                return Err(ProtocolError::IncompletePacket { needed: 1, have: 0 }.into());
            }

            let byte = self.buf[self.pos];
            self.pos += 1;

            value += ((byte & 0x7F) as u32) * multiplier;

            if multiplier > 128 * 128 * 128 {
                return Err(ProtocolError::InvalidRemainingLength.into());
            }

            if (byte & 0x80) == 0 {
                return Ok(value);
            }

            multiplier *= 128;
        }
    }

    /// Read MQTT v5 CONNECT properties.
    fn read_connect_properties(&mut self) -> Result<ConnectProperties> {
        let prop_len = self.read_variable_byte_integer()? as usize;
        let end_pos = self.pos + prop_len;
        let mut props = ConnectProperties::default();

        while self.pos < end_pos {
            let prop_id = self.read_u8()?;
            match prop_id {
                0x11 => props.session_expiry_interval = Some(self.read_u32()?),
                0x21 => props.receive_maximum = Some(self.read_u16()?),
                0x27 => props.maximum_packet_size = Some(self.read_u32()?),
                0x22 => props.topic_alias_maximum = Some(self.read_u16()?),
                0x19 => props.request_response_information = self.read_u8()? != 0,
                0x17 => props.request_problem_information = self.read_u8()? != 0,
                0x26 => {
                    let key = self.read_string()?;
                    let value = self.read_string()?;
                    props.user_properties.push((key, value));
                }
                0x15 => props.authentication_method = Some(self.read_string()?),
                0x16 => props.authentication_data = Some(self.read_binary()?),
                _ => {
                    return Err(ProtocolError::MalformedPacket(format!(
                        "Unknown CONNECT property: 0x{:02x}",
                        prop_id
                    ))
                    .into());
                }
            }
        }

        Ok(props)
    }

    /// Read MQTT v5 Will properties.
    fn read_will_properties(&mut self) -> Result<WillProperties> {
        let prop_len = self.read_variable_byte_integer()? as usize;
        let end_pos = self.pos + prop_len;
        let mut props = WillProperties::default();

        while self.pos < end_pos {
            let prop_id = self.read_u8()?;
            match prop_id {
                0x18 => props.will_delay_interval = Some(self.read_u32()?),
                0x01 => props.payload_format_indicator = Some(self.read_u8()?),
                0x02 => props.message_expiry_interval = Some(self.read_u32()?),
                0x03 => props.content_type = Some(self.read_string()?),
                0x08 => props.response_topic = Some(self.read_string()?),
                0x09 => props.correlation_data = Some(self.read_binary()?),
                0x26 => {
                    let key = self.read_string()?;
                    let value = self.read_string()?;
                    props.user_properties.push((key, value));
                }
                _ => {
                    return Err(ProtocolError::MalformedPacket(format!(
                        "Unknown Will property: 0x{:02x}",
                        prop_id
                    ))
                    .into());
                }
            }
        }

        Ok(props)
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
/// protocol_version: 0 = unknown (for CONNECT), 3 = MQTT 3.1, 4 = MQTT 3.1.1, 5 = MQTT 5.0
/// max_packet_size: Maximum allowed packet size (0 = no limit).
pub fn decode_packet(
    buf: &[u8],
    protocol_version: u8,
    max_packet_size: u32,
) -> Result<Option<(Packet, usize)>> {
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

    // Check packet size limit
    if max_packet_size > 0 && total_len > max_packet_size as usize {
        return Err(ProtocolError::PacketTooLarge {
            size: total_len,
            max: max_packet_size as usize,
        }
        .into());
    }

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

    let is_v5 = protocol_version == 5;

    let packet = match packet_type {
        PacketType::Connect => decode_connect(payload)?,
        PacketType::Publish => decode_publish(flags, payload, is_v5)?,
        PacketType::Puback => decode_puback(payload)?,
        PacketType::Pubrec => decode_pubrec(payload)?,
        PacketType::Pubrel => decode_pubrel(payload)?,
        PacketType::Pubcomp => decode_pubcomp(payload)?,
        PacketType::Subscribe => decode_subscribe(payload, is_v5)?,
        PacketType::Unsubscribe => decode_unsubscribe(payload, is_v5)?,
        PacketType::Pingreq => Packet::Pingreq,
        PacketType::Disconnect => decode_disconnect(payload, is_v5)?,
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
    let is_v5 = protocol_version == 5;
    if protocol_version != 5 && protocol_version != 4 && protocol_version != 3 {
        // 5 = MQTT 5.0, 4 = MQTT 3.1.1, 3 = MQTT 3.1
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

    // MQTT-3.1.2-22: If Username Flag is 0, Password Flag MUST be 0 (v3.1.1 only)
    // Note: MQTT v5 allows password without username
    if !is_v5 && !username_flag && password_flag {
        return Err(ProtocolError::MalformedPacket(
            "Password Flag must be 0 when Username Flag is 0".into(),
        )
        .into());
    }

    // Keep alive
    let keep_alive = dec.read_u16()?;

    // MQTT v5: Read CONNECT properties
    let properties = if is_v5 {
        Some(dec.read_connect_properties()?)
    } else {
        None
    };

    // Client ID
    let client_id = dec.read_string()?;

    // Will
    let will = if will_flag {
        // MQTT v5: Read Will properties first
        let will_properties = if is_v5 {
            Some(dec.read_will_properties()?)
        } else {
            None
        };
        let topic = dec.read_string()?;
        let message = dec.read_binary()?;
        Some(Will {
            topic,
            message,
            qos: will_qos,
            retain: will_retain,
            properties: will_properties,
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
        properties,
    }))
}

fn decode_publish(flags: u8, payload: &[u8], is_v5: bool) -> Result<Packet> {
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

    // MQTT v5: parse and store properties for forwarding
    let properties = if is_v5 {
        let prop_len = dec.read_variable_byte_integer()? as usize;
        let prop_bytes = dec.read_bytes(prop_len)?;
        if prop_len > 0 {
            Some(Bytes::copy_from_slice(prop_bytes))
        } else {
            None
        }
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
        properties,
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

fn decode_subscribe(payload: &[u8], is_v5: bool) -> Result<Packet> {
    let mut dec = Decoder::new(payload);
    let packet_id = dec.read_u16()?;

    // MQTT v5: Parse properties and store Subscription Identifier
    let mut subscription_id = None;
    if is_v5 {
        let prop_len = dec.read_variable_byte_integer()? as usize;
        let prop_end = dec.pos + prop_len;

        while dec.pos < prop_end {
            let prop_id = dec.read_variable_byte_integer()?;
            match prop_id {
                0x0B => {
                    // Subscription Identifier
                    let sub_id = dec.read_variable_byte_integer()?;
                    // MQTT-3.8.2-1: Subscription Identifier MUST be 1 to 268,435,455
                    if sub_id == 0 {
                        return Err(ProtocolError::MalformedPacket(
                            "Subscription Identifier must be non-zero".into(),
                        )
                        .into());
                    }
                    subscription_id = Some(sub_id);
                }
                0x26 => {
                    // User Property - skip key and value strings
                    dec.read_string()?;
                    dec.read_string()?;
                }
                _ => {
                    // Unknown property - protocol error
                    return Err(ProtocolError::MalformedPacket(format!(
                        "Unknown SUBSCRIBE property: 0x{:02X}",
                        prop_id
                    ))
                    .into());
                }
            }
        }
    }

    let mut topics = Vec::new();
    while dec.remaining() > 0 {
        let topic = dec.read_string()?;

        // MQTT-4.7.0-1: Topic Filter must be at least 1 character
        if topic.is_empty() {
            return Err(ProtocolError::MalformedPacket(
                "Topic filter must be at least 1 character".into(),
            )
            .into());
        }

        // MQTT-3.8.3-4: Topic Filter must not contain embedded null
        if topic.contains('\0') {
            return Err(ProtocolError::MalformedPacket(
                "Topic filter must not contain embedded null character".into(),
            )
            .into());
        }

        // Read subscription options byte and parse v5 options
        let options_byte = dec.read_u8()?;
        let options = SubscriptionOptions::from_byte(options_byte)?;
        topics.push((topic, options));
    }

    if topics.is_empty() {
        return Err(ProtocolError::MalformedPacket("SUBSCRIBE with no topics".into()).into());
    }

    Ok(Packet::Subscribe(Subscribe {
        packet_id,
        topics,
        subscription_id,
    }))
}

fn decode_unsubscribe(payload: &[u8], is_v5: bool) -> Result<Packet> {
    let mut dec = Decoder::new(payload);
    let packet_id = dec.read_u16()?;

    // MQTT v5: Read and skip properties
    if is_v5 {
        let prop_len = dec.read_variable_byte_integer()? as usize;
        dec.read_bytes(prop_len)?;
    }

    let mut topics = Vec::new();
    while dec.remaining() > 0 {
        let topic = dec.read_string()?;

        // MQTT-4.7.0-1: Topic Filter must be at least 1 character
        if topic.is_empty() {
            return Err(ProtocolError::MalformedPacket(
                "Topic filter must be at least 1 character".into(),
            )
            .into());
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

/// Decode MQTT DISCONNECT packet.
/// In MQTT v5, includes reason code (0x00 = normal, 0x04 = with will).
/// In MQTT v3.1.1, always treated as normal disconnect (reason_code = 0x00).
fn decode_disconnect(payload: &[u8], is_v5: bool) -> Result<Packet> {
    if !is_v5 || payload.is_empty() {
        // MQTT v3.1.1 or v5 with empty payload: normal disconnect
        return Ok(Packet::Disconnect { reason_code: 0x00 });
    }

    // MQTT v5: parse reason code from variable header
    let mut dec = Decoder::new(payload);
    let reason_code = dec.read_u8()?;

    // Skip properties if present
    if dec.remaining() > 0 {
        let prop_len = dec.read_variable_byte_integer()? as usize;
        dec.read_bytes(prop_len)?;
    }

    Ok(Packet::Disconnect { reason_code })
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
        Packet::Unsuback(unsuback) => encode_unsuback(unsuback, buf),
        Packet::Pingresp => encode_pingresp(buf),
        _ => {} // Client-only packets, don't encode
    }
}

/// Encode a Variable Byte Integer into the buffer. Returns bytes written.
pub fn encode_variable_byte_integer(mut val: u32, buf: &mut Vec<u8>) -> usize {
    let start = buf.len();
    loop {
        let mut byte = (val % 128) as u8;
        val /= 128;
        if val > 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if val == 0 {
            break;
        }
    }
    buf.len() - start
}

/// Update Message Expiry Interval in properties for retained message delivery.
/// Returns None if the message has expired, Some(updated_properties) otherwise.
/// If no expiry interval was set, returns the original properties unchanged.
pub fn update_message_expiry(
    properties: Option<&Bytes>,
    elapsed_secs: u32,
) -> Option<Option<Bytes>> {
    let Some(props) = properties else {
        return Some(None); // No properties, no expiry
    };

    let props_bytes = props.as_ref();
    if props_bytes.is_empty() {
        return Some(Some(props.clone()));
    }

    // Parse through properties looking for Message Expiry Interval (0x02)
    let mut pos = 0;
    let mut expiry_pos: Option<usize> = None;
    let mut original_expiry: Option<u32> = None;

    while pos < props_bytes.len() {
        let prop_id = props_bytes[pos];
        pos += 1;

        match prop_id {
            0x02 => {
                // Message Expiry Interval - 4 byte integer
                if pos + 4 > props_bytes.len() {
                    return Some(Some(props.clone())); // Malformed, return as-is
                }
                expiry_pos = Some(pos - 1); // Position of property ID
                original_expiry = Some(u32::from_be_bytes([
                    props_bytes[pos],
                    props_bytes[pos + 1],
                    props_bytes[pos + 2],
                    props_bytes[pos + 3],
                ]));
                pos += 4;
            }
            0x01 | 0x17 => pos += 1, // Byte: Payload Format, Request Problem
            0x23 => pos += 2,        // Two Byte: Topic Alias
            0x03 | 0x08 => {
                // UTF-8 String: Content Type, Response Topic
                if pos + 2 > props_bytes.len() {
                    return Some(Some(props.clone()));
                }
                let len = u16::from_be_bytes([props_bytes[pos], props_bytes[pos + 1]]) as usize;
                pos += 2 + len;
            }
            0x09 => {
                // Binary Data: Correlation Data
                if pos + 2 > props_bytes.len() {
                    return Some(Some(props.clone()));
                }
                let len = u16::from_be_bytes([props_bytes[pos], props_bytes[pos + 1]]) as usize;
                pos += 2 + len;
            }
            0x0B => {
                // Variable Byte Integer: Subscription Identifier
                while pos < props_bytes.len() && (props_bytes[pos] & 0x80) != 0 {
                    pos += 1;
                }
                pos += 1;
            }
            0x26 => {
                // User Property: Two UTF-8 strings
                for _ in 0..2 {
                    if pos + 2 > props_bytes.len() {
                        return Some(Some(props.clone()));
                    }
                    let len = u16::from_be_bytes([props_bytes[pos], props_bytes[pos + 1]]) as usize;
                    pos += 2 + len;
                }
            }
            _ => {
                // Unknown property, can't parse further - return as-is
                return Some(Some(props.clone()));
            }
        }
    }

    // If no expiry interval, return original
    let Some(expiry_start) = expiry_pos else {
        return Some(Some(props.clone()));
    };
    let Some(original) = original_expiry else {
        return Some(Some(props.clone()));
    };

    // Check if expired
    if elapsed_secs >= original {
        return None; // Message has expired
    }

    // Calculate new expiry
    let new_expiry = original - elapsed_secs;

    // Build new properties with updated expiry
    let mut new_props = Vec::with_capacity(props_bytes.len());
    new_props.extend_from_slice(&props_bytes[..expiry_start + 1]); // Up to and including 0x02
    new_props.extend_from_slice(&new_expiry.to_be_bytes());
    new_props.extend_from_slice(&props_bytes[expiry_start + 5..]); // After the old expiry value

    Some(Some(Bytes::from(new_props)))
}

/// Encode CONNACK properties into the buffer.
fn encode_connack_properties(props: &ConnackProperties, buf: &mut Vec<u8>) {
    let mut prop_buf = Vec::new();

    if let Some(v) = props.session_expiry_interval {
        prop_buf.push(0x11);
        prop_buf.extend_from_slice(&v.to_be_bytes());
    }
    if let Some(v) = props.receive_maximum {
        prop_buf.push(0x21);
        prop_buf.extend_from_slice(&v.to_be_bytes());
    }
    if let Some(v) = props.maximum_qos {
        prop_buf.push(0x24);
        prop_buf.push(v);
    }
    if let Some(v) = props.retain_available {
        prop_buf.push(0x25);
        prop_buf.push(if v { 1 } else { 0 });
    }
    if let Some(v) = props.maximum_packet_size {
        prop_buf.push(0x27);
        prop_buf.extend_from_slice(&v.to_be_bytes());
    }
    if let Some(ref v) = props.assigned_client_identifier {
        prop_buf.push(0x12);
        prop_buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
        prop_buf.extend_from_slice(v.as_bytes());
    }
    if let Some(v) = props.topic_alias_maximum {
        prop_buf.push(0x22);
        prop_buf.extend_from_slice(&v.to_be_bytes());
    }
    if let Some(ref v) = props.reason_string {
        prop_buf.push(0x1F);
        prop_buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
        prop_buf.extend_from_slice(v.as_bytes());
    }
    for (key, value) in &props.user_properties {
        prop_buf.push(0x26);
        prop_buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
        prop_buf.extend_from_slice(key.as_bytes());
        prop_buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
        prop_buf.extend_from_slice(value.as_bytes());
    }
    if let Some(v) = props.wildcard_subscription_available {
        prop_buf.push(0x28);
        prop_buf.push(if v { 1 } else { 0 });
    }
    if let Some(v) = props.subscription_identifiers_available {
        prop_buf.push(0x29);
        prop_buf.push(if v { 1 } else { 0 });
    }
    if let Some(v) = props.shared_subscription_available {
        prop_buf.push(0x2A);
        prop_buf.push(if v { 1 } else { 0 });
    }
    if let Some(v) = props.server_keep_alive {
        prop_buf.push(0x13);
        prop_buf.extend_from_slice(&v.to_be_bytes());
    }
    if let Some(ref v) = props.response_information {
        prop_buf.push(0x1A);
        prop_buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
        prop_buf.extend_from_slice(v.as_bytes());
    }
    if let Some(ref v) = props.server_reference {
        prop_buf.push(0x1C);
        prop_buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
        prop_buf.extend_from_slice(v.as_bytes());
    }
    if let Some(ref v) = props.authentication_method {
        prop_buf.push(0x15);
        prop_buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
        prop_buf.extend_from_slice(v.as_bytes());
    }
    if let Some(ref v) = props.authentication_data {
        prop_buf.push(0x16);
        prop_buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
        prop_buf.extend_from_slice(v);
    }

    // Write property length and data
    encode_variable_byte_integer(prop_buf.len() as u32, buf);
    buf.extend_from_slice(&prop_buf);
}

fn encode_connack(connack: &Connack, buf: &mut Vec<u8>) {
    // Check if this is a v5 CONNACK (has reason_code or properties)
    let is_v5 = connack.reason_code.is_some() || connack.properties.is_some();

    if is_v5 {
        // Build variable header + properties first to calculate remaining length
        let mut var_header = Vec::new();

        // Connect acknowledge flags (session present)
        var_header.push(if connack.session_present { 1 } else { 0 });

        // Reason code
        var_header.push(connack.reason_code.unwrap_or(0));

        // Properties
        if let Some(ref props) = connack.properties {
            encode_connack_properties(props, &mut var_header);
        } else {
            var_header.push(0); // Empty properties
        }

        // Write fixed header
        buf.push((PacketType::Connack as u8) << 4);
        let mut len_buf = [0u8; 4];
        let len_bytes = encode_remaining_length(var_header.len(), &mut len_buf);
        buf.extend_from_slice(&len_buf[..len_bytes]);

        // Write variable header
        buf.extend_from_slice(&var_header);
    } else {
        // v3.1.1 format
        buf.push((PacketType::Connack as u8) << 4);
        buf.push(2); // Remaining length
        buf.push(if connack.session_present { 1 } else { 0 });
        buf.push(connack.code as u8);
    }
}

/// Encode a PUBLISH packet.
pub fn encode_publish(publish: &Publish, buf: &mut Vec<u8>) {
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

    // MQTT v5: Include properties if present
    let props_len = if let Some(ref props) = publish.properties {
        // Property length (variable byte integer) + property bytes
        let content_len = props.len();
        let header_len = if content_len < 128 {
            1
        } else if content_len < 16384 {
            2
        } else if content_len < 2097152 {
            3
        } else {
            4
        };
        header_len + content_len
    } else {
        0
    };

    let remaining = topic_len + packet_id_len + props_len + publish.payload.len();

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

    // MQTT v5: Properties (if present)
    if let Some(ref props) = publish.properties {
        let mut prop_len_buf = [0u8; 4];
        let prop_len_bytes = encode_remaining_length(props.len(), &mut prop_len_buf);
        buf.extend_from_slice(&prop_len_buf[..prop_len_bytes]);
        buf.extend_from_slice(props);
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

    if suback.is_v5 {
        // v5 format: packet_id + property_length (0) + reason_codes
        let remaining = 2 + 1 + suback.return_codes.len(); // 2 for packet_id, 1 for property length
        let mut len_buf = [0u8; 4];
        let len_bytes = encode_remaining_length(remaining, &mut len_buf);
        buf.extend_from_slice(&len_buf[..len_bytes]);

        buf.extend_from_slice(&suback.packet_id.to_be_bytes());
        buf.push(0); // Property length = 0
        buf.extend_from_slice(&suback.return_codes);
    } else {
        // v3.1.1 format: packet_id + return_codes
        let remaining = 2 + suback.return_codes.len();
        let mut len_buf = [0u8; 4];
        let len_bytes = encode_remaining_length(remaining, &mut len_buf);
        buf.extend_from_slice(&len_buf[..len_bytes]);

        buf.extend_from_slice(&suback.packet_id.to_be_bytes());
        buf.extend_from_slice(&suback.return_codes);
    }
}

fn encode_unsuback(unsuback: &Unsuback, buf: &mut Vec<u8>) {
    buf.push((PacketType::Unsuback as u8) << 4);

    if unsuback.is_v5 {
        // v5 format: packet_id + property_length (0) + reason_codes
        let remaining = 2 + 1 + unsuback.reason_codes.len();
        let mut len_buf = [0u8; 4];
        let len_bytes = encode_remaining_length(remaining, &mut len_buf);
        buf.extend_from_slice(&len_buf[..len_bytes]);

        buf.extend_from_slice(&unsuback.packet_id.to_be_bytes());
        buf.push(0); // Property length = 0
        buf.extend_from_slice(&unsuback.reason_codes);
    } else {
        // v3.1.1 format: just packet_id (no reason codes)
        buf.push(2); // Remaining length
        buf.extend_from_slice(&unsuback.packet_id.to_be_bytes());
    }
}

fn encode_pingresp(buf: &mut Vec<u8>) {
    buf.push((PacketType::Pingresp as u8) << 4);
    buf.push(0); // Remaining length
}

// === Client Packet Encoding ===

/// Encode a CONNECT packet.
pub fn encode_connect(connect: &Connect, buf: &mut Vec<u8>) {
    let mut payload = Vec::new();

    // Protocol name
    let protocol_name = connect.protocol_name.as_bytes();
    payload.extend_from_slice(&(protocol_name.len() as u16).to_be_bytes());
    payload.extend_from_slice(protocol_name);

    // Protocol version
    payload.push(connect.protocol_version);

    // Connect flags
    let mut flags = 0u8;
    if connect.clean_session {
        flags |= 0x02;
    }
    if connect.will.is_some() {
        flags |= 0x04;
        if let Some(ref will) = connect.will {
            flags |= (will.qos as u8) << 3;
            if will.retain {
                flags |= 0x20;
            }
        }
    }
    if connect.password.is_some() {
        flags |= 0x40;
    }
    if connect.username.is_some() {
        flags |= 0x80;
    }
    payload.push(flags);

    // Keep alive
    payload.extend_from_slice(&connect.keep_alive.to_be_bytes());

    // MQTT v5 properties
    if connect.protocol_version == 5 {
        if let Some(ref props) = connect.properties {
            encode_connect_properties(props, &mut payload);
        } else {
            payload.push(0); // No properties
        }
    }

    // Client ID
    let client_id = connect.client_id.as_bytes();
    payload.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    payload.extend_from_slice(client_id);

    // Will message
    if let Some(ref will) = connect.will {
        // Will properties (v5 only)
        if connect.protocol_version == 5 {
            if let Some(ref props) = will.properties {
                encode_will_properties(props, &mut payload);
            } else {
                payload.push(0);
            }
        }
        // Will topic
        let topic = will.topic.as_bytes();
        payload.extend_from_slice(&(topic.len() as u16).to_be_bytes());
        payload.extend_from_slice(topic);
        // Will message
        payload.extend_from_slice(&(will.message.len() as u16).to_be_bytes());
        payload.extend_from_slice(&will.message);
    }

    // Username
    if let Some(ref username) = connect.username {
        let username = username.as_bytes();
        payload.extend_from_slice(&(username.len() as u16).to_be_bytes());
        payload.extend_from_slice(username);
    }

    // Password
    if let Some(ref password) = connect.password {
        payload.extend_from_slice(&(password.len() as u16).to_be_bytes());
        payload.extend_from_slice(password);
    }

    // Fixed header
    buf.push((PacketType::Connect as u8) << 4);
    encode_remaining_length_vec(payload.len(), buf);
    buf.extend_from_slice(&payload);
}

fn encode_connect_properties(props: &ConnectProperties, buf: &mut Vec<u8>) {
    let mut props_buf = Vec::new();

    if let Some(session_expiry) = props.session_expiry_interval {
        props_buf.push(0x11);
        props_buf.extend_from_slice(&session_expiry.to_be_bytes());
    }
    if let Some(receive_max) = props.receive_maximum {
        props_buf.push(0x21);
        props_buf.extend_from_slice(&receive_max.to_be_bytes());
    }
    if let Some(max_packet) = props.maximum_packet_size {
        props_buf.push(0x27);
        props_buf.extend_from_slice(&max_packet.to_be_bytes());
    }
    if let Some(topic_alias_max) = props.topic_alias_maximum {
        props_buf.push(0x22);
        props_buf.extend_from_slice(&topic_alias_max.to_be_bytes());
    }

    encode_variable_byte_integer(props_buf.len() as u32, buf);
    buf.extend_from_slice(&props_buf);
}

fn encode_will_properties(props: &WillProperties, buf: &mut Vec<u8>) {
    let mut props_buf = Vec::new();

    if let Some(delay) = props.will_delay_interval {
        props_buf.push(0x18);
        props_buf.extend_from_slice(&delay.to_be_bytes());
    }
    if let Some(expiry) = props.message_expiry_interval {
        props_buf.push(0x02);
        props_buf.extend_from_slice(&expiry.to_be_bytes());
    }
    if let Some(ref content_type) = props.content_type {
        props_buf.push(0x03);
        props_buf.extend_from_slice(&(content_type.len() as u16).to_be_bytes());
        props_buf.extend_from_slice(content_type.as_bytes());
    }
    if let Some(ref response_topic) = props.response_topic {
        props_buf.push(0x08);
        props_buf.extend_from_slice(&(response_topic.len() as u16).to_be_bytes());
        props_buf.extend_from_slice(response_topic.as_bytes());
    }
    if let Some(ref correlation_data) = props.correlation_data {
        props_buf.push(0x09);
        props_buf.extend_from_slice(&(correlation_data.len() as u16).to_be_bytes());
        props_buf.extend_from_slice(correlation_data);
    }

    encode_variable_byte_integer(props_buf.len() as u32, buf);
    buf.extend_from_slice(&props_buf);
}

/// Encode a SUBSCRIBE packet.
pub fn encode_subscribe(subscribe: &Subscribe, buf: &mut Vec<u8>) {
    let mut payload = Vec::new();

    // Packet ID
    payload.extend_from_slice(&subscribe.packet_id.to_be_bytes());

    // Properties (v5 only) - we always send minimal for now
    // Note: subscription_id should be sent if present
    if subscribe.subscription_id.is_some() {
        let mut props = Vec::new();
        if let Some(sub_id) = subscribe.subscription_id {
            props.push(0x0B);
            encode_variable_byte_integer(sub_id, &mut props);
        }
        encode_variable_byte_integer(props.len() as u32, &mut payload);
        payload.extend_from_slice(&props);
    }

    // Topic filters
    for (topic, options) in &subscribe.topics {
        let topic_bytes = topic.as_bytes();
        payload.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        payload.extend_from_slice(topic_bytes);
        // Subscription options byte
        let opts_byte = (options.qos as u8)
            | if options.no_local { 0x04 } else { 0 }
            | if options.retain_as_published { 0x08 } else { 0 }
            | (options.retain_handling << 4);
        payload.push(opts_byte);
    }

    // Fixed header (flags must be 0x02)
    buf.push(((PacketType::Subscribe as u8) << 4) | 0x02);
    encode_remaining_length_vec(payload.len(), buf);
    buf.extend_from_slice(&payload);
}

/// Encode an UNSUBSCRIBE packet.
pub fn encode_unsubscribe(unsubscribe: &Unsubscribe, buf: &mut Vec<u8>) {
    let mut payload = Vec::new();

    // Packet ID
    payload.extend_from_slice(&unsubscribe.packet_id.to_be_bytes());

    // Topic filters
    for topic in &unsubscribe.topics {
        let topic_bytes = topic.as_bytes();
        payload.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        payload.extend_from_slice(topic_bytes);
    }

    // Fixed header (flags must be 0x02)
    buf.push(((PacketType::Unsubscribe as u8) << 4) | 0x02);
    encode_remaining_length_vec(payload.len(), buf);
    buf.extend_from_slice(&payload);
}

/// Encode a PINGREQ packet.
pub fn encode_pingreq(buf: &mut Vec<u8>) {
    buf.push((PacketType::Pingreq as u8) << 4);
    buf.push(0);
}

/// Encode a DISCONNECT packet.
pub fn encode_disconnect(reason_code: u8, buf: &mut Vec<u8>) {
    buf.push((PacketType::Disconnect as u8) << 4);
    if reason_code == 0 {
        buf.push(0); // No payload for normal disconnect
    } else {
        buf.push(1);
        buf.push(reason_code);
    }
}

/// Helper to encode remaining length into a Vec buffer.
fn encode_remaining_length_vec(mut len: usize, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if len == 0 {
            break;
        }
    }
}

// === Topic Validation ===

/// Validate a topic name or filter against length and depth limits.
/// Returns Ok(()) if valid, or an appropriate ProtocolError.
/// Zero-allocation on success, early-exits on depth violation.
#[inline]
pub fn validate_topic(topic: &[u8], max_length: usize, max_levels: usize) -> Result<()> {
    // Check length (0 = no limit)
    if max_length > 0 && topic.len() > max_length {
        return Err(ProtocolError::TopicTooLong {
            len: topic.len(),
            max: max_length,
        }
        .into());
    }

    // Check levels with early exit (0 = no limit)
    if max_levels > 0 {
        let mut levels = 1usize;
        for &b in topic {
            if b == b'/' {
                levels += 1;
                if levels > max_levels {
                    return Err(ProtocolError::TopicTooDeep {
                        levels,
                        max: max_levels,
                    }
                    .into());
                }
            }
        }
    }

    Ok(())
}
