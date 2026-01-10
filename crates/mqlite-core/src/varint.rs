//! Variable Byte Integer encoding/decoding for MQTT.
//!
//! MQTT uses a variable-length encoding scheme for representing integers.
//! This encoding allows small numbers to be represented efficiently while
//! still supporting larger values when needed.
//!
//! The encoding uses 7 bits per byte for the value, with the high bit
//! indicating whether more bytes follow. This allows:
//! - 0-127: 1 byte
//! - 128-16383: 2 bytes
//! - 16384-2097151: 3 bytes
//! - 2097152-268435455: 4 bytes

use crate::error::{ProtocolError, Result};

/// Decode a variable byte integer from a buffer.
///
/// Returns `Ok(Some((value, bytes_consumed)))` if successful,
/// `Ok(None)` if more data is needed, or `Err` if the encoding is invalid.
///
/// # Example
/// ```
/// use mqlite_core::varint::decode;
/// let buf = [0x80, 0x01]; // Encodes 128
/// let (value, consumed) = decode(&buf).unwrap().unwrap();
/// assert_eq!(value, 128);
/// assert_eq!(consumed, 2);
/// ```
pub fn decode(buf: &[u8]) -> Result<Option<(usize, usize)>> {
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

/// Encode a value as a variable byte integer into a fixed-size buffer.
///
/// Returns the number of bytes written. The buffer must be at least 4 bytes.
///
/// # Example
/// ```
/// use mqlite_core::varint::encode_to_slice;
/// let mut buf = [0u8; 4];
/// let written = encode_to_slice(128, &mut buf);
/// assert_eq!(written, 2);
/// assert_eq!(&buf[..2], &[0x80, 0x01]);
/// ```
pub fn encode_to_slice(mut value: usize, buf: &mut [u8]) -> usize {
    let mut i = 0;
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        buf[i] = byte;
        i += 1;
        if value == 0 {
            break;
        }
    }
    i
}

/// Encode a value as a variable byte integer, appending to a Vec.
///
/// Returns the number of bytes written.
///
/// # Example
/// ```
/// use mqlite_core::varint::encode_to_vec;
/// let mut buf = Vec::new();
/// let written = encode_to_vec(300, &mut buf);
/// assert_eq!(written, 2);
/// assert_eq!(&buf, &[0xAC, 0x02]); // 300 = 44 + 2*128 = 0x2C + 0x80, then 0x02
/// ```
pub fn encode_to_vec(mut value: u32, buf: &mut Vec<u8>) -> usize {
    let start = buf.len();
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
    buf.len() - start
}

/// Calculate the number of bytes needed to encode a value.
///
/// # Example
/// ```
/// use mqlite_core::varint::encoded_len;
/// assert_eq!(encoded_len(0), 1);
/// assert_eq!(encoded_len(127), 1);
/// assert_eq!(encoded_len(128), 2);
/// assert_eq!(encoded_len(16383), 2);
/// assert_eq!(encoded_len(16384), 3);
/// ```
pub fn encoded_len(mut value: u32) -> usize {
    let mut len = 0;
    loop {
        len += 1;
        value /= 128;
        if value == 0 {
            break;
        }
    }
    len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_single_byte() {
        assert_eq!(decode(&[0]).unwrap(), Some((0, 1)));
        assert_eq!(decode(&[0x7F]).unwrap(), Some((127, 1)));
    }

    #[test]
    fn test_decode_two_bytes() {
        assert_eq!(decode(&[0x80, 0x01]).unwrap(), Some((128, 2)));
        assert_eq!(decode(&[0xFF, 0x7F]).unwrap(), Some((16383, 2)));
    }

    #[test]
    fn test_decode_three_bytes() {
        assert_eq!(decode(&[0x80, 0x80, 0x01]).unwrap(), Some((16384, 3)));
        assert_eq!(decode(&[0xFF, 0xFF, 0x7F]).unwrap(), Some((2097151, 3)));
    }

    #[test]
    fn test_decode_four_bytes() {
        assert_eq!(
            decode(&[0x80, 0x80, 0x80, 0x01]).unwrap(),
            Some((2097152, 4))
        );
        assert_eq!(
            decode(&[0xFF, 0xFF, 0xFF, 0x7F]).unwrap(),
            Some((268435455, 4))
        );
    }

    #[test]
    fn test_decode_incomplete() {
        assert_eq!(decode(&[]).unwrap(), None);
        assert_eq!(decode(&[0x80]).unwrap(), None);
        assert_eq!(decode(&[0x80, 0x80]).unwrap(), None);
    }

    #[test]
    fn test_decode_invalid() {
        // More than 4 bytes with continuation bit
        assert!(decode(&[0x80, 0x80, 0x80, 0x80, 0x01]).is_err());
    }

    #[test]
    fn test_encode_to_slice() {
        let mut buf = [0u8; 4];

        assert_eq!(encode_to_slice(0, &mut buf), 1);
        assert_eq!(buf[0], 0);

        assert_eq!(encode_to_slice(127, &mut buf), 1);
        assert_eq!(buf[0], 0x7F);

        assert_eq!(encode_to_slice(128, &mut buf), 2);
        assert_eq!(&buf[..2], &[0x80, 0x01]);

        assert_eq!(encode_to_slice(16384, &mut buf), 3);
        assert_eq!(&buf[..3], &[0x80, 0x80, 0x01]);
    }

    #[test]
    fn test_encode_to_vec() {
        let mut buf = Vec::new();

        assert_eq!(encode_to_vec(0, &mut buf), 1);
        assert_eq!(&buf, &[0]);

        buf.clear();
        assert_eq!(encode_to_vec(127, &mut buf), 1);
        assert_eq!(&buf, &[0x7F]);

        buf.clear();
        assert_eq!(encode_to_vec(128, &mut buf), 2);
        assert_eq!(&buf, &[0x80, 0x01]);
    }

    #[test]
    fn test_encoded_len() {
        assert_eq!(encoded_len(0), 1);
        assert_eq!(encoded_len(127), 1);
        assert_eq!(encoded_len(128), 2);
        assert_eq!(encoded_len(16383), 2);
        assert_eq!(encoded_len(16384), 3);
        assert_eq!(encoded_len(2097151), 3);
        assert_eq!(encoded_len(2097152), 4);
        assert_eq!(encoded_len(268435455), 4);
    }

    #[test]
    fn test_roundtrip() {
        for value in [0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455] {
            let mut buf = Vec::new();
            encode_to_vec(value, &mut buf);
            let (decoded, consumed) = decode(&buf).unwrap().unwrap();
            assert_eq!(decoded, value as usize);
            assert_eq!(consumed, buf.len());
        }
    }
}
