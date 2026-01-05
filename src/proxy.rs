//! PROXY Protocol Parser
//!
//! Parses HAProxy PROXY protocol v1 (text) and v2 (binary) headers.
//! Used to preserve real client IPs when running behind load balancers.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use mio::net::TcpStream;

/// PROXY v1 signature: "PROXY "
const PROXY_V1_SIGNATURE: &[u8] = b"PROXY ";

/// PROXY v2 signature (12 bytes)
const PROXY_V2_SIGNATURE: &[u8] = b"\r\n\r\n\x00\r\nQUIT\n";

/// Maximum PROXY header size (v2 can be up to 536 bytes)
const MAX_HEADER_SIZE: usize = 536;

/// Errors that can occur during PROXY header parsing.
#[derive(Debug)]
pub enum ProxyError {
    /// Timeout waiting for PROXY header.
    Timeout,
    /// Invalid or malformed PROXY header.
    InvalidHeader(String),
    /// IO error reading from socket.
    Io(io::Error),
    /// Connection closed before header received.
    ConnectionClosed,
    /// PROXY protocol not detected (no signature).
    NotProxyProtocol,
}

impl std::fmt::Display for ProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyError::Timeout => write!(f, "PROXY header timeout"),
            ProxyError::InvalidHeader(msg) => write!(f, "invalid PROXY header: {}", msg),
            ProxyError::Io(e) => write!(f, "IO error: {}", e),
            ProxyError::ConnectionClosed => write!(f, "connection closed"),
            ProxyError::NotProxyProtocol => write!(f, "no PROXY protocol signature"),
        }
    }
}

impl std::error::Error for ProxyError {}

impl From<io::Error> for ProxyError {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::TimedOut || e.kind() == io::ErrorKind::WouldBlock {
            ProxyError::Timeout
        } else {
            ProxyError::Io(e)
        }
    }
}

/// Parse PROXY protocol header from a TCP socket.
///
/// This function:
/// 1. Sets a read timeout on the socket
/// 2. Reads initial bytes to detect v1 vs v2
/// 3. Parses the appropriate format
/// 4. Returns the real client address and any remaining bytes
///
/// The remaining bytes must be prepended to the MQTT read buffer.
pub fn parse_proxy_header(
    socket: &TcpStream,
    timeout: Duration,
) -> Result<(SocketAddr, Vec<u8>), ProxyError> {
    use std::os::unix::io::AsRawFd;
    let fd = socket.as_raw_fd();

    // mio sockets are non-blocking, we need to temporarily set to blocking
    // and configure a read timeout for PROXY header parsing
    unsafe {
        // Get current flags
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags < 0 {
            return Err(ProxyError::Io(io::Error::last_os_error()));
        }

        // Set to blocking mode
        if libc::fcntl(fd, libc::F_SETFL, flags & !libc::O_NONBLOCK) < 0 {
            return Err(ProxyError::Io(io::Error::last_os_error()));
        }

        // Set read timeout
        #[allow(deprecated)]
        let timeout_tv = libc::timeval {
            tv_sec: timeout.as_secs() as libc::time_t,
            tv_usec: timeout.subsec_micros() as libc::suseconds_t,
        };
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &timeout_tv as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        );

        // Parse header, then restore non-blocking mode
        let result = parse_proxy_header_blocking(fd);

        // Restore non-blocking mode
        libc::fcntl(fd, libc::F_SETFL, flags);

        // Clear timeout
        clear_read_timeout(fd);

        result
    }
}

/// Internal blocking PROXY header parser.
fn parse_proxy_header_blocking(fd: i32) -> Result<(SocketAddr, Vec<u8>), ProxyError> {
    let mut buf = vec![0u8; MAX_HEADER_SIZE];
    let mut total_read = 0;

    // Read initial 16 bytes to detect version
    while total_read < 16 {
        let n = unsafe {
            libc::recv(
                fd,
                buf[total_read..].as_mut_ptr() as *mut libc::c_void,
                16 - total_read,
                0,
            )
        };
        if n < 0 {
            return Err(io::Error::last_os_error().into());
        }
        if n == 0 {
            return Err(ProxyError::ConnectionClosed);
        }
        total_read += n as usize;
    }

    // Detect version from signature
    if buf[..12] == *PROXY_V2_SIGNATURE {
        // V2: Read the full header based on length field
        let header_len = u16::from_be_bytes([buf[14], buf[15]]) as usize;
        let total_len = 16 + header_len;

        if total_len > MAX_HEADER_SIZE {
            return Err(ProxyError::InvalidHeader(format!(
                "v2 header too large: {} bytes",
                total_len
            )));
        }

        // Read remaining bytes
        while total_read < total_len {
            let n = unsafe {
                libc::recv(
                    fd,
                    buf[total_read..].as_mut_ptr() as *mut libc::c_void,
                    total_len - total_read,
                    0,
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error().into());
            }
            if n == 0 {
                return Err(ProxyError::ConnectionClosed);
            }
            total_read += n as usize;
        }

        buf.truncate(total_read);
        parse_v2_header(&buf)
    } else if buf[..6] == *PROXY_V1_SIGNATURE {
        // V1: Read until CRLF (max 107 bytes for v1)
        loop {
            if buf[..total_read].windows(2).any(|w| w == b"\r\n") {
                break;
            }
            if total_read >= 107 {
                return Err(ProxyError::InvalidHeader("v1 header too long".to_string()));
            }
            let n = unsafe {
                libc::recv(
                    fd,
                    buf[total_read..].as_mut_ptr() as *mut libc::c_void,
                    1,
                    0,
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error().into());
            }
            if n == 0 {
                return Err(ProxyError::ConnectionClosed);
            }
            total_read += n as usize;
        }

        buf.truncate(total_read);
        parse_v1_header(&buf)
    } else {
        Err(ProxyError::NotProxyProtocol)
    }
}

/// Clear read timeout on socket
fn clear_read_timeout(fd: i32) {
    let timeout_tv = libc::timeval {
        tv_sec: 0,
        tv_usec: 0,
    };
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &timeout_tv as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        );
    }
}

/// Parse a PROXY v1 (text) header.
fn parse_v1_header(buf: &[u8]) -> Result<(SocketAddr, Vec<u8>), ProxyError> {
    // Find end of header (CRLF)
    let header_end = buf
        .windows(2)
        .position(|w| w == b"\r\n")
        .ok_or_else(|| ProxyError::InvalidHeader("no CRLF found".to_string()))?
        + 2;

    // Parse using ppp crate
    match ppp::v1::Header::try_from(&buf[..header_end]) {
        Ok(header) => {
            let client_addr = match header.addresses {
                ppp::v1::Addresses::Tcp4(addrs) => {
                    SocketAddr::new(IpAddr::V4(addrs.source_address), addrs.source_port)
                }
                ppp::v1::Addresses::Tcp6(addrs) => {
                    SocketAddr::new(IpAddr::V6(addrs.source_address), addrs.source_port)
                }
                ppp::v1::Addresses::Unknown => {
                    // UNKNOWN protocol - use placeholder
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
                }
            };

            let remaining = buf[header_end..].to_vec();
            Ok((client_addr, remaining))
        }
        Err(e) => Err(ProxyError::InvalidHeader(format!(
            "v1 parse error: {:?}",
            e
        ))),
    }
}

/// Parse a PROXY v2 (binary) header.
fn parse_v2_header(buf: &[u8]) -> Result<(SocketAddr, Vec<u8>), ProxyError> {
    match ppp::v2::Header::try_from(buf) {
        Ok(header) => {
            let client_addr = match &header.addresses {
                ppp::v2::Addresses::IPv4(addrs) => {
                    SocketAddr::new(IpAddr::V4(addrs.source_address), addrs.source_port)
                }
                ppp::v2::Addresses::IPv6(addrs) => {
                    SocketAddr::new(IpAddr::V6(addrs.source_address), addrs.source_port)
                }
                ppp::v2::Addresses::Unix(_) => {
                    // Unix sockets - use placeholder IP
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
                }
                ppp::v2::Addresses::Unspecified => {
                    // LOCAL command or UNSPEC - use placeholder
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
                }
            };

            // Calculate remaining bytes (header includes 16-byte prefix + length)
            let header_len = u16::from_be_bytes([buf[14], buf[15]]) as usize;
            let total_header_len = 16 + header_len;
            let remaining = buf[total_header_len..].to_vec();

            Ok((client_addr, remaining))
        }
        Err(e) => Err(ProxyError::InvalidHeader(format!(
            "v2 parse error: {:?}",
            e
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_v1_signature() {
        assert_eq!(PROXY_V1_SIGNATURE, b"PROXY ");
    }

    #[test]
    fn test_proxy_v2_signature() {
        assert_eq!(PROXY_V2_SIGNATURE.len(), 12);
    }

    #[test]
    fn test_parse_v1_tcp4() {
        let header = b"PROXY TCP4 192.168.1.1 10.0.0.1 12345 80\r\n";
        let (addr, remaining) = parse_v1_header(header).unwrap();

        assert_eq!(addr, "192.168.1.1:12345".parse::<SocketAddr>().unwrap());
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_parse_v1_tcp4_with_remaining() {
        let mut data = b"PROXY TCP4 192.168.1.1 10.0.0.1 12345 80\r\n".to_vec();
        data.extend_from_slice(b"\x10\x04MQTT"); // MQTT CONNECT start

        let (addr, remaining) = parse_v1_header(&data).unwrap();

        assert_eq!(addr, "192.168.1.1:12345".parse::<SocketAddr>().unwrap());
        assert_eq!(remaining, b"\x10\x04MQTT");
    }

    #[test]
    fn test_parse_v1_tcp6() {
        let header = b"PROXY TCP6 ::1 ::2 12345 80\r\n";
        let (addr, remaining) = parse_v1_header(header).unwrap();

        assert_eq!(addr, "[::1]:12345".parse::<SocketAddr>().unwrap());
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_parse_v1_unknown() {
        let header = b"PROXY UNKNOWN\r\n";
        let (addr, _) = parse_v1_header(header).unwrap();

        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    }

    #[test]
    fn test_parse_v2_tcp4() {
        // Build a valid v2 header for TCP4
        let mut header = Vec::new();
        // Signature
        header.extend_from_slice(PROXY_V2_SIGNATURE);
        // Version (2) | Command (PROXY=1)
        header.push(0x21);
        // Family (AF_INET=1) | Protocol (STREAM=1)
        header.push(0x11);
        // Length (12 bytes for IPv4 addresses)
        header.extend_from_slice(&12u16.to_be_bytes());
        // Source address: 192.168.1.100
        header.extend_from_slice(&[192, 168, 1, 100]);
        // Dest address: 10.0.0.1
        header.extend_from_slice(&[10, 0, 0, 1]);
        // Source port: 12345
        header.extend_from_slice(&12345u16.to_be_bytes());
        // Dest port: 1883
        header.extend_from_slice(&1883u16.to_be_bytes());

        let (addr, remaining) = parse_v2_header(&header).unwrap();

        assert_eq!(addr, "192.168.1.100:12345".parse::<SocketAddr>().unwrap());
        assert!(remaining.is_empty());
    }
}
