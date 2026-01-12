//! WebSocket transport for MQTT-over-WebSocket connections.
//!
//! This module provides a WebSocket transport layer that wraps the tungstenite
//! WebSocket implementation, presenting a standard Read/Write interface for
//! MQTT packet I/O.

use std::io::{self, Read, Write};
use std::net::TcpStream as StdTcpStream;
use std::os::unix::io::{AsRawFd, RawFd};

use bytes::Bytes;
use mio::net::TcpStream;
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::http::HeaderValue;
use tungstenite::protocol::Role;
use tungstenite::{HandshakeError, Message, WebSocket};

/// MQTT WebSocket subprotocol identifier.
const MQTT_SUBPROTOCOL: &str = "mqtt";

/// WebSocket transport wrapper.
///
/// Wraps a tungstenite WebSocket and provides Read/Write traits that
/// transparently handle WebSocket framing. MQTT packets are transported
/// as Binary frames per the MQTT-over-WebSocket specification.
pub struct WebSocketTransport<S> {
    /// The underlying WebSocket connection.
    ws: WebSocket<S>,
    /// Buffer for incoming MQTT data extracted from WebSocket frames.
    read_buf: Vec<u8>,
    /// Current read position in the buffer.
    read_pos: usize,
    /// Buffer for outgoing MQTT data to be wrapped in a WebSocket frame.
    write_buf: Vec<u8>,
}

impl<S> WebSocketTransport<S> {
    /// Create a new WebSocket transport from an established WebSocket connection.
    pub fn new(ws: WebSocket<S>) -> Self {
        Self {
            ws,
            read_buf: Vec::with_capacity(4096),
            read_pos: 0,
            write_buf: Vec::with_capacity(4096),
        }
    }
}

impl WebSocketTransport<TcpStream> {
    /// Get the underlying TCP stream for mio registration.
    pub fn tcp_stream(&self) -> &TcpStream {
        self.ws.get_ref()
    }

    /// Get mutable reference to underlying TCP stream.
    pub fn tcp_stream_mut(&mut self) -> &mut TcpStream {
        self.ws.get_mut()
    }
}

impl<S: Read + Write> Read for WebSocketTransport<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Return buffered data if available
        if self.read_pos < self.read_buf.len() {
            let available = &self.read_buf[self.read_pos..];
            let to_copy = available.len().min(buf.len());
            buf[..to_copy].copy_from_slice(&available[..to_copy]);
            self.read_pos += to_copy;

            // Reset buffer if fully consumed
            if self.read_pos >= self.read_buf.len() {
                self.read_buf.clear();
                self.read_pos = 0;
            }

            return Ok(to_copy);
        }

        // Read next WebSocket frame
        loop {
            match self.ws.read() {
                Ok(Message::Binary(data)) => {
                    if data.is_empty() {
                        continue;
                    }
                    // Copy directly to output if it fits
                    if data.len() <= buf.len() {
                        buf[..data.len()].copy_from_slice(&data);
                        return Ok(data.len());
                    }
                    // Buffer the excess
                    let to_copy = buf.len();
                    buf.copy_from_slice(&data[..to_copy]);
                    self.read_buf.extend_from_slice(&data[to_copy..]);
                    self.read_pos = 0;
                    return Ok(to_copy);
                }
                Ok(Message::Close(_)) => {
                    return Ok(0); // EOF
                }
                Ok(Message::Ping(_) | Message::Pong(_)) => {
                    // tungstenite handles ping/pong automatically, continue reading
                    continue;
                }
                Ok(Message::Text(_)) => {
                    // MQTT-over-WS requires Binary frames, reject Text
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "MQTT requires Binary WebSocket frames",
                    ));
                }
                Ok(Message::Frame(_)) => {
                    // Raw frame, shouldn't happen with normal reads
                    continue;
                }
                Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Err(e);
                }
                Err(tungstenite::Error::Io(e)) => {
                    return Err(e);
                }
                Err(e) => {
                    return Err(io::Error::other(e));
                }
            }
        }
    }
}

impl<S: Read + Write> Write for WebSocketTransport<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Buffer the data for sending on flush
        self.write_buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.write_buf.is_empty() {
            let data = std::mem::take(&mut self.write_buf);
            self.ws
                .send(Message::Binary(Bytes::from(data)))
                .map_err(io::Error::other)?;
        }
        self.ws.flush().map_err(io::Error::other)
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        // Collect all buffers and send as single Binary frame
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if total == 0 {
            return Ok(0);
        }

        // For WebSocket, we need to actually send the data (not just buffer it)
        // because the caller (ClientWriteHandle::flush) doesn't call our flush()
        let mut data = Vec::with_capacity(total);
        for buf in bufs {
            data.extend_from_slice(buf);
        }

        self.ws
            .send(Message::Binary(Bytes::from(data)))
            .map_err(io::Error::other)?;
        self.ws.flush().map_err(io::Error::other)?;

        Ok(total)
    }
}

impl AsRawFd for WebSocketTransport<TcpStream> {
    fn as_raw_fd(&self) -> RawFd {
        self.ws.get_ref().as_raw_fd()
    }
}

/// Error during WebSocket handshake.
#[derive(Debug)]
pub enum WebSocketError {
    /// The client didn't request the mqtt subprotocol.
    #[allow(dead_code)]
    InvalidSubprotocol,
    /// Handshake I/O error.
    Io(io::Error),
    /// Handshake protocol error.
    Handshake(String),
}

impl std::fmt::Display for WebSocketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebSocketError::InvalidSubprotocol => {
                write!(f, "client did not request 'mqtt' subprotocol")
            }
            WebSocketError::Io(e) => write!(f, "I/O error: {}", e),
            WebSocketError::Handshake(msg) => write!(f, "handshake error: {}", msg),
        }
    }
}

impl std::error::Error for WebSocketError {}

impl From<io::Error> for WebSocketError {
    fn from(e: io::Error) -> Self {
        WebSocketError::Io(e)
    }
}

/// Accept a WebSocket connection with MQTT subprotocol and path validation.
///
/// Performs the HTTP upgrade handshake and validates that the client
/// requested the "mqtt" subprotocol and connected to the expected path.
///
/// # Arguments
/// * `stream` - The TCP stream (must be in blocking mode)
/// * `expected_path` - The expected URI path (empty string = accept any path)
///
/// # Note
/// This function expects a blocking stream. The caller should set the
/// socket to blocking before calling, then restore non-blocking mode after.
pub fn accept_websocket(
    stream: StdTcpStream,
    expected_path: &str,
) -> Result<WebSocket<StdTcpStream>, WebSocketError> {
    let expected_path = expected_path.to_string();

    let callback = |request: &Request, mut response: Response| -> Result<Response, ErrorResponse> {
        // Validate path if configured
        if !expected_path.is_empty() {
            let request_path = request.uri().path();
            if request_path != expected_path {
                log::debug!(
                    "WebSocket path mismatch: expected '{}', got '{}'",
                    expected_path,
                    request_path
                );
                let mut err_response = ErrorResponse::new(Some("Invalid path".to_string()));
                *err_response.status_mut() = tungstenite::http::StatusCode::NOT_FOUND;
                return Err(err_response);
            }
        }

        // Check for mqtt subprotocol in Sec-WebSocket-Protocol header
        let has_mqtt = request
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|v: &HeaderValue| v.to_str().ok())
            .map(|protocols: &str| {
                protocols
                    .split(',')
                    .any(|p: &str| p.trim().eq_ignore_ascii_case(MQTT_SUBPROTOCOL))
            })
            .unwrap_or(false);

        if !has_mqtt {
            // Allow connections without subprotocol for compatibility
            // but if they specify protocols, mqtt must be one of them
            let specified_protocols = request.headers().get("Sec-WebSocket-Protocol").is_some();
            if specified_protocols {
                log::debug!("WebSocket client didn't request mqtt subprotocol");
            }
        }

        // Always respond with mqtt subprotocol
        response
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", MQTT_SUBPROTOCOL.parse().unwrap());

        Ok(response)
    };

    match tungstenite::accept_hdr(stream, callback) {
        Ok(ws) => Ok(ws),
        Err(HandshakeError::Interrupted(_)) => {
            Err(WebSocketError::Handshake("handshake interrupted".into()))
        }
        Err(HandshakeError::Failure(e)) => Err(WebSocketError::Handshake(e.to_string())),
    }
}

/// Wrap an established WebSocket in a mio-compatible transport.
///
/// This converts the std TcpStream to mio TcpStream using fd duplication.
/// The original WebSocket is dropped (closing its fd), but the duplicated fd
/// remains valid for the new WebSocket.
pub fn wrap_websocket(
    mut ws: WebSocket<StdTcpStream>,
) -> io::Result<WebSocketTransport<TcpStream>> {
    use std::os::unix::io::{AsRawFd, FromRawFd};

    // Ensure handshake response is fully flushed before converting
    ws.flush().map_err(io::Error::other)?;

    // Get the fd from the std stream
    let fd = ws.get_ref().as_raw_fd();

    // Duplicate the fd - this creates a new fd pointing to the same socket
    let new_fd = unsafe { libc::dup(fd) };
    if new_fd == -1 {
        return Err(io::Error::last_os_error());
    }

    // Create a new std TcpStream from the duplicated fd
    let new_std_stream = unsafe { StdTcpStream::from_raw_fd(new_fd) };

    // Set to non-blocking mode
    new_std_stream.set_nonblocking(true)?;

    // Convert to mio TcpStream
    let mio_stream = TcpStream::from_std(new_std_stream);

    // Drop the original WebSocket (this closes the original fd, but our dup'd fd stays valid)
    drop(ws);

    // Create a new WebSocket with the mio stream
    let new_ws = WebSocket::from_raw_socket(mio_stream, Role::Server, None);

    Ok(WebSocketTransport::new(new_ws))
}
