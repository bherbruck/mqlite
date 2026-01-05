//! Client error types.

use std::io;

use thiserror::Error;

/// Client error type.
#[derive(Error, Debug)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Protocol error: {0}")]
    Protocol(#[from] mqlite_core::ProtocolError),

    #[error("Connection refused: {0}")]
    ConnectionRefused(String),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Connection timeout")]
    ConnectionTimeout,

    #[error("Not connected")]
    NotConnected,

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("TLS error: {0}")]
    Tls(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;
