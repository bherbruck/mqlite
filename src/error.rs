//! Error types for mqlite.

use std::io;

use thiserror::Error;

/// Main error type for mqlite.
#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("Client error: {0}")]
    #[allow(dead_code)]
    Client(String),
}

/// MQTT protocol errors.
#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Invalid packet type: {0}")]
    InvalidPacketType(u8),

    #[error("Invalid remaining length encoding")]
    InvalidRemainingLength,

    #[error("Incomplete packet: need {needed} bytes, have {have}")]
    IncompletePacket { needed: usize, have: usize },

    #[error("Invalid protocol name: expected 'MQTT', got '{0}'")]
    InvalidProtocolName(String),

    #[error("Unsupported protocol version: {0}")]
    UnsupportedProtocolVersion(u8),

    #[error("Invalid connect flags: {0:#04x}")]
    InvalidConnectFlags(u8),

    #[error("Invalid UTF-8 string")]
    InvalidUtf8,

    #[error("Malformed packet: {0}")]
    MalformedPacket(String),

    #[error("First packet must be CONNECT")]
    FirstPacketNotConnect,
}

pub type Result<T> = std::result::Result<T, Error>;
