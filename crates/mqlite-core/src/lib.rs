//! mqlite-core - Core MQTT types and utilities.
//!
//! This crate provides the fundamental types for MQTT packet encoding/decoding,
//! shared between the server and client crates.

pub mod error;
pub mod packet;
pub mod varint;

pub use error::{Error, ProtocolError, Result};
pub use packet::*;
