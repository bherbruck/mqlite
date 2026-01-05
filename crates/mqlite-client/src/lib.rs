//! mqlite-client - Efficient MQTT client library.
//!
//! This crate provides an MQTT client implementation optimized for bridging
//! and other client applications. Uses mio for non-blocking I/O.

mod client;
mod error;

pub use client::{Client, ClientConfig, ClientEvent, ConnectOptions};
pub use error::{ClientError, Result};

// Re-export useful types from core
pub use mqlite_core::packet::{Publish, QoS};
