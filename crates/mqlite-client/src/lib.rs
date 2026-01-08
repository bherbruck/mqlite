//! mqlite-client - Efficient MQTT client library.
//!
//! This crate provides an MQTT client implementation optimized for bridging
//! and other client applications. Uses mio for non-blocking I/O.
//!
//! # Example
//!
//! ```ignore
//! use mqlite_client::{Client, ClientConfig, QoS};
//!
//! let config = ClientConfig::new("localhost:1883")
//!     .client_id("my-client")
//!     .mqtt5();
//!
//! let mut client = Client::new(config)?;
//! client.connect(None)?;
//!
//! // Subscribe to topics
//! client.subscribe(&[("sensors/#", QoS::AtLeastOnce)])?;
//!
//! // Publish a message
//! client.publish("sensors/temp", b"25.5", QoS::AtMostOnce, false)?;
//! ```
//!
//! # MQTT 5.0 Subscription Options
//!
//! Use `subscribe_with_options` for full MQTT 5.0 subscription control:
//!
//! ```ignore
//! use mqlite_client::{Client, SubscriptionOptions, QoS};
//!
//! // Subscribe with NoLocal to prevent receiving own publishes (bridge loop prevention)
//! client.subscribe_with_options(&[
//!     ("sensors/#", SubscriptionOptions {
//!         qos: QoS::AtLeastOnce,
//!         no_local: true,
//!         ..Default::default()
//!     }),
//! ])?;
//! ```

mod client;
mod config;
mod error;
mod events;

pub use client::Client;
pub use config::{ClientConfig, ConnectOptions};
pub use error::{ClientError, Result};
pub use events::ClientEvent;

// Re-export useful types from core
pub use mqlite_core::packet::{Publish, QoS, SubscriptionOptions};
