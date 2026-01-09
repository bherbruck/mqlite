//! mqlite-client - Efficient MQTT client library.
//!
//! This crate provides an MQTT client implementation optimized for bridging
//! and other client applications. Uses mio for non-blocking I/O.
//!
//! # Three API Styles
//!
//! - **Polling API**: Direct control for advanced use cases and fuzz testing
//! - **Callback API**: Simple event-driven interface with `MqttHandler` trait
//! - **Async API**: Tokio-based async/await interface (feature = "async-tokio")
//!
//! # Polling API Example
//!
//! ```ignore
//! use mqlite_client::{Client, ClientConfig, ClientEvent, QoS};
//! use std::time::Duration;
//!
//! let config = ClientConfig::new("localhost:1883")
//!     .client_id("my-client")
//!     .mqtt5();
//!
//! let mut client = Client::new(config)?;
//! client.connect(None)?;
//!
//! loop {
//!     client.poll(Some(Duration::from_millis(100)))?;
//!     while let Some(event) = client.next_event() {
//!         match event {
//!             ClientEvent::Connected { session_present } => {
//!                 client.subscribe(&[("sensors/#", QoS::AtLeastOnce)])?;
//!             }
//!             ClientEvent::Message { topic, payload, .. } => {
//!                 println!("Received: {:?}", payload);
//!             }
//!             _ => {}
//!         }
//!     }
//! }
//! ```
//!
//! # Callback API Example
//!
//! ```ignore
//! use mqlite_client::{CallbackClient, Client, ClientConfig, MqttHandler, QoS};
//!
//! struct MyHandler;
//!
//! impl MqttHandler for MyHandler {
//!     fn on_connect(&mut self, client: &mut Client, session_present: bool) {
//!         client.subscribe(&[("sensors/#", QoS::AtLeastOnce)]).unwrap();
//!     }
//!
//!     fn on_message(&mut self, topic: &str, payload: &[u8], qos: QoS, retain: bool) {
//!         println!("Received on {}: {:?}", topic, payload);
//!     }
//! }
//!
//! let config = ClientConfig::new("localhost:1883").client_id("my-client");
//! let mut client = CallbackClient::new(config, MyHandler)?;
//! client.run()?;
//! ```
//!
//! # Async API Example (feature = "async-tokio")
//!
//! ```ignore
//! use mqlite_client::{AsyncClient, ClientConfig, Event, QoS};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ClientConfig::new("localhost:1883")
//!         .client_id("async-client");
//!
//!     // Split architecture: client is Clone, eventloop owns socket
//!     let (client, mut eventloop) = AsyncClient::new(config, 10);
//!
//!     // Client can be cloned and shared across tasks
//!     tokio::spawn({
//!         let client = client.clone();
//!         async move {
//!             client.subscribe(&[("sensors/#", QoS::AtLeastOnce)]).await.unwrap();
//!         }
//!     });
//!
//!     // Must poll eventloop to drive I/O
//!     while let Ok(event) = eventloop.poll().await {
//!         if let Event::Message { topic, payload, .. } = event {
//!             println!("{}: {:?}", topic, payload);
//!         }
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Features
//!
//! - `tls` (default): TLS support via rustls
//! - `async-tokio`: Async/await API via tokio

mod callback;
mod client;
mod config;
mod error;
mod events;
mod packet_id;
mod session;
mod will;

#[cfg(feature = "tls")]
mod tls;

#[cfg(feature = "async-tokio")]
mod async_client;

pub use callback::{CallbackClient, MqttHandler};
pub use client::Client;
pub use config::{BackoffConfig, ClientConfig, ConnectOptions, TlsConfig};
pub use error::{ClientError, Result};
pub use events::ClientEvent;
pub use packet_id::PacketIdAllocator;
pub use session::{PendingPublish, PendingQos2In, PendingQos2Out, Qos2OutState, Session};
pub use will::Will;

#[cfg(feature = "async-tokio")]
pub use async_client::{AsyncClient, Event, EventLoop, Message, MessageStream};

// Re-export useful types from core
pub use mqlite_core::packet::{Publish, QoS, SubscriptionOptions};
