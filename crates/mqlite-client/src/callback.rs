//! Callback-based MQTT client API.
//!
//! Provides an event-driven interface using the `MqttHandler` trait.

use std::time::Duration;

use mqlite_core::packet::QoS;

use crate::client::Client;
use crate::config::ClientConfig;
use crate::error::{ClientError, Result};
use crate::events::ClientEvent;

/// Handler trait for MQTT events.
///
/// Implement this trait to handle MQTT events in a callback style.
/// All methods have default implementations that do nothing.
pub trait MqttHandler {
    /// Called when connected to the broker.
    ///
    /// # Arguments
    /// * `client` - Reference to the client for sending messages
    /// * `session_present` - Whether a previous session was restored
    #[allow(unused_variables)]
    fn on_connect(&mut self, client: &mut Client, session_present: bool) {}

    /// Called when a message is received.
    ///
    /// # Arguments
    /// * `topic` - The topic the message was published to
    /// * `payload` - The message payload
    /// * `qos` - The QoS level
    /// * `retain` - Whether this is a retained message
    #[allow(unused_variables)]
    fn on_message(&mut self, topic: &str, payload: &[u8], qos: QoS, retain: bool) {}

    /// Called when a QoS 1/2 publish has been acknowledged.
    ///
    /// # Arguments
    /// * `packet_id` - The packet identifier of the acknowledged publish
    #[allow(unused_variables)]
    fn on_published(&mut self, packet_id: u16) {}

    /// Called when a subscribe request has been acknowledged.
    ///
    /// # Arguments
    /// * `packet_id` - The packet identifier of the subscribe request
    /// * `return_codes` - The return codes for each topic (0x00-0x02 = granted QoS, 0x80 = failure)
    #[allow(unused_variables)]
    fn on_subscribed(&mut self, packet_id: u16, return_codes: &[u8]) {}

    /// Called when an unsubscribe request has been acknowledged.
    ///
    /// # Arguments
    /// * `packet_id` - The packet identifier of the unsubscribe request
    #[allow(unused_variables)]
    fn on_unsubscribed(&mut self, packet_id: u16) {}

    /// Called when disconnected from the broker.
    ///
    /// # Arguments
    /// * `reason` - The reason for disconnection, if known
    #[allow(unused_variables)]
    fn on_disconnect(&mut self, reason: Option<&str>) {}

    /// Called when an error occurs.
    ///
    /// # Arguments
    /// * `error` - The error that occurred
    #[allow(unused_variables)]
    fn on_error(&mut self, error: &ClientError) {}
}

/// Callback-based MQTT client.
///
/// Wraps the core `Client` and dispatches events to a handler.
pub struct CallbackClient<H: MqttHandler> {
    client: Client,
    handler: H,
    running: bool,
}

impl<H: MqttHandler> CallbackClient<H> {
    /// Create a new callback client.
    pub fn new(config: ClientConfig, handler: H) -> Result<Self> {
        let client = Client::new(config)?;
        Ok(Self {
            client,
            handler,
            running: false,
        })
    }

    /// Connect to the broker and start receiving events.
    ///
    /// This does not block. Use `run()` to start the event loop.
    pub fn connect(&mut self) -> Result<()> {
        self.client.connect(None)
    }

    /// Publish a message.
    pub fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<Option<u16>> {
        self.client.publish(topic, payload, qos, retain)
    }

    /// Subscribe to topics.
    pub fn subscribe(&mut self, topics: &[(&str, QoS)]) -> Result<u16> {
        self.client.subscribe(topics)
    }

    /// Unsubscribe from topics.
    pub fn unsubscribe(&mut self, topics: &[&str]) -> Result<u16> {
        self.client.unsubscribe(topics)
    }

    /// Disconnect from the broker.
    pub fn disconnect(&mut self) -> Result<()> {
        self.running = false;
        self.client.disconnect()
    }

    /// Stop the event loop.
    pub fn stop(&mut self) {
        self.running = false;
    }

    /// Check if connected.
    pub fn is_connected(&self) -> bool {
        self.client.is_connected()
    }

    /// Get a reference to the handler.
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// Get a mutable reference to the handler.
    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.handler
    }

    /// Get a reference to the underlying client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get a mutable reference to the underlying client.
    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Run the event loop until disconnected or `stop()` is called.
    ///
    /// This method blocks and processes events, dispatching them to the handler.
    pub fn run(&mut self) -> Result<()> {
        self.running = true;
        while self.running {
            self.poll_once(Some(Duration::from_millis(100)))?;
        }
        Ok(())
    }

    /// Run the event loop until the condition returns true.
    ///
    /// # Arguments
    /// * `condition` - Called after each poll, returns true to stop
    pub fn run_until<F>(&mut self, mut condition: F) -> Result<()>
    where
        F: FnMut(&Client) -> bool,
    {
        self.running = true;
        while self.running {
            self.poll_once(Some(Duration::from_millis(100)))?;
            if condition(&self.client) {
                break;
            }
        }
        Ok(())
    }

    /// Poll once and dispatch any events.
    pub fn poll_once(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.client.poll(timeout)?;

        while let Some(event) = self.client.next_event() {
            self.dispatch_event(event);
        }

        Ok(())
    }

    /// Dispatch an event to the handler.
    fn dispatch_event(&mut self, event: ClientEvent) {
        match event {
            ClientEvent::Connected { session_present } => {
                self.handler.on_connect(&mut self.client, session_present);
            }
            ClientEvent::Disconnected { reason } => {
                self.handler.on_disconnect(reason.as_deref());
                self.running = false;
            }
            ClientEvent::Message {
                topic,
                payload,
                qos,
                retain,
                ..
            } => {
                // Convert Bytes to &str safely
                if let Ok(topic_str) = std::str::from_utf8(&topic) {
                    self.handler.on_message(topic_str, &payload, qos, retain);
                }
            }
            ClientEvent::PubAck { packet_id } | ClientEvent::PubComp { packet_id } => {
                self.handler.on_published(packet_id);
            }
            ClientEvent::SubAck {
                packet_id,
                return_codes,
            } => {
                self.handler.on_subscribed(packet_id, &return_codes);
            }
            ClientEvent::UnsubAck { packet_id } => {
                self.handler.on_unsubscribed(packet_id);
            }
            ClientEvent::PubRec { .. } => {
                // Internal QoS 2 flow, don't notify handler
            }
        }
    }
}
