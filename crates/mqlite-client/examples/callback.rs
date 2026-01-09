//! Callback API example - simple event-driven interface.
//!
//! Run with: cargo run -p mqlite-client-examples --bin callback
//!
//! This style is ideal for:
//! - Simple applications
//! - When you want the library to manage the event loop
//! - Familiar pattern from other MQTT libraries

use std::sync::atomic::{AtomicUsize, Ordering};

use mqlite_client::{CallbackClient, Client, ClientConfig, MqttHandler, QoS};

/// Our handler struct - can hold any state you need
struct MyHandler {
    message_count: AtomicUsize,
}

impl MyHandler {
    fn new() -> Self {
        Self {
            message_count: AtomicUsize::new(0),
        }
    }
}

impl MqttHandler for MyHandler {
    fn on_connect(&mut self, client: &mut Client, session_present: bool) {
        println!("Connected! Session present: {}", session_present);

        // Subscribe to topics
        if let Err(e) = client.subscribe(&[
            ("example/callback/#", QoS::AtLeastOnce),
        ]) {
            eprintln!("Subscribe error: {}", e);
        }

        // Publish a test message
        if let Err(e) = client.publish(
            "example/callback/hello",
            b"Hello from callback client!",
            QoS::AtLeastOnce,
            false,
        ) {
            eprintln!("Publish error: {}", e);
        }
    }

    fn on_message(&mut self, topic: &str, payload: &[u8], qos: QoS, retain: bool) {
        let payload_str = String::from_utf8_lossy(payload);
        println!(
            "Message: {} -> {} (QoS={:?}, retain={})",
            topic, payload_str, qos, retain
        );

        let count = self.message_count.fetch_add(1, Ordering::SeqCst) + 1;
        println!("Total messages received: {}", count);
    }

    fn on_published(&mut self, packet_id: u16) {
        println!("Publish acknowledged (packet_id={})", packet_id);
    }

    fn on_subscribed(&mut self, packet_id: u16, return_codes: &[u8]) {
        println!("Subscribed (packet_id={}): {:?}", packet_id, return_codes);
    }

    fn on_disconnect(&mut self, reason: Option<&str>) {
        println!("Disconnected: {:?}", reason);
    }

    fn on_error(&mut self, error: &mqlite_client::ClientError) {
        eprintln!("Error: {}", error);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure the client
    let config = ClientConfig::new("localhost:1883")
        .client_id("callback-example")
        .clean_session(true)
        .keep_alive(30);

    // Create the callback client with our handler
    let handler = MyHandler::new();
    let mut client = CallbackClient::new(config, handler)?;

    println!("Connecting to broker...");
    client.connect()?;

    // Run until we've received at least one message or disconnected
    let start = std::time::Instant::now();
    client.run_until(|c| {
        // Stop if disconnected
        if !c.is_connected() && start.elapsed() > std::time::Duration::from_millis(500) {
            return true;
        }
        // Stop after 2 seconds (we should have received our message by then)
        start.elapsed() > std::time::Duration::from_secs(2)
    })?;

    // Or use client.run()? to run forever until disconnect

    // Access final handler state
    let final_count = client.handler().message_count.load(Ordering::SeqCst);
    println!("Final message count: {}", final_count);

    Ok(())
}
