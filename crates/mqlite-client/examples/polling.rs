//! Polling API example - direct control for advanced use cases.
//!
//! Run with: cargo run -p mqlite-client-examples --bin polling
//!
//! This style is ideal for:
//! - Fuzz testing (precise control over timing)
//! - Custom event loops
//! - Embedding in game loops or other polling systems

use std::time::Duration;

use mqlite_client::{Client, ClientConfig, ClientEvent, QoS};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure the client
    let config = ClientConfig::new("localhost:1883")
        .client_id("polling-example")
        .clean_session(true)
        .keep_alive(30);

    // Create and connect
    let mut client = Client::new(config)?;
    println!("Connecting to broker...");
    client.connect(None)?;

    let mut connected = false;
    let mut message_count = 0;

    // Main event loop
    loop {
        // Poll with 100ms timeout
        client.poll(Some(Duration::from_millis(100)))?;

        // Process all pending events
        while let Some(event) = client.next_event() {
            match event {
                ClientEvent::Connected { session_present } => {
                    println!(
                        "Connected! Session present: {}",
                        session_present
                    );
                    connected = true;

                    // Subscribe to topics
                    client.subscribe(&[
                        ("example/polling/#", QoS::AtLeastOnce),
                    ])?;

                    // Publish a test message
                    client.publish(
                        "example/polling/hello",
                        b"Hello from polling client!",
                        QoS::AtLeastOnce,
                        false,
                    )?;
                }

                ClientEvent::SubAck { packet_id, return_codes } => {
                    println!(
                        "Subscribed (packet_id={}): {:?}",
                        packet_id, return_codes
                    );
                }

                ClientEvent::Message { topic, payload, qos, retain, .. } => {
                    let topic_str = String::from_utf8_lossy(&topic);
                    let payload_str = String::from_utf8_lossy(&payload);
                    println!(
                        "Message: {} -> {} (QoS={:?}, retain={})",
                        topic_str, payload_str, qos, retain
                    );
                    message_count += 1;

                    // Exit after receiving our own message
                    if message_count >= 1 {
                        println!("Received {} message(s), disconnecting...", message_count);
                        client.disconnect()?;
                    }
                }

                ClientEvent::PubAck { packet_id } => {
                    println!("Publish acknowledged (packet_id={})", packet_id);
                }

                ClientEvent::Disconnected { reason } => {
                    println!("Disconnected: {:?}", reason);
                    return Ok(());
                }

                _ => {}
            }
        }

        // You can do other work here between polls
        if connected && client.pending_count() > 0 {
            println!("Pending messages: {}", client.pending_count());
        }
    }
}
