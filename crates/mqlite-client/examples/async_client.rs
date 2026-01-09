//! Async API example - tokio-based async/await interface.
//!
//! Run with: cargo run -p mqlite-client-examples --bin async-client
//!
//! This example demonstrates:
//! - Split architecture (AsyncClient + EventLoop)
//! - subscribe_stream() for per-subscription message handling
//! - Sharing client across tasks

use std::time::Duration;

use mqlite_client::{AsyncClient, ClientConfig, Event, QoS};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new("localhost:1883")
        .client_id("async-example")
        .clean_session(true)
        .keep_alive(30);

    // Create client + eventloop pair
    // Client is Clone - can be shared across tasks
    // EventLoop owns the socket - must be polled
    let (client, mut eventloop) = AsyncClient::new(config, 10);

    // Wait for connection, then set up subscriptions
    let setup_client = client.clone();
    tokio::spawn(async move {
        // Wait for connection
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Use subscribe_stream for per-subscription handling
        // Each stream only receives messages matching its filter
        let mut sensors = match setup_client
            .subscribe_stream("example/sensors/#", QoS::AtLeastOnce)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to subscribe to sensors: {}", e);
                return;
            }
        };
        println!("Subscribed to example/sensors/#");

        // Handle sensor messages in this task
        tokio::spawn(async move {
            while let Some(msg) = sensors.recv().await {
                println!(
                    "[SENSOR] {} = {}",
                    msg.topic,
                    String::from_utf8_lossy(&msg.payload)
                );
            }
        });

        // Subscribe to another topic with its own stream
        let mut commands = match setup_client
            .subscribe_stream("example/commands/#", QoS::AtLeastOnce)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to subscribe to commands: {}", e);
                return;
            }
        };
        println!("Subscribed to example/commands/#");

        // Handle command messages in this task
        tokio::spawn(async move {
            while let Some(msg) = commands.recv().await {
                println!(
                    "[COMMAND] {} = {}",
                    msg.topic,
                    String::from_utf8_lossy(&msg.payload)
                );
            }
        });

        // Publish some test messages
        tokio::time::sleep(Duration::from_millis(100)).await;

        let _ = setup_client
            .publish("example/sensors/temp", b"23.5", QoS::AtLeastOnce, false)
            .await;
        let _ = setup_client
            .publish("example/sensors/humidity", b"65%", QoS::AtLeastOnce, false)
            .await;
        let _ = setup_client
            .publish("example/commands/restart", b"now", QoS::AtLeastOnce, false)
            .await;

        println!("Published test messages");
    });

    // Poll the eventloop - this drives all I/O
    // Messages are routed to their streams automatically
    let mut count = 0;
    let start = std::time::Instant::now();

    loop {
        match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
            Ok(Ok(event)) => {
                match &event {
                    Event::Connected { .. } => println!("Connected!"),
                    Event::SubAck { packet_id, .. } => {
                        println!("SubAck: packet_id={}", packet_id);
                        count += 1;
                    }
                    Event::PubAck { packet_id } => {
                        println!("PubAck: packet_id={}", packet_id);
                        count += 1;
                    }
                    Event::Message { topic, .. } => {
                        // This only fires for messages that don't match any stream
                        println!("Unrouted message on: {}", topic);
                    }
                    Event::Disconnected => {
                        println!("Disconnected");
                        break;
                    }
                    _ => {}
                }

                // Exit after receiving some acks
                if count >= 5 {
                    break;
                }
            }
            Ok(Err(e)) => {
                eprintln!("EventLoop error: {}", e);
                break;
            }
            Err(_) => {
                // Timeout - check if we should exit
                if start.elapsed() > Duration::from_secs(5) {
                    println!("Timeout");
                    break;
                }
            }
        }
    }

    client.disconnect().await?;
    println!("Done!");

    Ok(())
}
