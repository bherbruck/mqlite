//! Prometheus metrics HTTP endpoint.
//!
//! Provides a minimal HTTP server that exposes broker metrics in Prometheus
//! exposition format. Runs in a dedicated thread with blocking I/O.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use log::{debug, error, info, warn};

use crate::shared::SharedState;

/// Start the Prometheus metrics HTTP server in a background thread.
pub fn start_metrics_server(bind: SocketAddr, shared: Arc<SharedState>, start_time: Instant) {
    thread::Builder::new()
        .name("prometheus".to_string())
        .spawn(move || {
            if let Err(e) = run_metrics_server(bind, shared, start_time) {
                error!("Prometheus metrics server error: {}", e);
            }
        })
        .expect("Failed to spawn prometheus thread");

    info!("Prometheus metrics endpoint enabled at http://{}/metrics", bind);
}

/// Run the metrics HTTP server (blocking).
fn run_metrics_server(
    bind: SocketAddr,
    shared: Arc<SharedState>,
    start_time: Instant,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind)?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(e) = handle_request(stream, &shared, start_time) {
                    debug!("Metrics request error: {}", e);
                }
            }
            Err(e) => {
                warn!("Metrics accept error: {}", e);
            }
        }
    }

    Ok(())
}

/// Handle a single HTTP request.
fn handle_request(
    mut stream: TcpStream,
    shared: &SharedState,
    start_time: Instant,
) -> std::io::Result<()> {
    // Set a short timeout for reading
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;

    let mut reader = BufReader::new(&stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;

    // Parse request line: "GET /metrics HTTP/1.1"
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return send_response(&mut stream, 400, "Bad Request", "Invalid request");
    }

    let method = parts[0];
    let path = parts[1];

    // Only handle GET /metrics
    if method != "GET" {
        return send_response(&mut stream, 405, "Method Not Allowed", "Only GET is supported");
    }

    if path != "/metrics" && path != "/metrics/" {
        return send_response(&mut stream, 404, "Not Found", "Use /metrics");
    }

    // Drain the rest of the request headers
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            break;
        }
    }

    // Generate metrics
    let body = format_metrics(shared, start_time);
    send_response(
        &mut stream,
        200,
        "OK",
        &body,
    )
}

/// Send an HTTP response.
fn send_response(stream: &mut TcpStream, status: u16, status_text: &str, body: &str) -> std::io::Result<()> {
    let content_type = if status == 200 {
        "text/plain; version=0.0.4; charset=utf-8"
    } else {
        "text/plain; charset=utf-8"
    };

    let response = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: {}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        status, status_text, content_type, body.len(), body
    );

    stream.write_all(response.as_bytes())?;
    stream.flush()
}

/// Format all metrics in Prometheus exposition format.
fn format_metrics(shared: &SharedState, start_time: Instant) -> String {
    let metrics = &shared.metrics;
    let mut out = String::with_capacity(4096);

    // Helper macro for counter metrics
    macro_rules! counter {
        ($name:expr, $help:expr, $value:expr) => {
            out.push_str("# HELP ");
            out.push_str($name);
            out.push(' ');
            out.push_str($help);
            out.push('\n');
            out.push_str("# TYPE ");
            out.push_str($name);
            out.push_str(" counter\n");
            out.push_str($name);
            out.push(' ');
            out.push_str(&$value.to_string());
            out.push('\n');
        };
    }

    // Helper macro for gauge metrics
    macro_rules! gauge {
        ($name:expr, $help:expr, $value:expr) => {
            out.push_str("# HELP ");
            out.push_str($name);
            out.push(' ');
            out.push_str($help);
            out.push('\n');
            out.push_str("# TYPE ");
            out.push_str($name);
            out.push_str(" gauge\n");
            out.push_str($name);
            out.push(' ');
            out.push_str(&$value.to_string());
            out.push('\n');
        };
    }

    // Uptime
    gauge!(
        "mqlite_uptime_seconds",
        "Broker uptime in seconds",
        start_time.elapsed().as_secs()
    );

    // Client metrics
    gauge!(
        "mqlite_clients_connected",
        "Number of currently connected clients",
        metrics.clients_connected.load(Ordering::Relaxed)
    );

    gauge!(
        "mqlite_clients_maximum",
        "Maximum number of concurrently connected clients",
        metrics.clients_maximum.load(Ordering::Relaxed)
    );

    counter!(
        "mqlite_clients_total",
        "Total number of client connections",
        metrics.connections_total.load(Ordering::Relaxed)
    );

    counter!(
        "mqlite_clients_expired_total",
        "Total number of expired client sessions",
        metrics.clients_expired.load(Ordering::Relaxed)
    );

    counter!(
        "mqlite_sockets_opened_total",
        "Total number of socket connections opened",
        metrics.sockets_opened.load(Ordering::Relaxed)
    );

    // Message metrics
    counter!(
        "mqlite_messages_received_total",
        "Total MQTT messages received",
        metrics.msgs_received.load(Ordering::Relaxed)
    );

    counter!(
        "mqlite_messages_sent_total",
        "Total MQTT messages sent",
        metrics.msgs_sent.load(Ordering::Relaxed)
    );

    // Publish metrics
    counter!(
        "mqlite_publish_messages_received_total",
        "Total PUBLISH messages received",
        metrics.pub_msgs_received.load(Ordering::Relaxed)
    );

    counter!(
        "mqlite_publish_messages_sent_total",
        "Total PUBLISH messages sent to subscribers",
        metrics.pub_msgs_sent.load(Ordering::Relaxed)
    );

    counter!(
        "mqlite_publish_messages_dropped_total",
        "Total PUBLISH messages dropped due to backpressure",
        metrics.pub_msgs_dropped.load(Ordering::Relaxed)
    );

    counter!(
        "mqlite_publish_bytes_received_total",
        "Total PUBLISH payload bytes received",
        metrics.pub_bytes_received.load(Ordering::Relaxed)
    );

    counter!(
        "mqlite_publish_bytes_sent_total",
        "Total PUBLISH payload bytes sent",
        metrics.pub_bytes_sent.load(Ordering::Relaxed)
    );

    // Byte metrics
    counter!(
        "mqlite_bytes_received_total",
        "Total bytes received",
        metrics.bytes_received.load(Ordering::Relaxed)
    );

    counter!(
        "mqlite_bytes_sent_total",
        "Total bytes sent",
        metrics.bytes_sent.load(Ordering::Relaxed)
    );

    // Subscription metrics (requires read lock)
    let subscriptions_count = shared.subscriptions.read().subscription_count();
    gauge!(
        "mqlite_subscriptions_count",
        "Current number of subscriptions",
        subscriptions_count
    );

    // Retained message metrics
    let (retained_count, retained_bytes) = {
        let retained = shared.retained_messages.read();
        let count = retained.len();
        let bytes: usize = retained.values().map(|r| r.publish.payload.len()).sum();
        (count, bytes)
    };

    gauge!(
        "mqlite_retained_messages_count",
        "Number of retained messages stored",
        retained_count
    );

    gauge!(
        "mqlite_retained_messages_bytes",
        "Total bytes of retained message payloads",
        retained_bytes
    );

    // Session metrics
    let sessions_count = shared.sessions.read().len();
    gauge!(
        "mqlite_sessions_count",
        "Number of persistent sessions",
        sessions_count
    );

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::SharedState;

    #[test]
    fn test_format_metrics() {
        let shared = Arc::new(SharedState::new());
        let start_time = Instant::now();

        // Add some test data
        shared.metrics.add_msgs_received(100);
        shared.metrics.client_connected();
        shared.metrics.client_connected();

        let output = format_metrics(&shared, start_time);

        assert!(output.contains("mqlite_uptime_seconds"));
        assert!(output.contains("mqlite_clients_connected 2"));
        assert!(output.contains("mqlite_messages_received_total 100"));
        assert!(output.contains("# TYPE mqlite_clients_connected gauge"));
        assert!(output.contains("# TYPE mqlite_messages_received_total counter"));
    }
}
