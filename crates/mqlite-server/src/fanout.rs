//! Publish message fanout to subscribers.
//!
//! This module handles delivering publish messages to matching subscribers,
//! including:
//! - QoS downgrade based on subscriber's max QoS
//! - Flow control (MQTT 5 quotas)
//! - Backpressure handling for slow clients
//! - Pending message tracking for QoS 1/2
//! - Cross-worker message delivery
//!
//! Note: This module provides reusable fanout functions used by worker.rs
//! for will message delivery and (future) regular publish fanout.

use std::io;
use std::sync::Arc;
use std::time::Instant;

use ahash::AHashMap;
use mio::Token;

use mqlite_core::packet::{Publish, QoS};

use crate::client::{Client, PendingPublish};
use crate::publish_encoder::PublishEncoder;
use crate::shared::{Session, SharedStateHandle};
use crate::subscription::Subscriber;
use crate::util::RateLimitedCounter;

/// Result of a fanout operation.
#[derive(Default)]
pub struct FanoutResult {
    /// Number of messages dropped due to soft limit backpressure (QoS 0).
    pub backpressure_drops: u32,
    /// Last subscriber that experienced backpressure.
    pub last_backpressure_sub: Option<(usize, Token)>,
    /// Number of messages dropped due to hard limit (16MB buffer exceeded).
    pub hardlimit_drops: u32,
    /// Last subscriber that exceeded hard limit.
    pub last_hardlimit_sub: Option<(usize, Token)>,
}

/// Cross-worker pending message to be written to session store.
pub struct CrossWorkerPending {
    pub client_id: Arc<str>,
    pub packet_id: u16,
    pub qos: QoS,
    pub publish: Publish,
}

/// Context for a fanout operation.
pub struct FanoutContext<'a> {
    /// Worker ID performing the fanout.
    pub worker_id: usize,
    /// Token of the publishing client (for NoLocal checks).
    pub from_token: Option<Token>,
    /// Local clients owned by this worker.
    pub clients: &'a mut AHashMap<Token, Client>,
    /// Shared state handle for session access.
    pub shared: &'a SharedStateHandle,
    /// Publish encoder for zero-copy message encoding.
    pub factory: &'a mut PublishEncoder,
    /// The original publish message.
    pub publish: &'a Publish,
}

/// Fanout a publish message to a list of subscribers.
///
/// Returns the fanout result and a list of cross-worker pending messages
/// that need to be written to the session store.
pub fn fanout_to_subscribers(
    ctx: &mut FanoutContext<'_>,
    subscribers: &[Subscriber],
) -> (FanoutResult, Vec<CrossWorkerPending>) {
    let mut result = FanoutResult::default();
    let mut cross_worker_pending = Vec::new();
    let payload_len = ctx.publish.payload.len() as u64;

    for sub in subscribers {
        // MQTT-3.8.3.1-2: NoLocal - don't deliver to publishing client
        if sub.options.no_local {
            if let Some(from_token) = ctx.from_token {
                if sub.token() == from_token && sub.worker_id() == ctx.worker_id {
                    continue;
                }
            }
        }

        let worker_id = sub.worker_id();
        let sub_token = sub.token();
        let effective_qos = std::cmp::min(ctx.publish.qos as u8, sub.qos as u8);
        let out_qos = match QoS::try_from(effective_qos) {
            Ok(q) => q,
            Err(_) => continue,
        };

        // Allocate packet ID from handle's atomic counter for QoS > 0
        let packet_id = if out_qos != QoS::AtMostOnce {
            Some(sub.handle.allocate_packet_id())
        } else {
            None
        };

        // For local clients, check if disconnected or handle flow control
        let is_local = worker_id == ctx.worker_id;
        let mut quota_consumed = false;

        if is_local {
            if !ctx.clients.contains_key(&sub_token) {
                // Client disconnected - queue for offline delivery
                if !sub.client_id.is_empty() && out_qos != QoS::AtMostOnce {
                    let pkt_id = packet_id.unwrap();
                    let queued_publish = create_outgoing_publish(ctx.publish, out_qos, pkt_id);
                    let mut sessions = ctx.shared.sessions.write();
                    if let Some(session) = sessions.get_mut(&*sub.client_id) {
                        queue_to_session(session, pkt_id, out_qos, queued_publish);
                    }
                }
                continue;
            }

            // MQTT 5 flow control: check quota before sending QoS 1/2
            if out_qos != QoS::AtMostOnce {
                if let Some(client) = ctx.clients.get_mut(&sub_token) {
                    if !client.consume_quota() {
                        // No quota available - skip this message (flow control)
                        result.backpressure_drops += 1;
                        result.last_backpressure_sub = Some((worker_id, sub_token));
                        continue;
                    }
                    quota_consumed = true;
                }
            }
        }

        // MQTT-3.8.3-4: If Retain As Published is 1, preserve the RETAIN flag
        let out_retain = if sub.options.retain_as_published {
            ctx.publish.retain
        } else {
            false
        };

        // MQTT-3.8.2.1.2: Include subscription identifier if subscriber has one
        let queue_result = sub.handle.queue_publish_with_sub_id(
            ctx.factory,
            out_qos,
            packet_id,
            out_retain,
            sub.subscription_id,
        );

        match queue_result {
            Ok(()) => {
                // Track successful publish sent for $SYS metrics
                ctx.shared.metrics.add_pub_msgs_sent(1);
                ctx.shared.metrics.add_pub_bytes_sent(payload_len);

                // Track pending ONLY after successful queue
                if let Some(pid) = packet_id {
                    if is_local {
                        track_local_pending(ctx.clients, sub_token, ctx.publish, out_qos, pid);
                    } else if !sub.client_id.is_empty() && out_qos != QoS::AtMostOnce {
                        let pending_publish = create_outgoing_publish(ctx.publish, out_qos, pid);
                        cross_worker_pending.push(CrossWorkerPending {
                            client_id: sub.client_id.clone(),
                            packet_id: pid,
                            qos: out_qos,
                            publish: pending_publish,
                        });
                    }
                }
            }
            Err(e) => {
                // Restore quota if we consumed it but failed to send
                if quota_consumed {
                    if let Some(client) = ctx.clients.get_mut(&sub_token) {
                        client.restore_quota();
                    }
                }

                handle_queue_error(&e, &mut result, sub, ctx.shared);
            }
        }
    }

    (result, cross_worker_pending)
}

/// Fanout for will messages (simpler - no NoLocal check, no flow control).
pub fn fanout_will_to_subscribers(
    worker_id: usize,
    clients: &mut AHashMap<Token, Client>,
    shared: &SharedStateHandle,
    factory: &mut PublishEncoder,
    publish: &Publish,
    subscribers: &[Subscriber],
) -> (FanoutResult, Vec<CrossWorkerPending>) {
    let mut result = FanoutResult::default();
    let mut cross_worker_pending = Vec::new();

    for sub in subscribers {
        let sub_worker_id = sub.worker_id();
        let sub_token = sub.token();
        let effective_qos = std::cmp::min(publish.qos as u8, sub.qos as u8);
        let out_qos = match QoS::try_from(effective_qos) {
            Ok(q) => q,
            Err(_) => continue,
        };

        let packet_id = if out_qos != QoS::AtMostOnce {
            Some(sub.handle.allocate_packet_id())
        } else {
            None
        };

        let is_local = sub_worker_id == worker_id;

        // Direct write via handle (works for both local and cross-thread)
        let queue_result = sub.handle.queue_publish(factory, out_qos, packet_id, false);

        match queue_result {
            Ok(()) => {
                if let Some(pid) = packet_id {
                    if is_local {
                        track_local_pending(clients, sub_token, publish, out_qos, pid);
                    } else if !sub.client_id.is_empty() && out_qos != QoS::AtMostOnce {
                        let pending_publish = create_outgoing_publish(publish, out_qos, pid);
                        cross_worker_pending.push(CrossWorkerPending {
                            client_id: sub.client_id.clone(),
                            packet_id: pid,
                            qos: out_qos,
                            publish: pending_publish,
                        });
                    }
                }
            }
            Err(e) => {
                handle_queue_error(&e, &mut result, sub, shared);
            }
        }
    }

    (result, cross_worker_pending)
}

/// Write cross-worker pending messages to sessions (batch operation).
pub fn write_cross_worker_pending(shared: &SharedStateHandle, pending: Vec<CrossWorkerPending>) {
    if pending.is_empty() {
        return;
    }

    let mut sessions = shared.sessions.write();
    for p in pending {
        if let Some(session) = sessions.get_mut(&*p.client_id) {
            queue_to_session(session, p.packet_id, p.qos, p.publish);
        }
    }
}

/// Log accumulated backpressure drops (rate limited).
pub fn log_backpressure(counter: &mut RateLimitedCounter, result: &FanoutResult, context: &str) {
    if result.backpressure_drops > 0 {
        if let Some(count) = counter.increment_by(result.backpressure_drops as u64) {
            if let Some((worker_id, token)) = result.last_backpressure_sub {
                log::warn!(
                    "Backpressure: dropped {} {} to slow subscribers (last: worker={}, token={:?})",
                    count,
                    context,
                    worker_id,
                    token
                );
            }
        }
    }
}

/// Log accumulated hard limit drops (rate limited).
pub fn log_hardlimit(counter: &mut RateLimitedCounter, result: &FanoutResult, context: &str) {
    if result.hardlimit_drops > 0 {
        if let Some(count) = counter.increment_by(result.hardlimit_drops as u64) {
            if let Some((worker_id, token)) = result.last_hardlimit_sub {
                log::error!(
                    "HARD LIMIT: dropped {} {} - client buffer exceeded 16MB (worker={}, token={:?})",
                    count,
                    context,
                    worker_id,
                    token
                );
            }
        }
    }
}

/// Create an outgoing publish message with the specified QoS and packet ID.
fn create_outgoing_publish(original: &Publish, qos: QoS, packet_id: u16) -> Publish {
    Publish {
        dup: false,
        qos,
        retain: false,
        topic: original.topic.clone(),
        packet_id: Some(packet_id),
        payload: original.payload.clone(),
        properties: original.properties.clone(),
    }
}

/// Track a pending message for a local client.
fn track_local_pending(
    clients: &mut AHashMap<Token, Client>,
    token: Token,
    publish: &Publish,
    qos: QoS,
    packet_id: u16,
) {
    if let Some(client) = clients.get_mut(&token) {
        let pending_publish = create_outgoing_publish(publish, qos, packet_id);
        let pending = PendingPublish {
            publish: pending_publish,
            sent_at: Instant::now(),
        };
        log::debug!(
            "Tracking pending {:?} pid={} for local client {:?}",
            qos,
            packet_id,
            client.client_id
        );
        match qos {
            QoS::AtLeastOnce => {
                client.pending_qos1.insert(packet_id, pending);
            }
            QoS::ExactlyOnce => {
                client.pending_qos2.insert(packet_id, pending);
            }
            _ => {}
        }
    }
}

/// Queue a message to a session's pending list.
fn queue_to_session(session: &mut Session, packet_id: u16, qos: QoS, publish: Publish) {
    match qos {
        QoS::AtLeastOnce => {
            session.pending_qos1.push((packet_id, publish));
        }
        QoS::ExactlyOnce => {
            session.pending_qos2.push((packet_id, publish));
        }
        _ => {}
    }
}

/// Handle a queue error (backpressure or hard limit).
fn handle_queue_error(
    e: &io::Error,
    result: &mut FanoutResult,
    sub: &Subscriber,
    shared: &SharedStateHandle,
) {
    if e.kind() == io::ErrorKind::WouldBlock {
        result.backpressure_drops += 1;
        result.last_backpressure_sub = Some((sub.handle.worker_id(), sub.handle.token()));
        shared.metrics.add_pub_msgs_dropped(1);
    } else if e.kind() == io::ErrorKind::OutOfMemory {
        result.hardlimit_drops += 1;
        result.last_hardlimit_sub = Some((sub.handle.worker_id(), sub.handle.token()));
        shared.metrics.add_pub_msgs_dropped(1);
    }
}
