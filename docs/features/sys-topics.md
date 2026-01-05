# Zero-Copy $SYS Topics Implementation Plan

## Overview

Implement $SYS broker statistics topics with zero-copy, zero-allocation on the hot path. Metrics collection uses atomic counters; publishing uses static topic strings and stack-allocated formatting.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         BrokerMetrics                           │
│  (Global atomic counters - updated by workers, read by timer)   │
├─────────────────────────────────────────────────────────────────┤
│  bytes_received: AtomicU64      bytes_sent: AtomicU64           │
│  msgs_received: AtomicU64       msgs_sent: AtomicU64            │
│  pub_msgs_received: AtomicU64   pub_msgs_sent: AtomicU64        │
│  pub_msgs_dropped: AtomicU64    pub_bytes_received: AtomicU64   │
│  pub_bytes_sent: AtomicU64      connections_total: AtomicU64    │
│  sockets_opened: AtomicU64                                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SysTreePublisher                           │
│  (Runs on timer in main thread - publishes changed values)      │
├─────────────────────────────────────────────────────────────────┤
│  - Reads atomic counters with Relaxed ordering                  │
│  - Compares with previous values (change detection)             │
│  - Formats numbers into stack buffer [u8; 32]                   │
│  - Creates Bytes::copy_from_slice() for payload                 │
│  - Uses Bytes::from_static() for topic names                    │
│  - Publishes as retained to SharedState                         │
│  - Fans out to $SYS/# subscribers                               │
└─────────────────────────────────────────────────────────────────┘
```

## Files to Create/Modify

### New: `src/sys_tree.rs` (~400 lines)

```rust
// Static topic constants - zero allocation
pub mod topics {
    pub const VERSION: &str = "$SYS/broker/version";
    pub const UPTIME: &str = "$SYS/broker/uptime";
    pub const CLIENTS_CONNECTED: &str = "$SYS/broker/clients/connected";
    // ... etc
}

// Global metrics - atomic counters
pub struct BrokerMetrics {
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    // ... all counters
}

impl BrokerMetrics {
    pub const fn new() -> Self { ... }

    // Inline increment methods for hot path
    #[inline]
    pub fn add_bytes_received(&self, n: u64) {
        self.bytes_received.fetch_add(n, Ordering::Relaxed);
    }
}

// Publisher that runs on timer
pub struct SysTreePublisher {
    metrics: Arc<BrokerMetrics>,
    shared: Arc<SharedState>,
    start_time: Instant,
    interval_secs: u64,
    // Previous values for change detection
    prev: PreviousValues,
    // Load average state
    load_state: LoadAverageState,
}

impl SysTreePublisher {
    pub fn publish_if_changed(&mut self) { ... }
}
```

### Modify: `src/shared.rs`

Add `BrokerMetrics` to `SharedState`:

```rust
pub struct SharedState {
    // ... existing fields
    pub metrics: BrokerMetrics,  // NOT behind RwLock - uses atomics
}
```

### Modify: `src/worker.rs`

Add metric increments at key points:

```rust
// In handle_publish() after successful delivery:
self.shared.metrics.add_pub_msgs_sent(1);
self.shared.metrics.add_pub_bytes_sent(payload_len as u64);

// In read_from_client() after successful read:
self.shared.metrics.add_bytes_received(n as u64);

// In flush_write_buffer() after successful write:
self.shared.metrics.add_bytes_sent(n as u64);
```

### Modify: `src/server.rs`

Add timer for $SYS publishing in accept loop:

```rust
// Register timer fd for sys_interval
let sys_timer = TimerFd::new()?;
sys_timer.set_interval(config.sys_interval)?;
poll.registry().register(&mut sys_timer, SYS_TIMER_TOKEN, Interest::READABLE)?;

// In event loop:
if event.token() == SYS_TIMER_TOKEN {
    sys_publisher.publish_if_changed();
}
```

### Modify: `src/config.rs`

Add `sys_interval` to server config:

```rust
pub struct ServerConfig {
    pub bind: String,
    pub workers: usize,
    pub sys_interval: u64,  // 0 = disabled, default 10 seconds
}
```

## Zero-Copy Techniques

### 1. Static Topic Strings
```rust
// Compile-time constant, Bytes::from_static() is zero-copy
const TOPIC: &'static str = "$SYS/broker/uptime";
let topic_bytes = Bytes::from_static(TOPIC.as_bytes());
```

### 2. Stack-Allocated Number Formatting
```rust
// Format number without heap allocation
let mut buf = [0u8; 32];
let n = itoa::write(&mut buf[..], value)?;
let payload = Bytes::copy_from_slice(&buf[..n]);
// Only one small allocation for the final Bytes
```

### 3. Atomic Counters with Relaxed Ordering
```rust
// Hot path: minimal synchronization overhead
self.bytes_received.fetch_add(n, Ordering::Relaxed);

// Read side (timer thread): also relaxed, eventual consistency OK
let value = self.bytes_received.load(Ordering::Relaxed);
```

### 4. Change Detection (Avoid Redundant Publishes)
```rust
struct PreviousValues {
    bytes_received: u64,
    clients_connected: u64,
    // ...
}

// Only publish if value changed
if current != self.prev.bytes_received {
    self.publish_metric(topics::BYTES_RECEIVED, current);
    self.prev.bytes_received = current;
}
```

### 5. Reuse PublishEncoder Pattern
```rust
// Stack allocation for small $SYS payloads (all are < 64 bytes)
let publish = Publish {
    topic: Bytes::from_static(topic.as_bytes()),
    payload,  // Bytes from stack buffer
    retain: true,
    qos: QoS::AtMostOnce,
    // ...
};
```

## $SYS Topics to Implement (Full Mosquitto Parity)

### Static (set once at startup)
- `$SYS/broker/version` - "mqlite version X.Y.Z"

### Client Statistics
- `$SYS/broker/clients/connected` - currently connected
- `$SYS/broker/clients/disconnected` - persistent sessions, not connected
- `$SYS/broker/clients/total` - connected + disconnected
- `$SYS/broker/clients/maximum` - max concurrent ever seen
- `$SYS/broker/clients/expired` - expired persistent sessions

### Uptime
- `$SYS/broker/uptime` - "12345 seconds"

### Message Counters
- `$SYS/broker/messages/received` - total MQTT messages in
- `$SYS/broker/messages/sent` - total MQTT messages out
- `$SYS/broker/store/messages/count` - messages in store
- `$SYS/broker/store/messages/bytes` - bytes in store

### Publish Statistics
- `$SYS/broker/publish/messages/received`
- `$SYS/broker/publish/messages/sent`
- `$SYS/broker/publish/messages/dropped`
- `$SYS/broker/publish/bytes/received`
- `$SYS/broker/publish/bytes/sent`

### Byte Counters
- `$SYS/broker/bytes/received` - total bytes in
- `$SYS/broker/bytes/sent` - total bytes out

### Subscription/Retained Counts
- `$SYS/broker/subscriptions/count`
- `$SYS/broker/retained messages/count`

### Load Averages (1min, 5min, 15min each = 27 topics)
- `$SYS/broker/load/messages/received/{1,5,15}min`
- `$SYS/broker/load/messages/sent/{1,5,15}min`
- `$SYS/broker/load/publish/dropped/{1,5,15}min`
- `$SYS/broker/load/publish/received/{1,5,15}min`
- `$SYS/broker/load/publish/sent/{1,5,15}min`
- `$SYS/broker/load/bytes/received/{1,5,15}min`
- `$SYS/broker/load/bytes/sent/{1,5,15}min`
- `$SYS/broker/load/sockets/{1,5,15}min`
- `$SYS/broker/load/connections/{1,5,15}min`

### Memory (optional, requires tracking)
- `$SYS/broker/heap/current`
- `$SYS/broker/heap/maximum`

## Implementation Steps

1. **Add `itoa` dependency** to Cargo.toml for fast integer formatting

2. **Create `src/sys_tree.rs`**:
   - Define `BrokerMetrics` with atomic counters
   - Define static topic string constants
   - Implement `SysTreePublisher` with change detection

3. **Integrate into `SharedState`**:
   - Add `metrics: BrokerMetrics` field
   - Initialize in constructor

4. **Add metric increments to hot paths**:
   - `worker.rs`: bytes/messages received/sent
   - `client.rs`: connection events
   - Track where publishes are dropped

5. **Add timer to server event loop**:
   - Use `timerfd` on Linux for precise intervals
   - Fall back to polling `Instant::now()` if unavailable
   - Call `sys_publisher.publish_if_changed()` on timer tick

6. **Add config option**:
   - `sys_interval` in `[server]` section
   - Default 10 seconds, 0 to disable

7. **Handle $SYS publish**:
   - Store as retained message
   - Fan out to matching subscribers
   - Use existing route cache

## Performance Considerations

- **Hot path overhead**: Single `fetch_add` with Relaxed ordering (~1-2 cycles on x86)
- **Timer thread**: Runs every N seconds, reads atomics, formats ~20 numbers, publishes retained
- **Memory**: ~200 bytes for atomic counters, ~2KB for previous values + load state
- **No contention**: Workers never wait on metrics (atomics are lock-free)

## Dependencies

```toml
[dependencies]
itoa = "1.0"  # Fast integer-to-string formatting
```

## Testing

1. Subscribe to `$SYS/#` and verify all topics publish
2. Verify values update correctly over time
3. Benchmark: Ensure no measurable impact on publish throughput
4. Verify change detection: Unchanged values don't re-publish
