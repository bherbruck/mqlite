# Fjall Persistence Layer Design

## Overview

Add crash-recovery persistence to mqlite using [Fjall 3.0](https://github.com/fjall-rs/fjall), a pure-Rust LSM-tree embedded database. This enables the broker to survive restarts without losing retained messages.

**Current Scope (v1)**: Retained messages only. Sessions and pending QoS messages are deferred to a future version.

**MQTT Spec Note**: Per MQTT 5.0 Section 6.1.2, persistence is "SHOULD" not "MUST" - implementations SHOULD provide persistent storage for retained messages, but it's not strictly required.

## Why Fjall?

| Requirement | Fjall Capability |
|-------------|------------------|
| Write-heavy workload | LSM-tree optimized for writes |
| Crash recovery | WAL + configurable durability |
| Low latency | In-memory memtable, async compaction |
| Pure Rust | No FFI, fast compile (~3.5s), small binary |
| Key locality | Lexicographic sorting + prefix compression |

## Implementation Status

### Completed (v1)

- [x] Add `persistence` feature flag (optional)
- [x] Add `fjall` and `bincode` dependencies
- [x] Implement `Persistence` struct with open/close
- [x] Implement retained message save/remove/load
- [x] Implement Message Expiry Interval handling on load
- [x] Add configuration options (`persistence.enabled`, `path`, `sync_interval_secs`)
- [x] Integrate with `SharedState` (load on startup)
- [x] Integrate with publish handlers (persist on retained message)
- [x] Integrate with will handlers (persist on retained will)
- [x] Integrate with internal publish (persist on broker-originated retained)

### Deferred (Future)

- [ ] Session persistence (CleanSession=0 / SessionExpiry)
- [ ] Pending QoS 1/2 message persistence
- [ ] Periodic sync timer
- [ ] Graceful shutdown sync

## Key Schema

Single database with one keyspace:

| Keyspace | Purpose | Key Pattern | Value |
|----------|---------|-------------|-------|
| `retained` | Retained messages | `{topic}` | `RetainedData` (bincode) |

Topics naturally share prefixes for good compression:
```
home/kitchen/temp
home/kitchen/humidity
home/living/temp
sensors/outdoor/wind
```

## Data Structure

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetainedData {
    /// QoS level (0, 1, or 2)
    pub qos: u8,
    /// Message payload bytes
    pub payload: Vec<u8>,
    /// MQTT v5 properties (raw bytes, None for v3.1.1)
    pub properties: Option<Vec<u8>>,
    /// Unix timestamp when stored (for expiry countdown)
    pub stored_at: u64,
    /// Message Expiry Interval in seconds (v5 only)
    pub message_expiry_interval: Option<u32>,
}
```

## File Structure

```
crates/mqlite-server/src/
├── persistence/
│   ├── mod.rs           # Persistence struct, open/save/remove/load
│   └── retained.rs      # RetainedData struct and expiry logic
├── config/
│   └── persistence.rs   # PersistenceConfig struct
└── shared.rs            # SharedState::with_persistence(), persist_retained(), etc.
```

## Configuration

```toml
[persistence]
enabled = true
path = "./data"                # Directory for persistence files
sync_interval_secs = 30        # Periodic sync (not yet implemented)
```

Environment variables:
```bash
MQLITE__PERSISTENCE__ENABLED=true
MQLITE__PERSISTENCE__PATH=/var/lib/mqlite/data
```

## Usage

Persistence is enabled by default. Just build and run:

```bash
cargo build
cargo run
```

To disable persistence at compile time (minimal binary):
```bash
cargo build --no-default-features --features jemalloc
```

### Start Broker with Persistence

```bash
# Create config file
cat > mqlite.toml <<EOF
[persistence]
enabled = true
path = "./data"
EOF

# Run broker
cargo run --features persistence
```

### What Gets Persisted

1. **Retained messages** - Stored when:
   - Client publishes with `retain=true` and non-empty payload
   - Will message with `retain=true` is triggered
   - Internal broker publish (e.g., $SYS topics) with `retain=true`

2. **Deleted retained** - Removed from disk when:
   - Client publishes with `retain=true` and empty payload
   - Will message with `retain=true` and empty payload

3. **On startup** - Loaded from disk:
   - All retained messages
   - Expired messages (Message Expiry Interval elapsed) are skipped

## Write Flow

```
Client PUBLISH (retain=true)
    │
    ├─► Store in memory (retained_messages HashMap)
    │
    └─► persist_retained() ─► serialize with bincode
                           │
                           └─► fjall keyspace.insert(topic, bytes)
```

## Read Flow (Startup)

```
Server::run()
    │
    ├─► SharedState::with_persistence()
    │       │
    │       └─► Persistence::open() ─► Open fjall database
    │       │
    │       └─► Persistence::load_retained()
    │               │
    │               ├─► Iterate all keys in keyspace
    │               │
    │               ├─► Deserialize each value
    │               │
    │               ├─► Check Message Expiry Interval
    │               │   └─► Skip if expired
    │               │
    │               └─► Convert to RetainedMessage
    │
    └─► Populate retained_messages HashMap
```

## Performance Notes

1. **Writes are async** - Memtable absorbs writes, WAL provides crash safety
2. **Reads only at startup** - In-memory after recovery
3. **Topics compress well** - Prefix compression in fjall
4. **No hot-path blocking** - Persistence errors are logged but don't fail publishes

## Error Handling

Persistence errors are logged but don't fail MQTT operations:

```rust
#[cfg(feature = "persistence")]
if let Err(e) = shared.persist_retained(&topic_str, &publish) {
    log::warn!("Failed to persist retained message: {}", e);
}
```

This means:
- Broker continues operating even if disk is full
- Retained messages may be lost on crash if persistence fails
- Monitor logs for persistence warnings

## Dependencies

```toml
[dependencies]
fjall = { version = "3", optional = true }
bincode = { version = "1.3", optional = true }

[features]
default = ["jemalloc", "persistence"]
persistence = ["fjall", "bincode"]
```

Persistence is a default feature - included in normal builds but can be disabled with `--no-default-features`.

## Future Work

### Session Persistence (v2)

Per MQTT spec, sessions with CleanSession=0 should persist:
- Subscriptions
- Pending QoS 1 messages (awaiting PUBACK)
- Pending QoS 2 messages (awaiting PUBCOMP)

However, the spec says "SHOULD" not "MUST", and other brokers like FlashMQ explicitly don't persist pending messages due to complexity.

### Proposed Key Schema (Future)

```
sessions/{client_id}              → SessionData
subscriptions/{client_id}/{filter} → SubscriptionData
pending/{client_id}/{packet_id}    → PendingData
```

### Periodic Sync (v2)

Add a timer to periodically sync to disk:
```rust
if sync_interval > 0 {
    spawn_sync_timer(shared, Duration::from_secs(sync_interval));
}
```

### Graceful Shutdown (v2)

Force sync on SIGTERM/SIGINT before exit.
