# mqlite Codebase Restructuring Plan

This document outlines the plan to modularize the mqlite codebase, breaking up large files and long functions into smaller, focused modules.

---

## Completed Work (Phase 1)

The following extractions have been completed:

### New Modules Created

| Module | Lines | Description |
|--------|-------|-------------|
| `mqlite-server/src/will.rs` | 277 | Will message handling (DelayedWill, WillManager) |
| `mqlite-core/src/varint.rs` | 230 | Variable byte integer codec with tests |

### Line Count Changes

| File | Before | After | Change |
|------|--------|-------|--------|
| `worker.rs` | 2,208 | 1,865 | -343 (-15.5%) |
| `packet.rs` | 2,195 | 2,140 | -55 (-2.5%) |
| `fanout.rs` | 374 | 371 | -3 (integrated) |

### Key Improvements

1. **will.rs**: Extracted will message management from worker.rs
   - `DelayedWill` struct for scheduled wills
   - `WillManager` for managing delayed will queue
   - `extract_will()`, `store_retained_will()` helper functions
   - Unit tests for will manager

2. **fanout.rs Integration**: Connected existing fanout module to worker.rs
   - Will message fanout now uses `fanout_will_to_subscribers()`
   - Eliminated ~150 lines of duplicate fanout code
   - Rate-limited logging via `log_backpressure()` and `log_hardlimit()`

3. **varint.rs**: Extracted variable byte integer handling from packet.rs
   - `decode()`, `encode_to_slice()`, `encode_to_vec()`, `encoded_len()`
   - Comprehensive tests including roundtrip validation
   - Doc-tests for all public functions

### Remaining Work

The following items are documented below but not yet implemented:
- Handler extractions (connect, publish, subscribe) - complex due to tight coupling
- Properties extraction from packet.rs - large scope (~400 lines)
- Session management extraction
- Trie extraction from subscription.rs

---

## Table of Contents

1. [Current State](#1-current-state)
2. [Target State](#2-target-state)
3. [Function Breakdown Tables](#3-function-breakdown-tables)
4. [Test Organization](#4-test-organization)
5. [Migration Plan](#5-migration-plan)

---

## 1. Current State

### 1.1 Directory Structure

```
crates/
├── mqlite-client/          # MQTT client library
│   └── src/
│       ├── async_client.rs   (1,347 lines)
│       ├── client.rs         (1,125 lines)
│       ├── session.rs          (471 lines)
│       ├── callback.rs         (281 lines)
│       ├── config.rs           (238 lines)
│       ├── tls.rs              (198 lines)
│       ├── packet_id.rs        (149 lines)
│       ├── lib.rs              (127 lines)
│       ├── will.rs
│       ├── error.rs
│       └── events.rs
│
├── mqlite-core/            # Shared MQTT protocol types
│   └── src/
│       ├── packet.rs         (2,195 lines) ← CRITICAL
│       ├── lib.rs
│       └── error.rs
│
└── mqlite-server/          # MQTT broker
    └── src/
        ├── worker.rs         (2,208 lines) ← CRITICAL
        ├── config.rs           (948 lines)
        ├── sys_tree.rs         (887 lines)
        ├── write_buffer.rs     (828 lines)
        ├── subscription.rs     (558 lines)
        ├── client.rs           (527 lines)
        ├── publish_encoder.rs  (508 lines)
        ├── client_handle.rs    (447 lines)
        ├── server.rs           (410 lines)
        ├── fanout.rs           (374 lines) ← stub, not integrated
        ├── proxy.rs            (372 lines)
        ├── bridge.rs           (367 lines)
        ├── prometheus.rs       (338 lines)
        ├── route_cache.rs      (247 lines)
        ├── util.rs             (243 lines)
        ├── shared.rs           (177 lines)
        ├── main.rs             (167 lines)
        └── auth/
            ├── acl.rs          (359 lines)
            ├── static_auth.rs  (276 lines)
            └── mod.rs          (271 lines)
```

### 1.2 Files Requiring Refactoring

| File | Lines | Priority | Issues |
|------|-------|----------|--------|
| `mqlite-server/src/worker.rs` | 2,208 | **CRITICAL** | 5 functions >180 lines, 3 duplicate fanout loops |
| `mqlite-core/src/packet.rs` | 2,195 | **HIGH** | Properties mixed with codecs, no separation |
| `mqlite-server/src/config.rs` | 948 | MEDIUM | Multiple config types in one file |
| `mqlite-server/src/sys_tree.rs` | 887 | LOW | Well-organized, optional split |
| `mqlite-server/src/write_buffer.rs` | 828 | LOW | Well-organized, good tests |
| `mqlite-server/src/subscription.rs` | 558 | MEDIUM | Trie could be generic |
| `mqlite-server/src/client.rs` | 527 | MEDIUM | Transport abstraction mixed in |

### 1.3 Code Duplication

**Three nearly-identical fanout loops exist in worker.rs:**

1. `handle_publish()` lines 1423-1596 (173 lines)
2. `cleanup_clients()` will fanout lines 1868-1951 (83 lines)
3. `publish_delayed_wills()` lines 2055-2137 (82 lines)

Each loop:
- Iterates subscribers
- Calculates effective QoS
- Allocates packet IDs
- Handles local vs cross-worker delivery
- Tracks pending messages
- Manages backpressure (WouldBlock) and hard limits (OutOfMemory)

**Solution:** The `fanout.rs` module already exists with consolidated functions but is not integrated.

### 1.4 Test Locations (Current)

Tests are inline `#[cfg(test)]` modules at the bottom of each file:

| File | Test Module Line | Approx Test Lines |
|------|------------------|-------------------|
| `write_buffer.rs` | 509 | ~320 lines |
| `packet.rs` | 1989 | ~206 lines |
| `config.rs` | 793 | ~155 lines |
| `subscription.rs` | 467 | ~91 lines |
| `sys_tree.rs` | 834 | ~53 lines |
| `client.rs` | 489 | ~38 lines |
| `client_handle.rs` | 322, 330 | ~125 lines |
| `publish_encoder.rs` | 439 | ~69 lines |
| `proxy.rs` | 295 | ~77 lines |
| `prometheus.rs` | 315 | ~23 lines |
| `util.rs` | 130 | ~113 lines |

---

## 2. Target State

### 2.1 Target Directory Structure

```
crates/
├── mqlite-core/
│   └── src/
│       ├── lib.rs
│       ├── error.rs
│       ├── packet.rs           (~800 lines) - Core types and dispatch
│       ├── properties.rs       (~400 lines) - MQTT 5 properties
│       ├── varint.rs           (~60 lines)  - Variable byte integer codec
│       └── codec/              (optional, Phase 3)
│           ├── mod.rs
│           ├── connect.rs
│           ├── publish.rs
│           └── subscribe.rs
│
└── mqlite-server/
    └── src/
        ├── main.rs
        ├── server.rs
        │
        ├── worker.rs           (~600 lines) - Event loop only
        ├── fanout.rs           (~400 lines) - Consolidated fanout logic
        ├── will.rs             (~250 lines) - Will message handling
        ├── session.rs          (~150 lines) - Session save/restore
        │
        ├── handlers/           NEW - MQTT packet handlers
        │   ├── mod.rs
        │   ├── connect.rs      (~300 lines)
        │   ├── publish.rs      (~200 lines)
        │   └── subscribe.rs    (~200 lines)
        │
        ├── client.rs           (~350 lines) - Client state
        ├── transport.rs        (~180 lines) - TCP/TLS abstraction
        │
        ├── subscription.rs     (~300 lines) - Store facade
        ├── trie.rs             (~250 lines) - Topic trie implementation
        │
        ├── config.rs           (~200 lines) - Main config + loading
        ├── config/             (optional, Phase 3)
        │   ├── mod.rs
        │   ├── server.rs
        │   ├── mqtt.rs
        │   ├── tls.rs
        │   └── auth.rs
        │
        ├── [unchanged files]
        │   ├── auth/
        │   ├── bridge.rs
        │   ├── client_handle.rs
        │   ├── prometheus.rs
        │   ├── proxy.rs
        │   ├── publish_encoder.rs
        │   ├── route_cache.rs
        │   ├── shared.rs
        │   ├── sys_tree.rs
        │   ├── util.rs
        │   └── write_buffer.rs
        │
        └── tests/              NEW - Extracted tests
            ├── worker_tests.rs
            ├── handler_tests.rs
            └── integration/
```

### 2.2 New Module Responsibilities

#### `will.rs` - Will Message Handling
```rust
pub struct DelayedWill {
    pub publish_at: Instant,
    pub publish: Publish,
}

pub struct WillManager {
    delayed_wills: Vec<DelayedWill>,
}

impl WillManager {
    pub fn new() -> Self;

    /// Extract will from disconnecting client, returns (publish, delay_duration)
    pub fn extract_will(client: &mut Client) -> Option<(Publish, Option<Duration>)>;

    /// Schedule a will for delayed publication
    pub fn schedule(&mut self, will: DelayedWill);

    /// Get wills ready for publication, removing them from the queue
    pub fn take_ready_wills(&mut self) -> Vec<Publish>;

    /// Time until next scheduled will (for poll timeout)
    pub fn next_timeout(&self) -> Option<Duration>;
}
```

#### `session.rs` - Session Management
```rust
/// Save client session on disconnect (for CleanSession=false)
pub fn save_session(
    client_id: &str,
    subscriptions: Vec<(String, SubscriptionOptions)>,
    pending: Vec<PendingPublish>,
    sessions: &RwLock<Sessions>,
);

/// Restore subscriptions from saved session
pub fn restore_subscriptions(
    session: &Session,
    client_handle: Arc<ClientWriteHandle>,
    subscription_store: &RwLock<SubscriptionStore>,
) -> Vec<String>; // returns restored topic filters

/// Queue publish to offline session
pub fn queue_to_session(
    session: &mut Session,
    publish: Publish,
    packet_id: u16,
    qos: QoS,
);
```

#### `handlers/connect.rs` - CONNECT Packet Handler
```rust
pub enum ConnectError {
    InvalidClientId,
    AuthenticationFailed,
    NotAuthorized,
    ServerUnavailable,
    BadUsernamePassword,
    ProtocolError(String),
}

pub struct ConnectResult {
    pub session_present: bool,
    pub assigned_client_id: Option<String>,
    pub pending_messages: Vec<Publish>,
}

/// Validate CONNECT packet requirements
pub fn validate_connect(connect: &Connect, config: &MqttConfig) -> Result<(), ConnectError>;

/// Authenticate client credentials
pub fn authenticate(
    connect: &Connect,
    auth: &AuthHandler,
) -> Result<(String, AuthRole), ConnectError>;

/// Handle duplicate client (takeover)
pub fn handle_takeover(
    client_id: &str,
    new_worker_id: usize,
    new_token: Token,
    registry: &RwLock<ClientRegistry>,
    worker_senders: &[Sender<WorkerMsg>],
) -> Option<ClientLocation>;

/// Restore or create session
pub fn setup_session(
    client_id: &str,
    clean_session: bool,
    sessions: &RwLock<Sessions>,
    subscriptions: &RwLock<SubscriptionStore>,
    handle: Arc<ClientWriteHandle>,
) -> ConnectResult;

/// Build CONNACK packet
pub fn build_connack(
    result: &ConnectResult,
    config: &Config,
    is_v5: bool,
) -> Packet;
```

#### `handlers/publish.rs` - PUBLISH Packet Handler
```rust
pub enum PublishError {
    InvalidTopic(String),
    QosNotSupported,
    RetainNotSupported,
    NotAuthorized,
    QuotaExceeded,
}

/// Validate PUBLISH packet
pub fn validate_publish(
    publish: &Publish,
    config: &MqttConfig,
) -> Result<(), PublishError>;

/// Check publish authorization
pub fn check_acl(
    topic: &[u8],
    client_id: &str,
    auth_role: Option<&str>,
    auth: &AuthHandler,
) -> bool;

/// Store or delete retained message
pub fn handle_retained(
    publish: &Publish,
    retained_messages: &RwLock<RetainedMessages>,
);

/// Deliver to offline sessions (CleanSession=false clients)
pub fn deliver_to_offline(
    publish: &Publish,
    sessions: &RwLock<Sessions>,
    packet_id_generator: &mut u16,
);
```

#### `handlers/subscribe.rs` - SUBSCRIBE Packet Handler
```rust
pub enum SubscribeError {
    InvalidFilter(String),
    WildcardNotSupported,
    SharedSubNotSupported,
    NotAuthorized,
}

/// Validate topic filter
pub fn validate_filter(
    filter: &str,
    config: &MqttConfig,
) -> Result<(), SubscribeError>;

/// Check subscribe authorization
pub fn check_acl(
    filter: &str,
    client_id: &str,
    auth_role: Option<&str>,
    auth: &AuthHandler,
) -> bool;

/// Send matching retained messages
pub fn send_retained(
    filter: &str,
    retain_handling: RetainHandling,
    is_new_subscription: bool,
    subscription_id: Option<u32>,
    client: &mut Client,
    handle: &ClientWriteHandle,
    retained_messages: &RwLock<RetainedMessages>,
    is_v5: bool,
);
```

#### `transport.rs` - Transport Abstraction
```rust
pub enum Transport {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl Transport {
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;
    pub fn write(&mut self, buf: &[u8]) -> io::Result<usize>;
    pub fn flush(&mut self) -> io::Result<()>;
    pub fn shutdown(&mut self);
}

impl AsRawFd for Transport { ... }

#[cfg(feature = "tls")]
impl From<TlsStream<TcpStream>> for Transport { ... }
```

#### `trie.rs` - Topic Trie Implementation
```rust
pub struct TrieNode<T> {
    children: HashMap<String, TrieNode<T>>,
    values: Vec<T>,
    plus_wildcard: Option<Box<TrieNode<T>>>,
    hash_wildcard: Vec<T>,
}

impl<T: Clone> TrieNode<T> {
    pub fn new() -> Self;
    pub fn insert(&mut self, path: &[&str], value: T);
    pub fn remove(&mut self, path: &[&str], predicate: impl Fn(&T) -> bool);
    pub fn collect_matches(&self, path: &[&str], result: &mut Vec<T>);
}
```

#### `properties.rs` (mqlite-core) - MQTT 5 Properties
```rust
pub enum PropertyType {
    PayloadFormatIndicator,
    MessageExpiryInterval,
    ContentType,
    // ... all 42 property types
}

impl PropertyType {
    pub fn from_byte(byte: u8) -> Result<Self>;
    pub fn to_byte(&self) -> u8;
}

pub struct PropertiesDecoder<'a> { ... }

impl<'a> PropertiesDecoder<'a> {
    pub fn new(data: &'a [u8]) -> Self;
    pub fn decode_connect_properties(&mut self) -> Result<ConnectProperties>;
    pub fn decode_publish_properties(&mut self) -> Result<PublishProperties>;
    pub fn decode_will_properties(&mut self) -> Result<WillProperties>;
    // ...
}

pub fn encode_connect_properties(props: &ConnectProperties, buf: &mut Vec<u8>);
pub fn encode_publish_properties(props: &PublishProperties, buf: &mut Vec<u8>);
pub fn encode_connack_properties(props: &ConnackProperties, buf: &mut Vec<u8>);
// ...
```

#### `varint.rs` (mqlite-core) - Variable Byte Integer
```rust
/// Decode variable byte integer, returns (value, bytes_consumed) or None if incomplete
pub fn decode(buf: &[u8]) -> Result<Option<(u32, usize)>>;

/// Encode variable byte integer to buffer, returns bytes written
pub fn encode(value: u32, buf: &mut [u8]) -> usize;

/// Encode variable byte integer to Vec
pub fn encode_to_vec(value: u32, buf: &mut Vec<u8>) -> usize;

/// Calculate encoded length of a value
pub fn encoded_len(value: u32) -> usize;
```

---

## 3. Function Breakdown Tables

### 3.1 worker.rs Functions (Current)

| Function | Lines | Start | End | Complexity | Action |
|----------|-------|-------|-----|------------|--------|
| `new()` | 32 | 122 | 153 | Low | Keep |
| `run()` | 6 | 156 | 161 | Low | Keep |
| `run_once()` | 104 | 163 | 266 | Medium | Keep, simplify |
| `accept_client()` | 31 | 268 | 298 | Low | Keep |
| `get_subscribers_cached()` | 5 | 302 | 306 | Low | Keep |
| `handle_readable()` | 54 | 307 | 360 | Medium | Keep |
| `handle_writable()` | 16 | 361 | 376 | Low | Keep |
| `handle_packet()` | 116 | 377 | 492 | Medium | Keep (dispatch) |
| **`handle_connect()`** | **407** | 493 | 899 | **High** | **Extract to handlers/** |
| **`handle_subscribe()`** | **268** | 900 | 1167 | **High** | **Extract to handlers/** |
| `handle_unsubscribe()` | 55 | 1168 | 1222 | Medium | Keep or extract |
| **`handle_publish()`** | **484** | 1223 | 1706 | **High** | **Extract to handlers/** |
| **`cleanup_clients()`** | **300** | 1707 | 2006 | **High** | **Split: cleanup + will** |
| **`publish_delayed_wills()`** | **184** | 2008 | 2191 | **High** | **Extract to will.rs** |
| `next_delayed_will_timeout()` | 15 | 2194 | 2208 | Low | Extract to will.rs |

### 3.2 handle_connect() Breakdown (407 lines → 6 functions)

| Section | Lines | Description | Target Function |
|---------|-------|-------------|-----------------|
| Validation | 24 | ClientId requirements | `handlers::connect::validate_connect()` |
| Authentication | 43 | Auth + rejection | `handlers::connect::authenticate()` |
| Takeover | 65 | Duplicate detection | `handlers::connect::handle_takeover()` |
| Session restore | 115 | Cross-worker wait, subscription restore | `handlers::connect::setup_session()` |
| Client config | 55 | Keep-alive, will, protocol version | Inline in worker |
| CONNACK | 61 | Build + send response | `handlers::connect::build_connack()` |
| Pending resend | 19 | Resend QoS 1/2 | Inline in worker |

### 3.3 handle_publish() Breakdown (484 lines → 5 functions)

| Section | Lines | Description | Target Function |
|---------|-------|-------------|-----------------|
| Validation | 70 | Topic wildcards, length, depth | `handlers::publish::validate_publish()` |
| ACL check | 45 | Authorization | `handlers::publish::check_acl()` |
| Acknowledgment | 29 | PUBACK/PUBREC | Inline in worker |
| Retained storage | 23 | Store/delete retained | `handlers::publish::handle_retained()` |
| **Fanout loop** | **173** | Subscriber delivery | `fanout::fanout_to_subscribers()` |
| Cross-worker batch | 17 | Batch writes | `fanout::write_cross_worker_pending()` |
| Offline delivery | 56 | Session queuing | `handlers::publish::deliver_to_offline()` |

### 3.4 handle_subscribe() Breakdown (268 lines → 4 functions)

| Section | Lines | Description | Target Function |
|---------|-------|-------------|-----------------|
| Feature validation | 35 | Shared subs, wildcards, sub IDs | `handlers::subscribe::validate_filter()` |
| Filter validation | 40 | Per-filter validation | `handlers::subscribe::validate_filter()` |
| ACL check | 25 | Per-filter authorization | `handlers::subscribe::check_acl()` |
| Subscription storage | 30 | Add to trie + session | Inline in worker |
| **Retained delivery** | **80** | RetainHandling, expiry | `handlers::subscribe::send_retained()` |
| SUBACK | 20 | Build response | Inline in worker |

### 3.5 cleanup_clients() Breakdown (300 lines → 4 functions)

| Section | Lines | Description | Target Function |
|---------|-------|-------------|-----------------|
| Deregistration | 90 | Poll, metrics, token cleanup | Keep in worker |
| Subscription removal | 25 | Remove from trie | Keep in worker |
| Session save | 35 | Save pending to session | `session::save_session()` |
| **Will extraction** | **34** | Build Publish from will | `will::WillManager::extract_will()` |
| **Will fanout** | **83** | Publish will messages | `fanout::fanout_will_to_subscribers()` |
| Delayed will schedule | 3 | Add to delayed queue | `will::WillManager::schedule()` |

### 3.6 packet.rs Functions (Current)

| Function | Lines | Start | Description | Target |
|----------|-------|-------|-------------|--------|
| `PropertyType::from_byte()` | 155 | 319 | Property type parsing | `properties.rs` |
| `decode_remaining_length()` | 23 | 656 | Varint decode | `varint.rs` |
| `encode_remaining_length()` | 22 | 679 | Varint encode | `varint.rs` |
| `variable_byte_integer_len()` | 13 | 1310 | Varint length | `varint.rs` |
| `encode_variable_byte_integer()` | 19 | 1344 | Varint to vec | `varint.rs` |
| `decode_connect()` | 117 | 786 | CONNECT decode | Keep (or codec/) |
| `encode_connect()` | 88 | 1720 | CONNECT encode | Keep (or codec/) |
| `encode_connack_properties()` | 86 | 1468 | CONNACK props | `properties.rs` |
| `decode_publish()` | 41 | 903 | PUBLISH decode | Keep |
| `encode_publish()` | 61 | 1593 | PUBLISH encode | Keep |
| `decode_subscribe()` | 79 | 1054 | SUBSCRIBE decode | Keep |
| `update_message_expiry()` | 105 | 1363 | Expiry handling | `properties.rs` |
| `Decoder::read_connect_properties()` | 35 | 586 | Props decode | `properties.rs` |
| `Decoder::read_will_properties()` | 35 | 621 | Props decode | `properties.rs` |

---

## 4. Test Organization

### 4.1 Current Test Locations

All tests are inline `#[cfg(test)] mod tests` at the bottom of source files.

### 4.2 Target Test Organization

**Principle:** Tests stay inline for focused unit tests. Extract to separate files when:
1. Tests are >200 lines
2. Tests span multiple modules (integration)
3. Tests require complex fixtures

#### Keep Inline (Unit Tests)
- `write_buffer.rs` - Buffer operations are self-contained
- `publish_encoder.rs` - Encoding is self-contained
- `subscription.rs` / `trie.rs` - Trie operations are self-contained
- `route_cache.rs` - Cache behavior is self-contained
- `properties.rs` - Property parsing is self-contained
- `varint.rs` - Varint codec is self-contained

#### Extract to tests/ (Integration/Complex)

**`tests/handler_tests.rs`** - Handler integration tests
```rust
//! Tests for MQTT packet handlers
//! Tests connect, publish, subscribe handlers with mock clients

mod connect_tests {
    // Test authentication flows
    // Test session restoration
    // Test client takeover
}

mod publish_tests {
    // Test ACL enforcement
    // Test retained message handling
    // Test offline delivery
}

mod subscribe_tests {
    // Test filter validation
    // Test retained message delivery
    // Test subscription ID injection
}
```

**`tests/fanout_tests.rs`** - Fanout integration tests
```rust
//! Tests for message fanout to subscribers
//! Tests local delivery, cross-worker delivery, backpressure

mod local_fanout_tests { }
mod cross_worker_tests { }
mod backpressure_tests { }
mod will_fanout_tests { }
```

**`tests/session_tests.rs`** - Session persistence tests
```rust
//! Tests for session save/restore
//! Tests CleanSession=false behavior

mod save_tests { }
mod restore_tests { }
mod offline_queue_tests { }
```

### 4.3 Test File Mapping

| Source Module | Test Location | Reason |
|--------------|---------------|--------|
| `handlers/connect.rs` | `tests/handler_tests.rs` | Needs mock client setup |
| `handlers/publish.rs` | `tests/handler_tests.rs` | Needs mock client setup |
| `handlers/subscribe.rs` | `tests/handler_tests.rs` | Needs mock client setup |
| `fanout.rs` | `tests/fanout_tests.rs` | Needs multi-subscriber setup |
| `will.rs` | Inline + `tests/fanout_tests.rs` | Split: unit + integration |
| `session.rs` | `tests/session_tests.rs` | Needs session store setup |
| `trie.rs` | Inline | Self-contained |
| `transport.rs` | Inline | Self-contained I/O |
| `properties.rs` | Inline | Self-contained parsing |
| `varint.rs` | Inline | Self-contained codec |

---

## 5. Migration Plan

### 5.1 Phase 1: Core Extractions (worker.rs)

**Goal:** Reduce worker.rs from 2,208 to ~600 lines

#### Step 1.1: Extract will.rs (~220 lines)
```
Files: NEW will.rs, MODIFY worker.rs, MODIFY main.rs
Duration: Self-contained, no dependencies
Verify: make test
```

Extract:
- `DelayedWill` struct
- `next_delayed_will_timeout()` method
- Will extraction logic from `cleanup_clients()`
- `publish_delayed_wills()` method

#### Step 1.2: Integrate fanout.rs (~200 lines removed from worker)
```
Files: MODIFY fanout.rs, MODIFY worker.rs
Duration: Requires careful refactoring
Verify: make test, make conformance-ci
```

Replace duplicate loops:
- `handle_publish()` fanout loop → `fanout::fanout_to_subscribers()`
- `cleanup_clients()` will fanout → `fanout::fanout_will_to_subscribers()`
- `publish_delayed_wills()` fanout → `fanout::fanout_will_to_subscribers()`

#### Step 1.3: Extract handlers/connect.rs (~300 lines)
```
Files: NEW handlers/mod.rs, NEW handlers/connect.rs, MODIFY worker.rs
Duration: Largest extraction
Verify: make test, test client connections manually
```

Extract:
- `validate_connect()`
- `authenticate()`
- `handle_takeover()`
- `setup_session()`
- `build_connack()`

#### Step 1.4: Extract handlers/publish.rs (~150 lines)
```
Files: NEW handlers/publish.rs, MODIFY worker.rs
Duration: Depends on fanout integration
Verify: make test, mosquitto_pub/sub tests
```

Extract:
- `validate_publish()`
- `check_acl()`
- `handle_retained()`
- `deliver_to_offline()`

#### Step 1.5: Extract handlers/subscribe.rs (~150 lines)
```
Files: NEW handlers/subscribe.rs, MODIFY worker.rs
Duration: Similar to publish
Verify: make test, subscription tests
```

Extract:
- `validate_filter()`
- `check_acl()`
- `send_retained()`

#### Step 1.6: Extract session.rs (~100 lines)
```
Files: NEW session.rs, MODIFY worker.rs, MODIFY handlers/connect.rs
Duration: Cross-cutting concern
Verify: make test, session persistence tests
```

Extract:
- `save_session()`
- `restore_subscriptions()`
- `queue_to_session()`

### 5.2 Phase 2: Protocol Extractions (packet.rs)

**Goal:** Reduce packet.rs from 2,195 to ~1,400 lines

#### Step 2.1: Extract varint.rs (~60 lines)
```
Files: NEW varint.rs, MODIFY packet.rs, MODIFY lib.rs
Duration: Simple, no dependencies
Verify: cargo test -p mqlite-core
```

Extract:
- `decode_remaining_length()`
- `encode_remaining_length()`
- `variable_byte_integer_len()`
- `encode_variable_byte_integer()`

#### Step 2.2: Extract properties.rs (~400 lines)
```
Files: NEW properties.rs, MODIFY packet.rs, MODIFY lib.rs
Duration: Medium complexity
Verify: cargo test -p mqlite-core
```

Extract:
- `PropertyType` enum and `from_byte()`
- `Decoder::read_connect_properties()`
- `Decoder::read_will_properties()`
- `encode_connack_properties()`
- `update_message_expiry()`
- All property structs

### 5.3 Phase 3: Secondary Extractions (Optional)

#### Step 3.1: Extract transport.rs from client.rs
```
Files: NEW transport.rs, MODIFY client.rs
Verify: make test
```

#### Step 3.2: Extract trie.rs from subscription.rs
```
Files: NEW trie.rs, MODIFY subscription.rs
Verify: make test
```

#### Step 3.3: Split config.rs (if needed)
```
Files: NEW config/mod.rs, config/server.rs, config/mqtt.rs, etc.
Verify: make test
```

### 5.4 Verification Checkpoints

After each step:
1. `cargo build` - Compiles without errors
2. `cargo clippy` - No new warnings
3. `make test` - All unit tests pass
4. `make test-integration` - Integration tests pass

After Phase 1 complete:
5. `make conformance-ci` - MQTT conformance tests pass
6. Manual testing with mosquitto_pub/sub

After Phase 2 complete:
7. `cargo test -p mqlite-core` - Core tests pass
8. Full test suite

### 5.5 Rollback Points

Each extraction is independent. If issues arise:
1. Revert the specific extraction commit
2. All previous extractions remain valid
3. Re-attempt with different approach

---

## Appendix A: Line Count Summary

### Current State
| Crate | Total Lines | Largest File |
|-------|-------------|--------------|
| mqlite-server | ~9,500 | worker.rs (2,208) |
| mqlite-core | ~2,300 | packet.rs (2,195) |
| mqlite-client | ~4,000 | async_client.rs (1,347) |

### Target State (After Phase 1+2)
| Crate | Total Lines | Largest File |
|-------|-------------|--------------|
| mqlite-server | ~9,500 | config.rs (~948) |
| mqlite-core | ~2,300 | packet.rs (~1,400) |

### New Files Created
| File | Lines | Phase |
|------|-------|-------|
| `will.rs` | ~250 | 1.1 |
| `handlers/mod.rs` | ~20 | 1.3 |
| `handlers/connect.rs` | ~300 | 1.3 |
| `handlers/publish.rs` | ~200 | 1.4 |
| `handlers/subscribe.rs` | ~200 | 1.5 |
| `session.rs` | ~150 | 1.6 |
| `varint.rs` | ~60 | 2.1 |
| `properties.rs` | ~400 | 2.2 |
| `transport.rs` | ~180 | 3.1 |
| `trie.rs` | ~250 | 3.2 |

---

## Appendix B: Dependency Graph

```
worker.rs
├── handlers/
│   ├── connect.rs → session.rs, shared.rs, auth/
│   ├── publish.rs → fanout.rs, shared.rs, auth/
│   └── subscribe.rs → shared.rs, auth/
├── fanout.rs → publish_encoder.rs, client_handle.rs
├── will.rs → fanout.rs
├── session.rs → shared.rs
└── route_cache.rs → subscription.rs

packet.rs
├── properties.rs
└── varint.rs

subscription.rs
└── trie.rs

client.rs
└── transport.rs
```
