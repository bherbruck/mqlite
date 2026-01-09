# mqlite-client Architecture

This document defines the architecture for the `mqlite-client` crate - a conformant, high-performance MQTT 3.1.1/5.0 client library.

## Design Goals

1. **RFC Conformance** - Implement all MUST requirements from MQTT 3.1.1 and 5.0 specs
2. **Performance** - Use mio for non-blocking I/O (same as broker)
3. **Ergonomics** - Provide callback API for simple usage, polling for advanced control
4. **Testability** - Raw packet access for fuzz testing the broker
5. **Flexibility** - Optional tokio integration for async applications

## Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     User-Facing APIs                        │
├─────────────────────────────────────────────────────────────┤
│  CallbackClient              │  AsyncClient (tokio feature) │
│  - on_connect()              │  - async connect()           │
│  - on_message()              │  - async publish()           │
│  - on_disconnect()           │  - async subscribe()         │
│  - run() / run_until()       │  - async next_message()      │
├─────────────────────────────────────────────────────────────┤
│                      Core Client                            │
│  - poll() / next_event()     (mio-based event loop)         │
│  - connect() / disconnect()                                 │
│  - publish() / subscribe() / unsubscribe()                  │
│  - Session state management                                 │
│  - Inflight message tracking                                │
│  - Packet ID allocation                                     │
├─────────────────────────────────────────────────────────────┤
│                    Protocol Layer                           │
│  - Packet encoding/decoding (mqlite-core)                   │
│  - QoS flow state machines                                  │
│  - Keep-alive management                                    │
├─────────────────────────────────────────────────────────────┤
│                    Transport Layer                          │
│  - TCP (mio::net::TcpStream)                                │
│  - TLS (rustls, optional)                                   │
│  - WebSocket (future)                                       │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Session State (`session.rs`)

Tracks client-side session state per [MQTT-3.1.2-4] through [MQTT-3.1.2-6]:

```rust
pub struct Session {
    /// Client identifier
    pub client_id: String,

    /// Whether this is a clean session
    pub clean_session: bool,

    /// QoS 1 messages sent but not yet acknowledged (awaiting PUBACK)
    pub pending_qos1_out: HashMap<u16, PendingPublish>,

    /// QoS 2 messages sent, awaiting PUBREC
    pub pending_qos2_out_pubrec: HashMap<u16, PendingPublish>,

    /// QoS 2 messages where PUBREC received, awaiting PUBCOMP
    pub pending_qos2_out_pubcomp: HashMap<u16, PendingPubrel>,

    /// QoS 2 messages received, awaiting PUBREL (we sent PUBREC)
    pub pending_qos2_in: HashMap<u16, ()>,

    /// Subscriptions (for session restore)
    pub subscriptions: Vec<Subscription>,
}

pub struct PendingPublish {
    pub packet_id: u16,
    pub topic: Bytes,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
    pub sent_at: Instant,
    pub retry_count: u32,
}
```

### 2. Packet ID Allocator (`packet_id.rs`)

Manages packet identifiers per [MQTT-2.3.1-2] and [MQTT-2.3.1-3]:

```rust
pub struct PacketIdAllocator {
    next_id: u16,
    in_use: HashSet<u16>,
}

impl PacketIdAllocator {
    /// Allocate a new unused packet ID
    pub fn allocate(&mut self) -> u16;

    /// Release a packet ID after completion
    pub fn release(&mut self, id: u16);

    /// Check if ID is in use
    pub fn is_in_use(&self, id: u16) -> bool;
}
```

### 3. QoS State Machines (`qos.rs`)

Implements the protocol flows from Section 7 of the MQTT spec:

**QoS 1 Sender:**
```
IDLE -> WAIT_PUBACK (send PUBLISH) -> IDLE (receive PUBACK)
                 \-> WAIT_PUBACK (timeout, resend with DUP=1)
```

**QoS 2 Sender:**
```
IDLE -> WAIT_PUBREC (send PUBLISH) -> WAIT_PUBCOMP (receive PUBREC, send PUBREL) -> IDLE (receive PUBCOMP)
                 \-> WAIT_PUBREC (timeout, resend PUBLISH with DUP=1)
                                            \-> WAIT_PUBCOMP (timeout, resend PUBREL)
```

**QoS 2 Receiver:**
```
IDLE -> WAIT_PUBREL (receive PUBLISH, send PUBREC) -> IDLE (receive PUBREL, send PUBCOMP)
                                   \-> WAIT_PUBREL (receive dup PUBLISH, resend PUBREC)
```

### 4. Will Message (`will.rs`)

Support for Last Will and Testament per [MQTT-3.1.2-8]:

```rust
pub struct Will {
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
}
```

### 5. Client Configuration (`config.rs`)

Builder pattern for client configuration:

```rust
pub struct ClientConfig {
    pub address: String,
    pub client_id: String,
    pub protocol_version: u8,  // 4 = 3.1.1, 5 = 5.0
    pub clean_session: bool,
    pub keep_alive: u16,
    pub username: Option<String>,
    pub password: Option<Bytes>,
    pub will: Option<Will>,
    pub connect_timeout: Duration,
    pub retry_interval: Duration,
    pub max_inflight: usize,
    pub auto_reconnect: bool,
    pub reconnect_backoff: BackoffConfig,
}

impl ClientConfig {
    pub fn new(address: impl Into<String>) -> Self;
    pub fn client_id(self, id: impl Into<String>) -> Self;
    pub fn will(self, will: Will) -> Self;
    pub fn credentials(self, username: &str, password: &str) -> Self;
    pub fn clean_session(self, clean: bool) -> Self;
    pub fn keep_alive(self, seconds: u16) -> Self;
    pub fn mqtt5(self) -> Self;
    // ... more builders
}
```

## API Layers

### 1. Core Polling API (Advanced/Fuzz Testing)

Direct control over the event loop:

```rust
pub struct Client {
    config: ClientConfig,
    state: ConnectionState,
    session: Session,
    packet_ids: PacketIdAllocator,
    poll: Poll,
    stream: Option<TcpStream>,
    // ... buffers, events queue
}

impl Client {
    pub fn new(config: ClientConfig) -> Result<Self>;

    // Connection
    pub fn connect(&mut self) -> Result<()>;
    pub fn disconnect(&mut self) -> Result<()>;
    pub fn reconnect(&mut self) -> Result<()>;

    // Messaging
    pub fn publish(&mut self, topic: &str, payload: &[u8], qos: QoS, retain: bool) -> Result<Option<u16>>;
    pub fn subscribe(&mut self, topics: &[(&str, QoS)]) -> Result<u16>;
    pub fn unsubscribe(&mut self, topics: &[&str]) -> Result<u16>;

    // Event loop
    pub fn poll(&mut self, timeout: Option<Duration>) -> Result<bool>;
    pub fn next_event(&mut self) -> Option<ClientEvent>;

    // State inspection
    pub fn is_connected(&self) -> bool;
    pub fn pending_messages(&self) -> usize;
    pub fn session(&self) -> &Session;
}

pub enum ClientEvent {
    Connected { session_present: bool },
    Disconnected { reason: Option<String> },
    Message { topic: Bytes, payload: Bytes, qos: QoS, retain: bool, packet_id: Option<u16> },
    Published { packet_id: u16 },  // QoS 1/2 complete
    Subscribed { packet_id: u16, return_codes: Vec<u8> },
    Unsubscribed { packet_id: u16 },
    Error { error: ClientError },
}
```

### 2. Callback API (Simple Usage)

Event-driven interface with callbacks:

```rust
pub struct CallbackClient<H: MqttHandler> {
    client: Client,
    handler: H,
}

pub trait MqttHandler {
    fn on_connect(&mut self, client: &Client, session_present: bool) {
        // Default: do nothing
    }

    fn on_message(&mut self, topic: &str, payload: &[u8], qos: QoS, retain: bool) {
        // Default: do nothing
    }

    fn on_published(&mut self, packet_id: u16) {
        // Default: do nothing
    }

    fn on_subscribed(&mut self, packet_id: u16, return_codes: &[u8]) {
        // Default: do nothing
    }

    fn on_disconnect(&mut self, reason: Option<&str>) {
        // Default: do nothing
    }

    fn on_error(&mut self, error: &ClientError) {
        // Default: do nothing
    }
}

impl<H: MqttHandler> CallbackClient<H> {
    pub fn new(config: ClientConfig, handler: H) -> Result<Self>;

    // Delegated methods
    pub fn publish(&mut self, topic: &str, payload: &[u8], qos: QoS, retain: bool) -> Result<Option<u16>>;
    pub fn subscribe(&mut self, topics: &[(&str, QoS)]) -> Result<u16>;
    pub fn unsubscribe(&mut self, topics: &[&str]) -> Result<u16>;
    pub fn disconnect(&mut self) -> Result<()>;

    // Run the event loop
    pub fn run(&mut self) -> Result<()>;  // Blocks until disconnect
    pub fn run_until<F: FnMut(&Client) -> bool>(&mut self, condition: F) -> Result<()>;

    // Access handler
    pub fn handler(&self) -> &H;
    pub fn handler_mut(&mut self) -> &mut H;
}

// Closure-based alternative (simpler for basic cases)
pub struct SimpleCallbacks {
    on_connect: Option<Box<dyn FnMut(bool) + Send>>,
    on_message: Option<Box<dyn FnMut(&str, &[u8], QoS, bool) + Send>>,
    on_disconnect: Option<Box<dyn FnMut(Option<&str>) + Send>>,
}
```

### 3. Async API (feature = "tokio")

Async/await interface for tokio users:

```rust
#[cfg(feature = "tokio")]
pub struct AsyncClient {
    inner: Arc<Mutex<Client>>,
    event_rx: mpsc::Receiver<ClientEvent>,
    _task: JoinHandle<()>,
}

#[cfg(feature = "tokio")]
impl AsyncClient {
    pub async fn connect(config: ClientConfig) -> Result<Self>;
    pub async fn publish(&self, topic: &str, payload: &[u8], qos: QoS, retain: bool) -> Result<Option<u16>>;
    pub async fn subscribe(&self, topics: &[(&str, QoS)]) -> Result<u16>;
    pub async fn unsubscribe(&self, topics: &[&str]) -> Result<u16>;
    pub async fn disconnect(&self) -> Result<()>;

    /// Receive next message (None when disconnected)
    pub async fn next_message(&mut self) -> Option<Message>;

    /// Stream of all events
    pub fn events(&mut self) -> impl Stream<Item = ClientEvent>;
}
```

## Conformance Requirements

### Client MUST (from Appendix A)

| Requirement | Implementation |
|-------------|----------------|
| [MQTT-3.1.0-1] First packet must be CONNECT | `connect()` enforces this |
| [MQTT-2.3.1-2] Assign unused packet IDs | `PacketIdAllocator` |
| [MQTT-2.3.1-3] Reuse same ID on retransmit | `Session.pending_*` tracking |
| [MQTT-3.3.1-1] Set DUP=1 on re-delivery | QoS state machines |
| [MQTT-4.4.0-1] Re-send unacked on reconnect | `reconnect()` implementation |
| [MQTT-4.6.0-1] Re-send in order | Ordered `VecDeque` for pending |
| [MQTT-4.6.0-2/3/4] ACKs in receive order | Process in order received |
| [MQTT-3.14.4-1] Close after DISCONNECT | `disconnect()` closes stream |

### Session State Persistence

For `clean_session=false`, client must track:
- Pending outbound QoS 1 messages (until PUBACK)
- Pending outbound QoS 2 messages (until PUBCOMP)
- Pending inbound QoS 2 message IDs (until PUBREL)

On reconnect with `clean_session=false`:
1. Re-send all pending QoS 1 PUBLISH with DUP=1
2. Re-send all pending QoS 2 PUBLISH (if awaiting PUBREC) with DUP=1
3. Re-send all pending PUBREL (if awaiting PUBCOMP)

## Fuzzing Support

For testing the broker, expose raw packet capabilities:

```rust
#[cfg(feature = "fuzzing")]
impl Client {
    /// Send raw bytes (for malformed packet testing)
    pub fn send_raw(&mut self, data: &[u8]) -> Result<()>;

    /// Receive raw bytes
    pub fn recv_raw(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Send a valid packet (bypassing normal flow)
    pub fn send_packet(&mut self, packet: &Packet) -> Result<()>;

    /// Inject delays between packet parts
    pub fn send_raw_chunked(&mut self, chunks: &[&[u8]], delays: &[Duration]) -> Result<()>;
}
```

## File Structure

```
crates/mqlite-client/src/
├── lib.rs              # Public exports, documentation
├── client.rs           # Core Client implementation (mio-based)
├── config.rs           # ClientConfig builder with BackoffConfig
├── error.rs            # ClientError types
├── events.rs           # ClientEvent, ConnectionState
├── session.rs          # Session state tracking (QoS 1/2 pending messages)
├── packet_id.rs        # Packet ID allocator
├── will.rs             # Will message type
├── callback.rs         # CallbackClient, MqttHandler trait
└── async_client.rs     # AsyncClient (feature = "async-tokio")
```

## Dependencies

```toml
[dependencies]
mqlite-core = { workspace = true }
mio = { version = "1.0", features = ["net", "os-poll"] }
bytes = { workspace = true }
thiserror = { workspace = true }
parking_lot = { workspace = true }
log = { workspace = true }
rustls = { version = "0.23", optional = true }
rustls-pemfile = { version = "2", optional = true }
tokio = { version = "1", features = ["rt", "sync", "time", "macros"], optional = true }

[features]
default = ["tls"]
tls = ["dep:rustls", "dep:rustls-pemfile"]
async-tokio = ["dep:tokio"]
```

## Implementation Status

1. **Phase 1: Core Conformance** ✅
   - [x] Session state tracking (`session.rs`)
   - [x] Packet ID allocator (`packet_id.rs`)
   - [x] Will message support (`will.rs`)
   - [x] QoS state machines with retry
   - [x] Reconnect with message re-delivery
   - [x] DUP flag handling

2. **Phase 2: Callback API** ✅
   - [x] `MqttHandler` trait
   - [x] `CallbackClient` wrapper
   - [x] `run()` and `run_until()` event loops

3. **Phase 3: Async API** ✅
   - [x] Tokio feature flag (`async-tokio`)
   - [x] `AsyncClient` implementation
   - [x] Channel-based event delivery
   - [x] `next_message()` for receiving messages

4. **Phase 4: Testing & Fuzzing** (TODO)
   - [ ] Fuzzing feature with raw packet injection
   - [ ] Conformance test suite against broker
   - [ ] Integration tests with broker

## Usage Examples

### Basic Callback Usage

```rust
use mqlite_client::{ClientConfig, CallbackClient, MqttHandler, QoS};

struct MyHandler;

impl MqttHandler for MyHandler {
    fn on_connect(&mut self, client: &Client, session_present: bool) {
        println!("Connected! Session present: {}", session_present);
        client.subscribe(&[("sensors/#", QoS::AtLeastOnce)]).unwrap();
    }

    fn on_message(&mut self, topic: &str, payload: &[u8], qos: QoS, retain: bool) {
        println!("Message on {}: {:?}", topic, payload);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new("localhost:1883")
        .client_id("my-sensor-reader")
        .clean_session(true);

    let mut client = CallbackClient::new(config, MyHandler)?;
    client.run()?;
    Ok(())
}
```

### Polling for Fuzz Testing

```rust
use mqlite_client::{Client, ClientConfig, ClientEvent};
use std::time::Duration;

fn fuzz_test_broker() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new("localhost:1883")
        .client_id("fuzzer");

    let mut client = Client::new(config)?;
    client.connect()?;

    // Wait for CONNACK
    loop {
        client.poll(Some(Duration::from_secs(5)))?;
        if let Some(ClientEvent::Connected { .. }) = client.next_event() {
            break;
        }
    }

    // Send malformed packet
    #[cfg(feature = "fuzzing")]
    client.send_raw(&[0xFF, 0x00])?;  // Invalid packet type

    // Check broker response
    client.poll(Some(Duration::from_secs(1)))?;

    Ok(())
}
```

### Async Usage

```rust
use mqlite_client::{AsyncClient, ClientConfig, QoS};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new("localhost:1883")
        .client_id("async-client");

    let mut client = AsyncClient::connect(config).await?;
    client.subscribe(&[("events/#", QoS::ExactlyOnce)]).await?;

    while let Some(msg) = client.next_message().await {
        println!("{}: {:?}", msg.topic, msg.payload);
    }

    Ok(())
}
```
