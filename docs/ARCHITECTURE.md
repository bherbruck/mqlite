# mqlite Architecture

A high-performance MQTT broker written in Rust.

## Table of Contents

1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Core Components](#core-components)
4. [Data Structures](#data-structures)
5. [Message Flow](#message-flow)
6. [Hot Path vs Cold Path](#hot-path-vs-cold-path)
7. [Concurrency Model](#concurrency-model)
8. [Protocol Support](#protocol-support)

---

## Overview

mqlite is a multi-threaded, event-driven MQTT broker optimized for low latency and high throughput. It uses `mio` for async I/O with epoll, direct cross-thread writes for publish delivery, and zero-copy message sharing.

**Key Design Principles:**
- Lock-free publishing via direct writes + epoll_ctl
- Zero-copy payload sharing with `Bytes`
- Memory reuse through preallocated buffers
- Atomic packet ID allocation without locks

---

## System Architecture

```
                           ┌─────────────────────────────────────────────┐
                           │              MAIN THREAD                     │
                           │   Server: Accept loop + round-robin dist    │
                           │                                             │
                           │   TcpListener (mio Poll)                    │
                           │         │                                    │
                           │         ▼                                    │
                           │   accept_connections()                       │
                           └──────────┬──────────────────────────────────┘
                                      │
                            Round-robin distribution
                       ┌──────────────┼──────────────┐
                       │              │              │
                       ▼              ▼              ▼
              ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
              │  WORKER 0   │ │  WORKER 1   │ │  WORKER N   │
              │  (Thread)   │ │  (Thread)   │ │  (Thread)   │
              │             │ │             │ │             │
              │ mio Poll    │ │ mio Poll    │ │ mio Poll    │
              │ (epoll)     │ │ (epoll)     │ │ (epoll)     │
              └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
                     │              │              │
                     ▼              ▼              ▼
              ┌───────────┐ ┌───────────┐  ┌───────────┐
              │[CLIENT]   │ │[CLIENT]   │  │[CLIENT]   │
              │[CLIENT]   │ │[CLIENT]   │  │           │
              │[CLIENT]   │ │           │  │           │
              └───────────┘ └───────────┘  └───────────┘
                     │              │              │
                     └──────────────┼──────────────┘
                                    │
                                    ▼
              ┌─────────────────────────────────────────────┐
              │         SHARED STATE (Arc<RwLock>)          │
              │                                             │
              │  ┌─────────────────────────────────────┐   │
              │  │ SubscriptionStore (topic trie)      │   │
              │  └─────────────────────────────────────┘   │
              │  ┌─────────────────────────────────────┐   │
              │  │ Sessions (persistent client state)  │   │
              │  └─────────────────────────────────────┘   │
              │  ┌─────────────────────────────────────┐   │
              │  │ RetainedMessages                    │   │
              │  └─────────────────────────────────────┘   │
              │  ┌─────────────────────────────────────┐   │
              │  │ ClientRegistry (routing table)      │   │
              │  └─────────────────────────────────────┘   │
              └─────────────────────────────────────────────┘
```

### Modes of Operation

**Single-Worker Mode (1 thread):**
- Worker runs in main thread (no spawn overhead)
- Lower latency, simpler execution path
- Server accept loop: 1ms timeout, Worker poll: 10ms

**Multi-Worker Mode (N threads):**
- Dedicated worker threads
- Main thread handles only accept loop (100ms timeout)
- Worker poll: 10ms timeout per worker

---

## Core Components

### Server (`src/server.rs`)

Entry point that accepts TCP connections and distributes them to workers.

```
┌──────────────────────────────────────────────────────────────┐
│ Server                                                        │
├──────────────────────────────────────────────────────────────┤
│ Fields:                                                       │
│   listener: TcpListener         # Bound socket               │
│   poll: Poll                    # mio event loop             │
│   worker_senders: Vec<Sender>   # Channels to workers        │
│   next_worker: usize            # Round-robin counter        │
├──────────────────────────────────────────────────────────────┤
│ Methods:                                                      │
│   new(addr) -> Server                                         │
│   with_workers(addr, n) -> Server                            │
│   run() -> !                    # Main event loop (blocks)   │
│   accept_connections()          # Accept + distribute        │
└──────────────────────────────────────────────────────────────┘
```

### Worker (`src/worker.rs`)

Handles a subset of client connections with its own event loop.

```
┌──────────────────────────────────────────────────────────────┐
│ Worker                                                        │
├──────────────────────────────────────────────────────────────┤
│ Fields:                                                       │
│   id: usize                     # Worker identifier          │
│   poll: Poll                    # mio event loop             │
│   epoll_fd: i32                 # For cross-thread epoll_ctl │
│   clients: AHashMap<Token, Client>    # Owned connections    │
│   token_to_handle: AHashMap<Token, ClientWriteHandle>        │
│   shared: Arc<SharedState>      # Global state reference     │
│   rx: Receiver<WorkerMsg>       # Control channel            │
│   worker_senders: Vec<Sender>   # Cross-worker comms         │
│   subscriber_buf: Vec<Subscriber>     # Reusable (hot path)  │
│   dedup_buf: AHashMap<...>            # Reusable (hot path)  │
│   route_cache: AHashMap<Bytes, CachedRoute>  # Topic→subs    │
│   cache_stats: CacheStats             # Adaptive caching     │
├──────────────────────────────────────────────────────────────┤
│ Event Loop (run_once):                                        │
│   1. poll.poll() with timeout                                 │
│   2. Process readable/writable events                         │
│   3. Check control channel (non-blocking)                     │
│   4. Clean up disconnected clients                            │
│   5. Check keep-alive timeouts                                │
└──────────────────────────────────────────────────────────────┘
```

### Client (`src/client.rs`)

Per-connection state and buffer management.

```
┌──────────────────────────────────────────────────────────────┐
│ Client                                                        │
├──────────────────────────────────────────────────────────────┤
│ Fields:                                                       │
│   socket: TcpStream             # Raw socket                 │
│   state: ClientState            # State machine              │
│   read_buf: Vec<u8>             # Input buffer               │
│   read_start/read_end: usize    # Data window (avoids copy)  │
│   pending_qos1: HashMap<u16, Publish>   # Awaiting PUBACK   │
│   pending_qos2: HashMap<u16, Publish>   # Awaiting PUBCOMP  │
│   handle: Arc<ClientWriteHandle>      # For cross-thread    │
│   keep_alive_secs: u16          # Timeout setting            │
│   last_activity: Instant        # For timeout check          │
├──────────────────────────────────────────────────────────────┤
│ State Machine:                                                │
│   Connecting ──(CONNECT)──► Connected ──(error)──► Disconnecting │
└──────────────────────────────────────────────────────────────┘
```

### ClientWriteHandle (`src/client_handle.rs`)

Enables cross-thread writes to client buffers without channels.

```
┌──────────────────────────────────────────────────────────────┐
│ ClientWriteHandle                                             │
├──────────────────────────────────────────────────────────────┤
│ Fields:                                                       │
│   write_buf: Mutex<WriteBuffer>       # Thread-safe output   │
│   socket: TcpStream (clone)           # For flush()          │
│   socket_fd: RawFd                    # For epoll_ctl        │
│   epoll_fd: i32                       # Worker's epoll       │
│   next_packet_id: AtomicU16           # Lock-free allocation │
│   needs_write: AtomicBool             # Debounce epoll_ctl   │
├──────────────────────────────────────────────────────────────┤
│ Key Methods:                                                  │
│   queue_packet(packet)          # Write from any thread      │
│   queue_publish(encoder, qos, id)     # Optimized publish    │
│   flush() -> Result             # Write to socket            │
│   allocate_packet_id() -> u16   # Atomic ID (wraps 1-65535)  │
│   set_ready_for_writing()       # epoll_ctl(EPOLL_CTL_MOD)   │
└──────────────────────────────────────────────────────────────┘
```

### WriteBuffer (`src/write_buffer.rs`)

Circular buffer for outgoing packets with vectored I/O support.

```
┌──────────────────────────────────────────────────────────────┐
│ WriteBuffer                                                   │
├──────────────────────────────────────────────────────────────┤
│ Layout (power-of-two capacity):                               │
│                                                               │
│   ┌───────────────────────────────────────────────┐          │
│   │░░░░░░░│▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓│░░░░░░░░░░░░░░░░░░░│          │
│   └───────────────────────────────────────────────┘          │
│          head              tail                               │
│          (read)            (write)                            │
│                                                               │
│   ░ = free space    ▓ = pending data                         │
│                                                               │
│ Limits:                                                       │
│   MIN_SIZE = 4KB                                              │
│   SOFT_LIMIT = 1MB    (returns WouldBlock for backpressure)  │
│   MAX_SIZE = 16MB     (returns OutOfMemory, causes disconnect)│
├──────────────────────────────────────────────────────────────┤
│ Key Methods:                                                  │
│   write_all(data) -> Result   # Append data (may grow)       │
│   as_io_slices() -> (IoSlice, IoSlice)   # For vectored I/O  │
│   consume(n)                  # Mark bytes as sent           │
└──────────────────────────────────────────────────────────────┘
```

### SubscriptionStore (`src/subscription.rs`)

Topic trie for efficient subscription matching.

```
┌──────────────────────────────────────────────────────────────┐
│ SubscriptionStore                                             │
├──────────────────────────────────────────────────────────────┤
│ Trie Structure:                                               │
│                                                               │
│   ROOT                                                        │
│    ├─ "home" ──► "sensor" ──► "temperature"                  │
│    │                │              │                          │
│    │                │              └─[Subscriber A, QoS 1]   │
│    │                │                                         │
│    │                └─ "+" ──────────[Subscriber B, QoS 0]   │
│    │                                                         │
│    └─ "#" ─────────────────────────[Subscriber C, QoS 2]    │
│                                                               │
│ Wildcard Rules (MQTT 4.7):                                    │
│   + = single level match                                      │
│   # = multi-level match (must be last)                        │
│   $ = system topic (not matched by wildcards at root)        │
├──────────────────────────────────────────────────────────────┤
│ Key Methods:                                                  │
│   subscribe(filter, subscriber)                               │
│   unsubscribe(filter, worker_id, token)                       │
│   match_topic_bytes_into(topic, result_buf)   # Hot path     │
└──────────────────────────────────────────────────────────────┘
```

### PublishEncoder (`src/publish_encoder.rs`)

Zero-copy packet encoding factory with QoS-based caching.

```
┌──────────────────────────────────────────────────────────────┐
│ PublishEncoder                                                │
├──────────────────────────────────────────────────────────────┤
│ Fields:                                                       │
│   topic: Bytes                  # Shared reference           │
│   payload: Bytes                # Shared reference           │
│   retain: bool                                                │
│   cache_qos0: Option<Bytes>     # Pre-encoded packet         │
├──────────────────────────────────────────────────────────────┤
│ Encoding Strategy by QoS:                                     │
│                                                               │
│   QoS 0: ┌─────────────────────────────────────┐             │
│          │ Complete packet (cached)             │             │
│          └─────────────────────────────────────┘             │
│          Same bytes for all subscribers                       │
│                                                               │
│   QoS 1/2: ┌────────────────┬────────┬─────────┐             │
│            │ Fixed header   │ Pkt ID │ Payload │             │
│            └────────────────┴────────┴─────────┘             │
│            Header cached, packet_id inserted per-subscriber   │
│                                                               │
│ Stack Optimization:                                           │
│   Packets ≤256 bytes: Encode to stack buffer                 │
│   Packets >256 bytes: Encode to heap buffer                  │
└──────────────────────────────────────────────────────────────┘
```

---

## Data Structures

### Where Structures Are Used

| Structure | Location | Purpose |
|-----------|----------|---------|
| `Client` | Worker (owned) | Connection state, I/O buffers |
| `ClientWriteHandle` | Worker + SharedState | Cross-thread writes |
| `WriteBuffer` | ClientWriteHandle | Output buffering |
| `SubscriptionStore` | SharedState | Topic matching |
| `Session` | SharedState | Persistent state (CleanSession=0) |
| `Subscriber` | SubscriptionStore nodes | Routing info (worker_id, token, handle) |
| `PublishEncoder` | Worker (stack/temp) | Publish fan-out |
| `Packet` | Worker (decoded) | MQTT protocol messages |

### Memory Layout Decisions

**Why `AHashMap` instead of `HashMap`:**
- Faster hashing for non-cryptographic use
- Better performance in tight loops
- Used for: clients, token mappings, dedup buffer

**Why `parking_lot::RwLock`:**
- Faster than std RwLock
- No poisoning (simpler error handling)
- Used for: all SharedState fields

**Why `Bytes` for topic/payload:**
- Zero-copy sharing across threads
- Reference counting (Arc internally)
- Cheap clones for fan-out

**Why circular buffer (`WriteBuffer`):**
- No allocations in steady state
- Power-of-two sizing for fast modulo
- Vectored I/O for wraparound

---

## Message Flow

### Publish Flow (Complete Data Path)

```
┌─────────────┐
│  PUBLISHER  │
│  (Client)   │
└──────┬──────┘
       │
       │ TCP: PUBLISH packet bytes
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ WORKER (Publisher's)                                          │
│                                                               │
│ ┌─────────────────────────────────────────────────────────┐  │
│ │ handle_readable()                                        │  │
│ │   │                                                      │  │
│ │   ▼                                                      │  │
│ │ socket.read() ──► read_buf (BytesMut)                   │  │
│ │   │                                                      │  │
│ │   ▼                                                      │  │
│ │ decode_packet() ──► Packet::Publish { topic, payload }  │  │
│ └──────────────────────────────┬──────────────────────────┘  │
│                                │                              │
│ ┌──────────────────────────────▼──────────────────────────┐  │
│ │ handle_publish()                                         │  │
│ │                                                          │  │
│ │   ┌─────────────────────────────────────────────────┐   │  │
│ │   │ 1. QoS Acknowledgment                           │   │  │
│ │   │    QoS 0: (none)                                │   │  │
│ │   │    QoS 1: PUBACK ──► publisher.handle           │   │  │
│ │   │    QoS 2: PUBREC ──► publisher.handle           │   │  │
│ │   └─────────────────────────────────────────────────┘   │  │
│ │                                                          │  │
│ │   ┌─────────────────────────────────────────────────┐   │  │
│ │   │ 2. Retained Message (if retain flag set)        │   │  │
│ │   │    shared.retained_messages.write()             │   │  │
│ │   │    └── HashMap<topic, Publish>                  │   │  │
│ │   └─────────────────────────────────────────────────┘   │  │
│ │                                                          │  │
│ │   ┌─────────────────────────────────────────────────┐   │  │
│ │   │ 3. Find Matching Subscribers (Route Cache)      │   │  │
│ │   │    get_subscribers_cached(&topic)               │   │  │
│ │   │    ├── Check route_cache[topic]                 │   │  │
│ │   │    │   └── Valid if generation matches          │   │  │
│ │   │    ├── Cache HIT: clone pre-deduped subs (O(1)) │   │  │
│ │   │    └── Cache MISS: trie lookup + dedup + cache  │   │  │
│ │   │        └── Returns: Vec<Subscriber> (deduped)   │   │  │
│ │   └─────────────────────────────────────────────────┘   │  │
│ │                                                          │  │
│ │   NOTE: Deduplication is done at cache time, not here.  │  │
│ │   Subscriber.client_id is Arc<str> for zero-copy clone. │  │
│ │                                                          │  │
│ │   ┌─────────────────────────────────────────────────┐   │  │
│ │   │ 5. Create PublishEncoder (zero-copy)            │   │  │
│ │   │    PublishEncoder::new(topic, payload, retain)  │   │  │
│ │   │    └── Caches QoS 0 encoding                    │   │  │
│ │   └─────────────────────────────────────────────────┘   │  │
│ │                                                          │  │
│ │   ┌─────────────────────────────────────────────────┐   │  │
│ │   │ 6. For Each Subscriber:                         │   │  │
│ │   │                                                  │   │  │
│ │   │    effective_qos = min(pub_qos, sub_qos)        │   │  │
│ │   │                                                  │   │  │
│ │   │    if effective_qos > 0:                        │   │  │
│ │   │      packet_id = sub.handle.allocate_packet_id()│   │  │
│ │   │      (atomic, wraps 1-65535)                    │   │  │
│ │   │                                                  │   │  │
│ │   │    if same_worker:                              │   │  │
│ │   │      client.pending_qos1/2.push(packet_id, pub) │   │  │
│ │   │                                                  │   │  │
│ │   │    sub.handle.queue_publish(encoder, qos, id)   │   │  │
│ │   │    └── Direct write to WriteBuffer (Mutex)      │   │  │
│ │   │    └── set_ready_for_writing() ──► epoll_ctl    │   │  │
│ │   └─────────────────────────────────────────────────┘   │  │
│ └──────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────┘
       │
       │ (epoll wakeup on other workers)
       │
       ▼
┌───────────────────────────────────────────────────────────────┐
│ WORKER (Subscriber's)                                          │
│                                                                │
│ ┌────────────────────────────────────────────────────────────┐│
│ │ handle_writable()                                          ││
│ │   │                                                        ││
│ │   ▼                                                        ││
│ │ handle.flush()                                             ││
│ │   │                                                        ││
│ │   ▼                                                        ││
│ │ write_buf.as_io_slices() ──► (slice1, slice2)             ││
│ │   │                       (handles wraparound)             ││
│ │   ▼                                                        ││
│ │ socket.write_vectored() ──► TCP                            ││
│ │   │                                                        ││
│ │   ▼                                                        ││
│ │ write_buf.consume(bytes_written)                           ││
│ └────────────────────────────────────────────────────────────┘│
└───────────────────────────────────────────────────────────────┘
       │
       │ TCP: PUBLISH packet bytes
       │
       ▼
┌─────────────┐
│ SUBSCRIBER  │
│  (Client)   │
└─────────────┘
```

### Subscribe Flow

```
┌──────────────┐
│   CLIENT     │
└──────┬───────┘
       │
       │ SUBSCRIBE { topic_filters: [(filter, qos), ...] }
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ WORKER                                                        │
│                                                               │
│ handle_subscribe()                                            │
│   │                                                           │
│   ├─► For each (filter, requested_qos):                      │
│   │     │                                                     │
│   │     ├─► shared.subscriptions.write()                     │
│   │     │     └─► Add Subscriber { worker_id, token,         │
│   │     │                          handle, qos } to trie     │
│   │     │                                                     │
│   │     ├─► shared.sessions.write() (if CleanSession=0)      │
│   │     │     └─► session.subscriptions.push(filter, qos)    │
│   │     │                                                     │
│   │     └─► Match retained messages                          │
│   │           shared.retained_messages.read()                │
│   │           └─► For each matching: queue_publish()         │
│   │                                                           │
│   └─► Send SUBACK { return_codes: [granted_qos, ...] }       │
│         └─► client.handle.queue_packet()                     │
└──────────────────────────────────────────────────────────────┘
```

### Session Reconnect Flow

```
┌──────────────────────────────────────────────────────────────┐
│ SAME WORKER RECONNECT                                         │
│                                                               │
│ New CONNECT from existing client_id                          │
│   │                                                           │
│   ├─► shared.client_registry.read()                          │
│   │     └─► Find: ClientLocation { worker_id, token }        │
│   │                                                           │
│   ├─► (worker_id == self.id) ──► Same worker                 │
│   │     │                                                     │
│   │     ├─► Save old subscriptions from client               │
│   │     ├─► old_client.state = Disconnecting                 │
│   │     ├─► Remove old subscriptions from trie               │
│   │     │                                                     │
│   │     ├─► shared.sessions.read()                           │
│   │     │     └─► Get saved subscriptions + pending msgs     │
│   │     │                                                     │
│   │     ├─► Re-add subscriptions with new token              │
│   │     ├─► Re-send pending QoS 1/2 messages                 │
│   │     │                                                     │
│   │     └─► CONNACK { session_present: true }                │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ CROSS-WORKER RECONNECT                                        │
│                                                               │
│     WORKER A (old)                    WORKER B (new)         │
│         │                                  │                  │
│         │                    ┌─────────────┤                  │
│         │                    │ New CONNECT │                  │
│         │                    └─────────────┘                  │
│         │                                  │                  │
│         │        ◄───────────────────────  │                  │
│         │        Disconnect { token }      │                  │
│         │        (via channel)             │                  │
│         │                                  │                  │
│  ┌──────▼──────┐                          │                  │
│  │ Save pending│                          │ (waiting for     │
│  │ to session  │                          │  takeover_complete│
│  │             │                          │  with 100ms       │
│  │ takeover_   │                          │  timeout)         │
│  │ complete=   │                          │                  │
│  │ true        │                          │                  │
│  └──────┬──────┘                          │                  │
│         │                                  │                  │
│         │         ───────────────────────► │                  │
│         │         (session updated)        │                  │
│                                            │                  │
│                               ┌────────────▼───────────────┐ │
│                               │ Restore from session       │ │
│                               │ Re-add subscriptions       │ │
│                               │ Re-send pending messages   │ │
│                               │ CONNACK { session: true }  │ │
│                               └────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

---

## Hot Path vs Cold Path

### End-to-End Hot Path (Byte-Level Flow)

This is the complete data flow for a publish message, showing every buffer
and transformation from TCP read to TCP write.

```
══════════════════════════════════════════════════════════════════════════════
                        PUBLISHER SIDE (Ingress)
══════════════════════════════════════════════════════════════════════════════

  TCP Socket (Publisher)
       │
       │ kernel recv buffer → user space
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Client.read_buf (Vec<u8> with read_start/read_end pointers)                 │
│                                                                             │
│ socket.read() appends bytes at read_end:                                    │
│ ┌───────────────────────────────────────────────────────────────┐          │
│ │░░░░░░░│ 30 0E 00 04 74 65 73 74 68 65 6C 6C 6F │░░░░░░░░░░░░░│          │
│ │       ▲ read_start                              ▲ read_end   │          │
│ │       │ PUBLISH header + payload                │            │          │
│ └───────────────────────────────────────────────────────────────┘          │
│                                                                             │
│ Pointer-based approach (avoids memmove on every packet):                   │
│   • read_start advances after each decoded packet                          │
│   • Compact only when: >50% wasted OR need to grow                         │
│   • Shrink to 1KB when idle and >16KB (with 2-tick hysteresis)            │
│                                                                             │
│ decode_packet() parses:                                                     │
│   • Fixed header: type=PUBLISH, flags=0x0, remaining_len=14                │
│   • Variable header: topic_len=4, topic="test"                             │
│   • Payload: "hello world"                                                 │
│                                                                             │
│ Output: Packet::Publish { topic: Bytes, payload: Bytes, ... }              │
│         Topic/payload copied to Bytes for zero-copy fan-out                │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       │ Packet::Publish { topic, payload, qos, ... }
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Worker.handle_publish()                                                     │
│                                                                             │
│ Step 1: Find subscribers (via Route Cache)                                  │
│ ┌─────────────────────────────────────────────────────────────────────────┐│
│ │ get_subscribers_cached(&topic)                                          ││
│ │                                                                         ││
│ │ ┌─ Check route_cache[topic] ─────────────────────────────────────────┐ ││
│ │ │                                                                     │ ││
│ │ │ if cached.generation == subscriptions.generation():                 │ ││
│ │ │   → CACHE HIT: clone cached.subscribers (Arc refcounts only)       │ ││
│ │ │                                                                     │ ││
│ │ │ else:                                                               │ ││
│ │ │   → CACHE MISS: trie lookup + dedup + cache insert                 │ ││
│ │ │     subscriptions.read().match_topic_bytes_into(topic, buf)        │ ││
│ │ │     dedup_subscriber_buf()  // keeps highest QoS per client        │ ││
│ │ │     route_cache.insert(topic, CachedRoute { subs, gen })           │ ││
│ │ └─────────────────────────────────────────────────────────────────────┘ ││
│ │                                                                         ││
│ │ Result: subscriber_buf contains PRE-DEDUPLICATED subscribers            ││
│ │ ┌───────────────────────────────────────────────────────────┐          ││
│ │ │ [0] Subscriber { handle, qos: 1, client_id: Arc<str> }    │          ││
│ │ │ [1] Subscriber { handle, qos: 0, client_id: Arc<str> }    │          ││
│ │ │ [2] Subscriber { handle, qos: 2, client_id: Arc<str> }    │          ││
│ │ └───────────────────────────────────────────────────────────┘          ││
│ └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│ NOTE: Deduplication is done at cache time (see Step 1 above)              │
│ ┌─────────────────────────────────────────────────────────────────────────┐│
│ │ subscriber_buf already contains deduplicated subscribers               ││
│ │                                                                         ││
│ │ Dedup algorithm (at cache time only):                                   ││
│ │   For sub in raw_subscribers:                                           ││
│ │     key = (sub.worker_id, sub.token)                                    ││
│ │     dedup_buf.entry(key).or_insert(sub)  // keeps highest QoS          ││
│ └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│ Step 3: Create encoder (zero-copy)                                         │
│ ┌─────────────────────────────────────────────────────────────────────────┐│
│ │ PublishEncoder::new(topic.clone(), payload.clone(), retain)             ││
│ │                       ───────────  ──────────────                       ││
│ │                           │              │                              ││
│ │                           ▼              ▼                              ││
│ │                    Arc refcount++   Arc refcount++                      ││
│ │                    (NO DATA COPY)   (NO DATA COPY)                      ││
│ │                                                                         ││
│ │ encoder.cache_qos0 = Some(pre_encoded_packet)  // if first QoS 0 sub   ││
│ └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
       │
       │ For each unique subscriber...
       │
       ▼
══════════════════════════════════════════════════════════════════════════════
                        SUBSCRIBER SIDE (Egress)
══════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│ ClientWriteHandle.queue_publish()                                           │
│                                                                             │
│ Step 4: Allocate packet ID (QoS > 0 only)                                  │
│ ┌─────────────────────────────────────────────────────────────────────────┐│
│ │ next_packet_id: AtomicU16 (starts at 1)                                 ││
│ │                                                                         ││
│ │ loop {                                                                  ││
│ │   id = next_packet_id.fetch_add(1, Relaxed)   // returns OLD value     ││
│ │   if id != 0 { return id }                    // 0 is invalid per MQTT ││
│ │   // Wrapped from 65535→0, loop to skip 0                              ││
│ │ }                                                                       ││
│ │                                                                         ││
│ │ Sequence: 1, 2, 3, ... 65535, (skip 0), 1, 2, ...                       ││
│ │ NO LOCK - single atomic fetch_add (rarely loops)                        ││
│ └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│ Step 5: Encode packet                                                       │
│ ┌─────────────────────────────────────────────────────────────────────────┐│
│ │ encoder.encode_to(qos=1, packet_id=42, &write_buf)                      ││
│ │                                                                         ││
│ │ QoS 0 path:                         QoS 1/2 path:                       ││
│ │ ┌─────────────────────────┐        ┌──────────────────────────────┐    ││
│ │ │ Return cached bytes     │        │ Encode header + packet_id    │    ││
│ │ │ (clone = Arc refcount++)│        │                              │    ││
│ │ └─────────────────────────┘        │ Small packet (≤256 bytes):   │    ││
│ │                                    │   [u8; 256] on STACK         │    ││
│ │                                    │                              │    ││
│ │                                    │ Large packet (>256 bytes):   │    ││
│ │                                    │   Vec<u8> on heap            │    ││
│ │                                    └──────────────────────────────┘    ││
│ │                                                                         ││
│ │ Encoded bytes: 32 0F 00 04 74 65 73 74 00 2A 68 65 6C 6C 6F ...        ││
│ │                ▲▲ ▲▲ ────────────────── ───── ─────────────────        ││
│ │                │  │  topic "test"       pkt_id payload                 ││
│ │                │  remaining_length=15   =42                            ││
│ │                PUBLISH+QoS1 flags                                       ││
│ └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│ Step 6: Write to circular buffer                                           │
│ ┌─────────────────────────────────────────────────────────────────────────┐│
│ │ write_buf: Mutex<WriteBuffer>                                           ││
│ │                                                                         ││
│ │ let mut buf = write_buf.lock();  // parking_lot Mutex (fast)           ││
│ │ buf.write_all(&encoded_bytes)?;                                         ││
│ │                                                                         ││
│ │ WriteBuffer internals:                                                  ││
│ │ ┌─────────────────────────────────────────────────────────────────┐    ││
│ │ │ data: Vec<u8>     // power-of-two capacity (e.g., 4096)         │    ││
│ │ │ head: usize       // read position (what's been sent)           │    ││
│ │ │ len: usize        // bytes pending                               │    ││
│ │ │ mask: usize       // capacity - 1 (for fast modulo)             │    ││
│ │ └─────────────────────────────────────────────────────────────────┘    ││
│ │                                                                         ││
│ │ BEFORE write_all (head=100, len=50, capacity=4096):                     ││
│ │ ┌───────────────────────────────────────────────────────────────────┐  ││
│ │ │░░░░░░░░░░░░░░░░░░░░│▓▓▓▓▓▓▓▓▓▓▓▓▓│░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│  ││
│ │ └───────────────────────────────────────────────────────────────────┘  ││
│ │                       ▲head=100     ▲tail=150                          ││
│ │                       (read pos)    (write pos)                        ││
│ │                                                                         ││
│ │ AFTER write_all (17 bytes written, head=100, len=67):                   ││
│ │ ┌───────────────────────────────────────────────────────────────────┐  ││
│ │ │░░░░░░░░░░░░░░░░░░░░│▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓│░░░░░░░░░░░░░│  ││
│ │ └───────────────────────────────────────────────────────────────────┘  ││
│ │                       ▲head=100                      ▲tail=167         ││
│ │                                                                         ││
│ │ tail = (head + len) & mask   // fast modulo via bitmask                ││
│ │                                                                         ││
│ │ WRAPAROUND case (head=4000, len=200, capacity=4096):                    ││
│ │ ┌───────────────────────────────────────────────────────────────────┐  ││
│ │ │▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓│░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│▓▓▓▓▓▓▓▓▓▓▓▓▓│  ││
│ │ └───────────────────────────────────────────────────────────────────┘  ││
│ │  ▲tail=104 (wrapped)                                 ▲head=4000        ││
│ │  ├──────────────────────────────────────────────────►│                 ││
│ │              logically contiguous data                                  ││
│ └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│ Step 7: Signal epoll (if needed)                                           │
│ ┌─────────────────────────────────────────────────────────────────────────┐│
│ │ if !needs_write.swap(true, Relaxed) {                                   ││
│ │     // First write since last flush - update epoll interest            ││
│ │     epoll_ctl(epoll_fd, EPOLL_CTL_MOD, socket_fd,                       ││
│ │               EPOLLIN | EPOLLOUT | EPOLLET)                             ││
│ │ }                                                                       ││
│ │                                                                         ││
│ │ Debouncing: Only ONE epoll_ctl per write batch                          ││
│ │ (multiple queue_publish calls → single syscall)                         ││
│ └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
       │
       │ epoll wakes subscriber's worker thread
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Worker.handle_writable() → ClientWriteHandle.flush()                        │
│                                                                             │
│ Step 8: Flush circular buffer to socket                                    │
│ ┌─────────────────────────────────────────────────────────────────────────┐│
│ │ let mut buf = write_buf.lock();                                         ││
│ │                                                                         ││
│ │ // Get contiguous slices (handles wraparound)                           ││
│ │ let (slice1, slice2) = buf.as_io_slices();                              ││
│ │                                                                         ││
│ │ Normal case (no wraparound):                                            ││
│ │ ┌─────────────────────────────────────────────────────────────────┐    ││
│ │ │ slice1: [head..tail]  (all data)                                │    ││
│ │ │ slice2: [] (empty)                                              │    ││
│ │ └─────────────────────────────────────────────────────────────────┘    ││
│ │                                                                         ││
│ │ Wraparound case:                                                        ││
│ │ ┌─────────────────────────────────────────────────────────────────┐    ││
│ │ │ slice1: [head..capacity]  (end of buffer)                       │    ││
│ │ │ slice2: [0..tail]         (start of buffer)                     │    ││
│ │ └─────────────────────────────────────────────────────────────────┘    ││
│ │                                                                         ││
│ │ SINGLE syscall for both slices:                                         ││
│ │ let n = socket.write_vectored(&[slice1, slice2])?;                      ││
│ │                                                                         ││
│ │ buf.consume(n);  // advance head by n bytes                             ││
│ │                                                                         ││
│ │ After consume:                                                          ││
│ │   head = (head + n) & mask                                              ││
│ │   len -= n                                                              ││
│ │                                                                         ││
│ │ if len == 0 {                                                           ││
│ │     needs_write.store(false, Relaxed);                                  ││
│ │     // Remove EPOLLOUT interest (stop waking for writes)               ││
│ │     epoll_ctl(epoll_fd, EPOLL_CTL_MOD, socket_fd, EPOLLIN | EPOLLET)   ││
│ │ }                                                                       ││
│ └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
       │
       │ kernel send buffer → network
       │
       ▼
  TCP Socket (Subscriber)
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Subscriber Client receives PUBLISH packet                                   │
│                                                                             │
│ 32 0F 00 04 74 65 73 74 00 2A 68 65 6C 6C 6F 20 77 6F 72 6C 64            │
│ ─────────────────────────────────────────────────────────────              │
│ PUBLISH QoS1, topic="test", packet_id=42, payload="hello world"            │
└─────────────────────────────────────────────────────────────────────────────┘


══════════════════════════════════════════════════════════════════════════════
                        SUMMARY: Zero-Copy Pipeline
══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────┐
  │                                                                         │
  │  TCP recv     read_buf      Bytes       encoder     WriteBuffer    TCP  │
  │  ─────────►  (BytesMut)  ──────────►  ──────────►  ───────────►  send   │
  │                  │            │                         │               │
  │                  │            │                         │               │
  │                  ▼            ▼                         ▼               │
  │              COPY #1     Arc clone     encode      COPY #2             │
  │           (kernel→user)  (refcount)   (format)   (user→kernel)         │
  │                                                                         │
  │  Copies: 2 (unavoidable: kernel↔userspace)                             │
  │  Allocations: 0 (steady state - all buffers preallocated)              │
  │  Locks: 1 mutex (WriteBuffer, fast path)                               │
  │  Syscalls: 2 (read + write_vectored)                                   │
  │                                                                         │
  └─────────────────────────────────────────────────────────────────────────┘
```

### Payload Lifecycle (When Memory is Freed)

`Bytes` uses reference counting (Arc internally). Payload is freed when refcount → 0.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ REFERENCE COUNT LIFECYCLE                                                    │
└─────────────────────────────────────────────────────────────────────────────┘

  decode_packet() creates Publish { payload: Bytes }
       │
       │ refcount = 1
       ▼
  PublishEncoder::new(payload.clone())
       │
       │ refcount = 2
       ▼
  ┌────────────────────────────┬────────────────────────────────────────────┐
  │ QoS 0 path                 │ QoS 1/2 path                               │
  ├────────────────────────────┼────────────────────────────────────────────┤
  │                            │                                            │
  │ encode → WriteBuffer       │ encode → WriteBuffer                       │
  │ (bytes copied to buffer)   │ (bytes copied to buffer)                   │
  │                            │                                            │
  │                            │ pending_qos1.insert(publish.clone())       │
  │                            │ refcount = 3                               │
  │                            │                                            │
  ├────────────────────────────┼────────────────────────────────────────────┤
  │ encoder dropped            │ encoder dropped                            │
  │ refcount = 1               │ refcount = 2                               │
  │                            │                                            │
  │ original Publish dropped   │ original Publish dropped                   │
  │ refcount = 0               │ refcount = 1                               │
  │                            │                                            │
  │ ══════════════════════     │ (still held by pending_qos1)               │
  │ ▶ PAYLOAD FREED ◀          │                                            │
  │ ══════════════════════     │ ... time passes ...                        │
  │                            │                                            │
  │                            │ PUBACK received                            │
  │                            │ pending_qos1.remove(&packet_id)            │
  │                            │ refcount = 0                               │
  │                            │                                            │
  │                            │ ══════════════════════                     │
  │                            │ ▶ PAYLOAD FREED ◀                          │
  │                            │ ══════════════════════                     │
  └────────────────────────────┴────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ WHAT EXTENDS PAYLOAD LIFETIME                                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ 1. QoS 1/2 pending ACKs (pending_qos1/2 HashMap)                           │
│    └─► Freed on: PUBACK/PUBCOMP received                                   │
│                                                                             │
│ 2. Retained messages (shared.retained_messages)                             │
│    └─► Freed on: Replaced by new retained msg, or empty payload clears    │
│                                                                             │
│ 3. Session persistence on disconnect (session.pending_qos1/2)              │
│    └─► Freed on: Client reconnects and messages resent + ACKed            │
│                                                                             │
│ 4. Will message (client.will)                                               │
│    └─► Freed on: Client disconnects (published) or clean disconnect       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ MEMORY IMPLICATIONS BY QoS                                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│ QoS 0: Payload freed immediately after fan-out completes                   │
│        Memory: O(1) per message - no persistence                           │
│                                                                             │
│ QoS 1: Payload held until PUBACK from each subscriber                      │
│        Memory: O(subscribers) per message while in-flight                  │
│                                                                             │
│ QoS 2: Payload held until PUBCOMP from each subscriber                     │
│        Memory: O(subscribers) per message, longer than QoS 1               │
│                                                                             │
│ Retained: Payload held indefinitely (one per topic)                         │
│        Memory: O(retained_topics)                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Hot Path Operations Summary

```
┌──────────────────────────────────────────────────────────────┐
│ HOT PATH OPERATIONS                                           │
│ (Optimized for minimal latency and allocations)              │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│ 1. SOCKET READ                                                │
│    └─► Non-blocking read into BytesMut                       │
│    └─► Buffer grows only when needed                         │
│                                                               │
│ 2. PACKET DECODE                                              │
│    └─► Streaming decode (no full buffer copy)                │
│    └─► Zero-copy Bytes for topic/payload                     │
│                                                               │
│ 3. ROUTE CACHE LOOKUP                                         │
│    └─► Check route_cache[topic] with generation validation   │
│    └─► Cache HIT: O(1) hash lookup + Arc refcount clones     │
│    └─► Cache MISS: trie traversal + dedup + cache insert     │
│    └─► Adaptive: disables if hit rate < 50% (high entropy)   │
│                                                               │
│ 4. DEDUPLICATION (at cache time only)                         │
│    └─► Done once when caching, not per-publish               │
│    └─► Cache stores pre-deduplicated subscribers             │
│    └─► Subscriber.client_id is Arc<str> (zero-copy clone)    │
│                                                               │
│ 5. PUBLISH ENCODING                                           │
│    └─► QoS 0: Cached complete packet (clone only)            │
│    └─► Small packets: Stack buffer (256 bytes)               │
│    └─► Zero-copy topic/payload via Bytes                     │
│                                                               │
│ 6. CROSS-THREAD WRITE                                         │
│    └─► Direct Mutex lock (no channel)                        │
│    └─► Write to circular buffer (no allocation)              │
│    └─► Atomic epoll_ctl (debounced with AtomicBool)          │
│                                                               │
│ 7. PACKET ID ALLOCATION                                       │
│    └─► AtomicU16::fetch_add (no lock)                        │
│    └─► Returns 1-65535, skips 0 on wrap (MQTT requirement)   │
│                                                               │
│ 8. SOCKET FLUSH                                               │
│    └─► Vectored I/O (single syscall for wraparound)          │
│    └─► IoSlice pair from circular buffer                     │
│                                                               │
└──────────────────────────────────────────────────────────────┘

Memory Reuse in Worker:

┌────────────────────────────────────────────────────────────┐
│ struct Worker {                                             │
│     subscriber_buf: Vec<Subscriber>,  // initial cap: 1024 │
│     dedup_buf: AHashMap<(usize, Token), Subscriber>,       │
│                                       // initial cap: 1024 │
│     route_cache: AHashMap<Bytes, CachedRoute>,             │
│                                       // max 10,000 entries│
│ }                                                          │
│                                                            │
│ Per-publish cycle (cache HIT):                             │
│   subscriber_buf.clear()    // keeps capacity              │
│   subscriber_buf.extend(cached.subscribers.iter().cloned())│
│   // Zero heap allocation - all clones are Arc refcounts! │
│                                                            │
│ Per-publish cycle (cache MISS):                            │
│   subscriber_buf.clear()    // keeps capacity              │
│   dedup_buf.clear()         // keeps capacity              │
│   // Trie lookup → dedup → cache insert                   │
│                                                            │
│ Route Cache:                                               │
│   • Generation-based invalidation (AtomicU64)             │
│   • Pre-deduplicated subscribers stored                   │
│   • Adaptive: disables if hit rate < 50%                  │
│   • Clears all on overflow (simple eviction)              │
│                                                            │
│ Result: O(1) lookups in steady state                      │
└────────────────────────────────────────────────────────────┘
```

### Cold Path (Less Frequent)

```
┌──────────────────────────────────────────────────────────────┐
│ COLD PATH OPERATIONS                                          │
│ (Less frequent, more complex, may allocate)                  │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│ 1. CLIENT CONNECTION                                          │
│    └─► New Client struct allocation                          │
│    └─► New WriteBuffer (4KB minimum)                         │
│    └─► HashMap insertions                                    │
│    └─► epoll_ctl(EPOLL_CTL_ADD)                             │
│                                                               │
│ 2. CLIENT DISCONNECTION                                       │
│    └─► Subscription cleanup (write lock)                     │
│    └─► Session save (if CleanSession=0)                      │
│    └─► HashMap removals                                      │
│    └─► epoll_ctl(EPOLL_CTL_DEL)                             │
│                                                               │
│ 3. SESSION RESTORATION                                        │
│    └─► RwLock write acquisitions                             │
│    └─► Vec cloning for subscriptions                         │
│    └─► Pending message re-transmission                       │
│                                                               │
│ 4. RETAINED MESSAGE MATCHING                                  │
│    └─► Full trie traversal                                   │
│    └─► May match many messages                               │
│                                                               │
│ 5. KEEP-ALIVE TIMEOUT CHECK                                   │
│    └─► Linear iteration over all clients                     │
│    └─► Instant::now() comparison                             │
│                                                               │
│ 6. CROSS-WORKER SESSION TAKEOVER                              │
│    └─► Channel message passing                               │
│    └─► Synchronization wait (100ms timeout)                  │
│                                                               │
│ 7. BUFFER GROWTH                                              │
│    └─► WriteBuffer resize (doubles capacity)                 │
│    └─► ReadBuffer resize (BytesMut reallocation)            │
│                                                               │
│ 8. ERROR HANDLING                                             │
│    └─► Disconnect + cleanup                                  │
│    └─► Will message publication                              │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### Performance Characteristics

```
┌────────────────────────────────────────────────────────────────┐
│ LATENCY OPTIMIZATION                                           │
├────────────────────────────────────────────────────────────────┤
│ Single-worker mode:                                            │
│   • Runs in main thread (no spawn overhead)                   │
│   • Accept: 1ms, Worker poll: 10ms                            │
│   • No cross-thread communication                             │
│                                                                │
│ Multi-worker mode:                                             │
│   • Worker poll: 10ms (balance CPU/latency)                   │
│   • Direct writes (no channel queuing)                        │
│   • Atomic epoll_ctl updates                                  │
├────────────────────────────────────────────────────────────────┤
│ THROUGHPUT OPTIMIZATION                                        │
├────────────────────────────────────────────────────────────────┤
│ • Round-robin load balancing                                   │
│ • Batch processing (all readable clients per poll)            │
│ • Vectored I/O (single syscall for wraparound)               │
│ • Zero-copy payload sharing                                   │
│ • Fast hashing (ahash)                                        │
├────────────────────────────────────────────────────────────────┤
│ MEMORY EFFICIENCY                                              │
├────────────────────────────────────────────────────────────────┤
│ • Circular buffers (no steady-state allocation)               │
│ • Pre-sized subscriber/dedup buffers (self-tuning)            │
│ • Stack allocation for small packets                          │
│ • Soft limit backpressure (1MB)                               │
│ • Reference-counted payloads (Bytes)                          │
└────────────────────────────────────────────────────────────────┘
```

---

## Concurrency Model

### Thread Safety Strategy

```
┌──────────────────────────────────────────────────────────────┐
│ DATA PROTECTION                                               │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│ SharedState (Arc<RwLock> for each field):                    │
│   ┌─────────────────┬─────────────────────────────────────┐  │
│   │ Field           │ Access Pattern                      │  │
│   ├─────────────────┼─────────────────────────────────────┤  │
│   │ subscriptions   │ Read: publish matching (frequent)   │  │
│   │                 │ Write: subscribe/unsubscribe        │  │
│   ├─────────────────┼─────────────────────────────────────┤  │
│   │ sessions        │ Read: reconnect (check existence)   │  │
│   │                 │ Write: connect/disconnect           │  │
│   ├─────────────────┼─────────────────────────────────────┤  │
│   │ retained_msgs   │ Read: subscribe (send retained)     │  │
│   │                 │ Write: publish with retain flag     │  │
│   ├─────────────────┼─────────────────────────────────────┤  │
│   │ client_registry │ Read: reconnect (find old client)   │  │
│   │                 │ Write: connect/disconnect           │  │
│   └─────────────────┴─────────────────────────────────────┘  │
│                                                               │
│ ClientWriteHandle (Mutex<WriteBuffer>):                      │
│   • Locked only during write operations                      │
│   • Quick lock/unlock cycle                                  │
│   • No contention in single-subscriber case                  │
│                                                               │
│ Atomic Operations (no locks):                                 │
│   • next_packet_id: AtomicU16 (fetch_add)                   │
│   • needs_write: AtomicBool (debounce epoll_ctl)            │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### Communication Channels

```
┌──────────────────────────────────────────────────────────────┐
│ CONTROL PLANE (Channels)                                      │
│ Used for: Administrative messages, lifecycle events          │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│ enum WorkerMsg {                                              │
│     NewClient { socket, addr },    // Server → Worker        │
│     Disconnect { token },          // Worker → Worker        │
│     Shutdown,                      // Server → Workers       │
│ }                                                             │
│                                                               │
│ Characteristics:                                              │
│   • crossbeam-channel (MPSC)                                 │
│   • Non-blocking receive in event loop                       │
│   • Used infrequently (connection events only)               │
│                                                               │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ DATA PLANE (Direct Writes)                                    │
│ Used for: Publish message delivery                           │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│ Cross-thread publish delivery:                                │
│                                                               │
│   Worker A                          Worker B                 │
│      │                                  │                     │
│      │ subscriber.handle               │                     │
│      │    .queue_publish()             │                     │
│      │         │                       │                     │
│      │         ▼                       │                     │
│      │  ┌─────────────────────────┐   │                     │
│      │  │ Mutex<WriteBuffer>      │   │                     │
│      │  │ (lock, write, unlock)   │   │                     │
│      │  └───────────┬─────────────┘   │                     │
│      │              │                  │                     │
│      │              ▼                  │                     │
│      │  epoll_ctl(EPOLL_CTL_MOD) ─────►│ (wakeup)           │
│      │                                 │                     │
│                                        ▼                     │
│                              handle_writable()               │
│                                        │                     │
│                                        ▼                     │
│                              flush() → socket.write()        │
│                                                               │
│ Why not channels?                                             │
│   • Lower latency (no queue)                                 │
│   • Less memory (no channel buffers)                         │
│   • Direct backpressure via WriteBuffer limits               │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### Token Management

```
┌──────────────────────────────────────────────────────────────┐
│ TOKEN SYSTEM                                                  │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│ struct Token(usize);   // From mio                           │
│                                                               │
│ Allocation:                                                   │
│   • Worker maintains next_token counter                      │
│   • Starts at 1 (0 is reserved for listener)                │
│   • Incremented for each new client                          │
│                                                               │
│ Usage:                                                        │
│   • epoll registration: poll.register(socket, token)         │
│   • Client lookup: clients.get(&token)                       │
│   • Subscription identity: Subscriber { worker_id, token }   │
│   • Deduplication key: (worker_id, token)                    │
│                                                               │
│ Uniqueness:                                                   │
│   • Unique within a worker                                   │
│   • (worker_id, token) unique globally                       │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

---

## Protocol Support

### MQTT 3.1.1 Compliance

```
┌──────────────────────────────────────────────────────────────┐
│ SUPPORTED PACKET TYPES                                        │
├────────────────────┬─────────────────────────────────────────┤
│ Client → Broker    │ Broker → Client                         │
├────────────────────┼─────────────────────────────────────────┤
│ CONNECT            │ CONNACK                                 │
│ PUBLISH            │ PUBLISH                                 │
│ PUBACK (QoS 1)     │ PUBACK (QoS 1)                         │
│ PUBREC (QoS 2)     │ PUBREC (QoS 2)                         │
│ PUBREL (QoS 2)     │ PUBREL (QoS 2)                         │
│ PUBCOMP (QoS 2)    │ PUBCOMP (QoS 2)                        │
│ SUBSCRIBE          │ SUBACK                                  │
│ UNSUBSCRIBE        │ UNSUBACK                                │
│ PINGREQ            │ PINGRESP                                │
│ DISCONNECT         │                                         │
└────────────────────┴─────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ FEATURE SUPPORT                                               │
├──────────────────────────────────────────────────────────────┤
│ [x] Clean Session flag                                       │
│ [x] Session state persistence (CleanSession=0)               │
│ [x] Will messages (on abnormal disconnect)                   │
│ [x] Retained messages                                        │
│ [x] QoS 0, 1, 2 message delivery                            │
│ [x] Topic wildcards (+ and #)                               │
│ [x] Keep-alive timeout enforcement                           │
│ [x] $ topic handling (MQTT-4.7.2-1)                         │
│ [x] Pending message resend on reconnect                     │
│ [x] MQTT v5 properties and features                         │
│ [x] TLS/SSL (rustls)                                        │
│ [x] PROXY protocol v1/v2 support                            │
│ [x] Authentication (static users, ACLs)                     │
│                                                               │
│ [ ] Message expiry                                           │
│ [ ] Shared subscriptions                                     │
└──────────────────────────────────────────────────────────────┘
```

### QoS Flows

```
QoS 0 (At Most Once):
┌──────────┐         ┌──────────┐
│ Publisher│         │ Broker   │
└────┬─────┘         └────┬─────┘
     │    PUBLISH         │
     │ ──────────────────►│
     │                    │ (deliver to subscribers)
     │                    │

QoS 1 (At Least Once):
┌──────────┐         ┌──────────┐
│ Publisher│         │ Broker   │
└────┬─────┘         └────┬─────┘
     │  PUBLISH (id=N)    │
     │ ──────────────────►│
     │                    │ (deliver to subscribers)
     │    PUBACK (id=N)   │
     │ ◄──────────────────│

QoS 2 (Exactly Once):
┌──────────┐         ┌──────────┐
│ Publisher│         │ Broker   │
└────┬─────┘         └────┬─────┘
     │  PUBLISH (id=N)    │
     │ ──────────────────►│
     │                    │ (store message)
     │    PUBREC (id=N)   │
     │ ◄──────────────────│
     │    PUBREL (id=N)   │
     │ ──────────────────►│
     │                    │ (deliver to subscribers)
     │   PUBCOMP (id=N)   │
     │ ◄──────────────────│
```

---

## File Structure

```
src/
├── main.rs              # CLI, config parsing, server startup
├── lib.rs               # Public exports
├── server.rs            # Accept loop, worker distribution
├── worker.rs            # Event loop, packet handling
├── client.rs            # Per-connection state
├── client_handle.rs     # Cross-thread write handle
├── write_buffer.rs      # Circular output buffer
├── shared.rs            # Global state (subscriptions, sessions)
├── subscription.rs      # Topic trie, matching
├── packet.rs            # MQTT codec
├── publish_encoder.rs   # Zero-copy publish factory
└── error.rs             # Error types

docs/
├── ARCHITECTURE.md      # This document
└── mqtt-rfc/            # MQTT specification reference
```

---

## Summary

mqlite achieves high performance through:

1. **Lock-free data plane** - Direct writes to subscriber buffers instead of channel queuing
2. **Zero-copy messaging** - `Bytes` for topic/payload sharing across threads
3. **Memory reuse** - Preallocated buffers for hot path operations
4. **Efficient I/O** - Vectored writes, epoll with debounced updates
5. **Smart encoding** - QoS-based caching, stack allocation for small packets
6. **Minimal contention** - Read-heavy locks, atomic operations where possible
7. **jemalloc allocator** - Better memory return behavior than system allocator
8. **Elastic buffers** - Read/write buffers grow under load, shrink when idle
