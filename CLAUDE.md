# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

mqlite is a high-performance MQTT 3.1.1 broker written in Rust, optimized for low latency and high throughput. It uses `mio` for async I/O with epoll, direct cross-thread writes for publish delivery, and zero-copy message sharing with `Bytes`.

## Build Commands

```bash
# Build
make build              # Debug build
make build-release      # Release build (optimized)
make build-profiling    # Profiling build (optimized + debug symbols)

# Run
make run                # Run debug broker on 0.0.0.0:1883
make run-release        # Run release broker
cargo run -- -b 127.0.0.1:1883  # Custom bind address

# Tests
make test               # Run all tests (unit + integration + conformance)
make test-unit          # Run unit tests only: cargo test --lib
make test-integration   # Run integration tests: cargo test --test integration
make test-conformance   # Run conformance tests: cargo test --test conformance

# External conformance tests (requires running broker)
make conformance-ci     # Build, start broker, run tests, stop broker
make conformance-v3     # MQTT 3.1.1 tests against running broker
make conformance-v5     # MQTT 5.0 tests against running broker

# Lint
cargo clippy

# Clean
make clean              # Clean build artifacts and bin/
```

## Architecture

### Thread Model
- **Main thread**: Accept loop distributes connections round-robin to workers
- **Worker threads**: Each owns a subset of clients with its own mio Poll (epoll)
- **Single-worker mode**: Runs in main thread for lower latency

### Core Components (src/)
- `server.rs` - TCP accept loop, worker distribution
- `worker.rs` - Event loop, MQTT packet handling, publish routing (~1200 lines, most complex)
- `client.rs` - Per-connection state, buffer management
- `client_handle.rs` - Cross-thread write handle with `Mutex<WriteBuffer>`
- `write_buffer.rs` - Circular buffer with vectored I/O support
- `subscription.rs` - Topic trie for wildcard matching (+, #)
- `packet.rs` - MQTT codec (encode/decode)
- `publish_encoder.rs` - Zero-copy publish encoding with QoS-based caching
- `shared.rs` - Global state (subscriptions, sessions, retained messages)

### Key Design Patterns

**Cross-thread publishing**: Workers write directly to subscriber's `WriteBuffer` via `ClientWriteHandle`, then call `epoll_ctl` to wake the owning worker. No channels for data plane.

**Zero-copy**: `Bytes` (reference-counted) for topic/payload sharing. `PublishEncoder` caches QoS 0 packets; small packets (≤256 bytes) use stack allocation.

**Route caching**: Workers cache topic→subscribers mappings with generation-based invalidation. Deduplication happens at cache time.

**Buffer reuse**: Workers maintain preallocated `subscriber_buf` and `dedup_buf` that get cleared but retain capacity.

### Shared State (protected by `parking_lot::RwLock`)
- `SubscriptionStore` - Topic trie
- `Sessions` - Persistent client state (CleanSession=0)
- `RetainedMessages` - Last retained message per topic
- `ClientRegistry` - Maps client_id → worker location

## MQTT Protocol Notes
- Supports QoS 0, 1, 2 with proper acknowledgment flows
- Topic wildcards: `+` (single level), `#` (multi-level, must be last)
- `$` topics not matched by wildcards at root level (MQTT-4.7.2-1)
- Keep-alive timeout enforcement
- Will messages on abnormal disconnect
- Session restoration on reconnect
