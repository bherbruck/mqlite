# mqlite

A high-performance MQTT broker written in Rust.

## Features

- MQTT 3.1.1 protocol support
- QoS 0, 1, and 2 message delivery
- Topic wildcards (`+` and `#`)
- Retained messages
- Session persistence (CleanSession=0)
- Will messages
- Multi-threaded with lock-free publish path

## Quick Start

```bash
# Build
cargo build --release

# Run (default bind: 0.0.0.0:1883, threads: number of CPU cores)
./target/release/mqlite

# Custom bind address and thread count
./target/release/mqlite -b 127.0.0.1:1883 -t 4
```

## Docker

```bash
docker compose up
```

## Testing

```bash
make test              # Run all tests
make conformance-ci    # Run MQTT conformance tests
```

## Documentation

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed internals including thread model, data structures, and message flow.

## Roadmap

- [x] MQTT v5 conformance
- [ ] Plugin system (auth, ACL, etc.)
- [ ] TLS/SSL support
- [ ] WebSocket support
- [ ] Metrics and observability
- [ ] Enhanced persistence options (database backends)
- [ ] Clustering and high availability

## License

MIT
