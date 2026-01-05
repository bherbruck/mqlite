# PROXY Protocol Example

This example demonstrates mqlite running behind HAProxy with PROXY protocol v2 enabled to preserve real client IP addresses.

## Architecture

```
Client (192.168.1.100) --> HAProxy:1883 --> mqlite:1883
                          (adds PROXY header)
```

## Usage

```bash
# Start the stack
docker compose up --build

# In another terminal, connect with mosquitto
mosquitto_sub -h localhost -p 1883 -t '#' -v

# In another terminal, publish a message
mosquitto_pub -h localhost -p 1883 -t test -m "hello"
```

## Verify PROXY Protocol

With `log.level = "debug"` in mqlite.toml, you'll see logs like:

```
PROXY protocol: 172.18.0.1:54321 -> 192.168.1.100:54321 (real client IP)
```

The first IP is the HAProxy internal address, the second is your real client IP extracted from the PROXY header.

## Configuration

**haproxy.cfg** - Sends PROXY v2 headers:
```
backend mqtt_backend
    server mqlite mqlite:1883 send-proxy-v2
```

**mqlite.toml** - Enables PROXY protocol parsing:
```toml
[server.proxy_protocol]
enabled = true
timeout_secs = 5
```
