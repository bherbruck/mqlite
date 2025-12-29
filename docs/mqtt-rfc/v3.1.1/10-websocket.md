# Section 10: Using WebSocket as a Network Transport

## 10.1 Overview

MQTT can be transported over WebSocket to enable browser-based clients and to traverse firewalls that block non-HTTP traffic.

When using WebSocket as the transport, MQTT Control Packets are sent as WebSocket binary data frames.

## 10.2 WebSocket URI

MQTT over WebSocket uses the following URI schemes:

| Scheme | Default Port | Description |
|--------|--------------|-------------|
| `ws://` | 80 | Unencrypted WebSocket |
| `wss://` | 443 | WebSocket over TLS |

**Example URIs:**
```
ws://broker.example.com/mqtt
wss://broker.example.com:8884/mqtt
```

## 10.3 WebSocket Subprotocol

### 10.3.1 Subprotocol Identifier

When establishing the WebSocket connection, clients MUST include the WebSocket subprotocol:

```
Sec-WebSocket-Protocol: mqtt
```

The server MUST respond with the same subprotocol identifier if it supports MQTT:

```
Sec-WebSocket-Protocol: mqtt
```

### 10.3.2 IANA Registration

### Figure 10-1: IANA WebSocket Subprotocol Identifier

| Field | Value |
|-------|-------|
| Subprotocol Identifier | mqtt |
| Subprotocol Common Name | MQTT |
| Subprotocol Definition | MQTT Version 3.1.1 |

## 10.4 Protocol Binding

### 10.4.1 Frame Type

**[MQTT-6.0-1]** MQTT Control Packets MUST be sent in WebSocket binary data frames. If any other type of data frame is received the recipient MUST close the Network Connection.

### 10.4.2 Message Fragmentation

A single MQTT Control Packet MAY span multiple WebSocket frames. Conversely, multiple MQTT Control Packets MAY be contained in a single WebSocket frame.

### Figure 10-2: MQTT over WebSocket Framing

```
┌─────────────────────────────────────────────────────────┐
│                 WebSocket Connection                     │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐               │
│  │ WebSocket Frame │  │ WebSocket Frame │               │
│  ├─────────────────┤  ├─────────────────┤               │
│  │ MQTT Packet 1   │  │ MQTT Packet 2   │               │
│  │ (complete)      │  │ (part 1)        │               │
│  └─────────────────┘  └─────────────────┘               │
│                                                          │
│  ┌─────────────────┐  ┌─────────────────┐               │
│  │ WebSocket Frame │  │ WebSocket Frame │               │
│  ├─────────────────┤  ├─────────────────┤               │
│  │ MQTT Packet 2   │  │ MQTT Packet 3   │               │
│  │ (part 2)        │  │ MQTT Packet 4   │               │
│  └─────────────────┘  └─────────────────┘               │
└─────────────────────────────────────────────────────────┘
```

### 10.4.3 Connection Lifecycle

1. Client initiates WebSocket handshake with `mqtt` subprotocol
2. Server completes handshake
3. Client sends MQTT CONNECT as binary frame
4. Server sends MQTT CONNACK as binary frame
5. Normal MQTT communication proceeds
6. Either party may close using WebSocket close frame or MQTT DISCONNECT

## 10.5 Security

### 10.5.1 Transport Security

When security is required, use WebSocket over TLS (`wss://`).

### 10.5.2 Origin Validation

Servers SHOULD validate the `Origin` header in the WebSocket handshake to prevent cross-site WebSocket hijacking.

### 10.5.3 Authentication

In addition to MQTT-level authentication, WebSocket connections may use:
- HTTP Basic authentication during handshake
- Cookies for session management
- Token-based authentication in query parameters

## 10.6 Implementation Considerations

### 10.6.1 Buffering

Implementations SHOULD buffer incoming WebSocket data and process complete MQTT packets. Partial packets MUST be accumulated across frames.

### 10.6.2 Keep-Alive

Both WebSocket and MQTT have keep-alive mechanisms:
- WebSocket ping/pong frames
- MQTT PINGREQ/PINGRESP

Implementations SHOULD use MQTT keep-alive as the primary mechanism. WebSocket pings MAY be used additionally but MUST NOT substitute for MQTT keep-alive.

### 10.6.3 Connection Close

| Initiator | Method | MQTT Effect |
|-----------|--------|-------------|
| Client | MQTT DISCONNECT then WebSocket close | Clean disconnect |
| Client | WebSocket close without DISCONNECT | Will Message published |
| Server | WebSocket close | Will Message published (if applicable) |

### 10.6.4 Proxy Traversal

WebSocket connections typically traverse HTTP proxies successfully. For maximum compatibility:
- Use port 443 with `wss://`
- Support HTTP CONNECT method for proxy tunneling
- Handle proxy authentication

## 10.7 Example Handshake

### Client Request

```http
GET /mqtt HTTP/1.1
Host: broker.example.com
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Protocol: mqtt
Sec-WebSocket-Version: 13
Origin: https://example.com
```

### Server Response

```http
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
Sec-WebSocket-Protocol: mqtt
```

After this handshake, binary MQTT packets are exchanged over the WebSocket connection.
