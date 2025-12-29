# Section 11: Conformance

## 11.1 Conformance Targets

This specification defines two conformance targets:

1. **MQTT Server** - A program or device that acts as an intermediary between Clients
2. **MQTT Client** - A program or device that connects to an MQTT Server

## 11.2 MQTT Server Conformance

### 11.2.1 Requirements

An MQTT Server conforming to this specification MUST:

1. Support all Control Packet types defined in this specification
2. Handle QoS 0, QoS 1, and QoS 2 message delivery
3. Implement session state management as specified
4. Support topic wildcards (`#` and `+`) as defined
5. Enforce all MUST requirements marked in this specification
6. Close Network Connections when protocol violations occur

### 11.2.2 Optional Features

An MQTT Server MAY:

1. Impose restrictions on:
   - Client identifier format and length
   - Topic name length
   - Message payload size
   - Number of subscriptions per client
   - Number of concurrent connections
   
2. Provide additional features:
   - Authentication mechanisms beyond username/password
   - Fine-grained authorization
   - Message persistence
   - Clustering and high availability
   - WebSocket transport
   - TLS/SSL encryption

### 11.2.3 Interoperability

Servers SHOULD:

1. Support the standard TCP port 1883 for unencrypted connections
2. Support the standard TCP port 8883 for TLS connections
3. Accept Client Identifiers up to 23 characters using alphanumeric characters
4. Support topic filters with wildcards

## 11.3 MQTT Client Conformance

### 11.3.1 Requirements

An MQTT Client conforming to this specification MUST:

1. Send CONNECT as the first packet after establishing a Network Connection
2. Not send more than one CONNECT packet
3. Implement the QoS flows it wishes to use
4. Respect Packet Identifier rules
5. Handle all response packets from the Server
6. Enforce all MUST requirements marked in this specification

### 11.3.2 Optional Features

An MQTT Client MAY:

1. Implement any subset of QoS levels (0, 1, 2)
2. Use Will Messages
3. Use Clean Session or persistent sessions
4. Use authentication
5. Use TLS/SSL

### 11.3.3 Minimal Client

A minimal conforming client:

1. Supports only QoS 0
2. Uses Clean Session = 1
3. Does not use Will Messages
4. Does not use authentication

Such a client can still participate fully in publish/subscribe messaging at QoS 0.

## 11.4 Conformance Statement Template

Implementations claiming conformance SHOULD provide a statement including:

```
Product Name: [Name]
Version: [Version]
Conformance Target: [Server | Client | Both]

Supported Features:
- QoS Levels: [0, 1, 2]
- Clean Session: [Yes/No]
- Persistent Session: [Yes/No]
- Will Messages: [Yes/No]
- Retained Messages: [Yes/No]
- Topic Wildcards: [Yes/No] (Server only)
- WebSocket Transport: [Yes/No]
- TLS: [Yes/No]

Restrictions:
- Maximum Client ID Length: [N characters]
- Maximum Topic Length: [N characters]
- Maximum Message Size: [N bytes]
- Maximum Subscriptions: [N per client]

Notes:
[Any additional conformance notes]
```

## 11.5 Normative Statement Reference

All normative statements (MUST, MUST NOT, SHOULD, etc.) in this specification are collected in [Appendix A: Normative Statements](./appendix-a-normative.md).

Conforming implementations MUST implement all MUST and MUST NOT requirements. SHOULD and SHOULD NOT requirements represent best practices that may be deviated from with good reason. MAY requirements are optional.

## 11.6 Testing Conformance

### 11.6.1 Recommended Test Categories

| Category | Description |
|----------|-------------|
| Connection | CONNECT/CONNACK handling, session management |
| Publishing | QoS 0/1/2 flows, retained messages |
| Subscription | SUBSCRIBE/SUBACK, wildcards, UNSUBSCRIBE |
| Keep Alive | PINGREQ/PINGRESP, timeout handling |
| Error Handling | Protocol violations, malformed packets |
| Edge Cases | Empty payloads, maximum sizes, special characters |

### 11.6.2 Interoperability Testing

Implementations SHOULD be tested against:

1. Multiple conforming client/server implementations
2. The Eclipse Paho test suite
3. Industry-standard MQTT testing tools
