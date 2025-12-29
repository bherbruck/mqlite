# MQTT Version 5.0 Protocol Specification

**Version:** 5.0  
**Status:** Final (OASIS Standard)  
**Last Updated:** 2024-12-22  
**Original Publication:** 07 March 2019

## Abstract

MQTT (Message Queuing Telemetry Transport) Version 5.0 is a lightweight, publish-subscribe messaging protocol designed for constrained devices and low-bandwidth, high-latency networks. This major revision adds significant new features including enhanced error reporting, session and message expiry, request/response patterns, shared subscriptions, and extensibility through user properties.

## Key Features in MQTT 5.0

- **Properties**: Extensible metadata on most packet types
- **Reason Codes**: Comprehensive error and status reporting
- **Session Expiry**: Configurable session lifetime
- **Message Expiry**: Time-to-live for published messages
- **Topic Aliases**: Reduce bandwidth for repeated topics
- **Shared Subscriptions**: Load balancing across clients
- **Flow Control**: Receive Maximum limits concurrent messages
- **Request/Response**: Built-in correlation support
- **Enhanced Authentication**: Multi-step AUTH exchange
- **Server Disconnect**: Server can send DISCONNECT with reason

## Table of Contents

1. [Abstract](./00-abstract.md)
2. [Introduction](./01-introduction.md)
3. [Terminology and Conventions](./02-terminology.md)
4. [Data Representations](./03-data-representations.md)
5. Control Packet Format
   - [5.1 Packet Structure](./04.01-packet-structure.md)
   - [5.2 Fixed Header](./04.02-fixed-header.md)
   - [5.3 Variable Header and Properties](./04.03-variable-header.md)
   - [5.4 Properties Reference](./04.04-properties.md)
   - [5.5 Reason Codes](./04.05-reason-codes.md)
6. Control Packets
   - [6.1 CONNECT](./05.01-connect.md)
   - [6.2 CONNACK](./05.02-connack.md)
   - [6.3 PUBLISH](./05.03-publish.md)
   - [6.4 PUBACK, PUBREC, PUBREL, PUBCOMP](./05.04-publish-ack.md)
   - [6.5 SUBSCRIBE and SUBACK](./05.05-subscribe.md)
   - [6.6 UNSUBSCRIBE and UNSUBACK](./05.06-unsubscribe.md)
   - [6.7 PINGREQ and PINGRESP](./05.07-ping.md)
   - [6.8 DISCONNECT](./05.08-disconnect.md)
   - [6.9 AUTH](./05.09-auth.md)
7. [Session State](./06-session-state.md)
8. [Quality of Service](./07-qos.md)
9. [Topic Names and Filters](./08-topics.md)
10. [Subscriptions](./09-subscriptions.md)
11. [Flow Control](./10-flow-control.md)
12. [Request/Response](./11-request-response.md)
13. [Enhanced Authentication](./12-enhanced-auth.md)
14. [Error Handling](./13-error-handling.md)
15. [Security Considerations](./14-security.md)
16. [WebSocket Transport](./15-websocket.md)
17. [Conformance](./16-conformance.md)
18. [References](./17-references.md)
19. [Appendix A: Normative Statements](./appendix-a-normative.md)
20. [Appendix B: Changes from v3.1.1](./appendix-b-changes.md)

## Document Index

| Section | File | Description | Lines |
|---------|------|-------------|-------|
| Abstract | [00-abstract.md](./00-abstract.md) | Status and summary | ~80 |
| Introduction | [01-introduction.md](./01-introduction.md) | Background and scope | ~150 |
| Terminology | [02-terminology.md](./02-terminology.md) | Definitions and conventions | ~200 |
| Data Representations | [03-data-representations.md](./03-data-representations.md) | Data types including Variable Byte Integer | ~250 |
| Packet Structure | [04.01-packet-structure.md](./04.01-packet-structure.md) | Overall packet format | ~100 |
| Fixed Header | [04.02-fixed-header.md](./04.02-fixed-header.md) | Fixed header format | ~180 |
| Variable Header | [04.03-variable-header.md](./04.03-variable-header.md) | Variable header and packet IDs | ~200 |
| Properties | [04.04-properties.md](./04.04-properties.md) | All property definitions | ~400 |
| Reason Codes | [04.05-reason-codes.md](./04.05-reason-codes.md) | All reason code definitions | ~250 |
| CONNECT | [05.01-connect.md](./05.01-connect.md) | Connection request packet | ~600 |
| CONNACK | [05.02-connack.md](./05.02-connack.md) | Connection acknowledgment | ~400 |
| PUBLISH | [05.03-publish.md](./05.03-publish.md) | Publish message packet | ~450 |
| Publish ACKs | [05.04-publish-ack.md](./05.04-publish-ack.md) | PUBACK/PUBREC/PUBREL/PUBCOMP | ~350 |
| SUBSCRIBE | [05.05-subscribe.md](./05.05-subscribe.md) | Subscribe and SUBACK | ~400 |
| UNSUBSCRIBE | [05.06-unsubscribe.md](./05.06-unsubscribe.md) | Unsubscribe and UNSUBACK | ~250 |
| PING | [05.07-ping.md](./05.07-ping.md) | PINGREQ and PINGRESP | ~100 |
| DISCONNECT | [05.08-disconnect.md](./05.08-disconnect.md) | Disconnect notification | ~250 |
| AUTH | [05.09-auth.md](./05.09-auth.md) | Authentication exchange | ~200 |
| Session State | [06-session-state.md](./06-session-state.md) | Session management | ~200 |
| QoS | [07-qos.md](./07-qos.md) | Quality of Service levels | ~300 |
| Topics | [08-topics.md](./08-topics.md) | Topic names and filters | ~250 |
| Subscriptions | [09-subscriptions.md](./09-subscriptions.md) | Shared and non-shared | ~200 |
| Flow Control | [10-flow-control.md](./10-flow-control.md) | Receive Maximum | ~150 |
| Request/Response | [11-request-response.md](./11-request-response.md) | Correlation patterns | ~150 |
| Enhanced Auth | [12-enhanced-auth.md](./12-enhanced-auth.md) | Multi-step authentication | ~200 |
| Error Handling | [13-error-handling.md](./13-error-handling.md) | Malformed packets, protocol errors | ~150 |
| Security | [14-security.md](./14-security.md) | Security considerations | ~350 |
| WebSocket | [15-websocket.md](./15-websocket.md) | WebSocket transport | ~100 |
| Conformance | [16-conformance.md](./16-conformance.md) | Conformance requirements | ~120 |
| References | [17-references.md](./17-references.md) | Normative and informative refs | ~150 |
| Appendix A | [appendix-a-normative.md](./appendix-a-normative.md) | All normative statements | ~500 |
| Appendix B | [appendix-b-changes.md](./appendix-b-changes.md) | Changes from v3.1.1 | ~150 |

## Quick Navigation

- **Implementers**: Start with [Properties](./04.04-properties.md) and [Reason Codes](./04.05-reason-codes.md)
- **Migrating from 3.1.1**: See [Appendix B: Changes](./appendix-b-changes.md)
- **New features**: [Shared Subscriptions](./09-subscriptions.md), [Flow Control](./10-flow-control.md), [Enhanced Auth](./12-enhanced-auth.md)
- **Security review**: See [Section 14: Security Considerations](./14-security.md)
- **Conformance testing**: See [Section 16: Conformance](./16-conformance.md)

## Protocol Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        MQTT 5.0 Architecture                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│    ┌──────────┐         ┌──────────┐         ┌──────────┐              │
│    │ Client A │         │  Server  │         │ Client B │              │
│    │(Publisher)│         │ (Broker) │         │(Subscriber)│            │
│    └────┬─────┘         └────┬─────┘         └────┬─────┘              │
│         │                    │                    │                     │
│         │ CONNECT            │                    │                     │
│         │ + Properties       │                    │                     │
│         │───────────────────>│                    │                     │
│         │ CONNACK            │                    │                     │
│         │ + Properties       │                    │                     │
│         │<───────────────────│                    │                     │
│         │                    │                    │                     │
│         │                    │ SUBSCRIBE         │                     │
│         │                    │ + Subscription ID  │                     │
│         │                    │<───────────────────│                     │
│         │                    │ SUBACK            │                     │
│         │                    │ + Reason Codes     │                     │
│         │                    │───────────────────>│                     │
│         │                    │                    │                     │
│         │ PUBLISH            │                    │                     │
│         │ + Topic Alias      │                    │                     │
│         │ + Message Expiry   │                    │                     │
│         │───────────────────>│ PUBLISH           │                     │
│         │                    │ + Subscription ID  │                     │
│         │                    │───────────────────>│                     │
│         │                    │                    │                     │
│         │ DISCONNECT         │                    │                     │
│         │ + Reason Code      │                    │                     │
│         │───────────────────>│                    │                     │
│         │                    │                    │                     │
└─────────────────────────────────────────────────────────────────────────┘
```

## Control Packet Types

| Type | Value | Direction | Description |
|------|-------|-----------|-------------|
| CONNECT | 1 | Client → Server | Connection request |
| CONNACK | 2 | Server → Client | Connection acknowledgment |
| PUBLISH | 3 | Both | Publish message |
| PUBACK | 4 | Both | QoS 1 publish acknowledgment |
| PUBREC | 5 | Both | QoS 2 publish received |
| PUBREL | 6 | Both | QoS 2 publish release |
| PUBCOMP | 7 | Both | QoS 2 publish complete |
| SUBSCRIBE | 8 | Client → Server | Subscribe request |
| SUBACK | 9 | Server → Client | Subscribe acknowledgment |
| UNSUBSCRIBE | 10 | Client → Server | Unsubscribe request |
| UNSUBACK | 11 | Server → Client | Unsubscribe acknowledgment |
| PINGREQ | 12 | Client → Server | Ping request |
| PINGRESP | 13 | Server → Client | Ping response |
| DISCONNECT | 14 | **Both** | Disconnect notification |
| AUTH | 15 | **Both** | Authentication exchange |

> **Note:** In MQTT 5.0, DISCONNECT can be sent by both Client and Server, and AUTH is a new packet type for enhanced authentication.
