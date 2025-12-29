# MQTT Version 3.1.1 Protocol Specification

**Version:** 3.1.1  
**Status:** Final (OASIS Standard)  
**Last Updated:** 2024-12-22  
**Original Publication:** 29 October 2014

## Abstract

MQTT (Message Queuing Telemetry Transport) is a lightweight, publish-subscribe messaging protocol designed for constrained devices and low-bandwidth, high-latency networks. This specification defines the wire protocol, packet formats, quality of service levels, and operational behaviors for MQTT version 3.1.1.

## Table of Contents

1. [Abstract](./00-abstract.md)
2. [Introduction](./01-introduction.md)
3. [Terminology and Conventions](./02-terminology.md)
4. [Data Representations](./03-data-representations.md)
5. Control Packet Format
   - [5.1 Packet Structure](./04.01-packet-structure.md)
   - [5.2 Fixed Header](./04.02-fixed-header.md)
   - [5.3 Variable Header](./04.03-variable-header.md)
   - [5.4 Payload](./04.04-payload.md)
6. Control Packets
   - [6.1 CONNECT](./05.01-connect.md)
   - [6.2 CONNACK](./05.02-connack.md)
   - [6.3 PUBLISH](./05.03-publish.md)
   - [6.4 PUBACK, PUBREC, PUBREL, PUBCOMP](./05.04-publish-ack.md)
   - [6.5 SUBSCRIBE and SUBACK](./05.05-subscribe.md)
   - [6.6 UNSUBSCRIBE and UNSUBACK](./05.06-unsubscribe.md)
   - [6.7 PINGREQ and PINGRESP](./05.07-ping.md)
   - [6.8 DISCONNECT](./05.08-disconnect.md)
7. [Operational Behavior](./06-operational-behavior.md)
8. [Quality of Service](./07-qos.md)
9. [Topic Names and Filters](./08-topics.md)
10. [Security Considerations](./09-security.md)
11. [WebSocket Transport](./10-websocket.md)
12. [Conformance](./11-conformance.md)
13. [References](./12-references.md)
14. [Appendix A: Normative Statements](./appendix-a-normative.md)

## Document Index

| Section | File | Description | Lines |
|---------|------|-------------|-------|
| Abstract | [00-abstract.md](./00-abstract.md) | Status and summary | ~60 |
| Introduction | [01-introduction.md](./01-introduction.md) | Background and scope | ~120 |
| Terminology | [02-terminology.md](./02-terminology.md) | Definitions and conventions | ~150 |
| Data Representations | [03-data-representations.md](./03-data-representations.md) | Bits, integers, UTF-8 | ~180 |
| Packet Structure | [04.01-packet-structure.md](./04.01-packet-structure.md) | Overall packet format | ~80 |
| Fixed Header | [04.02-fixed-header.md](./04.02-fixed-header.md) | Fixed header format | ~200 |
| Variable Header | [04.03-variable-header.md](./04.03-variable-header.md) | Variable header and packet IDs | ~150 |
| Payload | [04.04-payload.md](./04.04-payload.md) | Payload requirements | ~80 |
| CONNECT | [05.01-connect.md](./05.01-connect.md) | Connection request packet | ~450 |
| CONNACK | [05.02-connack.md](./05.02-connack.md) | Connection acknowledgment | ~200 |
| PUBLISH | [05.03-publish.md](./05.03-publish.md) | Publish message packet | ~350 |
| Publish ACKs | [05.04-publish-ack.md](./05.04-publish-ack.md) | PUBACK/PUBREC/PUBREL/PUBCOMP | ~250 |
| SUBSCRIBE | [05.05-subscribe.md](./05.05-subscribe.md) | Subscribe and SUBACK | ~300 |
| UNSUBSCRIBE | [05.06-unsubscribe.md](./05.06-unsubscribe.md) | Unsubscribe and UNSUBACK | ~180 |
| PING | [05.07-ping.md](./05.07-ping.md) | PINGREQ and PINGRESP | ~100 |
| DISCONNECT | [05.08-disconnect.md](./05.08-disconnect.md) | Disconnect notification | ~80 |
| Operations | [06-operational-behavior.md](./06-operational-behavior.md) | State, connections, retry | ~250 |
| QoS | [07-qos.md](./07-qos.md) | Quality of Service levels | ~300 |
| Topics | [08-topics.md](./08-topics.md) | Topic names and filters | ~200 |
| Security | [09-security.md](./09-security.md) | Security considerations | ~350 |
| WebSocket | [10-websocket.md](./10-websocket.md) | WebSocket transport | ~80 |
| Conformance | [11-conformance.md](./11-conformance.md) | Conformance requirements | ~100 |
| References | [12-references.md](./12-references.md) | Normative and informative refs | ~120 |
| Appendix A | [appendix-a-normative.md](./appendix-a-normative.md) | All normative statements | ~400 |

## Quick Navigation

- **Implementers**: Start with [Section 4: Packet Structure](./04.01-packet-structure.md) then [Section 5: Control Packets](./05.01-connect.md)
- **Protocol designers**: See [Section 7: Quality of Service](./07-qos.md) and [Section 6: Operational Behavior](./06-operational-behavior.md)
- **Security review**: See [Section 9: Security Considerations](./09-security.md)
- **Conformance testing**: See [Section 11: Conformance](./11-conformance.md) and [Appendix A](./appendix-a-normative.md)

## Protocol Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           MQTT Architecture                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│    ┌──────────┐         ┌──────────┐         ┌──────────┐              │
│    │ Client A │         │  Server  │         │ Client B │              │
│    │(Publisher)│         │ (Broker) │         │(Subscriber)│            │
│    └────┬─────┘         └────┬─────┘         └────┬─────┘              │
│         │                    │                    │                     │
│         │    CONNECT         │                    │                     │
│         │───────────────────>│                    │                     │
│         │    CONNACK         │                    │                     │
│         │<───────────────────│                    │                     │
│         │                    │    CONNECT         │                     │
│         │                    │<───────────────────│                     │
│         │                    │    CONNACK         │                     │
│         │                    │───────────────────>│                     │
│         │                    │                    │                     │
│         │                    │    SUBSCRIBE       │                     │
│         │                    │<───────────────────│                     │
│         │                    │    SUBACK          │                     │
│         │                    │───────────────────>│                     │
│         │                    │                    │                     │
│         │    PUBLISH         │                    │                     │
│         │───────────────────>│    PUBLISH         │                     │
│         │                    │───────────────────>│                     │
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
| DISCONNECT | 14 | Client → Server | Disconnect notification |
