# Abstract

MQTT is a Client Server publish/subscribe messaging transport protocol. It is lightweight, open, simple, and designed to be easy to implement. These characteristics make it ideal for use in many situations, including constrained environments such as for communication in Machine to Machine (M2M) and Internet of Things (IoT) contexts where a small code footprint is required and/or network bandwidth is at a premium.

The protocol runs over TCP/IP, or over other network protocols that provide ordered, lossless, bidirectional connections.

## Key Features

- **Publish/Subscribe Pattern**: One-to-many message distribution and decoupling of applications
- **Payload Agnostic**: Messaging transport agnostic to payload content
- **Three Quality of Service Levels**:
  - QoS 0: "At most once" - message loss can occur
  - QoS 1: "At least once" - duplicates can occur  
  - QoS 2: "Exactly once" - guaranteed single delivery
- **Small Transport Overhead**: Minimized protocol exchanges reduce network traffic
- **Last Will and Testament**: Notification mechanism for abnormal disconnection

## New Features in MQTT 5.0

### Enhanced Error Reporting
- Reason Codes on all acknowledgment packets
- Reason Strings for human-readable diagnostics
- Server can send DISCONNECT with reason

### Scalability Improvements
- Session Expiry Interval for configurable session lifetime
- Message Expiry Interval for time-to-live on messages
- Topic Aliases to reduce bandwidth for repeated topic names
- Receive Maximum for flow control
- Maximum Packet Size negotiation

### Extensibility
- User Properties on most packet types
- Payload Format Indicator and Content Type
- Request Problem Information / Request Response Information

### New Patterns
- Shared Subscriptions for load balancing
- Subscription Identifiers for routing
- Request/Response with Response Topic and Correlation Data
- Enhanced Authentication with AUTH packet

### Server Features
- Server Keep Alive override
- Assigned Client Identifier
- Server Reference for redirection
- Capability discovery (wildcards, subscription IDs, shared subscriptions)

## Status of This Document

**Status:** Final (OASIS Standard)  
**Version:** 5.0  
**Supersedes:** MQTT Version 3.1.1  
**Updates:** None

This document specifies the MQTT Version 5.0 protocol for the MQTT implementer community. Distribution of this document is unlimited.

## Specification URIs

**This version:**
- https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.pdf

**Latest version:**
- https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.pdf

## Editors

- Andrew Banks (IBM)
- Ed Briggs (Microsoft)
- Ken Borgendale (IBM)
- Rahul Gupta (IBM)

## Copyright Notice

Copyright © OASIS Open 2019. All Rights Reserved.

This document and translations of it may be copied and furnished to others, and derivative works that comment on or otherwise explain it or assist in its implementation may be prepared, copied, published, and distributed, without restriction of any kind, provided that the above copyright notice and this section are included on all such copies and derivative works.
