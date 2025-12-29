# Abstract

MQTT is a Client Server publish/subscribe messaging transport protocol. It is lightweight, open, simple, and designed to be easy to implement. These characteristics make it ideal for use in many situations, including constrained environments such as for communication in Machine to Machine (M2M) and Internet of Things (IoT) contexts where a small code footprint is required and/or network bandwidth is at a premium.

The protocol runs over TCP/IP, or over other network protocols that provide ordered, lossless, bidirectional connections.

## Key Features

- **Publish/Subscribe Pattern**: Provides one-to-many message distribution and decoupling of applications
- **Payload Agnostic**: Messaging transport that is agnostic to the content of the payload
- **Three Quality of Service Levels**:
  - QoS 0: "At most once" - message loss can occur
  - QoS 1: "At least once" - duplicates can occur
  - QoS 2: "Exactly once" - guaranteed single delivery
- **Small Transport Overhead**: Minimized protocol exchanges reduce network traffic
- **Last Will and Testament**: Mechanism to notify interested parties of abnormal disconnection

## Status of This Document

**Status:** Final (OASIS Standard)  
**Version:** 3.1.1  
**Obsoletes:** MQTT V3.1  
**Updates:** None

This document specifies the MQTT Version 3.1.1 protocol for the MQTT implementer community. Distribution of this document is unlimited.

## Specification URIs

**This version:**
- http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf

**Latest version:**
- http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.pdf

## Original Editors

- Andrew Banks (IBM)
- Rahul Gupta (IBM)

## Copyright Notice

Copyright © OASIS Open 2014. All Rights Reserved.

This document and translations of it may be copied and furnished to others, and derivative works that comment on or otherwise explain it or assist in its implementation may be prepared, copied, published, and distributed, without restriction of any kind, provided that the above copyright notice and this section are included on all such copies and derivative works.
