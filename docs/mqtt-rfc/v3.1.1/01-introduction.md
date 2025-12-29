# Section 1: Introduction

## 1.1 Background

MQTT was invented by Dr. Andy Stanford-Clark of IBM, and Arlen Nipper of Arcom (now Eurotech), in 1999. It was designed as a lightweight messaging protocol for constrained devices with minimal bandwidth and unreliable network connections. The protocol has since become an OASIS standard and is widely deployed in IoT applications, home automation, mobile applications, and industrial telemetry.

The name "MQTT" originally stood for "MQ Telemetry Transport" due to its origins in connecting oil pipelines via satellite links. However, the protocol is now used far beyond telemetry applications.

## 1.2 Scope

This specification defines:

- The wire-level protocol format for MQTT Control Packets
- The behavior of MQTT Clients and Servers
- The three Quality of Service levels for message delivery
- The publish/subscribe messaging pattern
- Session state management
- Topic name and filter formats
- Keep-alive mechanisms

This specification does NOT define:

- Application-level semantics of payloads
- Specific authentication or authorization mechanisms
- Message persistence implementation details
- Clustering or high-availability configurations
- Specific network transport implementations (beyond TCP/IP requirements)

## 1.3 Goals and Requirements

The MQTT protocol MUST:

1. Provide a minimal packet overhead for efficient network utilization
2. Support unreliable network connections with appropriate delivery guarantees
3. Enable publish/subscribe messaging patterns
4. Be transport-layer agnostic (while requiring ordered, lossless, bidirectional streams)
5. Support both small constrained devices and powerful servers

The MQTT protocol SHOULD:

1. Be simple to implement on constrained devices
2. Provide mechanisms for detecting and handling abnormal disconnections
3. Support both transient and persistent sessions

## 1.4 Document Organization

This specification is organized as follows:

- [Section 2](./02-terminology.md) defines terminology and conventions used throughout
- [Section 3](./03-data-representations.md) describes data representation formats
- [Section 4](./04.01-packet-structure.md) defines the general MQTT Control Packet format
- [Section 5](./05.01-connect.md) specifies each Control Packet type in detail
- [Section 6](./06-operational-behavior.md) describes operational behavior and state management
- [Section 7](./07-qos.md) details Quality of Service levels and protocol flows
- [Section 8](./08-topics.md) defines topic names and topic filters
- [Section 9](./09-security.md) addresses security considerations
- [Section 10](./10-websocket.md) describes WebSocket transport
- [Section 11](./11-conformance.md) specifies conformance requirements
- [Section 12](./12-references.md) lists normative and informative references
- [Appendix A](./appendix-a-normative.md) collects all normative statements

## 1.5 Notation

Throughout this specification:

- Hexadecimal values are prefixed with "0x" (e.g., 0x04)
- Binary values are shown as sequences of 0s and 1s
- Bit positions within a byte are numbered 7-0, where bit 7 is the most significant
- Multi-byte integer values use big-endian byte ordering
