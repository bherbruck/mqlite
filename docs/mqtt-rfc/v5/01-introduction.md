# Section 1: Introduction

## 1.1 Background

MQTT was invented by Dr. Andy Stanford-Clark of IBM and Arlen Nipper of Arcom (now Eurotech) in 1999. MQTT 3.1.1 became an OASIS standard in 2014 and an ISO standard (ISO/IEC 20922:2016) in 2016.

MQTT 5.0 is a major revision that maintains backward compatibility at the protocol level while adding significant new capabilities. The "5.0" version number was chosen to clearly distinguish it from 3.1.1 and to signal the magnitude of changes.

## 1.2 Major Functional Objectives of MQTT 5.0

The major functional objectives of this revision are:

1. **Enhancements for scalability and large-scale systems**
   - Session and message expiry
   - Flow control via Receive Maximum
   - Topic Aliases for bandwidth reduction
   - Maximum Packet Size negotiation

2. **Improved error reporting**
   - Reason Codes on all acknowledgments
   - Reason Strings for diagnostics
   - Server-initiated DISCONNECT

3. **Formalize common patterns**
   - Capability discovery
   - Request/Response with correlation
   - Shared Subscriptions

4. **Extensibility mechanisms**
   - User Properties for application metadata
   - Payload Format Indicator and Content Type

5. **Performance improvements and support for small clients**
   - Topic Aliases
   - Optional properties
   - Streamlined acknowledgments

## 1.3 Scope

This specification defines:

- The wire-level protocol format for MQTT Control Packets
- The behavior of MQTT Clients and Servers
- The Property system for packet metadata
- The Reason Code system for status and error reporting
- Session state management with expiry
- Quality of Service levels and flows
- Topic names, filters, and wildcards
- Shared Subscriptions
- Enhanced authentication
- Flow control mechanisms

This specification does NOT define:

- Application-level semantics of payloads
- Specific authentication mechanisms (SASL, etc.)
- Message persistence implementation
- Clustering or high-availability configurations
- Specific network transport implementations

## 1.4 Document Organization

This specification is organized as follows:

- [Section 2](./02-terminology.md) defines terminology and conventions
- [Section 3](./03-data-representations.md) describes data representation formats
- [Section 4](./04.01-packet-structure.md) defines the MQTT Control Packet format, Properties, and Reason Codes
- [Section 5](./05.01-connect.md) specifies each Control Packet type in detail
- [Section 6](./06-session-state.md) describes session state management
- [Section 7](./07-qos.md) details Quality of Service levels
- [Section 8](./08-topics.md) defines topic names and filters
- [Section 9](./09-subscriptions.md) describes subscription types including Shared Subscriptions
- [Section 10](./10-flow-control.md) explains flow control mechanisms
- [Section 11](./11-request-response.md) describes request/response patterns
- [Section 12](./12-enhanced-auth.md) covers enhanced authentication
- [Section 13](./13-error-handling.md) addresses error handling
- [Section 14](./14-security.md) covers security considerations
- [Section 15](./15-websocket.md) describes WebSocket transport
- [Section 16](./16-conformance.md) specifies conformance requirements
- [Section 17](./17-references.md) lists references
- [Appendix A](./appendix-a-normative.md) collects normative statements
- [Appendix B](./appendix-b-changes.md) summarizes changes from v3.1.1

## 1.5 Notation

Throughout this specification:

- Hexadecimal values are prefixed with "0x" (e.g., 0x05)
- Binary values are shown as sequences of 0s and 1s
- Bit positions within a byte are numbered 7-0, where bit 7 is the most significant
- Multi-byte integer values use big-endian byte ordering
- Property Identifiers are shown as decimal with hexadecimal in parentheses
- Normative statements are identified as `[MQTT-x.x.x-y]`

## 1.6 Conformance Statement Notation

Text containing conformance statements is highlighted. Each conformance statement has a reference in the format `[MQTT-x.x.x-y]` where:
- `x.x.x` is the section number
- `y` is a sequential identifier within that section

All normative statements are collected in [Appendix A](./appendix-a-normative.md).
