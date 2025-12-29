# Appendix B: Changes from MQTT 3.1.1

## B.1 Major New Features

### B.1.1 Reason Codes
- All acknowledgment packets now include Reason Codes
- Comprehensive error and status reporting
- 40+ defined Reason Codes

### B.1.2 Properties System
- Extensible metadata on most packets
- 27 defined properties
- User Property for application data

### B.1.3 Session Management
- Session Expiry Interval (configurable lifetime)
- Clean Start replaces Clean Session
- Separation of session start and session expiry

### B.1.4 AUTH Packet
- New packet type (15) for enhanced authentication
- Challenge/response authentication flows
- Re-authentication during connection

### B.1.5 Server DISCONNECT
- Server can now send DISCONNECT with reason
- Graceful shutdown notification
- Server Reference for redirection

## B.2 Message Features

### B.2.1 Message Expiry
- Message Expiry Interval property
- Time-to-live for published messages
- Server adjusts on forwarding

### B.2.2 Topic Aliases
- Two Byte Integer substituting for Topic Name
- Reduces bandwidth for repeated topics
- Per-connection, bidirectional

### B.2.3 Payload Format
- Payload Format Indicator (0=bytes, 1=UTF-8)
- Content Type (MIME type)
- Server can validate UTF-8

## B.3 Subscription Features

### B.3.1 Shared Subscriptions
- `$share/{group}/{filter}` format
- Load balancing across subscribers
- Standardized (was implementation-specific)

### B.3.2 Subscription Identifiers
- Associate ID with subscription
- Returned in matching PUBLISH
- Client-side routing support

### B.3.3 Subscription Options
- No Local (don't receive own publishes)
- Retain As Published
- Retain Handling (when to send retained)

## B.4 Flow Control

### B.4.1 Receive Maximum
- Limits in-flight QoS > 0 messages
- Bidirectional (client and server)
- Prevents overwhelming receivers

### B.4.2 Maximum Packet Size
- Negotiated limit on packet size
- Prevents memory exhaustion

## B.5 Request/Response

### B.5.1 Response Topic
- Built-in support for responses
- Response Information from server

### B.5.2 Correlation Data
- Links response to request
- Binary data for flexibility

## B.6 Server Capabilities

### B.6.1 Capability Discovery
- Maximum QoS
- Retain Available
- Wildcard Subscription Available
- Subscription Identifier Available
- Shared Subscription Available

### B.6.2 Server Keep Alive
- Server can override client's Keep Alive

### B.6.3 Assigned Client Identifier
- Server assigns ID when client sends empty

## B.7 Will Message Enhancements

### B.7.1 Will Delay Interval
- Delay before publishing Will Message
- Allows reconnection without triggering Will

### B.7.2 Will Properties
- Message Expiry, Content Type, etc.
- Same properties as regular PUBLISH

## B.8 Backward Compatibility

### B.8.1 Protocol Version
- Version byte is 5 (0x05)
- 3.1.1 was version 4 (0x04)

### B.8.2 Clean Start vs Clean Session
- Clean Start (v5.0) + Session Expiry replaces Clean Session (v3.1.1)
- CleanSession=1 equivalent: Clean Start=1, Session Expiry=0
- CleanSession=0 equivalent: Clean Start=0, Session Expiry>0

### B.8.3 Password Without Username
- v5.0 allows Password without Username
- v3.1.1 required Username if Password present
