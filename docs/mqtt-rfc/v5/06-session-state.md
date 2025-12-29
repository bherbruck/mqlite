# Section 6: Session State

## 6.1 Session State Components

### Client Session State
- QoS 1 and QoS 2 messages sent to Server, not yet acknowledged
- QoS 2 messages received from Server, not yet acknowledged

### Server Session State
- Session existence
- Client subscriptions
- QoS 1 and QoS 2 messages sent to Client, not yet acknowledged
- QoS 1 and QoS 2 messages pending transmission
- QoS 2 messages received from Client, not yet acknowledged
- Optionally, QoS 0 messages pending transmission
- Will Message and Will Delay Interval

## 6.2 Session Expiry

- Controlled by Session Expiry Interval property
- 0 = Session ends when connection closes
- 0xFFFFFFFF = Session never expires
- Can be changed in DISCONNECT (but not from 0 to non-zero)

## 6.3 Clean Start

- Clean Start = 1: Discard existing session, create new
- Clean Start = 0: Resume existing session or create new

---

# Section 14: Security Considerations

## 14.1 Authentication
- Username/Password in CONNECT
- Enhanced authentication via AUTH packet
- TLS client certificates

## 14.2 Authorization
- Topic-level publish/subscribe permissions
- Implementation-specific

## 14.3 Transport Security
- TLS 1.2+ recommended
- Port 8883 for MQTT over TLS
- Consider ALPN for protocol negotiation

## 14.4 Threats
- Eavesdropping → Use TLS
- Man-in-the-middle → Validate certificates
- Denial of service → Rate limiting, quotas
- Unauthorized access → Authentication, ACLs

---

# Section 15: WebSocket Transport

## 15.1 Subprotocol
- Subprotocol name: `mqtt`
- Binary frames only

## 15.2 URIs
- `ws://` - Unencrypted (port 80)
- `wss://` - TLS (port 443)

## 15.3 Framing
- MQTT packets in WebSocket binary frames
- Multiple MQTT packets per frame allowed
- MQTT packet may span frames

---

# Section 16: Conformance

## 16.1 Server Conformance
- Support all Control Packet types
- Handle QoS 0, 1, 2
- Implement session management
- Support topic wildcards (SHOULD)

## 16.2 Client Conformance
- Send CONNECT first
- Not send second CONNECT
- Implement desired QoS levels
- Handle all response packets

---

# Section 17: References

## 17.1 Normative
- **[RFC2119]** Key words for RFCs
- **[RFC3629]** UTF-8
- **[RFC6455]** WebSocket Protocol
- **[Unicode]** Unicode Standard

## 17.2 Informative
- **[RFC793]** TCP
- **[RFC5246]** TLS 1.2
- **[RFC8446]** TLS 1.3
- **[MQTT-v3.1.1]** Previous version
