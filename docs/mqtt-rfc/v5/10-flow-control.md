# Section 10: Flow Control

## 10.1 Receive Maximum (New in v5.0)

Limits the number of unacknowledged QoS 1 and QoS 2 PUBLISH packets.

- Set in CONNECT (Client's limit for Server)
- Set in CONNACK (Server's limit for Client)
- Default: 65,535
- Value of 0 is a Protocol Error

## 10.2 Behavior

Sender MUST NOT send more QoS > 0 PUBLISH packets than Receive Maximum allows without acknowledgment.

When limit is reached:
- Wait for PUBACK (QoS 1) or PUBCOMP (QoS 2)
- QoS 0 messages are NOT limited

## 10.3 Example

If Receive Maximum = 10:
- Can have up to 10 in-flight QoS 1/2 PUBLISH packets
- 11th must wait for acknowledgment of one of the 10

---

# Section 11: Request/Response

## 11.1 Overview (New in v5.0)

Built-in support for request/response patterns using:
- **Response Topic** - Where to send response
- **Correlation Data** - Links response to request

## 11.2 Properties

| Property | Purpose |
|----------|---------|
| Response Topic (0x08) | Topic for response |
| Correlation Data (0x09) | Match response to request |
| Response Information (0x1A) | Server-provided topic prefix |

## 11.3 Flow

```
Requester                           Responder
    │                                   │
    │ PUBLISH to "requests/service"     │
    │ + Response Topic: "replies/abc"   │
    │ + Correlation Data: "req-123"     │
    │ ─────────────────────────────────►│
    │                                   │ Process
    │ PUBLISH to "replies/abc"          │
    │ + Correlation Data: "req-123"     │
    │◄─────────────────────────────────│
    │                                   │
```

---

# Section 13: Error Handling

## 13.1 Malformed Packet

Packet cannot be parsed according to specification.

**Action:** Send DISCONNECT (0x81) or CONNACK with error, then close connection.

## 13.2 Protocol Error

Packet is valid but violates protocol rules or state.

**Action:** Send DISCONNECT (0x82) or CONNACK with error, then close connection.

## 13.3 Error Responses

| Situation | Response |
|-----------|----------|
| Before CONNACK | CONNACK with error Reason Code |
| After CONNACK | DISCONNECT with Reason Code |
| PUBLISH error | PUBACK/PUBREC with error (QoS > 0) |
| SUBSCRIBE error | SUBACK with error per topic |
| UNSUBSCRIBE error | UNSUBACK with error per topic |

## 13.4 Reason Strings

Human-readable strings (Property 0x1F) for diagnostics.
- SHOULD NOT be parsed programmatically
- Controlled by Request Problem Information
