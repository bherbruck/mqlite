# Section 7: Quality of Service

## 7.1 QoS 0: At Most Once Delivery

The message is delivered at most once, or it may not be delivered at all. No acknowledgment or retry.

```
Sender ──── PUBLISH (QoS 0) ────► Receiver
                                  (no response)
```

- **DUP flag:** Always 0
- **Packet Identifier:** Not present
- **Use case:** Telemetry where occasional loss is acceptable

## 7.2 QoS 1: At Least Once Delivery

The message is delivered at least once. Duplicates are possible.

```
Sender ──── PUBLISH (QoS 1) ────► Receiver
       ◄──────── PUBACK ─────────
```

- **DUP flag:** Set to 1 on retransmission
- **Packet Identifier:** Required
- **Use case:** Commands where duplicates can be handled

## 7.3 QoS 2: Exactly Once Delivery

The message is delivered exactly once. Four-packet handshake ensures no duplicates.

```
Sender ──── PUBLISH (QoS 2) ────► Receiver (stores)
       ◄──────── PUBREC ─────────
Sender ──── PUBREL ─────────────► Receiver (delivers)
       ◄──────── PUBCOMP ────────
```

- **Use case:** Financial transactions, billing

## 7.4 QoS Downgrade

Delivery QoS = min(Published QoS, Subscription Max QoS)

| Published | Subscription | Delivery |
|-----------|--------------|----------|
| 0 | 0, 1, 2 | 0 |
| 1 | 0 | 0 |
| 1 | 1, 2 | 1 |
| 2 | 0 | 0 |
| 2 | 1 | 1 |
| 2 | 2 | 2 |

## 7.5 Message Ordering

**[MQTT-4.6.0-1]** When re-sending PUBLISH packets, they MUST be sent in original order.

**[MQTT-4.6.0-2]** PUBACK packets MUST be sent in order of corresponding PUBLISH packets.

**[MQTT-4.6.0-6]** For Ordered Topics (default), PUBLISH to subscribers MUST preserve sender order.
