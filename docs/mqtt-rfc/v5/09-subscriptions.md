# Section 9: Subscriptions

## 9.1 Non-Shared Subscriptions

Standard subscriptions where each matching message is delivered to the Client.

## 9.2 Shared Subscriptions (New in v5.0)

Allow multiple Clients to share a subscription for load balancing.

### 9.2.1 Shared Subscription Format

```
$share/{ShareName}/{TopicFilter}
```

- `$share` - Literal prefix
- `{ShareName}` - Group identifier (UTF-8, no `/`, `+`, `#`)
- `{TopicFilter}` - Standard topic filter

### 9.2.2 Examples

```
$share/consumer-group/orders/#
$share/workers/tasks/+
```

### 9.2.3 Behavior

- Message matching a shared subscription is sent to ONE subscriber in the group
- Server chooses which subscriber (implementation-defined)
- Different share groups receive their own copy
- Shared subscriptions MUST have No Local = 0

### 9.2.4 Load Balancing

```
                        ┌──► Client A (group: workers)
                        │
Server ── orders/123 ──┼──► Client B (group: workers)  [Only ONE receives]
                        │
                        └──► Client C (group: workers)
```

## 9.3 Subscription Identifiers (New in v5.0)

Associate an ID with a subscription; returned in matching PUBLISH.

- Set in SUBSCRIBE packet (Property 0x0B)
- Value 1-268,435,455
- Included in PUBLISH to subscriber
- Multiple IDs if message matches multiple subscriptions

### Use Case
Client-side routing without re-parsing topic names.

## 9.4 Subscription Options

| Option | Description |
|--------|-------------|
| QoS | Maximum QoS for delivery |
| No Local | Don't receive own publishes |
| Retain As Published | Preserve original RETAIN flag |
| Retain Handling | When to send retained messages |
