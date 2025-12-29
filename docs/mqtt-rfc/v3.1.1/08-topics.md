# Section 8: Topic Names and Topic Filters

## 8.1 Topic Name and Topic Filter Definitions

**Topic Name:** The label attached to an Application Message. Used by the Server to match against Subscriptions.

**Topic Filter:** An expression contained in a Subscription that may include wildcards to match multiple Topic Names.

## 8.2 Topic Name Requirements

### 8.2.1 Structure

- Topic Names are hierarchical, with levels separated by forward slash (`/`) characters
- Topic Names are case-sensitive
- Topic Names can include spaces
- Topic Names are UTF-8 encoded strings
- A leading `/` creates a distinct topic (e.g., `/sensors/temp` ≠ `sensors/temp`)

### Table 8-1: Valid Topic Name Examples

| Topic Name | Description |
|------------|-------------|
| `sensors/temperature/living-room` | Three-level hierarchy |
| `home/floor1/room2/light` | Four-level hierarchy |
| `/absolute/topic` | Leading slash (distinct from `absolute/topic`) |
| `sensors` | Single level |
| `sensors/` | Two levels, second is empty |
| `sensors/ /data` | Levels can contain spaces |

### 8.2.2 Topic Name Restrictions

**[MQTT-4.7.0-1]** Topic Names and Topic Filters MUST be at least one character long.

**[MQTT-4.7.3-1]** Topic Names MUST NOT include wildcard characters (`#` or `+`).

**[MQTT-4.7.3-2]** Topic Names in PUBLISH packets sent by Clients to Servers MUST NOT contain wildcard characters.

## 8.3 Topic Wildcards

Topic Filters can include wildcard characters to match multiple Topic Names.

### 8.3.1 Multi-Level Wildcard: `#`

The multi-level wildcard (`#`) matches any number of levels within a topic.

**[MQTT-4.7.1-1]** The multi-level wildcard character MUST be specified either on its own or following a topic level separator. In either case it MUST be the last character specified in the Topic Filter.

### Table 8-2: Multi-Level Wildcard Examples

| Topic Filter | Matches | Does Not Match |
|--------------|---------|----------------|
| `sensors/#` | `sensors`, `sensors/temp`, `sensors/temp/room1` | `other/sensors` |
| `sensors/temp/#` | `sensors/temp`, `sensors/temp/room1`, `sensors/temp/room1/value` | `sensors/humidity` |
| `#` | All topics | N/A |

### Figure 8-1: Multi-Level Wildcard Matching

```
Topic Filter: sensors/#

    sensors           ✓ matches
    sensors/temp      ✓ matches
    sensors/temp/1    ✓ matches
    sensors/temp/1/v  ✓ matches
    other/sensors     ✗ does not match
```

### 8.3.2 Single-Level Wildcard: `+`

The single-level wildcard (`+`) matches exactly one topic level.

**[MQTT-4.7.1-2]** The single-level wildcard can be used at any level in the Topic Filter, including first and last levels. Where it is used it MUST occupy an entire level of the filter.

### Table 8-3: Single-Level Wildcard Examples

| Topic Filter | Matches | Does Not Match |
|--------------|---------|----------------|
| `sensors/+/temp` | `sensors/room1/temp`, `sensors/room2/temp` | `sensors/temp`, `sensors/room1/room2/temp` |
| `+/temp` | `room1/temp`, `room2/temp` | `temp`, `room1/room2/temp` |
| `sensors/+` | `sensors/temp`, `sensors/humidity` | `sensors`, `sensors/temp/room1` |

### Figure 8-2: Single-Level Wildcard Matching

```
Topic Filter: sensors/+/temp

    sensors/room1/temp    ✓ matches
    sensors/room2/temp    ✓ matches
    sensors/floor1/temp   ✓ matches
    sensors/temp          ✗ does not match (missing level)
    sensors/a/b/temp      ✗ does not match (too many levels)
```

### 8.3.3 Combining Wildcards

Wildcards can be combined within a single Topic Filter:

### Table 8-4: Combined Wildcard Examples

| Topic Filter | Matches |
|--------------|---------|
| `+/sensors/#` | `home/sensors`, `home/sensors/temp`, `office/sensors/light/1` |
| `home/+/+/status` | `home/floor1/room1/status`, `home/floor2/room5/status` |

## 8.4 Topics Beginning with `$`

**[MQTT-4.7.2-1]** The Server MUST NOT match Topic Filters starting with a wildcard character (`#` or `+`) with Topic Names beginning with a `$` character.

Topics beginning with `$` are reserved for server-specific or system purposes:

### Table 8-5: System Topic Examples

| Topic Pattern | Typical Usage |
|---------------|---------------|
| `$SYS/broker/clients/connected` | Number of connected clients |
| `$SYS/broker/load/bytes/received` | Bytes received |
| `$SYS/broker/uptime` | Broker uptime |

> **Note:** Clients should not publish to `$` topics unless explicitly documented by the server implementation.

### 8.4.1 Subscribing to System Topics

To receive messages from `$` topics, subscribe explicitly:

```
$SYS/#           ✓ Matches all $SYS topics
$SYS/broker/+    ✓ Matches one level under $SYS/broker
#                ✗ Does NOT match $SYS topics
+/broker/#       ✗ Does NOT match $SYS/broker/*
```

## 8.5 Topic Matching Process

### 8.5.1 Algorithm

When a message is published to a Topic Name, the Server must match it against all active subscriptions:

```
function matches(topicFilter, topicName):
    filterLevels = split(topicFilter, '/')
    nameLevels = split(topicName, '/')
    
    for i = 0 to length(filterLevels):
        filter = filterLevels[i]
        
        if filter == '#':
            return true  // Matches remaining levels
            
        if i >= length(nameLevels):
            return false  // Topic name too short
            
        if filter == '+':
            continue  // Matches any single level
            
        if filter != nameLevels[i]:
            return false  // Exact match required
    
    return length(filterLevels) == length(nameLevels)
```

### 8.5.2 Matching Examples

### Table 8-6: Topic Matching Examples

| Topic Name | Topic Filter | Match? |
|------------|--------------|--------|
| `sport/tennis/player1` | `sport/tennis/player1` | Yes |
| `sport/tennis/player1` | `sport/tennis/+` | Yes |
| `sport/tennis/player1` | `sport/#` | Yes |
| `sport/tennis/player1` | `sport/tennis/player2` | No |
| `sport/tennis/player1` | `sport/+` | No |
| `sport` | `sport/#` | Yes |
| `/finance` | `+/+` | Yes |
| `/finance` | `/+` | Yes |
| `$SYS/broker/load` | `#` | No |
| `$SYS/broker/load` | `$SYS/#` | Yes |

## 8.6 Best Practices

### 8.6.1 Topic Design Guidelines

1. **Use meaningful hierarchies**: `building/floor/room/sensor/type`
2. **Keep topics reasonably short**: Reduces bandwidth
3. **Avoid leading `/`**: Unless intentionally creating distinct namespace
4. **Use lowercase with hyphens**: `sensor-data` not `SensorData`
5. **Include identifiers in hierarchy**: `devices/device-001/status`

### 8.6.2 Subscription Guidelines

1. **Be specific**: Avoid overly broad wildcards
2. **Consider load**: `#` subscriptions receive all messages
3. **Use `+` for enumeration**: `devices/+/status` for all device statuses
4. **Monitor `$SYS` carefully**: High-frequency updates possible
