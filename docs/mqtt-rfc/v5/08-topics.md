# Section 8: Topic Names and Topic Filters

## 8.1 Topic Names

- UTF-8 encoded string attached to published messages
- Case-sensitive
- Hierarchical with `/` separator
- MUST NOT contain wildcard characters (`+` or `#`)
- MUST be at least 1 character

### Valid Examples
```
sensors/temperature/room1
home/floor1/livingroom/light
/absolute/topic
sport/tennis/player1
```

## 8.2 Topic Filters

- Expression in subscription indicating interest
- Can include wildcard characters
- Matched against Topic Names

## 8.3 Wildcards

### 8.3.1 Single-Level Wildcard: `+`

Matches exactly one topic level.

| Filter | Matches | Doesn't Match |
|--------|---------|---------------|
| `sensors/+/temp` | `sensors/room1/temp`, `sensors/room2/temp` | `sensors/temp`, `sensors/a/b/temp` |
| `+/tennis/+` | `sport/tennis/player1` | `sport/tennis`, `tennis/player1` |

### 8.3.2 Multi-Level Wildcard: `#`

Matches any number of levels. Must be last character.

| Filter | Matches |
|--------|---------|
| `sensors/#` | `sensors`, `sensors/temp`, `sensors/room1/temp/value` |
| `#` | All topics (except $-topics) |

## 8.4 Topics Beginning with $

System topics (e.g., `$SYS/broker/clients`) are NOT matched by `#` or `+` at the start.

| Filter | Matches `$SYS/broker/load`? |
|--------|----------------------------|
| `#` | No |
| `+/broker/load` | No |
| `$SYS/#` | Yes |
| `$SYS/broker/+` | Yes |

## 8.5 Topic Aliases (v5.0)

Integer (1-65535) that substitutes for Topic Name to reduce packet size.

**Setting alias:** Include both Topic Name and Topic Alias
**Using alias:** Include Topic Alias with empty Topic Name

The mapping is per-connection and direction-specific.
