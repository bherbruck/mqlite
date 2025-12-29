# Section 3: Data Representations

## 3.1 Bits

Bits in a byte are labeled 7 through 0. Bit number 7 is the most significant bit, the least significant bit is assigned bit number 0.

```
Bit Position:   7   6   5   4   3   2   1   0
Significance:  MSB                         LSB
Example 0xC5:   1   1   0   0   0   1   0   1
```

## 3.2 Integer Data Values

Integer data values are 16 bits in big-endian order: the high order byte precedes the lower order byte. This means that a 16-bit word is presented on the network as Most Significant Byte (MSB), followed by Least Significant Byte (LSB).

### Figure 3-1: 16-bit Integer Encoding

```
Byte:          1           2
           ┌───────┐   ┌───────┐
           │  MSB  │   │  LSB  │
           └───────┘   └───────┘
              ↓           ↓
           High-order  Low-order
```

**Example:** The decimal value 1234 (0x04D2) is encoded as:
- Byte 1: 0x04 (MSB)
- Byte 2: 0xD2 (LSB)

## 3.3 UTF-8 Encoded Strings

Text fields in the Control Packets are encoded as UTF-8 strings. UTF-8 [RFC3629] is an efficient encoding of Unicode characters that optimizes the encoding of ASCII characters in support of text-based communications.

### 3.3.1 String Structure

Each UTF-8 encoded string is prefixed with a two-byte length field that gives the number of bytes in the UTF-8 encoded string itself. Consequently, there is a limit on the size of a string that can be passed: you cannot use a string that would encode to more than 65,535 bytes.

### Figure 3-2: UTF-8 Encoded String Structure

```
Bit         7   6   5   4   3   2   1   0
byte 1     [     String Length MSB      ]
byte 2     [     String Length LSB      ]
byte 3...  [  UTF-8 Encoded Character Data, if length > 0  ]
```

Unless stated otherwise, all UTF-8 encoded strings can have any length in the range 0 to 65,535 bytes.

### 3.3.2 UTF-8 String Requirements

The character data in a UTF-8 encoded string MUST be well-formed UTF-8 as defined by the Unicode specification and restated in RFC 3629. In particular this data MUST NOT include encodings of code points between U+D800 and U+DFFF. **[MQTT-1.5.3-1]** If a Server or Client receives a Control Packet containing ill-formed UTF-8 it MUST close the Network Connection.

**[MQTT-1.5.3-2]** A UTF-8 encoded string MUST NOT include an encoding of the null character U+0000. If a receiver (Server or Client) receives a Control Packet containing U+0000 it MUST close the Network Connection.

The data SHOULD NOT include encodings of the following Unicode code points. If a receiver (Server or Client) receives a Control Packet containing any of them it MAY close the Network Connection:

- U+0001..U+001F (control characters)
- U+007F..U+009F (control characters)
- Code points defined in the Unicode specification to be non-characters (e.g., U+0FFFF)

**[MQTT-1.5.3-3]** A UTF-8 encoded sequence 0xEF 0xBB 0xBF is always interpreted as U+FEFF ("ZERO WIDTH NO-BREAK SPACE") wherever it appears in a string and MUST NOT be skipped over or stripped off by a packet receiver.

### 3.3.3 Non-Normative Example

The string "A𪛔" (LATIN CAPITAL LETTER A followed by U+2A6D4, a CJK IDEOGRAPH EXTENSION B character) is encoded as:

### Table 3-1: UTF-8 Encoding Example

| Byte | Value | Description |
|------|-------|-------------|
| 1 | 0x00 | String Length MSB |
| 2 | 0x05 | String Length LSB (5 bytes) |
| 3 | 0x41 | 'A' |
| 4 | 0xF0 | First byte of U+2A6D4 |
| 5 | 0xAA | Second byte of U+2A6D4 |
| 6 | 0x9B | Third byte of U+2A6D4 |
| 7 | 0x94 | Fourth byte of U+2A6D4 |

## 3.4 Variable Length Integer Encoding

The Remaining Length field in the fixed header uses a variable length encoding scheme. This allows small values to be encoded efficiently while still supporting larger packet sizes.

### 3.4.1 Encoding Algorithm

Each byte encodes 7 bits of data (bits 0-6). Bit 7 is a continuation bit that indicates whether there are more bytes to follow:

- If bit 7 is 0, this is the last byte
- If bit 7 is 1, there are more bytes following

```
do
    encodedByte = X MOD 128
    X = X DIV 128
    if (X > 0)
        encodedByte = encodedByte OR 128
    endif
    output encodedByte
while (X > 0)
```

### 3.4.2 Decoding Algorithm

```
multiplier = 1
value = 0
do
    encodedByte = next byte from stream
    value += (encodedByte AND 127) * multiplier
    multiplier *= 128
    if (multiplier > 128*128*128)
        throw Error(Malformed Remaining Length)
while ((encodedByte AND 128) != 0)
```

### Table 3-2: Remaining Length Value Ranges

| Bytes | From | To | Range Description |
|-------|------|----|--------------------|
| 1 | 0 (0x00) | 127 (0x7F) | 0 to 127 |
| 2 | 128 (0x80, 0x01) | 16,383 (0xFF, 0x7F) | 128 to 16K |
| 3 | 16,384 (0x80, 0x80, 0x01) | 2,097,151 (0xFF, 0xFF, 0x7F) | 16K to 2M |
| 4 | 2,097,152 (0x80, 0x80, 0x80, 0x01) | 268,435,455 (0xFF, 0xFF, 0xFF, 0x7F) | 2M to 256M |

> **Note:** The maximum packet size allowed by MQTT 3.1.1 is 268,435,455 bytes (256 MB). The wire representation of this maximum value is: 0xFF, 0xFF, 0xFF, 0x7F.

### 3.4.3 Encoding Examples

| Decimal Value | Encoded Bytes | Explanation |
|---------------|---------------|-------------|
| 0 | 0x00 | Single byte, no continuation |
| 64 | 0x40 | Single byte |
| 127 | 0x7F | Maximum single-byte value |
| 128 | 0x80, 0x01 | First byte: 0 + continuation; Second byte: 1 |
| 321 | 0xC1, 0x02 | 65 + (128 × 2) = 321 |
| 16,383 | 0xFF, 0x7F | Maximum two-byte value |
| 16,384 | 0x80, 0x80, 0x01 | Three bytes needed |
