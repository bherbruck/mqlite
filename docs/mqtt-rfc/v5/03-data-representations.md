# Section 3: Data Representations

> **Note:** MQTT 5.0 adds three new data types compared to 3.1.1: **Four Byte Integer**, **Binary Data**, and **UTF-8 String Pair**.

## 3.1 Bits

Bits in a byte are labeled 7 through 0. Bit number 7 is the most significant bit, the least significant bit is assigned bit number 0.

```
Bit Position:   7   6   5   4   3   2   1   0
Significance:  MSB                         LSB
Example 0xC5:   1   1   0   0   0   1   0   1
```

## 3.2 Two Byte Integer

Two Byte Integer data values are 16-bit unsigned integers in big-endian order: the high order byte precedes the lower order byte.

### Figure 3-1: Two Byte Integer Encoding

```
Byte:          1           2
           ┌───────┐   ┌───────┐
           │  MSB  │   │  LSB  │
           └───────┘   └───────┘
```

**Range:** 0 to 65,535

**Example:** The decimal value 1234 (0x04D2) is encoded as:
- Byte 1: 0x04 (MSB)
- Byte 2: 0xD2 (LSB)

## 3.3 Four Byte Integer

Four Byte Integer data values are 32-bit unsigned integers in big-endian order: the high order byte precedes the successively lower order bytes.

### Figure 3-2: Four Byte Integer Encoding

```
Byte:      1       2       3       4
       ┌───────┬───────┬───────┬───────┐
       │  MSB  │       │       │  LSB  │
       └───────┴───────┴───────┴───────┘
```

**Range:** 0 to 4,294,967,295

**Example:** The decimal value 270,544,960 (0x10203040) is encoded as:
- Byte 1: 0x10
- Byte 2: 0x20
- Byte 3: 0x30
- Byte 4: 0x40

## 3.4 UTF-8 Encoded String

Text fields within MQTT Control Packets are encoded as UTF-8 strings. UTF-8 [RFC3629] is an efficient encoding of Unicode characters that optimizes the encoding of ASCII characters.

### 3.4.1 String Structure

Each UTF-8 Encoded String is prefixed with a Two Byte Integer length field that gives the number of bytes in the UTF-8 encoded string itself. Consequently, the maximum size of a UTF-8 Encoded String is 65,535 bytes.

### Figure 3-3: UTF-8 Encoded String Structure

```
Bit         7   6   5   4   3   2   1   0
byte 1     [     String Length MSB      ]
byte 2     [     String Length LSB      ]
byte 3...  [  UTF-8 Encoded Character Data, if length > 0  ]
```

Unless stated otherwise, all UTF-8 encoded strings can have any length in the range 0 to 65,535 bytes.

### 3.4.2 UTF-8 String Requirements

**[MQTT-1.5.4-1]** The character data in a UTF-8 Encoded String MUST be well-formed UTF-8 as defined by the Unicode specification and restated in RFC 3629. In particular, the character data MUST NOT include encodings of code points between U+D800 and U+DFFF. If the Client or Server receives an MQTT Control Packet containing ill-formed UTF-8 it is a Malformed Packet.

**[MQTT-1.5.4-2]** A UTF-8 Encoded String MUST NOT include an encoding of the null character U+0000. If a receiver receives an MQTT Control Packet containing U+0000 it is a Malformed Packet.

The data SHOULD NOT include encodings of the following Disallowed Unicode code points. If a receiver receives a Control Packet containing any of them it MAY treat it as a Malformed Packet:

- U+0001..U+001F (control characters)
- U+007F..U+009F (control characters)
- Code points defined in Unicode as non-characters (e.g., U+0FFFF)

**[MQTT-1.5.4-3]** A UTF-8 encoded sequence 0xEF 0xBB 0xBF is always interpreted as U+FEFF ("ZERO WIDTH NO-BREAK SPACE") wherever it appears in a string and MUST NOT be skipped over or stripped off by a packet receiver.

## 3.5 Variable Byte Integer

The Variable Byte Integer is encoded using an encoding scheme which uses a single byte for values up to 127. Larger values are handled as follows:

- The least significant seven bits of each byte encode the data
- The most significant bit (bit 7) is a continuation bit indicating more bytes follow
- Maximum of four bytes in the Variable Byte Integer field

**[MQTT-1.5.5-1]** The encoded value MUST use the minimum number of bytes necessary to represent the value.

### Table 3-1: Variable Byte Integer Ranges

| Bytes | From | To | Max Hex Encoding |
|-------|------|----|--------------------|
| 1 | 0 | 127 | 0x7F |
| 2 | 128 | 16,383 | 0xFF, 0x7F |
| 3 | 16,384 | 2,097,151 | 0xFF, 0xFF, 0x7F |
| 4 | 2,097,152 | 268,435,455 | 0xFF, 0xFF, 0xFF, 0x7F |

### 3.5.1 Encoding Algorithm

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

### 3.5.2 Decoding Algorithm

```
multiplier = 1
value = 0
do
    encodedByte = next byte from stream
    value += (encodedByte AND 127) * multiplier
    if (multiplier > 128*128*128)
        throw Error(Malformed Variable Byte Integer)
    multiplier *= 128
while ((encodedByte AND 128) != 0)
```

### 3.5.3 Encoding Examples

| Decimal | Encoded Bytes | Explanation |
|---------|---------------|-------------|
| 0 | 0x00 | Single byte |
| 127 | 0x7F | Maximum single byte |
| 128 | 0x80, 0x01 | Two bytes: 0 + (1 × 128) |
| 16,383 | 0xFF, 0x7F | Maximum two bytes |
| 16,384 | 0x80, 0x80, 0x01 | Three bytes |
| 2,097,151 | 0xFF, 0xFF, 0x7F | Maximum three bytes |
| 268,435,455 | 0xFF, 0xFF, 0xFF, 0x7F | Maximum (256 MB) |

## 3.6 Binary Data

Binary Data is represented by a Two Byte Integer length which indicates the number of data bytes, followed by that number of bytes.

### Figure 3-4: Binary Data Structure

```
Bit         7   6   5   4   3   2   1   0
byte 1     [      Data Length MSB       ]
byte 2     [      Data Length LSB       ]
byte 3...  [         Binary Data        ]
```

**Range:** 0 to 65,535 bytes

## 3.7 UTF-8 String Pair

A UTF-8 String Pair consists of two UTF-8 Encoded Strings. This data type is used to hold name-value pairs. The first string serves as the name, and the second string contains the value.

### Figure 3-5: UTF-8 String Pair Structure

```
┌─────────────────────────────────────────┐
│     Name Length (Two Byte Integer)      │
├─────────────────────────────────────────┤
│     Name (UTF-8 Encoded String)         │
├─────────────────────────────────────────┤
│     Value Length (Two Byte Integer)     │
├─────────────────────────────────────────┤
│     Value (UTF-8 Encoded String)        │
└─────────────────────────────────────────┘
```

**[MQTT-1.5.7-1]** Both strings MUST comply with the requirements for UTF-8 Encoded Strings. If a receiver receives a string pair which does not meet these requirements it is a Malformed Packet.

UTF-8 String Pairs are used for User Properties, which can appear multiple times in a packet.
