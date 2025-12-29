# Section 2: Terminology and Conventions

> **Note:** This section is similar to MQTT 3.1.1 but adds definitions for v5.0 features: Shared Subscription, Subscription Identifier, Topic Alias, Properties, Reason Code, Malformed Packet, and Protocol Error.

## 2.1 Requirements Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in RFC 2119, except where they appear in text that is marked as non-normative.

## 2.2 Definitions

**Application Message**
: The data carried by the MQTT protocol across the network for the application. When an Application Message is transported by MQTT it contains payload data, a Quality of Service (QoS), a collection of Properties, and a Topic Name.

**Client**
: A program or device that uses MQTT. A Client: opens the Network Connection to the Server; publishes Application Messages that other Clients might be interested in; subscribes to request Application Messages that it is interested in receiving; unsubscribes to remove a request for Application Messages; closes the Network Connection to the Server.

**Control Packet**
: A packet of information that is sent across the Network Connection. The MQTT specification defines fifteen different types of MQTT Control Packet, for example the PUBLISH packet is used to convey Application Messages.

**Disallowed Unicode Code Point**
: The set of Unicode Control Codes and Unicode Noncharacters which should not be included in a UTF-8 Encoded String. Refer to [Section 3](./03-data-representations.md) for more information.

**Malformed Packet**
: A control packet that cannot be parsed according to this specification. Refer to [Section 13](./13-error-handling.md) for information about error handling.

**Network Connection**
: A construct provided by the underlying transport protocol that is being used by MQTT. It connects the Client to the Server. It provides the means to send an ordered, lossless stream of bytes in both directions.

**Packet Identifier**
: A 16-bit unsigned integer used to identify specific Control Packets during message flows. Used with PUBLISH (QoS > 0), PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, and UNSUBACK.

**Properties**
: A set of typed key-value pairs attached to MQTT packets to convey metadata. Properties are a new feature in MQTT 5.0.

**Protocol Error**
: An error that is detected after the packet has been parsed and found to contain data that is not allowed by the protocol or is inconsistent with the state of the Client or Server. Refer to [Section 13](./13-error-handling.md) for information about error handling.

**Reason Code**
: A one-byte unsigned value that indicates the result of an operation. Reason Codes less than 0x80 indicate success; values 0x80 or greater indicate failure.

**Server**
: A program or device that acts as an intermediary between Clients which publish Application Messages and Clients which have made Subscriptions. A Server: accepts Network Connections from Clients; accepts Application Messages published by Clients; processes Subscribe and Unsubscribe requests from Clients; forwards Application Messages that match Client Subscriptions; closes the Network Connection from the Client.

**Session**
: A stateful interaction between a Client and a Server. Some Sessions last only as long as the Network Connection, others can span multiple consecutive Network Connections between a Client and a Server.

**Shared Subscription**
: A Subscription that can be associated with more than one Session to allow load balancing. An Application Message matching a Shared Subscription is only sent to one of the subscribing Clients. A Session can have both Shared and non-Shared Subscriptions.

**Subscription**
: A Subscription comprises a Topic Filter and a maximum QoS. A Subscription is associated with a single Session. A Session can contain more than one Subscription. Each Subscription within a Session has a different Topic Filter.

**Subscription Identifier**
: A Variable Byte Integer associated with a Subscription that is returned to the Client in matching PUBLISH packets.

**Topic Alias**
: A two-byte integer that can be used in place of a Topic Name to reduce packet size.

**Topic Filter**
: An expression contained in a Subscription to indicate an interest in one or more topics. A Topic Filter can include wildcard characters.

**Topic Name**
: The label attached to an Application Message which is matched against the Subscriptions known to the Server.

**Wildcard Subscription**
: A Subscription with a Topic Filter containing one or more wildcard characters. This allows the subscription to match more than one Topic Name.

**Will Message**
: An Application Message which is published by the Server after the Network Connection is closed in cases where the Network Connection is not closed normally. Refer to [Section 5.1](./05.01-connect.md) for information about Will Messages.

## 2.3 Abbreviations

| Abbreviation | Expansion |
|--------------|-----------|
| ACK | Acknowledgment |
| IoT | Internet of Things |
| LSB | Least Significant Byte |
| M2M | Machine to Machine |
| MQTT | Message Queuing Telemetry Transport |
| MSB | Most Significant Byte |
| QoS | Quality of Service |
| SASL | Simple Authentication and Security Layer |
| TCP | Transmission Control Protocol |
| TLS | Transport Layer Security |
| UTF-8 | Unicode Transformation Format - 8-bit |

## 2.4 Notation Conventions

### 2.4.1 Bit Numbering

Bits in a byte are labeled 7 through 0. Bit number 7 is the most significant bit, the least significant bit is assigned bit number 0.

```
Bit:    7   6   5   4   3   2   1   0
       MSB                         LSB
```

### 2.4.2 Byte Diagrams

Packet structures are illustrated using tables showing bit positions:

```
Bit     7   6   5   4   3   2   1   0
byte 1  [        Field Name        ]
byte 2  [   Field A   ][  Field B  ]
```

### 2.4.3 Property Notation

Properties are shown with their identifier in decimal and hexadecimal:

```
17 (0x11) Byte, Identifier of Session Expiry Interval
```

### 2.4.4 Normative Statement Identifiers

Conformance statements are identified using references in the format `[MQTT-x.x.x-y]`.
