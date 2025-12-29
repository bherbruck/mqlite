# Section 2: Terminology and Conventions

## 2.1 Requirements Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in RFC 2119.

## 2.2 Definitions

**Application Message**
: The data carried by the MQTT protocol across the network for the application. When Application Messages are transported by MQTT they have an associated Quality of Service and a Topic Name.

**Client**
: A program or device that uses MQTT. A Client always establishes the Network Connection to the Server. It can: publish Application Messages that other Clients might be interested in; subscribe to request Application Messages that it is interested in receiving; unsubscribe to remove a request for Application Messages; disconnect from the Server.

**Control Packet**
: A packet of information that is sent across the Network Connection. The MQTT specification defines fourteen different types of Control Packet, one of which (the PUBLISH packet) is used to convey Application Messages.

**Network Connection**
: A construct provided by the underlying transport protocol that is being used by MQTT. It connects the Client to the Server. It provides the means to send an ordered, lossless stream of bytes in both directions.

**Packet Identifier**
: A 16-bit unsigned integer used to identify specific Control Packets during QoS 1 and QoS 2 message delivery flows.

**Server**
: A program or device that acts as an intermediary between Clients which publish Application Messages and Clients which have made Subscriptions. A Server: accepts Network Connections from Clients; accepts Application Messages published by Clients; processes Subscribe and Unsubscribe requests from Clients; forwards Application Messages that match Client Subscriptions.

**Session**
: A stateful interaction between a Client and a Server. Some Sessions last only as long as the Network Connection, others can span multiple consecutive Network Connections between a Client and a Server.

**Subscription**
: A Subscription comprises a Topic Filter and a maximum QoS. A Subscription is associated with a single Session. A Session can contain more than one Subscription. Each Subscription within a session has a different Topic Filter.

**Topic Filter**
: An expression contained in a Subscription, to indicate an interest in one or more topics. A Topic Filter can include wildcard characters.

**Topic Name**
: The label attached to an Application Message which is matched against the Subscriptions known to the Server. The Server sends a copy of the Application Message to each Client that has a matching Subscription.

**Will Message**
: An Application Message that the Server publishes when the Network Connection is closed in cases where the DISCONNECT Packet is not received.

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

### 2.4.3 Normative Statement Identifiers

Conformance statements are identified using references in the format `[MQTT-x.x.x-y]` where:
- `x.x.x` indicates the section number
- `y` is a sequential identifier within that section

All normative statements are collected in [Appendix A](./appendix-a-normative.md).
