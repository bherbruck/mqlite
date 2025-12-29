# Appendix A: Normative Statements

> **Note:** This appendix is informative. It collects all normative statements from the specification for easy reference during implementation and conformance testing.

## A.1 Data Representations (Section 3)

**[MQTT-1.5.3-1]** If a Server or Client receives a Control Packet containing ill-formed UTF-8 it MUST close the Network Connection.

**[MQTT-1.5.3-2]** A UTF-8 encoded string MUST NOT include an encoding of the null character U+0000. If a receiver (Server or Client) receives a Control Packet containing U+0000 it MUST close the Network Connection.

**[MQTT-1.5.3-3]** A UTF-8 encoded sequence 0xEF 0xBB 0xBF is always interpreted as U+FEFF ("ZERO WIDTH NO-BREAK SPACE") wherever it appears in a string and MUST NOT be skipped over or stripped off by a packet receiver.

## A.2 Fixed Header (Section 4.2)

**[MQTT-2.2.2-1]** Where a flag bit is marked as "Reserved", it is reserved for future use and MUST be set to the value listed in the specification.

**[MQTT-2.2.2-2]** If invalid flags are received, the receiver MUST close the Network Connection.

## A.3 Variable Header (Section 4.3)

**[MQTT-2.3.1-1]** SUBSCRIBE, UNSUBSCRIBE, and PUBLISH (in cases where QoS > 0) Control Packets MUST contain a non-zero 16-bit Packet Identifier.

**[MQTT-2.3.1-2]** Each time a Client sends a new packet of one of these types it MUST assign it a currently unused Packet Identifier.

**[MQTT-2.3.1-3]** If a Client re-sends a particular Control Packet, then it MUST use the same Packet Identifier in subsequent re-sends of that packet.

**[MQTT-2.3.1-4]** The same conditions apply to a Server when it sends a PUBLISH with QoS > 0.

**[MQTT-2.3.1-5]** A PUBLISH Packet MUST NOT contain a Packet Identifier if its QoS value is set to 0.

**[MQTT-2.3.1-6]** A PUBACK, PUBREC, or PUBREL Packet MUST contain the same Packet Identifier as the PUBLISH Packet that was originally sent.

**[MQTT-2.3.1-7]** SUBACK and UNSUBACK MUST contain the Packet Identifier that was used in the corresponding SUBSCRIBE and UNSUBSCRIBE Packet respectively.

## A.4 CONNECT (Section 5.1)

**[MQTT-3.1.0-1]** After a Network Connection is established by a Client to a Server, the first Packet sent from the Client to the Server MUST be a CONNECT Packet.

**[MQTT-3.1.0-2]** A Client can only send the CONNECT Packet once over a Network Connection. The Server MUST process a second CONNECT Packet sent from a Client as a protocol violation and disconnect the Client.

**[MQTT-3.1.2-1]** If the protocol name is incorrect the Server MAY disconnect the Client, or it MAY continue processing the CONNECT packet in accordance with some other specification. In the latter case, the Server MUST NOT continue to process the CONNECT packet in line with this specification.

**[MQTT-3.1.2-2]** The Server MUST respond to the CONNECT Packet with a CONNACK return code 0x01 (unacceptable protocol level) and then disconnect the Client if the Protocol Level is not supported by the Server.

**[MQTT-3.1.2-3]** The Server MUST validate that the reserved flag in the CONNECT Control Packet is set to zero and disconnect the Client if it is not zero.

**[MQTT-3.1.2-4]** If CleanSession is set to 0, the Server MUST resume communications with the Client based on state from the current Session. If there is no Session associated with the Client identifier the Server MUST create a new Session. The Client and Server MUST store the Session after the Client and Server are disconnected.

**[MQTT-3.1.2-5]** After the disconnection of a Session that had CleanSession set to 0, the Server MUST store further QoS 1 and QoS 2 messages that match any subscriptions that the client had at the time of disconnection as part of the Session state.

**[MQTT-3.1.2-6]** If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new one. State data associated with this Session MUST NOT be reused in any subsequent Session.

**[MQTT-3.1.2-7]** Retained messages do not form part of the Session state in the Server, they MUST NOT be deleted when the Session ends.

**[MQTT-3.1.2-8]** If the Will Flag is set to 1, a Will Message MUST be stored on the Server and associated with the Network Connection. The Will Message MUST be published when the Network Connection is subsequently closed unless the Will Message has been deleted by the Server on receipt of a DISCONNECT Packet.

**[MQTT-3.1.2-9]** If the Will Flag is set to 1, the Will Topic and Will Message fields MUST be present in the payload.

**[MQTT-3.1.2-10]** The Will Message MUST be removed from stored Session state once it has been published or the Server has received a DISCONNECT packet from the Client.

**[MQTT-3.1.2-11]** If the Will Flag is set to 0, the Will QoS and Will Retain fields MUST be set to zero and the Will Topic and Will Message fields MUST NOT be present in the payload.

**[MQTT-3.1.2-12]** If the Will Flag is set to 0, a Will Message MUST NOT be published when this Network Connection ends.

**[MQTT-3.1.2-13]** If the Will Flag is set to 0, then the Will QoS MUST be set to 0 (0x00).

**[MQTT-3.1.2-14]** If the Will Flag is set to 1, the value of Will QoS can be 0, 1, or 2. It MUST NOT be 3.

**[MQTT-3.1.2-15]** If the Will Flag is set to 0, then the Will Retain Flag MUST be set to 0.

**[MQTT-3.1.2-16]** If Will Retain is 0 and Will Flag is 1, the Server MUST publish the Will Message as a non-retained message.

**[MQTT-3.1.2-17]** If Will Retain is 1 and Will Flag is 1, the Server MUST publish the Will Message as a retained message.

**[MQTT-3.1.2-18]** If the User Name Flag is set to 0, a user name MUST NOT be present in the payload.

**[MQTT-3.1.2-19]** If the User Name Flag is set to 1, a user name MUST be present in the payload.

**[MQTT-3.1.2-20]** If the Password Flag is set to 0, a password MUST NOT be present in the payload.

**[MQTT-3.1.2-21]** If the Password Flag is set to 1, a password MUST be present in the payload.

**[MQTT-3.1.2-22]** If the User Name Flag is set to 0, the Password Flag MUST be set to 0.

**[MQTT-3.1.2-23]** In the absence of sending any other Control Packets, the Client MUST send a PINGREQ Packet.

**[MQTT-3.1.2-24]** If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the Client within one and a half times the Keep Alive time period, it MUST disconnect the Network Connection to the Client as if the network had failed.

**[MQTT-3.1.3-1]** Payload fields, if present, MUST appear in the order: Client Identifier, Will Topic, Will Message, User Name, Password.

**[MQTT-3.1.3-2]** The Client Identifier MUST be used by Clients and by Servers to identify state that they hold relating to this MQTT Session between the Client and the Server.

**[MQTT-3.1.3-3]** The Client Identifier MUST be present and MUST be the first field in the CONNECT packet payload.

**[MQTT-3.1.3-4]** The ClientId MUST be a UTF-8 encoded string.

**[MQTT-3.1.3-5]** The Server MUST allow ClientIds which are between 1 and 23 UTF-8 encoded bytes in length, and that contain only the characters "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".

**[MQTT-3.1.3-6]** A Server MAY allow a Client to supply a ClientId that has a length of zero bytes. If it does so, the Server MUST treat this as a special case and assign a unique ClientId to that Client. It MUST then process the CONNECT packet as if the Client had provided that unique ClientId.

**[MQTT-3.1.3-7]** If the Client supplies a zero-byte ClientId, the Client MUST also set CleanSession to 1.

**[MQTT-3.1.3-8]** If the Client supplies a zero-byte ClientId with CleanSession set to 0, the Server MUST respond with a CONNACK return code 0x02 (Identifier rejected) and then close the Network Connection.

**[MQTT-3.1.3-9]** If the Server rejects the ClientId it MUST respond with a CONNACK return code 0x02 (Identifier rejected) and then close the Network Connection.

**[MQTT-3.1.3-10]** The Will Topic MUST be a UTF-8 encoded string.

**[MQTT-3.1.3-11]** The User Name MUST be a UTF-8 encoded string.

**[MQTT-3.1.4-1]** The Server MUST validate that the CONNECT Packet conforms to section 3.1 and close the Network Connection without sending a CONNACK if it does not conform.

**[MQTT-3.1.4-2]** If the ClientId represents a Client already connected to the Server, the Server MUST disconnect the existing Client.

**[MQTT-3.1.4-3]** The Server MUST perform the processing of CleanSession.

**[MQTT-3.1.4-4]** The Server MUST acknowledge the CONNECT Packet with a CONNACK Packet containing a zero return code.

**[MQTT-3.1.4-5]** If the Server rejects the CONNECT, it MUST NOT process any data sent by the Client after the CONNECT Packet.

## A.5 CONNACK (Section 5.2)

**[MQTT-3.2.0-1]** The first packet sent from the Server to the Client MUST be a CONNACK Packet.

**[MQTT-3.2.2-1]** If the Server accepts a connection with CleanSession set to 1, the Server MUST set Session Present to 0 in the CONNACK packet.

**[MQTT-3.2.2-2]** If the Server accepts a connection with CleanSession set to 0 and has stored Session state, it MUST set Session Present to 1 in the CONNACK packet.

**[MQTT-3.2.2-3]** If the Server accepts a connection with CleanSession set to 0 and does not have stored Session state, it MUST set Session Present to 0 in the CONNACK packet.

**[MQTT-3.2.2-4]** If a server sends a CONNACK packet containing a non-zero return code it MUST set Session Present to 0.

**[MQTT-3.2.2-5]** If a server sends a CONNACK packet containing a non-zero return code it MUST then close the Network Connection.

**[MQTT-3.2.2-6]** If none of the listed return codes are deemed applicable, the Server MUST close the Network Connection without sending a CONNACK.

## A.6 PUBLISH (Section 5.3)

**[MQTT-3.3.1-1]** The DUP flag MUST be set to 1 by the Client or Server when it attempts to re-deliver a PUBLISH Packet.

**[MQTT-3.3.1-2]** The DUP flag MUST be set to 0 for all QoS 0 messages.

**[MQTT-3.3.1-3]** The DUP flag in the outgoing PUBLISH packet is set independently to the incoming PUBLISH packet; its value MUST be determined solely by whether the outgoing PUBLISH packet is a retransmission.

**[MQTT-3.3.1-4]** A PUBLISH Packet MUST NOT have both QoS bits set to 1. If a Server or Client receives a PUBLISH Packet which has both QoS bits set to 1 it MUST close the Network Connection.

**[MQTT-3.3.1-5]** If the RETAIN flag is set to 1 in a PUBLISH Packet sent by a Client to a Server, the Server MUST store the Application Message and its QoS.

**[MQTT-3.3.1-6]** When a new subscription is established, the last retained message, if any, on each matching topic name MUST be sent to the subscriber.

**[MQTT-3.3.1-7]** If the Server receives a QoS 0 message with RETAIN set to 1, it MUST discard any message previously retained for that topic.

**[MQTT-3.3.1-8]** When sending a PUBLISH Packet to a Client, the Server MUST set the RETAIN flag to 1 if a message is sent as a result of a new subscription.

**[MQTT-3.3.1-9]** The Server MUST set the RETAIN flag to 0 when a PUBLISH Packet is sent to a Client because it matches an established subscription.

**[MQTT-3.3.1-10]** A PUBLISH Packet with RETAIN flag set to 1 and a zero-length payload removes existing retained message for that topic.

**[MQTT-3.3.1-11]** A zero byte retained message MUST NOT be stored as a retained message on the Server.

**[MQTT-3.3.1-12]** If the RETAIN flag is 0, the Server MUST NOT store the message and MUST NOT remove or replace any existing retained message.

**[MQTT-3.3.2-1]** The Topic Name MUST be present as the first field in the PUBLISH Packet Variable header. It MUST be a UTF-8 encoded string.

**[MQTT-3.3.2-2]** The Topic Name in the PUBLISH Packet MUST NOT contain wildcard characters.

**[MQTT-3.3.2-3]** The Topic Name in a PUBLISH Packet sent by a Server to a subscribing Client MUST match the Subscription's Topic Filter.

**[MQTT-3.3.4-1]** The receiver of a PUBLISH Packet MUST respond according to the QoS.

**[MQTT-3.3.5-1]** When subscriptions overlap, the Server MUST deliver the message to the Client respecting the maximum QoS of all the matching subscriptions.

**[MQTT-3.3.5-2]** If a Server implementation does not authorize a PUBLISH, it MUST either make a positive acknowledgement or close the Network Connection.

## A.7 PUBREL (Section 5.4)

**[MQTT-3.6.1-1]** Bits 3,2,1,0 of the fixed header in the PUBREL Control Packet MUST be set to 0,0,1,0 respectively. The Server MUST treat any other value as malformed and close the Network Connection.

## A.8 SUBSCRIBE (Section 5.5)

**[MQTT-3.8.1-1]** Bits 3,2,1,0 of the fixed header of the SUBSCRIBE Control Packet MUST be set to 0,0,1,0 respectively. The Server MUST treat any other value as malformed and close the Network Connection.

**[MQTT-3.8.3-1]** The Topic Filters in a SUBSCRIBE packet payload MUST be UTF-8 encoded strings.

**[MQTT-3.8.3-2]** A Server SHOULD support Topic filters that contain wildcard characters. If it does not, it MUST reject any Subscription request whose filter contains them.

**[MQTT-3.8.3-3]** The payload of a SUBSCRIBE packet MUST contain at least one Topic Filter / QoS pair.

**[MQTT-3.8.3-4]** The Server MUST treat a SUBSCRIBE packet as malformed and close the Network Connection if any of the Reserved bits in the payload are non-zero, or if QoS is not 0, 1, or 2.

**[MQTT-3.8.4-1]** When the Server receives a SUBSCRIBE Packet from a Client, the Server MUST respond with a SUBACK Packet.

**[MQTT-3.8.4-2]** The SUBACK Packet MUST have the same Packet Identifier as the SUBSCRIBE Packet.

**[MQTT-3.8.4-3]** If a Server receives a SUBSCRIBE Packet containing a Topic Filter identical to an existing Subscription's Topic Filter then it MUST completely replace that existing Subscription. Retained messages MUST be re-sent, but the flow of publications MUST NOT be interrupted.

**[MQTT-3.8.4-4]** If a Server receives a SUBSCRIBE packet that contains multiple Topic Filters it MUST handle that packet as if it had received a sequence of multiple SUBSCRIBE packets.

**[MQTT-3.8.4-5]** The SUBACK Packet MUST contain a return code for each Topic Filter/QoS pair.

## A.9 UNSUBSCRIBE (Section 5.6)

**[MQTT-3.10.1-1]** Bits 3,2,1,0 of the fixed header of the UNSUBSCRIBE Control Packet MUST be set to 0,0,1,0 respectively. The Server MUST treat any other value as malformed and close the Network Connection.

**[MQTT-3.10.3-1]** The Topic Filters in an UNSUBSCRIBE packet MUST be UTF-8 encoded strings.

**[MQTT-3.10.3-2]** The Payload of an UNSUBSCRIBE packet MUST contain at least one Topic Filter.

**[MQTT-3.10.4-1]** The Server MUST respond to an UNSUBSCRIBE request by sending an UNSUBACK packet.

**[MQTT-3.10.4-2]** Even if no Topic Filters are matched, the Server MUST respond with an UNSUBACK.

**[MQTT-3.10.4-3]** If a Server receives an UNSUBSCRIBE packet that contains multiple Topic Filters it MUST handle that packet as if it had received a sequence of multiple UNSUBSCRIBE packets.

**[MQTT-3.10.4-4]** The Topic Filters MUST be compared character-by-character with the current set of Topic Filters.

**[MQTT-3.10.4-5]** If a Server deletes a Subscription, it MUST stop adding any new messages for delivery to the Client and MUST complete delivery of any QoS 1 or QoS 2 messages it has started to send.

## A.10 PINGREQ (Section 5.7)

**[MQTT-3.12.4-1]** The Server MUST send a PINGRESP Packet in response to a PINGREQ Packet.

## A.11 DISCONNECT (Section 5.8)

**[MQTT-3.14.4-1]** After sending a DISCONNECT Packet the Client MUST close the Network Connection and MUST NOT send any more Control Packets.

**[MQTT-3.14.4-2]** On receipt of DISCONNECT the Server MUST discard any Will Message associated with the current connection without publishing it.

## A.12 Operational Behavior (Section 6)

**[MQTT-4.4.0-1]** When a Client reconnects with CleanSession set to 0, both Client and Server MUST re-send any unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using their original Packet Identifiers.

**[MQTT-4.5.0-1]** When a Server takes ownership of an incoming Application Message it MUST add it to the Session state of those Clients that have matching Subscriptions.

**[MQTT-4.6.0-1]** When a Client re-sends any PUBLISH packets, it MUST re-send them in the order in which the original PUBLISH packets were sent.

**[MQTT-4.6.0-2]** A Client MUST send PUBACK packets in the order in which the corresponding PUBLISH packets were received.

**[MQTT-4.6.0-3]** A Client MUST send PUBREC packets in the order in which the corresponding PUBLISH packets were received.

**[MQTT-4.6.0-4]** A Client MUST send PUBREL packets in the order in which the corresponding PUBREC packets were received.

**[MQTT-4.6.0-5]** A Server MUST by default treat each Topic as an "Ordered Topic".

**[MQTT-4.6.0-6]** When a Server processes a message to an Ordered Topic, it MUST send PUBLISH packets to subscribers in the order in which messages were received from any given Client.

**[MQTT-4.8.0-1]** If a Server or Client encounters a protocol error it MUST close the Network Connection.

## A.13 Topics (Section 8)

**[MQTT-4.7.0-1]** Topic Names and Topic Filters MUST be at least one character long.

**[MQTT-4.7.1-1]** The multi-level wildcard character MUST be specified either on its own or following a topic level separator. In either case it MUST be the last character.

**[MQTT-4.7.1-2]** The single-level wildcard can be used at any level in the Topic Filter. Where it is used it MUST occupy an entire level.

**[MQTT-4.7.2-1]** The Server MUST NOT match Topic Filters starting with a wildcard character with Topic Names beginning with a $.

**[MQTT-4.7.3-1]** Topic Names MUST NOT include wildcard characters.

**[MQTT-4.7.3-2]** Topic Names in PUBLISH packets sent by Clients to Servers MUST NOT contain wildcard characters.

## A.14 WebSocket (Section 10)

**[MQTT-6.0-1]** MQTT Control Packets MUST be sent in WebSocket binary data frames. If any other type of data frame is received the recipient MUST close the Network Connection.
