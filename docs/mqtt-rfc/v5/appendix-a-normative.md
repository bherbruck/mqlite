# Appendix A: Normative Statements (Summary)

This appendix collects key normative statements from the MQTT 5.0 specification.

## A.1 General

**[MQTT-1.5.4-1]** UTF-8 strings MUST be well-formed UTF-8, MUST NOT include U+D800-U+DFFF.

**[MQTT-1.5.4-2]** UTF-8 strings MUST NOT include null character U+0000.

**[MQTT-1.5.5-1]** Variable Byte Integer MUST use minimum bytes necessary.

## A.2 Fixed Header

**[MQTT-2.1.3-1]** Reserved flag bits MUST be set to specified values. Invalid flags are Malformed Packet.

## A.3 Packet Identifier

**[MQTT-2.2.1-2]** QoS 0 PUBLISH MUST NOT contain Packet Identifier.

**[MQTT-2.2.1-3]** Client MUST assign unused Packet Identifier to new SUBSCRIBE, UNSUBSCRIBE, or QoS>0 PUBLISH.

**[MQTT-2.2.1-5]** PUBACK, PUBREC, PUBREL, PUBCOMP MUST contain same Packet Identifier as original PUBLISH.

## A.4 CONNECT

**[MQTT-3.1.0-1]** First packet MUST be CONNECT.

**[MQTT-3.1.0-2]** Second CONNECT is Protocol Error; Server MUST close connection.

**[MQTT-3.1.2-3]** Reserved flag MUST be 0; if not, Malformed Packet.

**[MQTT-3.1.2-4]** Clean Start = 1: MUST discard existing Session.

**[MQTT-3.1.2-7]** Will Flag = 1: MUST store Will Message.

**[MQTT-3.1.2-22]** Server MUST close connection if no packet within 1.5x Keep Alive.

## A.5 CONNACK

**[MQTT-3.2.2-1]** Clean Start = 1: Session Present MUST be 0.

**[MQTT-3.2.2-4]** Non-zero Reason Code: Session Present MUST be 0.

**[MQTT-3.2.2-5]** Non-zero Reason Code: Server MUST close connection.

## A.6 PUBLISH

**[MQTT-3.3.1-1]** DUP MUST be 1 on re-delivery.

**[MQTT-3.3.1-2]** DUP MUST be 0 for QoS 0.

**[MQTT-3.3.1-4]** QoS MUST NOT be 3 (both bits set).

**[MQTT-3.3.2-2]** Topic Name MUST NOT contain wildcards.

## A.7 PUBREL

**[MQTT-3.6.1-1]** Flags MUST be 0010.

## A.8 SUBSCRIBE

**[MQTT-3.8.1-1]** Flags MUST be 0010.

**[MQTT-3.8.3-1]** Payload MUST contain at least one Topic Filter.

**[MQTT-3.8.3-3]** Shared Subscriptions MUST have No Local = 0.

## A.9 UNSUBSCRIBE

**[MQTT-3.10.3-1]** Payload MUST contain at least one Topic Filter.

## A.10 PINGREQ/PINGRESP

**[MQTT-3.12.4-1]** Server MUST send PINGRESP in response to PINGREQ.

## A.11 DISCONNECT

**[MQTT-3.14.2-1]** Cannot set Session Expiry to non-zero if it was zero in CONNECT.

## A.12 AUTH

**[MQTT-4.12.0-1]** With Auth Method in CONNECT, Client MUST NOT send packets other than AUTH/DISCONNECT before CONNACK.

**[MQTT-4.12.1-2]** Re-auth MUST use same Authentication Method as CONNECT.

## A.13 Session State

**[MQTT-3.1.2-23]** Session state MUST be stored if Session Expiry Interval > 0.

## A.14 Message Ordering

**[MQTT-4.6.0-1]** Re-sent PUBLISH packets MUST be in original order.

**[MQTT-4.6.0-2]** PUBACK MUST be sent in order of PUBLISH received.

## A.15 Flow Control

**[MQTT-3.3.4-7]** MUST NOT send more QoS>0 PUBLISH than Receive Maximum allows.

## A.16 Maximum Packet Size

**[MQTT-3.1.2-24]** Server MUST NOT send packets exceeding Client's Maximum Packet Size.

**[MQTT-3.2.2-15]** Client MUST NOT send packets exceeding Server's Maximum Packet Size.
