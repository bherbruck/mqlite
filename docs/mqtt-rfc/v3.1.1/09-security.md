# Section 9: Security Considerations

## 9.1 Introduction

MQTT is a transport protocol designed for lightweight messaging. This section describes security considerations for implementers and deployers of MQTT solutions.

The protocol itself provides limited built-in security. Secure deployments typically rely on:
- Transport layer security (TLS)
- Application-level authentication and authorization
- Network-level controls

## 9.2 Threat Model

### 9.2.1 Assets to Protect

| Asset | Description | Impact of Compromise |
|-------|-------------|---------------------|
| Message content | Application data in payloads | Data breach, privacy violation |
| Message metadata | Topics, client IDs, timestamps | Information disclosure |
| Credentials | Usernames, passwords, certificates | Unauthorized access |
| Session state | Subscriptions, pending messages | Service disruption |
| Broker availability | Server uptime and responsiveness | Denial of service |

### 9.2.2 Threat Actors

- **Passive eavesdroppers**: Monitor network traffic
- **Active attackers**: Inject, modify, or replay messages
- **Malicious clients**: Abuse legitimate access
- **Compromised brokers**: Insider threats

### 9.2.3 Attack Vectors

| Vector | Description | Mitigation |
|--------|-------------|------------|
| Eavesdropping | Reading unencrypted traffic | TLS encryption |
| Man-in-the-middle | Intercepting and modifying | TLS with certificate validation |
| Credential theft | Capturing authentication data | TLS, strong passwords, certificates |
| Denial of service | Overwhelming broker resources | Rate limiting, resource quotas |
| Unauthorized publish | Sending malicious messages | ACL enforcement |
| Unauthorized subscribe | Reading restricted topics | ACL enforcement |
| Session hijacking | Taking over client session | Unique client IDs, authentication |

## 9.3 Authentication

### 9.3.1 Authentication Mechanisms

MQTT 3.1.1 provides username/password fields in the CONNECT packet. Servers MAY use these for authentication.

**[Security-5.4.1]** Implementations SHOULD support additional authentication mechanisms:

| Mechanism | Description | Strength |
|-----------|-------------|----------|
| Username/Password | CONNECT packet fields | Weak (unless with TLS) |
| TLS Client Certificates | X.509 certificates | Strong |
| Pre-shared Keys | TLS-PSK | Medium |
| OAuth/JWT | Token in password field | Strong |
| External Authentication | LDAP, RADIUS, etc. | Varies |

### 9.3.2 Password Security

- Passwords SHOULD NOT be transmitted in cleartext (use TLS)
- Servers SHOULD store password hashes, not plaintext
- Implementations SHOULD support password complexity requirements
- Failed authentication attempts SHOULD be rate-limited

### 9.3.3 Client Certificate Authentication

For strong authentication:

1. Server presents its certificate (server authentication)
2. Server requests client certificate
3. Client presents certificate signed by trusted CA
4. Server validates certificate and extracts identity

## 9.4 Authorization

### 9.4.1 Access Control

Servers SHOULD implement authorization to control:

- Which topics a client can publish to
- Which topics a client can subscribe to
- Maximum QoS levels permitted
- Message size limits
- Connection quotas

### 9.4.2 Access Control List (ACL) Model

### Table 9-1: Example ACL Rules

| Client Pattern | Topic Pattern | Action | Permission |
|----------------|---------------|--------|------------|
| `device-*` | `devices/{clientid}/#` | publish | allow |
| `device-*` | `commands/{clientid}` | subscribe | allow |
| `admin` | `#` | publish/subscribe | allow |
| `*` | `$SYS/#` | subscribe | deny |

### 9.4.3 Dynamic Authorization

Implementations MAY support:
- Per-message authorization checks
- External authorization services
- Token-based authorization with expiration

## 9.5 Data Protection

### 9.5.1 Confidentiality

**[Security-5.4.5]** To protect message confidentiality:

- Use TLS 1.2 or higher for transport encryption
- Implement end-to-end encryption for sensitive payloads
- Consider encrypted storage for retained messages

### 9.5.2 Integrity

**[Security-5.4.4]** To protect message integrity:

- TLS provides integrity for transport
- Application-layer message signing for end-to-end integrity
- Consider HMAC or digital signatures for critical messages

### 9.5.3 TLS Configuration

### Table 9-2: Recommended TLS Settings

| Setting | Recommendation |
|---------|----------------|
| Protocol | TLS 1.2 or TLS 1.3 |
| Cipher suites | AEAD ciphers (AES-GCM, ChaCha20-Poly1305) |
| Key exchange | ECDHE preferred |
| Certificate validation | Full chain validation |
| Certificate revocation | OCSP or CRL checking |
| Session resumption | TLS session tickets (optional) |

## 9.6 Denial of Service Protection

### 9.6.1 Connection Limits

Implementations SHOULD enforce:

- Maximum concurrent connections per client ID
- Maximum connections per IP address
- Connection rate limiting
- CONNECT packet timeout

### 9.6.2 Message Limits

Implementations SHOULD enforce:

- Maximum message size
- Maximum messages per second per client
- Maximum pending messages per session
- Maximum topic length

### 9.6.3 Resource Quotas

| Resource | Recommendation |
|----------|----------------|
| Subscriptions per client | Limit to reasonable number |
| Retained messages | Limit count and size |
| Session storage | Limit per-client storage |
| Topic levels | Limit depth |

## 9.7 Privacy Considerations

### 9.7.1 Data Minimization

- Collect only necessary data in client identifiers
- Minimize sensitive data in topic names
- Consider pseudonymization of client identifiers

### 9.7.2 Data Retention

- Define retention policies for messages and logs
- Implement secure deletion
- Consider privacy regulations (GDPR, CCPA)

### 9.7.3 Logging

Logs SHOULD:
- Exclude message payloads (or encrypt them)
- Exclude credentials
- Be protected from unauthorized access
- Have defined retention periods

## 9.8 Implementation Notes

### 9.8.1 Client Identifier Security

- Use unpredictable client identifiers
- Validate client identifier format
- Enforce uniqueness constraints

### 9.8.2 Will Message Security

- Apply same authorization to Will Messages
- Consider Will Message as potential attack vector
- Limit Will Message size

### 9.8.3 Retained Message Security

- Apply authorization to retained message reads
- Consider encryption for sensitive retained messages
- Implement cleanup policies

### 9.8.4 Detecting Abnormal Behavior

Implementations SHOULD monitor for:

| Behavior | Possible Indication |
|----------|---------------------|
| High connection rate | Brute force attack |
| Many failed authentications | Credential guessing |
| Unusual subscription patterns | Reconnaissance |
| Large message volumes | DoS or data exfiltration |
| Connections from unexpected IPs | Unauthorized access |

## 9.9 Security Profiles

### 9.9.1 Minimal Security (Development Only)

- Unencrypted TCP
- No authentication
- No authorization

> **Warning:** NEVER use in production.

### 9.9.2 Basic Security

- TLS encryption
- Username/password authentication
- Basic ACLs

### 9.9.3 Enhanced Security

- TLS with mutual authentication
- Client certificates
- Fine-grained ACLs
- Rate limiting
- Audit logging

### 9.9.4 High Security

- TLS 1.3 only
- Certificate-based authentication with HSM
- External authorization service
- End-to-end encryption
- Intrusion detection
- Full audit trail
