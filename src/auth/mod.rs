//! Authentication and authorization module.
//!
//! This module provides traits and implementations for:
//! - Authentication: Validating client credentials on CONNECT
//! - Authorization (ACL): Checking publish/subscribe permissions
//!
//! The design prioritizes zero-overhead when auth is disabled,
//! and minimal overhead on the hot path (publish/subscribe).

mod acl;
mod static_auth;

pub use acl::AclAuthorizer;
pub use static_auth::StaticAuthenticator;

use crate::config::Config;
use std::net::SocketAddr;

/// Result of an authentication or authorization check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum AuthResult {
    /// Access granted.
    Allow,
    /// Access denied - bad credentials.
    DenyBadCredentials,
    /// Access denied - not authorized for this action.
    DenyNotAuthorized,
    /// Access denied - server error during auth check.
    DenyServerError,
}

impl AuthResult {
    /// Returns true if access is allowed.
    #[inline]
    pub fn is_allowed(&self) -> bool {
        matches!(self, AuthResult::Allow)
    }

    /// Convert to MQTT v3.1.1 CONNACK return code.
    #[allow(dead_code)]
    pub fn to_connack_code_v3(self) -> u8 {
        match self {
            AuthResult::Allow => 0x00,                // Connection Accepted
            AuthResult::DenyBadCredentials => 0x04,   // Bad username or password
            AuthResult::DenyNotAuthorized => 0x05,    // Not authorized
            AuthResult::DenyServerError => 0x03,      // Server unavailable
        }
    }

    /// Convert to MQTT v5 reason code.
    pub fn to_reason_code_v5(self) -> u8 {
        match self {
            AuthResult::Allow => 0x00,                // Success
            AuthResult::DenyBadCredentials => 0x86,   // Bad User Name or Password
            AuthResult::DenyNotAuthorized => 0x87,    // Not authorized
            AuthResult::DenyServerError => 0x88,      // Server unavailable
        }
    }
}

/// Access type for ACL checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum Access {
    /// Publishing to a topic.
    Publish,
    /// Subscribing to a topic filter.
    Subscribe,
}

/// Context for authentication (CONNECT packet).
#[derive(Debug)]
#[allow(dead_code)]
pub struct AuthContext<'a> {
    /// Client ID from CONNECT packet.
    pub client_id: &'a str,
    /// Username (optional).
    pub username: Option<&'a str>,
    /// Password (optional).
    pub password: Option<&'a [u8]>,
    /// Client's remote address.
    pub remote_addr: SocketAddr,
}

/// Information about an authenticated client (for ACL checks).
#[derive(Debug, Clone)]
pub struct ClientInfo {
    /// Client ID.
    pub client_id: String,
    /// Username (if authenticated with one).
    pub username: Option<String>,
    /// Role assigned during authentication.
    pub role: Option<String>,
    /// Whether this is an anonymous (unauthenticated) connection.
    pub is_anonymous: bool,
}

impl ClientInfo {
    /// Create a new ClientInfo for an anonymous client.
    pub fn anonymous(client_id: String) -> Self {
        Self {
            client_id,
            username: None,
            role: None,
            is_anonymous: true,
        }
    }

    /// Create a new ClientInfo for an authenticated client.
    pub fn authenticated(client_id: String, username: Option<String>, role: Option<String>) -> Self {
        Self {
            client_id,
            username,
            role,
            is_anonymous: false,
        }
    }
}

/// Authentication trait - validates credentials on CONNECT.
///
/// Implementations should be thread-safe (`Send + Sync`) as they may be
/// called from multiple worker threads concurrently.
pub trait Authenticator: Send + Sync + 'static {
    /// Authenticate a client connection.
    ///
    /// Returns `AuthResult::Allow` and an optional role on success.
    /// The role is used for ACL lookups.
    fn authenticate(&self, ctx: &AuthContext) -> (AuthResult, Option<String>);
}

/// Authorization trait - checks publish/subscribe permissions.
///
/// ACL checks happen on the hot path, so implementations should be fast.
/// Consider caching resolved permissions per-client.
pub trait Authorizer: Send + Sync + 'static {
    /// Check if a client can publish to a topic.
    fn check_publish(&self, client: &ClientInfo, topic: &str) -> AuthResult;

    /// Check if a client can subscribe to a topic filter.
    fn check_subscribe(&self, client: &ClientInfo, filter: &str) -> AuthResult;
}

/// No-op authenticator that allows all connections.
#[derive(Debug, Clone, Default)]
pub struct AllowAllAuth;

impl Authenticator for AllowAllAuth {
    #[inline]
    fn authenticate(&self, _ctx: &AuthContext) -> (AuthResult, Option<String>) {
        (AuthResult::Allow, None)
    }
}

/// No-op authorizer that allows all operations.
#[derive(Debug, Clone, Default)]
pub struct AllowAllAcl;

impl Authorizer for AllowAllAcl {
    #[inline]
    fn check_publish(&self, _client: &ClientInfo, _topic: &str) -> AuthResult {
        AuthResult::Allow
    }

    #[inline]
    fn check_subscribe(&self, _client: &ClientInfo, _filter: &str) -> AuthResult {
        AuthResult::Allow
    }
}

/// Combined auth provider that can be passed to workers.
pub struct AuthProvider {
    pub authenticator: Box<dyn Authenticator>,
    pub authorizer: Box<dyn Authorizer>,
    /// Whether authentication is enabled.
    pub auth_enabled: bool,
    /// Whether to allow anonymous connections when auth is enabled.
    pub allow_anonymous: bool,
    /// Whether ACL is enabled.
    pub acl_enabled: bool,
}

impl AuthProvider {
    /// Create a provider with all auth disabled (allow everything).
    pub fn allow_all() -> Self {
        Self {
            authenticator: Box::new(AllowAllAuth),
            authorizer: Box::new(AllowAllAcl),
            auth_enabled: false,
            allow_anonymous: true,
            acl_enabled: false,
        }
    }

    /// Create from configuration.
    pub fn from_config(config: &Config) -> Self {
        let (authenticator, auth_enabled, allow_anonymous): (Box<dyn Authenticator>, bool, bool) =
            if config.auth.enabled {
                (
                    Box::new(StaticAuthenticator::new(&config.auth)),
                    true,
                    config.auth.allow_anonymous,
                )
            } else {
                (Box::new(AllowAllAuth), false, true)
            };

        let (authorizer, acl_enabled): (Box<dyn Authorizer>, bool) =
            if config.acl.enabled {
                (Box::new(AclAuthorizer::new(&config.acl)), true)
            } else {
                (Box::new(AllowAllAcl), false)
            };

        Self {
            authenticator,
            authorizer,
            auth_enabled,
            allow_anonymous,
            acl_enabled,
        }
    }

    /// Authenticate a client connection.
    #[inline]
    pub fn authenticate(&self, ctx: &AuthContext) -> (AuthResult, Option<String>) {
        if !self.auth_enabled {
            return (AuthResult::Allow, None);
        }

        // Check if this is an anonymous connection
        if ctx.username.is_none() && ctx.password.is_none() {
            if self.allow_anonymous {
                return (AuthResult::Allow, None);
            } else {
                return (AuthResult::DenyNotAuthorized, None);
            }
        }

        self.authenticator.authenticate(ctx)
    }

    /// Check publish permission.
    #[inline]
    pub fn check_publish(&self, client: &ClientInfo, topic: &str) -> AuthResult {
        if !self.acl_enabled {
            return AuthResult::Allow;
        }
        self.authorizer.check_publish(client, topic)
    }

    /// Check subscribe permission.
    #[inline]
    pub fn check_subscribe(&self, client: &ClientInfo, filter: &str) -> AuthResult {
        if !self.acl_enabled {
            return AuthResult::Allow;
        }
        self.authorizer.check_subscribe(client, filter)
    }
}

impl Default for AuthProvider {
    fn default() -> Self {
        Self::allow_all()
    }
}
