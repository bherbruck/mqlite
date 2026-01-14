//! ACL (Access Control List) configuration.

use serde::Deserialize;

/// ACL (Access Control List) configuration.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AclConfig {
    /// Enable ACL.
    pub enabled: bool,
    /// Role definitions.
    #[serde(default)]
    pub roles: Vec<RoleConfig>,
    /// Default permissions for authenticated users without an explicit role.
    #[serde(default)]
    pub default: DefaultPermissions,
    /// Permissions for anonymous (unauthenticated) connections.
    #[serde(default)]
    pub anonymous: DefaultPermissions,
}

/// Role configuration with publish/subscribe patterns.
#[derive(Debug, Clone, Deserialize)]
pub struct RoleConfig {
    /// Role name.
    pub name: String,
    /// Topics this role can publish to.
    /// Supports %c (client_id) and %u (username) substitution.
    #[serde(default)]
    pub publish: Vec<String>,
    /// Topic filters this role can subscribe to.
    /// Supports %c (client_id) and %u (username) substitution.
    #[serde(default)]
    pub subscribe: Vec<String>,
}

/// Default permissions for users without an explicit role.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DefaultPermissions {
    /// Topics that can be published to.
    pub publish: Vec<String>,
    /// Topic filters that can be subscribed to.
    pub subscribe: Vec<String>,
}
