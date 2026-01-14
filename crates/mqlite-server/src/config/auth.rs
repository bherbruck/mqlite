//! Authentication configuration.

use serde::Deserialize;

/// Authentication configuration.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AuthConfig {
    /// Enable authentication.
    pub enabled: bool,
    /// Allow anonymous connections when auth is enabled.
    pub allow_anonymous: bool,
    /// Static user list.
    #[serde(default)]
    pub users: Vec<UserConfig>,
}

/// User configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct UserConfig {
    /// Username.
    pub username: String,
    /// Plaintext password (use password_hash for production).
    #[serde(default)]
    pub password: Option<String>,
    /// Argon2 password hash.
    #[serde(default)]
    pub password_hash: Option<String>,
    /// ACL role reference.
    #[serde(default)]
    pub role: Option<String>,
}
