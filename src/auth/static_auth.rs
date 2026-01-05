//! Static user authentication from configuration.
//!
//! Supports plaintext passwords (for development) and argon2 hashes (for production).
//!
//! Generate password hashes with:
//! ```bash
//! # Using argon2 CLI
//! echo -n "password" | argon2 $(openssl rand -base64 16) -id -e
//!
//! # Or using the password-hash crate's PHC format
//! ```

use ahash::AHashMap;
use argon2::{Argon2, PasswordHash, PasswordVerifier};

use super::{AuthContext, AuthResult, Authenticator};
use crate::config::AuthConfig;

/// A user entry with pre-processed credentials.
struct User {
    /// Plaintext password (if configured). Use only for development.
    password: Option<String>,
    /// Argon2 password hash in PHC string format (recommended for production).
    /// Example: $argon2id$v=19$m=65536,t=3,p=4$c29tZXNhbHQ$...
    password_hash: Option<String>,
    /// Role for ACL lookups.
    role: Option<String>,
}

/// Static authenticator that validates against a configured user list.
pub struct StaticAuthenticator {
    /// Map of username -> user credentials.
    users: AHashMap<String, User>,
}

impl StaticAuthenticator {
    /// Create a new static authenticator from config.
    pub fn new(config: &AuthConfig) -> Self {
        let mut users = AHashMap::with_capacity(config.users.len());

        for user_config in &config.users {
            users.insert(
                user_config.username.clone(),
                User {
                    password: user_config.password.clone(),
                    password_hash: user_config.password_hash.clone(),
                    role: user_config.role.clone(),
                },
            );
        }

        Self { users }
    }

    /// Verify a password against stored credentials.
    fn verify_password(&self, user: &User, password: &[u8]) -> bool {
        // Try argon2 hash first (production)
        if let Some(ref hash_str) = user.password_hash {
            return self.verify_argon2(hash_str, password);
        }

        // Fall back to plaintext (development/testing only)
        if let Some(ref stored) = user.password {
            return password == stored.as_bytes();
        }

        false
    }

    /// Verify password against argon2 hash in PHC string format.
    fn verify_argon2(&self, hash_str: &str, password: &[u8]) -> bool {
        let Ok(parsed_hash) = PasswordHash::new(hash_str) else {
            log::warn!("Invalid argon2 hash format in config");
            return false;
        };

        Argon2::default()
            .verify_password(password, &parsed_hash)
            .is_ok()
    }
}

impl Authenticator for StaticAuthenticator {
    fn authenticate(&self, ctx: &AuthContext) -> (AuthResult, Option<String>) {
        let username = match ctx.username {
            Some(u) => u,
            None => return (AuthResult::DenyBadCredentials, None),
        };

        let password = match ctx.password {
            Some(p) => p,
            None => return (AuthResult::DenyBadCredentials, None),
        };

        // Look up user
        let user = match self.users.get(username) {
            Some(u) => u,
            None => return (AuthResult::DenyBadCredentials, None),
        };

        // Verify password
        if self.verify_password(user, password) {
            (AuthResult::Allow, user.role.clone())
        } else {
            (AuthResult::DenyBadCredentials, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UserConfig;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_ctx<'a>(
        client_id: &'a str,
        username: Option<&'a str>,
        password: Option<&'a [u8]>,
    ) -> AuthContext<'a> {
        AuthContext {
            client_id,
            username,
            password,
            remote_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 12345),
        }
    }

    #[test]
    fn test_valid_credentials() {
        let config = AuthConfig {
            enabled: true,
            allow_anonymous: false,
            users: vec![UserConfig {
                username: "admin".to_string(),
                password: Some("secret".to_string()),
                password_hash: None,
                role: Some("admin".to_string()),
            }],
        };

        let auth = StaticAuthenticator::new(&config);
        let ctx = make_ctx("client1", Some("admin"), Some(b"secret"));
        let (result, role) = auth.authenticate(&ctx);

        assert_eq!(result, AuthResult::Allow);
        assert_eq!(role, Some("admin".to_string()));
    }

    #[test]
    fn test_invalid_password() {
        let config = AuthConfig {
            enabled: true,
            allow_anonymous: false,
            users: vec![UserConfig {
                username: "admin".to_string(),
                password: Some("secret".to_string()),
                password_hash: None,
                role: None,
            }],
        };

        let auth = StaticAuthenticator::new(&config);
        let ctx = make_ctx("client1", Some("admin"), Some(b"wrong"));
        let (result, role) = auth.authenticate(&ctx);

        assert_eq!(result, AuthResult::DenyBadCredentials);
        assert_eq!(role, None);
    }

    #[test]
    fn test_unknown_user() {
        let config = AuthConfig {
            enabled: true,
            allow_anonymous: false,
            users: vec![],
        };

        let auth = StaticAuthenticator::new(&config);
        let ctx = make_ctx("client1", Some("unknown"), Some(b"password"));
        let (result, _) = auth.authenticate(&ctx);

        assert_eq!(result, AuthResult::DenyBadCredentials);
    }

    #[test]
    fn test_missing_username() {
        let config = AuthConfig {
            enabled: true,
            allow_anonymous: false,
            users: vec![],
        };

        let auth = StaticAuthenticator::new(&config);
        let ctx = make_ctx("client1", None, Some(b"password"));
        let (result, _) = auth.authenticate(&ctx);

        assert_eq!(result, AuthResult::DenyBadCredentials);
    }

    #[test]
    fn test_argon2_hash_valid() {
        use argon2::{password_hash::SaltString, Argon2, PasswordHasher};
        use rand_core::OsRng;

        // Generate a hash for "secret123"
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let hash = argon2
            .hash_password(b"secret123", &salt)
            .unwrap()
            .to_string();

        let config = AuthConfig {
            enabled: true,
            allow_anonymous: false,
            users: vec![UserConfig {
                username: "hashuser".to_string(),
                password: None,
                password_hash: Some(hash),
                role: Some("admin".to_string()),
            }],
        };

        let auth = StaticAuthenticator::new(&config);

        // Correct password
        let ctx = make_ctx("client1", Some("hashuser"), Some(b"secret123"));
        let (result, role) = auth.authenticate(&ctx);
        assert_eq!(result, AuthResult::Allow);
        assert_eq!(role, Some("admin".to_string()));

        // Wrong password
        let ctx = make_ctx("client1", Some("hashuser"), Some(b"wrongpass"));
        let (result, _) = auth.authenticate(&ctx);
        assert_eq!(result, AuthResult::DenyBadCredentials);
    }

    #[test]
    fn test_argon2_hash_priority() {
        use argon2::{password_hash::SaltString, Argon2, PasswordHasher};
        use rand_core::OsRng;

        // Generate a hash for "hashpass"
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let hash = argon2
            .hash_password(b"hashpass", &salt)
            .unwrap()
            .to_string();

        // User has both plaintext AND hash - hash should take priority
        let config = AuthConfig {
            enabled: true,
            allow_anonymous: false,
            users: vec![UserConfig {
                username: "bothuser".to_string(),
                password: Some("plainpass".to_string()),
                password_hash: Some(hash),
                role: None,
            }],
        };

        let auth = StaticAuthenticator::new(&config);

        // Hash password works
        let ctx = make_ctx("client1", Some("bothuser"), Some(b"hashpass"));
        let (result, _) = auth.authenticate(&ctx);
        assert_eq!(result, AuthResult::Allow);

        // Plaintext password does NOT work (hash takes priority)
        let ctx = make_ctx("client1", Some("bothuser"), Some(b"plainpass"));
        let (result, _) = auth.authenticate(&ctx);
        assert_eq!(result, AuthResult::DenyBadCredentials);
    }
}
