//! Access Control List (ACL) authorization.
//!
//! Supports topic patterns with:
//! - MQTT wildcards: + (single level), # (multi-level)
//! - Variable substitution: %c (client_id), %u (username)

use ahash::AHashMap;

use super::{AuthResult, Authorizer, ClientInfo};
use crate::config::AclConfig;

/// Compiled ACL patterns for a role.
struct CompiledRole {
    /// Patterns this role can publish to.
    publish_patterns: Vec<String>,
    /// Patterns this role can subscribe to.
    subscribe_patterns: Vec<String>,
}

/// ACL authorizer that checks permissions against configured roles.
pub struct AclAuthorizer {
    /// Map of role name -> compiled patterns.
    roles: AHashMap<String, CompiledRole>,
    /// Default publish patterns (for authenticated users without a role).
    default_publish: Vec<String>,
    /// Default subscribe patterns (for authenticated users without a role).
    default_subscribe: Vec<String>,
    /// Anonymous publish patterns (for unauthenticated connections).
    anonymous_publish: Vec<String>,
    /// Anonymous subscribe patterns (for unauthenticated connections).
    anonymous_subscribe: Vec<String>,
}

impl AclAuthorizer {
    /// Create a new ACL authorizer from config.
    pub fn new(config: &AclConfig) -> Self {
        let mut roles = AHashMap::with_capacity(config.roles.len());

        for role_config in &config.roles {
            roles.insert(
                role_config.name.clone(),
                CompiledRole {
                    publish_patterns: role_config.publish.clone(),
                    subscribe_patterns: role_config.subscribe.clone(),
                },
            );
        }

        Self {
            roles,
            default_publish: config.default.publish.clone(),
            default_subscribe: config.default.subscribe.clone(),
            anonymous_publish: config.anonymous.publish.clone(),
            anonymous_subscribe: config.anonymous.subscribe.clone(),
        }
    }

    /// Check if a topic matches any of the given patterns.
    fn matches_any(
        &self,
        topic: &str,
        patterns: &[String],
        client_id: &str,
        username: Option<&str>,
    ) -> bool {
        for pattern in patterns {
            // Substitute variables
            let expanded = substitute_vars(pattern, client_id, username);
            if topic_matches_pattern(topic, &expanded) {
                return true;
            }
        }
        false
    }

    /// Get patterns for a client based on their role and anonymous status.
    fn get_patterns<'a>(
        &'a self,
        client: &ClientInfo,
        is_publish: bool,
    ) -> &'a [String] {
        // Anonymous users get anonymous permissions
        if client.is_anonymous {
            return if is_publish {
                &self.anonymous_publish
            } else {
                &self.anonymous_subscribe
            };
        }

        // Check for explicit role
        if let Some(ref role_name) = client.role {
            if let Some(compiled) = self.roles.get(role_name) {
                return if is_publish {
                    &compiled.publish_patterns
                } else {
                    &compiled.subscribe_patterns
                };
            }
        }
        // Fall back to defaults
        if is_publish {
            &self.default_publish
        } else {
            &self.default_subscribe
        }
    }
}

impl Authorizer for AclAuthorizer {
    fn check_publish(&self, client: &ClientInfo, topic: &str) -> AuthResult {
        let patterns = self.get_patterns(client, true);

        if self.matches_any(
            topic,
            patterns,
            &client.client_id,
            client.username.as_deref(),
        ) {
            AuthResult::Allow
        } else {
            AuthResult::DenyNotAuthorized
        }
    }

    fn check_subscribe(&self, client: &ClientInfo, filter: &str) -> AuthResult {
        let patterns = self.get_patterns(client, false);

        if self.matches_any(
            filter,
            patterns,
            &client.client_id,
            client.username.as_deref(),
        ) {
            AuthResult::Allow
        } else {
            AuthResult::DenyNotAuthorized
        }
    }
}

/// Substitute %c and %u variables in a pattern.
fn substitute_vars(pattern: &str, client_id: &str, username: Option<&str>) -> String {
    let mut result = pattern.replace("%c", client_id);
    if let Some(u) = username {
        result = result.replace("%u", u);
    } else {
        // If no username, replace %u with empty string
        result = result.replace("%u", "");
    }
    result
}

/// Check if a topic matches an ACL pattern.
///
/// This is similar to MQTT topic matching but operates on:
/// - For publish ACL: topic is a concrete topic, pattern may have wildcards
/// - For subscribe ACL: topic is a filter (may have wildcards), pattern may have wildcards
///
/// Wildcards in pattern:
/// - `+` matches exactly one topic level
/// - `#` matches zero or more levels (must be last)
fn topic_matches_pattern(topic: &str, pattern: &str) -> bool {
    // Split into levels
    let topic_levels: Vec<&str> = topic.split('/').collect();
    let pattern_levels: Vec<&str> = pattern.split('/').collect();

    let mut ti = 0;
    let mut pi = 0;

    while pi < pattern_levels.len() {
        let pattern_level = pattern_levels[pi];

        if pattern_level == "#" {
            // # matches everything remaining
            return true;
        }

        if ti >= topic_levels.len() {
            // Topic is shorter than pattern (and pattern isn't #)
            return false;
        }

        let topic_level = topic_levels[ti];

        if pattern_level == "+" {
            // + matches any single level
            ti += 1;
            pi += 1;
        } else if pattern_level == topic_level {
            // Exact match
            ti += 1;
            pi += 1;
        } else {
            // No match
            return false;
        }
    }

    // Both must be exhausted for a match
    ti == topic_levels.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DefaultPermissions, RoleConfig};

    #[test]
    fn test_exact_match() {
        assert!(topic_matches_pattern("sensors/temp", "sensors/temp"));
        assert!(!topic_matches_pattern("sensors/temp", "sensors/humidity"));
    }

    #[test]
    fn test_plus_wildcard() {
        assert!(topic_matches_pattern("sensors/temp", "sensors/+"));
        assert!(topic_matches_pattern("sensors/humidity", "sensors/+"));
        assert!(!topic_matches_pattern("sensors/room1/temp", "sensors/+"));
        assert!(topic_matches_pattern("sensors/room1/temp", "sensors/+/temp"));
        assert!(topic_matches_pattern("sensors/room1/temp", "+/+/+"));
    }

    #[test]
    fn test_hash_wildcard() {
        assert!(topic_matches_pattern("sensors", "sensors/#"));
        assert!(topic_matches_pattern("sensors/temp", "sensors/#"));
        assert!(topic_matches_pattern("sensors/room1/temp", "sensors/#"));
        assert!(topic_matches_pattern("a/b/c/d/e", "#"));
        assert!(!topic_matches_pattern("other/topic", "sensors/#"));
    }

    #[test]
    fn test_variable_substitution() {
        assert_eq!(
            substitute_vars("sensors/%c/data", "client123", Some("user1")),
            "sensors/client123/data"
        );
        assert_eq!(
            substitute_vars("users/%u/inbox", "client123", Some("user1")),
            "users/user1/inbox"
        );
        assert_eq!(
            substitute_vars("%c/%u/data", "client123", Some("user1")),
            "client123/user1/data"
        );
        assert_eq!(
            substitute_vars("users/%u/inbox", "client123", None),
            "users//inbox"
        );
    }

    #[test]
    fn test_acl_authorizer() {
        let config = AclConfig {
            enabled: true,
            roles: vec![
                RoleConfig {
                    name: "device".to_string(),
                    publish: vec!["sensors/%c/#".to_string()],
                    subscribe: vec!["commands/%c/#".to_string()],
                },
                RoleConfig {
                    name: "admin".to_string(),
                    publish: vec!["#".to_string()],
                    subscribe: vec!["#".to_string()],
                },
            ],
            default: DefaultPermissions {
                publish: vec![],
                subscribe: vec!["$SYS/#".to_string()],
            },
            anonymous: DefaultPermissions {
                publish: vec!["public/#".to_string()],
                subscribe: vec!["public/#".to_string(), "$SYS/#".to_string()],
            },
        };

        let acl = AclAuthorizer::new(&config);

        // Device role (authenticated with role)
        let device_client = ClientInfo {
            client_id: "device001".to_string(),
            username: None,
            role: Some("device".to_string()),
            is_anonymous: false,
        };

        assert_eq!(
            acl.check_publish(&device_client, "sensors/device001/temp"),
            AuthResult::Allow
        );
        assert_eq!(
            acl.check_publish(&device_client, "sensors/other/temp"),
            AuthResult::DenyNotAuthorized
        );
        assert_eq!(
            acl.check_subscribe(&device_client, "commands/device001/#"),
            AuthResult::Allow
        );

        // Admin role (authenticated with role)
        let admin_client = ClientInfo {
            client_id: "admin1".to_string(),
            username: Some("admin".to_string()),
            role: Some("admin".to_string()),
            is_anonymous: false,
        };

        assert_eq!(
            acl.check_publish(&admin_client, "any/topic/here"),
            AuthResult::Allow
        );
        assert_eq!(
            acl.check_subscribe(&admin_client, "any/filter/#"),
            AuthResult::Allow
        );

        // Authenticated without role (uses defaults)
        let default_client = ClientInfo {
            client_id: "user1".to_string(),
            username: Some("someuser".to_string()),
            role: None,
            is_anonymous: false,
        };

        assert_eq!(
            acl.check_publish(&default_client, "any/topic"),
            AuthResult::DenyNotAuthorized
        );
        assert_eq!(
            acl.check_subscribe(&default_client, "$SYS/broker/clients"),
            AuthResult::Allow
        );

        // Anonymous user (uses anonymous permissions)
        let anon_client = ClientInfo::anonymous("anon1".to_string());

        assert_eq!(
            acl.check_publish(&anon_client, "public/data"),
            AuthResult::Allow
        );
        assert_eq!(
            acl.check_publish(&anon_client, "private/data"),
            AuthResult::DenyNotAuthorized
        );
        assert_eq!(
            acl.check_subscribe(&anon_client, "public/#"),
            AuthResult::Allow
        );
        assert_eq!(
            acl.check_subscribe(&anon_client, "$SYS/broker/clients"),
            AuthResult::Allow
        );
        assert_eq!(
            acl.check_subscribe(&anon_client, "private/#"),
            AuthResult::DenyNotAuthorized
        );
    }
}
