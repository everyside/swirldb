// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

// Policy-based authorization for SwirlDB
//
// This is platform-agnostic and works in both browser and server.
// Contains:
// - Policy data structures
// - Path pattern matching with variable substitution
// - Policy evaluation engine (pure logic, no I/O)
// - Actor model

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// =============================================================================
// Configuration Structures
// =============================================================================

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SwirlDBConfig {
    pub policies: PoliciesConfig,
    #[serde(default)]
    pub adapters: AdaptersConfig,
    #[serde(default)]
    pub remotes: Vec<Remote>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PoliciesConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthConfig>,
    pub rules: Vec<PolicyRule>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audit: Option<AuditConfig>,
    #[serde(default)]
    pub jwt_providers: HashMap<String, JwtProvider>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AdaptersConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageAdapterConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption: Option<EncryptionAdapterConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync: Option<SyncAdapterConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageAdapterConfig {
    #[serde(rename = "type")]
    pub adapter_type: String, // "memory", "localStorage", "indexedDB", "redb", "sqlite"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>, // For file-based storage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_prefix: Option<String>, // For browser storage key prefixing (e.g., "myapp:")
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EncryptionAdapterConfig {
    #[serde(rename = "type")]
    pub adapter_type: String, // "unencrypted", "aes-gcm", "field-level"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>, // Base64-encoded key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>, // For password-derived keys
    #[serde(skip_serializing_if = "Option::is_none")]
    pub salt: Option<String>, // For password-derived keys
    #[serde(default)]
    pub field_patterns: Vec<String>, // For field-level encryption
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SyncAdapterConfig {
    #[serde(rename = "type")]
    pub adapter_type: String, // "websocket", "http", "webrtc"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    pub transport: Transport,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Transport {
    WebSocket,
    Http,
    Both,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthConfig {
    pub jwt_secret: String,
    pub jwt_algorithm: String,
    pub allow_anonymous: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PolicyRule {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _description: Option<String>,
    pub priority: u32,
    pub actor: ActorPattern,
    pub action: Action,
    pub path_pattern: String,
    pub effect: Effect,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum ActorPattern {
    User {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<String>,
    },
    App {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },
    Server {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },
    Anonymous,
    Any,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
    Subscribe,
    Replicate,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Remote {
    pub name: String,
    pub endpoint: String,
    pub transport: Transport,
    pub jwt_provider: String,
    pub path_patterns: Vec<String>,
    pub mode: SyncMode,
    pub auto_connect: bool,
    pub retry_strategy: RetryStrategy,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum SyncMode {
    Push,
    Pull,
    Bidirectional,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetryStrategy {
    pub max_retries: u32,
    pub backoff_ms: u64,
    pub max_backoff_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum JwtProvider {
    #[serde(rename = "static")]
    Static { token: String },
    #[serde(rename = "endpoint")]
    Endpoint {
        url: String,
        refresh_interval_seconds: u64,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuditConfig {
    pub enabled: bool,
    pub log_path: String,
    pub log_all_reads: bool,
    pub log_all_writes: bool,
    pub log_denied_actions: bool,
    #[serde(default = "default_log_format")]
    pub log_format: String,
}

fn default_log_format() -> String {
    "json".to_string()
}

// =============================================================================
// Runtime Actor Model
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Actor {
    pub actor_type: ActorType,
    pub id: String,
    pub org_id: Option<String>,
    pub team_id: Option<String>,
    pub app_id: Option<String>,
    pub role: Option<String>,
    pub claims: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActorType {
    User,
    App,
    Server,
    Anonymous,
}

impl Actor {
    /// Create an anonymous actor (no JWT)
    pub fn anonymous() -> Self {
        Actor {
            actor_type: ActorType::Anonymous,
            id: "anonymous".to_string(),
            org_id: None,
            team_id: None,
            app_id: None,
            role: None,
            claims: HashMap::new(),
        }
    }

    /// Create actor from JWT claims
    pub fn from_jwt_claims(claims: HashMap<String, serde_json::Value>) -> Result<Self, String> {
        // Extract actor type from "type" claim
        let actor_type = match claims.get("type").and_then(|v| v.as_str()) {
            Some("user") => ActorType::User,
            Some("app") => ActorType::App,
            Some("server") => ActorType::Server,
            _ => return Err("Missing or invalid 'type' claim in JWT".to_string()),
        };

        // Extract ID from "sub" claim (standard JWT subject)
        let id = claims
            .get("sub")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Missing 'sub' claim in JWT".to_string())?
            .to_string();

        // Extract optional fields
        let org_id = claims.get("org_id").and_then(|v| v.as_str()).map(String::from);
        let team_id = claims.get("team_id").and_then(|v| v.as_str()).map(String::from);
        let app_id = claims.get("app_id").and_then(|v| v.as_str()).map(String::from);
        let role = claims.get("role").and_then(|v| v.as_str()).map(String::from);

        Ok(Actor {
            actor_type,
            id,
            org_id,
            team_id,
            app_id,
            role,
            claims,
        })
    }

    /// Get a claim value by key
    pub fn get_claim(&self, key: &str) -> Option<&serde_json::Value> {
        self.claims.get(key)
    }
}

// =============================================================================
// Path Pattern Matching
// =============================================================================

pub struct PathPatternMatcher;

impl PathPatternMatcher {
    /// Check if a path matches a pattern with variable substitution
    ///
    /// Examples:
    /// - Pattern: "/user/{actor.id}/prefs/**"
    ///   Path: "/user/alice/prefs/theme"
    ///   Actor: { id: "alice" }
    ///   Result: true
    ///
    /// - Pattern: "/org/{actor.org_id}/teams/*/members"
    ///   Path: "/org/acme-corp/teams/engineering/members"
    ///   Actor: { org_id: "acme-corp" }
    ///   Result: true
    pub fn matches(pattern: &str, path: &str, actor: &Actor) -> bool {
        // First, substitute variables in the pattern
        let substituted = Self::substitute_variables(pattern, actor);

        // Now match the substituted pattern against the path
        Self::match_pattern(&substituted, path)
    }

    /// Substitute {actor.field} variables in a pattern
    fn substitute_variables(pattern: &str, actor: &Actor) -> String {
        let mut result = pattern.to_string();

        // Replace {actor.id}
        result = result.replace("{actor.id}", &actor.id);

        // Replace {actor.org_id}
        if let Some(org_id) = &actor.org_id {
            result = result.replace("{actor.org_id}", org_id);
        }

        // Replace {actor.team_id}
        if let Some(team_id) = &actor.team_id {
            result = result.replace("{actor.team_id}", team_id);
        }

        // Replace {actor.app_id}
        if let Some(app_id) = &actor.app_id {
            result = result.replace("{actor.app_id}", app_id);
        }

        // Replace {actor.role}
        if let Some(role) = &actor.role {
            result = result.replace("{actor.role}", role);
        }

        result
    }

    /// Match a pattern (with wildcards) against a path
    ///
    /// Supports:
    /// - `*` matches single path segment
    /// - `**` matches multiple path segments
    /// - Exact matches
    fn match_pattern(pattern: &str, path: &str) -> bool {
        let pattern_segments: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();
        let path_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        Self::match_segments(&pattern_segments, &path_segments)
    }

    fn match_segments(pattern: &[&str], path: &[&str]) -> bool {
        if pattern.is_empty() && path.is_empty() {
            return true;
        }

        if pattern.is_empty() || path.is_empty() {
            // Special case: "**" at end matches empty remainder
            return pattern.len() == 1 && pattern[0] == "**";
        }

        match pattern[0] {
            "**" => {
                // "**" matches zero or more segments
                // Try matching with 0 segments consumed, then 1, then 2, etc.
                for i in 0..=path.len() {
                    if Self::match_segments(&pattern[1..], &path[i..]) {
                        return true;
                    }
                }
                false
            }
            "*" => {
                // "*" matches exactly one segment
                Self::match_segments(&pattern[1..], &path[1..])
            }
            segment => {
                // Exact match required
                if segment == path[0] {
                    Self::match_segments(&pattern[1..], &path[1..])
                } else {
                    false
                }
            }
        }
    }
}

// =============================================================================
// Policy Engine (Pure Logic)
// =============================================================================

#[derive(Debug, Clone)]
pub struct PolicyEngine {
    config: SwirlDBConfig,
}

impl PolicyEngine {
    pub fn new(config: SwirlDBConfig) -> Self {
        PolicyEngine { config }
    }

    /// Parse config from JSON string
    pub fn from_json(json: &str) -> Result<Self, String> {
        let config: SwirlDBConfig = serde_json::from_str(json)
            .map_err(|e| format!("Failed to parse config JSON: {}", e))?;
        Ok(Self::new(config))
    }

    /// Evaluate if an actor can perform an action on a path
    ///
    /// Returns Ok(true) if allowed, Ok(false) if denied
    pub fn evaluate(&self, actor: &Actor, action: Action, path: &str) -> PolicyDecision {
        // Sort rules by priority (lower number = higher priority)
        let mut rules: Vec<&PolicyRule> = self.config.policies.rules.iter().collect();
        rules.sort_by_key(|r| r.priority);

        // Find first matching rule
        for rule in rules {
            if rule.action != action {
                continue;
            }

            // Check if actor matches rule
            if !Self::actor_matches(&rule.actor, actor) {
                continue;
            }

            // Check if path matches pattern
            if !PathPatternMatcher::matches(&rule.path_pattern, path, actor) {
                continue;
            }

            // Found matching rule!
            return PolicyDecision {
                effect: rule.effect,
                rule_priority: rule.priority,
                matched_pattern: rule.path_pattern.clone(),
            };
        }

        // No matching rule - default deny
        PolicyDecision {
            effect: Effect::Deny,
            rule_priority: u32::MAX,
            matched_pattern: "default-deny".to_string(),
        }
    }

    /// Check if an actor matches an actor pattern
    fn actor_matches(pattern: &ActorPattern, actor: &Actor) -> bool {
        match pattern {
            ActorPattern::Any => true,
            ActorPattern::Anonymous => actor.actor_type == ActorType::Anonymous,
            ActorPattern::User { id, role } => {
                if actor.actor_type != ActorType::User {
                    return false;
                }
                if let Some(pattern_id) = id {
                    if pattern_id != &actor.id {
                        return false;
                    }
                }
                if let Some(pattern_role) = role {
                    if actor.role.as_ref() != Some(pattern_role) {
                        return false;
                    }
                }
                true
            }
            ActorPattern::App { id } => {
                if actor.actor_type != ActorType::App {
                    return false;
                }
                if let Some(pattern_id) = id {
                    if pattern_id != &actor.id {
                        return false;
                    }
                }
                true
            }
            ActorPattern::Server { id } => {
                if actor.actor_type != ActorType::Server {
                    return false;
                }
                if let Some(pattern_id) = id {
                    if pattern_id != &actor.id {
                        return false;
                    }
                }
                true
            }
        }
    }

    /// Get the underlying config (useful for accessing sync_targets, etc.)
    pub fn config(&self) -> &SwirlDBConfig {
        &self.config
    }

    /// Update configuration (for mutable rules)
    pub fn update_config(&mut self, new_config: SwirlDBConfig) {
        self.config = new_config;
    }
}

/// Result of a policy evaluation
#[derive(Debug, Clone)]
pub struct PolicyDecision {
    pub effect: Effect,
    pub rule_priority: u32,
    pub matched_pattern: String,
}

impl PolicyDecision {
    pub fn is_allowed(&self) -> bool {
        self.effect == Effect::Allow
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_pattern_exact_match() {
        let actor = Actor {
            actor_type: ActorType::User,
            id: "alice".to_string(),
            org_id: None,
            team_id: None,
            app_id: None,
            role: None,
            claims: HashMap::new(),
        };

        assert!(PathPatternMatcher::matches("/config/version", "/config/version", &actor));
        assert!(!PathPatternMatcher::matches("/config/version", "/config/other", &actor));
    }

    #[test]
    fn test_path_pattern_single_wildcard() {
        let actor = Actor::anonymous();

        assert!(PathPatternMatcher::matches("/org/*/members", "/org/acme/members", &actor));
        assert!(!PathPatternMatcher::matches("/org/*/members", "/org/acme/teams/members", &actor));
    }

    #[test]
    fn test_path_pattern_multi_wildcard() {
        let actor = Actor::anonymous();

        assert!(PathPatternMatcher::matches("/user/**", "/user/alice/prefs/theme", &actor));
        assert!(PathPatternMatcher::matches("/user/**", "/user/alice", &actor));
        assert!(!PathPatternMatcher::matches("/user/**", "/org/acme", &actor));
    }

    #[test]
    fn test_path_pattern_variable_substitution() {
        let actor = Actor {
            actor_type: ActorType::User,
            id: "alice".to_string(),
            org_id: Some("acme-corp".to_string()),
            team_id: None,
            app_id: None,
            role: None,
            claims: HashMap::new(),
        };

        assert!(PathPatternMatcher::matches(
            "/user/{actor.id}/prefs/**",
            "/user/alice/prefs/theme",
            &actor
        ));

        assert!(!PathPatternMatcher::matches(
            "/user/{actor.id}/prefs/**",
            "/user/bob/prefs/theme",
            &actor
        ));

        assert!(PathPatternMatcher::matches(
            "/org/{actor.org_id}/shared/**",
            "/org/acme-corp/shared/docs",
            &actor
        ));
    }

    #[test]
    fn test_policy_evaluation() {
        let config_json = r#"{
            "policies": {
                "rules": [
                    {
                        "priority": 10,
                        "actor": { "type": "User" },
                        "action": "Read",
                        "path_pattern": "/user/{actor.id}/**",
                        "effect": "Allow"
                    },
                    {
                        "priority": 100,
                        "actor": { "type": "Any" },
                        "action": "Read",
                        "path_pattern": "/**",
                        "effect": "Deny"
                    }
                ]
            }
        }"#;

        let engine = PolicyEngine::from_json(config_json).unwrap();

        let alice = Actor {
            actor_type: ActorType::User,
            id: "alice".to_string(),
            org_id: None,
            team_id: None,
            app_id: None,
            role: None,
            claims: HashMap::new(),
        };

        // Alice can read her own data
        let decision = engine.evaluate(&alice, Action::Read, "/user/alice/prefs");
        assert!(decision.is_allowed());
        assert_eq!(decision.rule_priority, 10);

        // Alice cannot read Bob's data
        let decision = engine.evaluate(&alice, Action::Read, "/user/bob/prefs");
        assert!(!decision.is_allowed());
        assert_eq!(decision.rule_priority, 100);
    }
}
