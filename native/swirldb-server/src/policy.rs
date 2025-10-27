// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

// Policy-based authorization system for SwirlDB
//
// This module implements:
// - TOML config parsing
// - JWT-based actor authentication
// - Path pattern matching with variable substitution
// - Rule evaluation with priority ordering
// - Audit logging

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// =============================================================================
// Configuration Structures (matches TOML format)
// =============================================================================

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SwirlDBConfig {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub rules: Vec<PolicyRule>,
    #[serde(default)]
    pub sync_targets: Vec<SyncTarget>,
    pub audit: AuditConfig,
    #[serde(default)]
    pub jwt_providers: HashMap<String, JwtProvider>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub transport: Transport,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
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
    pub priority: u32,
    pub actor: ActorPattern,
    pub action: Action,
    pub path_pattern: String,
    pub effect: Effect,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
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
pub struct SyncTarget {
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

#[derive(Debug, Clone)]
pub struct Actor {
    pub actor_type: ActorType,
    pub id: String,
    pub org_id: Option<String>,
    pub team_id: Option<String>,
    pub app_id: Option<String>,
    pub role: Option<String>,
    pub claims: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub fn from_jwt_claims(claims: HashMap<String, serde_json::Value>) -> Result<Self> {
        // Extract actor type from "type" claim
        let actor_type = match claims.get("type").and_then(|v| v.as_str()) {
            Some("user") => ActorType::User,
            Some("app") => ActorType::App,
            Some("server") => ActorType::Server,
            _ => return Err(anyhow!("Missing or invalid 'type' claim in JWT")),
        };

        // Extract ID from "sub" claim (standard JWT subject)
        let id = claims
            .get("sub")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing 'sub' claim in JWT"))?
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
// Policy Engine
// =============================================================================

pub struct PolicyEngine {
    config: Arc<RwLock<SwirlDBConfig>>,
    audit_logger: Arc<AuditLogger>,
}

impl PolicyEngine {
    pub fn new(config: SwirlDBConfig) -> Self {
        let audit_logger = Arc::new(AuditLogger::new(config.audit.clone()));

        PolicyEngine {
            config: Arc::new(RwLock::new(config)),
            audit_logger,
        }
    }

    /// Load config from TOML file
    pub async fn from_file(path: &str) -> Result<Self> {
        let content = tokio::fs::read_to_string(path).await?;
        let config: SwirlDBConfig = toml::from_str(&content)?;
        Ok(Self::new(config))
    }

    /// Evaluate if an actor can perform an action on a path
    ///
    /// Returns true if allowed, false if denied
    pub async fn evaluate(&self, actor: &Actor, action: Action, path: &str) -> bool {
        let config = self.config.read().await;

        // Sort rules by priority (lower number = higher priority)
        let mut rules: Vec<&PolicyRule> = config.rules.iter().collect();
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
            let allowed = rule.effect == Effect::Allow;

            debug!(
                "Policy match: actor={:?} action={:?} path={} rule_priority={} effect={:?}",
                actor.id, action, path, rule.priority, rule.effect
            );

            // Log to audit log
            self.audit_logger.log_decision(
                actor,
                action,
                path,
                rule.effect,
                rule.priority,
                &config.audit,
            ).await;

            return allowed;
        }

        // No matching rule - default deny
        warn!(
            "No matching rule for actor={:?} action={:?} path={} - denying",
            actor.id, action, path
        );

        self.audit_logger.log_decision(
            actor,
            action,
            path,
            Effect::Deny,
            u32::MAX,
            &config.audit,
        ).await;

        false
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

    /// Hot reload configuration without restarting server
    pub async fn reload_config(&self, new_config: SwirlDBConfig) {
        info!("Reloading policy configuration");
        let mut config = self.config.write().await;
        *config = new_config;
    }

    /// Get sync targets from config
    pub async fn get_sync_targets(&self) -> Vec<SyncTarget> {
        let config = self.config.read().await;
        config.sync_targets.clone()
    }
}

// =============================================================================
// Audit Logger
// =============================================================================

pub struct AuditLogger {
    config: AuditConfig,
}

impl AuditLogger {
    pub fn new(config: AuditConfig) -> Self {
        AuditLogger { config }
    }

    pub async fn log_decision(
        &self,
        actor: &Actor,
        action: Action,
        path: &str,
        effect: Effect,
        rule_priority: u32,
        config: &AuditConfig,
    ) {
        if !config.enabled {
            return;
        }

        // Skip logging allowed reads if not configured
        if action == Action::Read && effect == Effect::Allow && !config.log_all_reads {
            return;
        }

        // Skip logging allowed writes if not configured
        if action == Action::Write && effect == Effect::Allow && !config.log_all_writes {
            return;
        }

        // Skip logging denied actions if not configured
        if effect == Effect::Deny && !config.log_denied_actions {
            return;
        }

        let entry = AuditLogEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            actor_type: format!("{:?}", actor.actor_type),
            actor_id: actor.id.clone(),
            org_id: actor.org_id.clone(),
            action: format!("{:?}", action),
            path: path.to_string(),
            effect: format!("{:?}", effect),
            rule_priority,
        };

        // TODO: Async write to file
        // For now, just log to tracing
        match effect {
            Effect::Allow => {
                info!(target: "audit", "{}", serde_json::to_string(&entry).unwrap_or_default());
            }
            Effect::Deny => {
                warn!(target: "audit", "{}", serde_json::to_string(&entry).unwrap_or_default());
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct AuditLogEntry {
    timestamp: String,
    actor_type: String,
    actor_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    org_id: Option<String>,
    action: String,
    path: String,
    effect: String,
    rule_priority: u32,
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
}
