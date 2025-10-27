// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

/// Subscription-based sync protocol for SwirlDB
///
/// This module implements efficient delta syncing using Automerge's built-in
/// change tracking with path-based subscription filtering and policy integration.
///
/// Key concepts:
/// - **Subscriptions**: Clients subscribe to path patterns (e.g., `/user/{actor.id}/**`)
/// - **Policy-aware**: Subscribe action validated by PolicyEngine
/// - **Change filtering**: Only send changes affecting subscribed paths
/// - **Platform-agnostic**: Lives in core, usable in browser and server

use serde::{Serialize, Deserialize};
use crate::policy::{PolicyEngine, Actor, Action};
use std::collections::HashMap;
use anyhow::{Result, anyhow};

/// Sync message types for the subscription-based protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SyncMessage {
    /// Client → Server: Initial connection with subscriptions
    Connect {
        client_id: String,
        /// Path patterns the client wants to subscribe to
        subscriptions: Vec<String>,
        /// Current client heads (for incremental sync)
        heads: Vec<u8>,
    },

    /// Server → Client: Response with initial sync data
    Sync {
        /// Changes filtered by subscriptions
        changes: Vec<u8>,
        /// New heads after applying these changes
        heads: Vec<u8>,
    },

    /// Client → Server: Update subscriptions
    Subscribe {
        /// Add these path patterns to subscriptions
        add: Vec<String>,
        /// Remove these path patterns from subscriptions
        remove: Vec<String>,
    },

    /// Server → Client: Subscription acknowledgement
    SubscribeAck {
        /// Successfully added subscriptions
        added: Vec<String>,
        /// Subscriptions denied by policy
        denied: Vec<String>,
    },

    /// Client → Server: Push local changes
    Push {
        client_id: String,
        changes: Vec<u8>,
        heads: Vec<u8>,
    },

    /// Server → Client: Broadcast changes from other clients
    /// Only includes changes affecting client's subscriptions
    Broadcast {
        from_client_id: String,
        changes: Vec<u8>,
        heads: Vec<u8>,
        /// Paths affected by these changes (for client filtering)
        affected_paths: Vec<String>,
    },

    /// Bidirectional: Heartbeat to keep connection alive
    Ping,
    Pong,

    /// Error messages
    Error {
        message: String,
    },
}

/// Client subscription state (managed by server)
#[derive(Debug, Clone)]
pub struct SubscriptionSet {
    /// Path patterns this client is subscribed to
    patterns: Vec<String>,
    /// The actor making these subscriptions (for policy validation)
    actor: Actor,
}

impl SubscriptionSet {
    pub fn new(actor: Actor) -> Self {
        Self {
            patterns: Vec::new(),
            actor,
        }
    }

    /// Check if this subscription set matches a given path
    pub fn matches_path(&self, path: &str) -> bool {
        use crate::policy::PathPatternMatcher;

        self.patterns.iter().any(|pattern| {
            PathPatternMatcher::matches(pattern, path, &self.actor)
        })
    }

    /// Get all subscription patterns
    pub fn patterns(&self) -> &[String] {
        &self.patterns
    }

    /// Add subscription patterns (with policy validation)
    pub fn add_subscriptions(
        &mut self,
        patterns: Vec<String>,
        policy: Option<&PolicyEngine>,
    ) -> (Vec<String>, Vec<String>) {
        let mut added = Vec::new();
        let mut denied = Vec::new();

        for pattern in patterns {
            // Check policy if available
            if let Some(engine) = policy {
                let decision = engine.evaluate(&self.actor, Action::Subscribe, &pattern);
                if !decision.is_allowed() {
                    denied.push(pattern);
                    continue;
                }
            }

            // Add to subscriptions if not already present
            if !self.patterns.contains(&pattern) {
                self.patterns.push(pattern.clone());
                added.push(pattern);
            }
        }

        (added, denied)
    }

    /// Remove subscription patterns
    pub fn remove_subscriptions(&mut self, patterns: Vec<String>) {
        self.patterns.retain(|p| !patterns.contains(p));
    }
}

/// Subscription manager (lives in server state)
#[derive(Debug)]
pub struct SubscriptionManager {
    /// Active subscriptions by client_id
    subscriptions: HashMap<String, SubscriptionSet>,
    /// Policy engine for validating Subscribe actions
    policy: Option<PolicyEngine>,
}

impl SubscriptionManager {
    pub fn new(policy: Option<PolicyEngine>) -> Self {
        Self {
            subscriptions: HashMap::new(),
            policy,
        }
    }

    /// Add a client with initial subscriptions
    pub fn add_client(
        &mut self,
        client_id: String,
        actor: Actor,
        patterns: Vec<String>,
    ) -> (Vec<String>, Vec<String>) {
        let mut sub_set = SubscriptionSet::new(actor);
        let (added, denied) = sub_set.add_subscriptions(patterns, self.policy.as_ref());
        self.subscriptions.insert(client_id, sub_set);
        (added, denied)
    }

    /// Update client subscriptions
    pub fn update_subscriptions(
        &mut self,
        client_id: &str,
        add: Vec<String>,
        remove: Vec<String>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let sub_set = self.subscriptions.get_mut(client_id)
            .ok_or_else(|| anyhow!("Client not found: {}", client_id))?;

        sub_set.remove_subscriptions(remove);
        let (added, denied) = sub_set.add_subscriptions(add, self.policy.as_ref());

        Ok((added, denied))
    }

    /// Remove a client
    pub fn remove_client(&mut self, client_id: &str) {
        self.subscriptions.remove(client_id);
    }

    /// Get clients subscribed to a specific path
    pub fn get_subscribers_for_path(&self, path: &str) -> Vec<String> {
        self.subscriptions
            .iter()
            .filter(|(_, sub_set)| sub_set.matches_path(path))
            .map(|(client_id, _)| client_id.clone())
            .collect()
    }

    /// Get all clients subscribed to any of the given paths
    pub fn get_subscribers_for_paths(&self, paths: &[String]) -> Vec<String> {
        self.subscriptions
            .iter()
            .filter(|(_, sub_set)| {
                paths.iter().any(|path| sub_set.matches_path(path))
            })
            .map(|(client_id, _)| client_id.clone())
            .collect()
    }

    /// Get a client's subscriptions
    pub fn get_client_subscriptions(&self, client_id: &str) -> Option<&SubscriptionSet> {
        self.subscriptions.get(client_id)
    }
}

/// Sync configuration
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// WebSocket URL for upstream server
    pub upstream_url: String,

    /// Client identifier (should be unique per browser/device)
    pub client_id: String,

    /// Initial subscription patterns
    pub subscriptions: Vec<String>,

    /// Reconnection settings
    pub reconnect_delay_ms: u64,
    pub max_reconnect_attempts: u32,

    /// Batching settings for performance
    pub batch_changes: bool,
    pub batch_delay_ms: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            upstream_url: String::new(),
            client_id: String::new(),
            subscriptions: Vec::new(),
            reconnect_delay_ms: 1000,
            max_reconnect_attempts: 10,
            batch_changes: true,
            batch_delay_ms: 100,
        }
    }
}

/// Sync state tracking
#[derive(Debug, Clone)]
pub struct SyncState {
    /// Current sync heads (what changes we have)
    pub local_heads: Vec<u8>,

    /// Last known upstream heads
    pub upstream_heads: Vec<u8>,

    /// Pending changes to send
    pub pending_changes: Vec<Vec<u8>>,

    /// Connection state
    pub connected: bool,

    /// Last successful sync timestamp
    pub last_sync: Option<u64>,
}

impl SyncState {
    pub fn new() -> Self {
        Self {
            local_heads: Vec::new(),
            upstream_heads: Vec::new(),
            pending_changes: Vec::new(),
            connected: false,
            last_sync: None,
        }
    }

    pub fn has_pending_changes(&self) -> bool {
        !self.pending_changes.is_empty()
    }

    pub fn add_pending_change(&mut self, change: Vec<u8>) {
        self.pending_changes.push(change);
    }

    pub fn drain_pending_changes(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.pending_changes)
    }
}

// Trait for sync adapters will be implemented in platform-specific crates
// (swirldb-browser for WebSocket/WebRTC, swirldb-server for native sockets)

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{ActorType, PolicyEngine};

    fn create_test_actor(id: &str) -> Actor {
        Actor {
            actor_type: ActorType::User,
            id: id.to_string(),
            org_id: None,
            team_id: None,
            app_id: None,
            role: None,
            claims: std::collections::HashMap::new(),
        }
    }

    fn create_test_policy() -> PolicyEngine {
        let config_json = r#"{
            "policies": {
                "rules": [
                    {
                        "priority": 10,
                        "actor": { "type": "User" },
                        "action": "Subscribe",
                        "path_pattern": "/user/{actor.id}/**",
                        "effect": "Allow"
                    },
                    {
                        "priority": 20,
                        "actor": { "type": "User" },
                        "action": "Subscribe",
                        "path_pattern": "/public/**",
                        "effect": "Allow"
                    },
                    {
                        "priority": 9999,
                        "actor": { "type": "Any" },
                        "action": "Subscribe",
                        "path_pattern": "/**",
                        "effect": "Deny"
                    }
                ]
            }
        }"#;

        PolicyEngine::from_json(config_json).unwrap()
    }

    #[test]
    fn test_subscription_set_matches_path() {
        let actor = create_test_actor("alice");
        let mut sub_set = SubscriptionSet::new(actor);

        sub_set.add_subscriptions(
            vec!["/user/alice/**".to_string(), "/public/**".to_string()],
            None,
        );

        assert!(sub_set.matches_path("/user/alice/prefs/theme"));
        assert!(sub_set.matches_path("/public/config"));
        assert!(!sub_set.matches_path("/user/bob/prefs/theme"));
    }

    #[test]
    fn test_subscription_with_policy() {
        let actor = create_test_actor("alice");
        let policy = create_test_policy();
        let mut sub_set = SubscriptionSet::new(actor);

        // Try to subscribe to allowed and denied patterns
        let (added, denied) = sub_set.add_subscriptions(
            vec![
                "/user/alice/**".to_string(),  // ✅ Allowed
                "/public/**".to_string(),        // ✅ Allowed
                "/user/bob/**".to_string(),      // ❌ Denied (not alice's data)
                "/admin/**".to_string(),          // ❌ Denied (no rule allows)
            ],
            Some(&policy),
        );

        assert_eq!(added.len(), 2);
        assert!(added.contains(&"/user/alice/**".to_string()));
        assert!(added.contains(&"/public/**".to_string()));

        assert_eq!(denied.len(), 2);
        assert!(denied.contains(&"/user/bob/**".to_string()));
        assert!(denied.contains(&"/admin/**".to_string()));
    }

    #[test]
    fn test_subscription_manager() {
        let policy = create_test_policy();
        let mut manager = SubscriptionManager::new(Some(policy));

        let actor = create_test_actor("alice");

        // Add client with subscriptions
        let (added, denied) = manager.add_client(
            "alice_client".to_string(),
            actor,
            vec![
                "/user/alice/**".to_string(),
                "/public/**".to_string(),
                "/admin/**".to_string(), // Should be denied
            ],
        );

        assert_eq!(added.len(), 2);
        assert_eq!(denied.len(), 1);

        // Check subscribers for specific paths
        let subscribers = manager.get_subscribers_for_path("/user/alice/prefs");
        assert_eq!(subscribers.len(), 1);
        assert_eq!(subscribers[0], "alice_client");

        let subscribers = manager.get_subscribers_for_path("/user/bob/prefs");
        assert_eq!(subscribers.len(), 0);

        let subscribers = manager.get_subscribers_for_path("/public/data");
        assert_eq!(subscribers.len(), 1);

        // Update subscriptions
        let (added, denied) = manager.update_subscriptions(
            "alice_client",
            vec!["/org/acme/**".to_string()], // Should be denied
            vec!["/public/**".to_string()],   // Remove this
        ).unwrap();

        assert_eq!(added.len(), 0);
        assert_eq!(denied.len(), 1);

        // Public should no longer match
        let subscribers = manager.get_subscribers_for_path("/public/data");
        assert_eq!(subscribers.len(), 0);

        // Remove client
        manager.remove_client("alice_client");
        let subscribers = manager.get_subscribers_for_path("/user/alice/prefs");
        assert_eq!(subscribers.len(), 0);
    }

    #[test]
    fn test_subscription_manager_multiple_clients() {
        let policy = create_test_policy();
        let mut manager = SubscriptionManager::new(Some(policy));

        let alice = create_test_actor("alice");
        let bob = create_test_actor("bob");

        manager.add_client(
            "alice_client".to_string(),
            alice,
            vec!["/user/alice/**".to_string(), "/public/**".to_string()],
        );

        manager.add_client(
            "bob_client".to_string(),
            bob,
            vec!["/user/bob/**".to_string(), "/public/**".to_string()],
        );

        // Both should get public updates
        let subscribers = manager.get_subscribers_for_path("/public/data");
        assert_eq!(subscribers.len(), 2);

        // Only alice gets her updates
        let subscribers = manager.get_subscribers_for_path("/user/alice/prefs");
        assert_eq!(subscribers.len(), 1);
        assert_eq!(subscribers[0], "alice_client");

        // Only bob gets his updates
        let subscribers = manager.get_subscribers_for_path("/user/bob/prefs");
        assert_eq!(subscribers.len(), 1);
        assert_eq!(subscribers[0], "bob_client");
    }
}
