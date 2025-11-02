// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

use crate::policy::{Actor, ActorType};
use std::collections::HashMap;
use super::AuthProvider;

/// Static auth provider - uses a fixed user ID
///
/// Browser usage: Generate random ID on first load, save to localStorage
/// Server usage: Load from config file or environment variable
#[derive(Debug, Clone)]
pub struct StaticAuth {
    actor: Actor,
}

impl StaticAuth {
    /// Create with a specific user ID
    pub fn new(user_id: &str) -> Self {
        Self {
            actor: Actor {
                actor_type: ActorType::User,
                id: user_id.to_string(),
                org_id: None,
                team_id: None,
                app_id: None,
                role: None,
                claims: HashMap::new(),
            },
        }
    }

    /// Create with additional metadata
    pub fn with_metadata(
        user_id: &str,
        org_id: Option<String>,
        team_id: Option<String>,
        role: Option<String>,
    ) -> Self {
        Self {
            actor: Actor {
                actor_type: ActorType::User,
                id: user_id.to_string(),
                org_id,
                team_id,
                app_id: None,
                role,
                claims: HashMap::new(),
            },
        }
    }

    /// Create for an app actor
    pub fn app(app_id: &str) -> Self {
        Self {
            actor: Actor {
                actor_type: ActorType::App,
                id: app_id.to_string(),
                org_id: None,
                team_id: None,
                app_id: Some(app_id.to_string()),
                role: None,
                claims: HashMap::new(),
            },
        }
    }

    /// Create for a server actor
    pub fn server(server_id: &str) -> Self {
        Self {
            actor: Actor {
                actor_type: ActorType::Server,
                id: server_id.to_string(),
                org_id: None,
                team_id: None,
                app_id: None,
                role: None,
                claims: HashMap::new(),
            },
        }
    }
}

impl AuthProvider for StaticAuth {
    fn get_actor(&self) -> Actor {
        self.actor.clone()
    }

    fn set_actor(&mut self, actor: Actor) {
        self.actor = actor;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_auth() {
        let auth = StaticAuth::new("alice");
        let actor = auth.get_actor();

        assert_eq!(actor.actor_type, ActorType::User);
        assert_eq!(actor.id, "alice");
    }

    #[test]
    fn test_static_auth_with_metadata() {
        let auth = StaticAuth::with_metadata(
            "alice",
            Some("acme-corp".to_string()),
            Some("engineering".to_string()),
            Some("admin".to_string()),
        );
        let actor = auth.get_actor();

        assert_eq!(actor.id, "alice");
        assert_eq!(actor.org_id, Some("acme-corp".to_string()));
        assert_eq!(actor.team_id, Some("engineering".to_string()));
        assert_eq!(actor.role, Some("admin".to_string()));
    }
}
