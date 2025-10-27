// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

use crate::policy::Actor;
use super::AuthProvider;

/// Anonymous auth provider - always returns anonymous actor
#[derive(Debug, Clone)]
pub struct AnonymousAuth;

impl AnonymousAuth {
    pub fn new() -> Self {
        AnonymousAuth
    }
}

impl Default for AnonymousAuth {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthProvider for AnonymousAuth {
    fn get_actor(&self) -> Actor {
        Actor::anonymous()
    }

    fn set_actor(&mut self, _actor: Actor) {
        // No-op - anonymous auth is immutable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::ActorType;

    #[test]
    fn test_anonymous_auth() {
        let auth = AnonymousAuth::new();
        let actor = auth.get_actor();

        assert_eq!(actor.actor_type, ActorType::Anonymous);
        assert_eq!(actor.id, "anonymous");
    }
}
