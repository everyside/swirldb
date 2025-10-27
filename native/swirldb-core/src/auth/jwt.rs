// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

use crate::policy::Actor;
use std::collections::HashMap;
use super::AuthProvider;

/// JWT identity provider - decodes JWT claims to get actor
///
/// Note: This only DECODES the JWT, it does NOT validate signatures.
/// Signature validation should be done server-side before creating this identity.
///
/// Browser usage: Fetch JWT from server, pass to this provider
/// Server usage: Validate JWT signature first, then decode claims
#[derive(Debug, Clone)]
pub struct JwtAuth {
    actor: Actor,
    token: String,
}

impl JwtAuth {
    /// Create from a pre-validated JWT token
    ///
    /// The token will be decoded to extract claims and create an Actor.
    /// **Important**: This does NOT validate the signature! Validate server-side first.
    pub fn from_token(token: &str) -> Result<Self, String> {
        let claims = Self::decode_claims(token)?;
        let actor = Actor::from_jwt_claims(claims)?;

        Ok(JwtAuth {
            actor,
            token: token.to_string(),
        })
    }

    /// Create from pre-parsed claims
    pub fn from_claims(claims: HashMap<String, serde_json::Value>, token: &str) -> Result<Self, String> {
        let actor = Actor::from_jwt_claims(claims)?;

        Ok(JwtAuth {
            actor,
            token: token.to_string(),
        })
    }

    /// Decode JWT claims (without signature validation)
    fn decode_claims(token: &str) -> Result<HashMap<String, serde_json::Value>, String> {
        let parts: Vec<&str> = token.split('.').collect();

        if parts.len() != 3 {
            return Err("Invalid JWT format - expected 3 parts".to_string());
        }

        let payload = parts[1];
        let decoded = Self::base64url_decode(payload)?;

        let json_str = String::from_utf8(decoded)
            .map_err(|_| "Invalid UTF-8 in JWT payload".to_string())?;

        let claims: HashMap<String, serde_json::Value> = serde_json::from_str(&json_str)
            .map_err(|e| format!("Failed to parse JWT claims: {}", e))?;

        Ok(claims)
    }

    /// Base64url decode (RFC 4648 Section 5)
    fn base64url_decode(input: &str) -> Result<Vec<u8>, String> {
        use base64::{Engine as _, engine::general_purpose};

        // Convert base64url to standard base64
        let standard_b64 = input
            .replace('-', "+")
            .replace('_', "/");

        // Add padding if needed
        let padding = match standard_b64.len() % 4 {
            2 => "==",
            3 => "=",
            _ => "",
        };

        let padded = format!("{}{}", standard_b64, padding);

        general_purpose::STANDARD
            .decode(&padded)
            .map_err(|e| format!("Base64 decode error: {}", e))
    }

    /// Get the raw JWT token
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Refresh the token and update actor
    pub fn refresh_token(&mut self, new_token: &str) -> Result<(), String> {
        let claims = Self::decode_claims(new_token)?;
        let actor = Actor::from_jwt_claims(claims)?;

        self.actor = actor;
        self.token = new_token.to_string();

        Ok(())
    }
}

impl AuthProvider for JwtAuth {
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
    use crate::policy::ActorType;

    #[test]
    fn test_jwt_decode() {
        // Example JWT (generated with HS256, secret: "test")
        // Payload: {"type":"user","sub":"alice","org_id":"acme-corp","iat":1234567890}
        let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0eXBlIjoidXNlciIsInN1YiI6ImFsaWNlIiwib3JnX2lkIjoiYWNtZS1jb3JwIiwiaWF0IjoxMjM0NTY3ODkwfQ.Xb7BPLgf36iiPPNYOkKTmJQBLZ4ZZFPx9m7pJjVqFnk";

        let auth = JwtAuth::from_token(token).unwrap();
        let actor = auth.get_actor();

        assert_eq!(actor.actor_type, ActorType::User);
        assert_eq!(actor.id, "alice");
        assert_eq!(actor.org_id, Some("acme-corp".to_string()));
    }

    #[test]
    fn test_jwt_invalid_format() {
        let result = JwtAuth::from_token("not.a.jwt.token");
        assert!(result.is_err());
    }
}
