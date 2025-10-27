// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Authentication providers for SwirlDB
//!
//! Platform-agnostic authentication system with pluggable strategies:
//! - `AnonymousAuth` - No authentication
//! - `StaticAuth` - Simple user ID (browser: localStorage, server: config)
//! - `JwtAuth` - JWT-based authentication (browser: decode, server: validate + decode)

use crate::policy::Actor;

/// Trait for authentication providers
///
/// Auth providers are responsible for determining the current actor
/// (who is performing actions). Different strategies can be used:
/// - Anonymous (no auth)
/// - Static user ID (simple, for development or single-user apps)
/// - JWT tokens (production - browser decodes, server validates signatures)
pub trait AuthProvider: Send + Sync {
    /// Get the current actor
    fn get_actor(&self) -> Actor;

    /// Update the authenticated actor (e.g., after login)
    fn set_actor(&mut self, actor: Actor);
}

// Implementation modules
mod anonymous;
mod static_auth;
mod jwt;

// Re-exports
pub use anonymous::AnonymousAuth;
pub use static_auth::StaticAuth;
pub use jwt::JwtAuth;
