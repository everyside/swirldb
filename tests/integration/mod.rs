// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Integration test suite for SwirlDB
//!
//! Tests sync across platforms:
//! - Browser WASM client
//! - Rust native client
//! - Rust Server
//!
//! Test scenarios:
//! - Multi-client sync (2-3 clients)
//! - Subscription filtering
//! - Policy enforcement
//! - Network resilience (disconnect/reconnect)
//! - Cross-platform serialization
//! - Browser ↔ Server sync

pub mod browser_client;
pub mod rust_client;
pub mod test_server;

// Test modules
mod browser_sync;
mod cross_platform;
mod multi_client_sync;
mod network_resilience;
mod policy_enforcement;
mod subscription_filtering;

/// Initialize test logging with clean formatting
///
/// - No timestamps (tests run quickly, timing doesn't matter)
/// - No target module names (cleaner output)
/// - Test-friendly writer (works with cargo test)
/// - Only initializes once (subsequent calls are no-ops)
pub fn init_test_logging() {
    tracing_subscriber::fmt()
        .with_test_writer()
        .without_time()
        .with_target(false)
        .with_level(true)
        .try_init()
        .ok();
}
