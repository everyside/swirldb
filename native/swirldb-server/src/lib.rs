// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! SwirlDB Server library
//!
//! This library exposes server components for use in integration tests.

pub mod state;
pub mod storage;

// Re-export commonly used types
pub use state::{ServerState, BroadcastMessage, ClientInfo, ActivityEvent, ServerStats, ConnectionInfo, SubscriptionInfo};
