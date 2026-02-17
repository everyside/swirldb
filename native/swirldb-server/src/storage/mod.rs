// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Storage implementations for SwirlDB server
//!
//! Server storage uses DocumentStorage from core for saving/loading state.
//! With the global CRDT design, we no longer need separate ChangeLog adapters.
//! Change history is maintained by Automerge and accessed via SwirlDB methods.

pub mod redb;

pub use self::redb::RedbAdapter;

use serde::{Deserialize, Serialize};

/// Storage statistics (server-specific helper)
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    pub connection_count: usize,
    pub change_count: usize,
    pub uptime_seconds: u64,
}
