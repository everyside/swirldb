// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

/// Storage implementations for SwirlDB server
///
/// Server storage uses DocumentStorage from core for saving/loading state.
/// With the global CRDT design, we no longer need separate ChangeLog adapters.
/// Change history is maintained by Automerge and accessed via SwirlDB methods.

use serde::{Deserialize, Serialize};

// Re-export core storage traits
pub use swirldb_core::storage::DocumentStorage;

/// Storage statistics (server-specific helper)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    pub connection_count: usize,
    pub change_count: usize,
    pub uptime_seconds: u64,
}
