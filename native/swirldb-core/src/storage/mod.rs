// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

// Storage traits for SwirlDB
//
// These traits define the storage interfaces that can be implemented
// by different backends (browser: localStorage/IndexedDB, server: redb/filesystem)

use anyhow::Result;
use async_trait::async_trait;

// =============================================================================
// Document Storage - Current State Snapshots
// =============================================================================

// Marker trait to add Send + Sync only on native targets
#[cfg(not(target_arch = "wasm32"))]
pub trait DocumentStorageMarker: Send + Sync {}

#[cfg(target_arch = "wasm32")]
pub trait DocumentStorageMarker {}

/// Storage adapter for saving/loading current document state
///
/// This is used to persist the current CRDT state to disk/storage.
/// Implementations:
/// - Browser: LocalStorageAdapter, IndexedDBAdapter (single-threaded, !Send)
/// - Server: RedbStorage, FileSystemStorage (multi-threaded, Send + Sync)
///
/// Note: For WASM targets, implement with #[async_trait(?Send)]
///       For native targets, implement with #[async_trait]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait DocumentStorage: DocumentStorageMarker {
    /// Save document state under a key
    async fn save(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Load document state by key
    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Delete document state
    async fn delete(&self, key: &str) -> Result<()>;

    /// List all stored document keys
    async fn list_keys(&self) -> Result<Vec<String>>;
}

// NOTE: ChangeLog trait removed - we use Automerge's built-in change tracking
// with the global SwirlDB instance. Change history is maintained by Automerge
// and accessed via SwirlDB::get_changes() and SwirlDB::apply_changes().

// =============================================================================
// In-Memory Implementations (for testing and default)
// =============================================================================

use std::collections::HashMap;
use std::sync::RwLock;

/// In-memory document storage (volatile - lost on restart)
pub struct InMemoryDocStorage {
    data: RwLock<HashMap<String, Vec<u8>>>,
}

impl InMemoryDocStorage {
    pub fn new() -> Self {
        InMemoryDocStorage {
            data: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryDocStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentStorageMarker for InMemoryDocStorage {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocumentStorage for InMemoryDocStorage {
    async fn save(&self, key: &str, data: &[u8]) -> Result<()> {
        self.data.write().unwrap().insert(key.to_string(), data.to_vec());
        Ok(())
    }

    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.data.read().unwrap().get(key).cloned())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.data.write().unwrap().remove(key);
        Ok(())
    }

    async fn list_keys(&self) -> Result<Vec<String>> {
        Ok(self.data.read().unwrap().keys().cloned().collect())
    }
}

