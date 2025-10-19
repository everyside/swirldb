/// Storage adapter trait for pluggable persistence backends
///
/// SwirlDB server uses a pluggable storage architecture where different
/// implementations can be swapped at runtime based on deployment needs.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub mod redb_adapter;
pub mod memory_adapter;

/// A single CRDT change (opaque binary blob)
/// The server doesn't understand CRDT semantics - it just stores/relays these blobs.
/// The actual SwirlDB CRDT logic runs on the client side.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub data: Vec<u8>,
    pub timestamp: i64,
}

/// Storage adapter trait - implement this for different backends
#[async_trait]
pub trait StorageAdapter: Send + Sync + 'static {
    /// Initialize the storage backend
    async fn init(&mut self) -> Result<()>;

    /// Get all changes for a namespace
    async fn get_namespace_changes(&self, namespace_id: &str) -> Result<Vec<Change>>;

    /// Append a new change to a namespace (append-only, CRDT-friendly)
    async fn append_change(&self, namespace_id: &str, change: Change) -> Result<()>;

    /// Append multiple changes atomically
    async fn append_changes(&self, namespace_id: &str, changes: Vec<Change>) -> Result<()>;

    /// Get changes after a specific timestamp (for delta syncing)
    async fn get_changes_since(&self, namespace_id: &str, since: i64) -> Result<Vec<Change>>;

    /// List all namespace IDs
    async fn list_namespaces(&self) -> Result<Vec<String>>;

    /// Delete a namespace and all its data
    async fn delete_namespace(&self, namespace_id: &str) -> Result<()>;

    /// Get storage statistics
    async fn stats(&self) -> Result<StorageStats>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    pub total_namespaces: usize,
    pub total_changes: usize,
    pub total_bytes: u64,
}
