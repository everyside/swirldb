/// In-memory storage adapter - useful for testing and development
///
/// All data is lost when the process exits. Fast but not durable.

use super::{Change, StorageAdapter, StorageStats};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// In-memory storage adapter
#[derive(Clone)]
pub struct MemoryAdapter {
    namespaces: Arc<RwLock<HashMap<String, Vec<Change>>>>,
}

impl MemoryAdapter {
    pub fn new() -> Self {
        Self {
            namespaces: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemoryAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageAdapter for MemoryAdapter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!("Initialized in-memory storage (no persistence)");
        Ok(())
    }

    async fn get_namespace_changes(&self, namespace_id: &str) -> Result<Vec<Change>> {
        let namespaces = self.namespaces.read().await;
        Ok(namespaces.get(namespace_id).cloned().unwrap_or_default())
    }

    async fn append_change(&self, namespace_id: &str, change: Change) -> Result<()> {
        let mut namespaces = self.namespaces.write().await;
        namespaces
            .entry(namespace_id.to_string())
            .or_insert_with(Vec::new)
            .push(change);
        Ok(())
    }

    async fn append_changes(&self, namespace_id: &str, changes: Vec<Change>) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        let mut namespaces = self.namespaces.write().await;
        namespaces
            .entry(namespace_id.to_string())
            .or_insert_with(Vec::new)
            .extend(changes);
        Ok(())
    }

    async fn get_changes_since(&self, namespace_id: &str, since: i64) -> Result<Vec<Change>> {
        let namespaces = self.namespaces.read().await;

        if let Some(changes) = namespaces.get(namespace_id) {
            Ok(changes
                .iter()
                .filter(|c| c.timestamp > since)
                .cloned()
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let namespaces = self.namespaces.read().await;
        Ok(namespaces.keys().cloned().collect())
    }

    async fn delete_namespace(&self, namespace_id: &str) -> Result<()> {
        let mut namespaces = self.namespaces.write().await;
        namespaces.remove(namespace_id);
        tracing::info!("Deleted namespace from memory: {}", namespace_id);
        Ok(())
    }

    async fn stats(&self) -> Result<StorageStats> {
        let namespaces = self.namespaces.read().await;

        let total_namespaces = namespaces.len();
        let mut total_changes = 0;
        let mut total_bytes = 0u64;

        for changes in namespaces.values() {
            total_changes += changes.len();
            for change in changes {
                total_bytes += change.data.len() as u64;
            }
        }

        Ok(StorageStats {
            total_namespaces,
            total_changes,
            total_bytes,
        })
    }
}
