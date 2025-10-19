/// redb storage adapter - ultra-fast, zero-copy embedded database
///
/// Uses memory-mapped files for zero-copy reads and ACID transactions
/// for durability. Perfect for append-heavy CRDT workloads.

use super::{Change, StorageAdapter, StorageStats};
use anyhow::{Context, Result};
use async_trait::async_trait;
use redb::{Database, ReadableTable, TableDefinition};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

// Table definitions (key type, value type)
const CHANGES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("changes");
const METADATA_TABLE: TableDefinition<&str, &str> = TableDefinition::new("metadata");

/// redb storage adapter
pub struct RedbAdapter {
    db: Arc<RwLock<Database>>,
    path: String,
}

impl RedbAdapter {
    /// Create a new redb adapter
    ///
    /// # Arguments
    /// * `path` - Path to the database file (e.g., "./data/swirldb.redb")
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)
                .context("Failed to create database directory")?;
        }

        let db = Database::create(&path_str)
            .context("Failed to create redb database")?;

        Ok(Self {
            db: Arc::new(RwLock::new(db)),
            path: path_str,
        })
    }

    /// Build a key for a change: "{namespace_id}/{timestamp}"
    fn change_key(namespace_id: &str, timestamp: i64) -> String {
        format!("{}/{:020}", namespace_id, timestamp)
    }

    /// Parse namespace_id from a change key
    fn parse_namespace_id(key: &str) -> Option<&str> {
        key.split('/').next()
    }
}

#[async_trait]
impl StorageAdapter for RedbAdapter {
    async fn init(&mut self) -> Result<()> {
        let db = self.db.write().await;

        // Initialize tables
        let write_txn = db.begin_write()?;
        {
            write_txn.open_table(CHANGES_TABLE)?;
            write_txn.open_table(METADATA_TABLE)?;
        }
        write_txn.commit()?;

        tracing::info!("Initialized redb storage at {}", self.path);
        Ok(())
    }

    async fn get_namespace_changes(&self, namespace_id: &str) -> Result<Vec<Change>> {
        let db = self.db.read().await;
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(CHANGES_TABLE)?;

        let prefix = format!("{}/", namespace_id);
        let mut changes = Vec::new();

        // Iterate over all keys with this prefix
        for result in table.range(prefix.as_str()..)? {
            let (key, value) = result?;
            let key_str = key.value();

            // Stop when we reach keys for a different namespace
            if !key_str.starts_with(&prefix) {
                break;
            }

            // Extract timestamp from key
            let timestamp: i64 = key_str
                .split('/')
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            changes.push(Change {
                data: value.value().to_vec(),
                timestamp,
            });
        }

        Ok(changes)
    }

    async fn append_change(&self, namespace_id: &str, change: Change) -> Result<()> {
        let db = self.db.write().await;
        let write_txn = db.begin_write()?;

        {
            let mut table = write_txn.open_table(CHANGES_TABLE)?;
            let key = Self::change_key(namespace_id, change.timestamp);
            table.insert(key.as_str(), change.data.as_slice())?;
        }

        write_txn.commit()?;
        Ok(())
    }

    async fn append_changes(&self, namespace_id: &str, changes: Vec<Change>) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        let db = self.db.write().await;
        let write_txn = db.begin_write()?;

        {
            let mut table = write_txn.open_table(CHANGES_TABLE)?;

            for change in changes {
                let key = Self::change_key(namespace_id, change.timestamp);
                table.insert(key.as_str(), change.data.as_slice())?;
            }
        }

        write_txn.commit()?;
        Ok(())
    }

    async fn get_changes_since(&self, namespace_id: &str, since: i64) -> Result<Vec<Change>> {
        let db = self.db.read().await;
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(CHANGES_TABLE)?;

        let start_key = Self::change_key(namespace_id, since);
        let prefix = format!("{}/", namespace_id);
        let mut changes = Vec::new();

        for result in table.range(start_key.as_str()..)? {
            let (key, value) = result?;
            let key_str = key.value();

            if !key_str.starts_with(&prefix) {
                break;
            }

            let timestamp: i64 = key_str
                .split('/')
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            if timestamp > since {
                changes.push(Change {
                    data: value.value().to_vec(),
                    timestamp,
                });
            }
        }

        Ok(changes)
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let db = self.db.read().await;
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(CHANGES_TABLE)?;

        let mut namespaces = std::collections::HashSet::new();

        for result in table.iter()? {
            let (key, _) = result?;
            if let Some(namespace_id) = Self::parse_namespace_id(key.value()) {
                namespaces.insert(namespace_id.to_string());
            }
        }

        Ok(namespaces.into_iter().collect())
    }

    async fn delete_namespace(&self, namespace_id: &str) -> Result<()> {
        let db = self.db.write().await;
        let write_txn = db.begin_write()?;

        {
            let mut table = write_txn.open_table(CHANGES_TABLE)?;
            let prefix = format!("{}/", namespace_id);

            // Collect keys to delete
            let keys_to_delete: Vec<String> = table
                .range(prefix.as_str()..)?
                .filter_map(|result| result.ok())
                .map(|(key, _)| key.value().to_string())
                .take_while(|key| key.starts_with(&prefix))
                .collect();

            // Delete all keys
            for key in keys_to_delete {
                table.remove(key.as_str())?;
            }
        }

        write_txn.commit()?;
        tracing::info!("Deleted namespace: {}", namespace_id);
        Ok(())
    }

    async fn stats(&self) -> Result<StorageStats> {
        let db = self.db.read().await;
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(CHANGES_TABLE)?;

        let mut namespaces = std::collections::HashSet::new();
        let mut total_changes = 0;
        let mut total_bytes = 0u64;

        for result in table.iter()? {
            let (key, value) = result?;
            if let Some(namespace_id) = Self::parse_namespace_id(key.value()) {
                namespaces.insert(namespace_id.to_string());
            }
            total_changes += 1;
            total_bytes += value.value().len() as u64;
        }

        Ok(StorageStats {
            total_namespaces: namespaces.len(),
            total_changes,
            total_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now_timestamp() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    #[tokio::test]
    async fn test_redb_basic_operations() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.redb");

        let mut adapter = RedbAdapter::new(&db_path)?;
        adapter.init().await?;

        // Append change
        let change = Change {
            data: vec![1, 2, 3, 4],
            timestamp: now_timestamp(),
        };
        adapter.append_change("test-namespace", change.clone()).await?;

        // Retrieve changes
        let changes = adapter.get_namespace_changes("test-namespace").await?;
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].data, vec![1, 2, 3, 4]);

        // List namespaces
        let namespaces = adapter.list_namespaces().await?;
        assert!(namespaces.contains(&"test-namespace".to_string()));

        // Stats
        let stats = adapter.stats().await?;
        assert_eq!(stats.total_namespaces, 1);
        assert_eq!(stats.total_changes, 1);

        Ok(())
    }
}
