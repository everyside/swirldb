// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! RedbAdapter - persistent, ACID-compliant storage for SwirlDB server
//!
//! Uses redb (pure Rust embedded database) for durable document storage.
//! Thread-safe via redb's internal locking (Database is Send + Sync).
//! All blocking redb I/O is offloaded to spawn_blocking to avoid
//! starving the Tokio runtime.

use anyhow::Result;
use async_trait::async_trait;
use redb::{Database, ReadableTable, TableDefinition};
use std::sync::Arc;
use swirldb_core::storage::{DocumentStorage, DocumentStorageMarker};

/// Table definition for document storage: key (string) -> value (bytes)
const DOCUMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("documents");

/// Persistent storage adapter backed by redb
///
/// Provides ACID-compliant, file-backed storage that survives server restarts.
/// All operations use `spawn_blocking` since redb is synchronous and may
/// block on fsync/write locks.
pub struct RedbAdapter {
    db: Arc<Database>,
}

impl RedbAdapter {
    /// Open or create a redb database at the given path
    pub fn new(path: &str) -> Result<Self> {
        let db = Database::create(path)?;

        // redb creates tables lazily; open a write txn at startup to guarantee
        // the table exists before any read txn tries to open it.
        let write_txn = db.begin_write()?;
        write_txn.open_table(DOCUMENTS)?;
        write_txn.commit()?;

        Ok(Self { db: Arc::new(db) })
    }
}

impl DocumentStorageMarker for RedbAdapter {}

#[async_trait]
impl DocumentStorage for RedbAdapter {
    async fn save(&self, key: &str, data: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let key = key.to_string();
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(DOCUMENTS)?;
                table.insert(key.as_str(), data.as_slice())?;
            }
            write_txn.commit()?;
            Ok(())
        })
        .await?
    }

    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let db = self.db.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(DOCUMENTS)?;
            let value = table.get(key.as_str())?;
            Ok(value.map(|v| v.value().to_vec()))
        })
        .await?
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let db = self.db.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(DOCUMENTS)?;
                table.remove(key.as_str())?;
            }
            write_txn.commit()?;
            Ok(())
        })
        .await?
    }

    async fn list_keys(&self) -> Result<Vec<String>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(DOCUMENTS)?;
            let keys: Result<Vec<String>> = table
                .iter()?
                .map(|entry| {
                    let (key, _) = entry?;
                    Ok(key.value().to_string())
                })
                .collect();
            keys
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_save_and_load() {
        let tmp = NamedTempFile::new().unwrap();
        let adapter = RedbAdapter::new(tmp.path().to_str().unwrap()).unwrap();

        adapter.save("test-key", b"hello world").await.unwrap();
        let loaded = adapter.load("test-key").await.unwrap();
        assert_eq!(loaded, Some(b"hello world".to_vec()));
    }

    #[tokio::test]
    async fn test_load_nonexistent() {
        let tmp = NamedTempFile::new().unwrap();
        let adapter = RedbAdapter::new(tmp.path().to_str().unwrap()).unwrap();

        let loaded = adapter.load("nonexistent").await.unwrap();
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn test_delete() {
        let tmp = NamedTempFile::new().unwrap();
        let adapter = RedbAdapter::new(tmp.path().to_str().unwrap()).unwrap();

        adapter.save("key", b"data").await.unwrap();
        adapter.delete("key").await.unwrap();
        let loaded = adapter.load("key").await.unwrap();
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn test_list_keys() {
        let tmp = NamedTempFile::new().unwrap();
        let adapter = RedbAdapter::new(tmp.path().to_str().unwrap()).unwrap();

        adapter.save("alpha", b"1").await.unwrap();
        adapter.save("beta", b"2").await.unwrap();
        adapter.save("gamma", b"3").await.unwrap();

        let mut keys = adapter.list_keys().await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["alpha", "beta", "gamma"]);
    }

    #[tokio::test]
    async fn test_overwrite() {
        let tmp = NamedTempFile::new().unwrap();
        let adapter = RedbAdapter::new(tmp.path().to_str().unwrap()).unwrap();

        adapter.save("key", b"old").await.unwrap();
        adapter.save("key", b"new").await.unwrap();
        let loaded = adapter.load("key").await.unwrap();
        assert_eq!(loaded, Some(b"new".to_vec()));
    }
}
