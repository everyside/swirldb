// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Rust client for integration tests
//!
//! Wraps `swirldb_client::SyncClient` to provide a test-friendly API with
//! blocking `wait_for_*` methods. This validates that the client library
//! works end-to-end against a real server.

use anyhow::Result;
use automerge::ScalarValue;
use swirldb_client::SyncClient;
use tokio::sync::broadcast;
use tracing::warn;

pub struct RustClient {
    inner: SyncClient,
    change_rx: broadcast::Receiver<Vec<String>>,
    ephemeral_rx: broadcast::Receiver<Vec<(String, Vec<u8>)>>,
}

impl RustClient {
    /// Connect to a test server
    pub async fn connect(ws_url: &str, subscriptions: Vec<String>) -> Result<Self> {
        let inner = SyncClient::connect(ws_url, subscriptions).await?;
        let change_rx = inner.on_change();
        let ephemeral_rx = inner.on_ephemeral();

        Ok(RustClient {
            inner,
            change_rx,
            ephemeral_rx,
        })
    }

    /// Set a value in the database and push to server
    pub async fn set_path(&mut self, path: &str, value: ScalarValue) -> Result<()> {
        self.inner.set_path(path, value).await
    }

    /// Get a value from the local database
    pub async fn get_path(&self, path: &str) -> Option<ScalarValue> {
        self.inner.get_path(path).await
    }

    /// Wait for a broadcast message from another client.
    ///
    /// The SyncClient background task applies changes automatically,
    /// so this just waits for the change notification. Returns an empty
    /// vec since the changes are already applied to the local DB.
    pub async fn wait_for_broadcast(&mut self) -> Result<Vec<Vec<u8>>> {
        match self.change_rx.recv().await {
            Ok(_paths) => Ok(Vec::new()),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("Change receiver lagged by {} messages", n);
                // Changes are already applied by background task, just return
                Ok(Vec::new())
            }
            Err(e) => anyhow::bail!("Change receiver error: {}", e),
        }
    }

    /// Wait for a broadcast with timeout
    pub async fn wait_for_broadcast_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Vec<Vec<u8>>> {
        tokio::time::timeout(timeout, self.wait_for_broadcast()).await?
    }

    /// Send an ephemeral message (bypasses CRDT/storage)
    pub async fn send_ephemeral(&mut self, path: &str, data: &[u8]) -> Result<()> {
        self.inner.send_ephemeral(path, data).await
    }

    /// Send a batch of ephemeral messages
    pub async fn send_ephemeral_batch(&mut self, updates: &[(&str, &[u8])]) -> Result<()> {
        self.inner.send_ephemeral_batch(updates).await
    }

    /// Wait for ephemeral messages with a timeout
    pub async fn wait_for_ephemeral_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        tokio::time::timeout(timeout, self.wait_for_ephemeral()).await?
    }

    /// Wait for an ephemeral message
    async fn wait_for_ephemeral(&mut self) -> Result<Vec<(String, Vec<u8>)>> {
        loop {
            match self.ephemeral_rx.recv().await {
                Ok(updates) => return Ok(updates),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Ephemeral receiver lagged by {} messages", n);
                    continue;
                }
                Err(e) => anyhow::bail!("Ephemeral receiver error: {}", e),
            }
        }
    }

    /// Close the connection explicitly.
    ///
    /// This is optional — dropping the `RustClient` has the same effect,
    /// as the underlying `SyncClient` drop closes the WebSocket channel.
    /// Provided for test readability when you want to express intent.
    pub async fn close(self) -> Result<()> {
        // Dropping the SyncClient drops the ws_tx sender,
        // which causes the background task to exit when the
        // WebSocket connection closes.
        drop(self.inner);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_server::TestServer;

    #[tokio::test]
    async fn test_rust_client_connect() {
        let server = TestServer::start().await.unwrap();
        let client = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
            .await
            .unwrap();

        assert_eq!(server.connection_count(), 1);
        client.close().await.unwrap();
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_rust_client_set_and_get() {
        let server = TestServer::start().await.unwrap();
        let mut client = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
            .await
            .unwrap();

        client
            .set_path("test.value", ScalarValue::Str("hello".into()))
            .await
            .unwrap();

        let value = client.get_path("test.value").await;
        assert_eq!(value, Some(ScalarValue::Str("hello".into())));

        client.close().await.unwrap();
        server.shutdown().await.unwrap();
    }
}
