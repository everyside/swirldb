// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Rust client for integration tests
//!
//! A pure Rust WebSocket client that connects to the test server.

use anyhow::Result;
use automerge::ScalarValue;
use futures::{SinkExt, StreamExt};
use swirldb_core::core::SwirlDB;
use swirldb_core::protocol::Message;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::Message as WsMessage, MaybeTlsStream, WebSocketStream,
};
use tracing::{info, warn};
use uuid::Uuid;

pub struct RustClient {
    pub client_id: String,
    pub db: SwirlDB,
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    subscriptions: Vec<String>,
}

impl RustClient {
    /// Connect to a test server
    pub async fn connect(ws_url: &str, subscriptions: Vec<String>) -> Result<Self> {
        let client_id = format!("rust-client-{}", Uuid::new_v4());
        let db = SwirlDB::new();

        let (ws_stream, _) = connect_async(ws_url).await?;
        let mut client = RustClient {
            client_id: client_id.clone(),
            db,
            ws: ws_stream,
            subscriptions: subscriptions.clone(),
        };

        // Send Connect message
        client.send_connect().await?;

        // Wait for SubscribeAck
        client.wait_for_subscribe_ack().await?;

        // Wait for initial Sync
        client.wait_for_sync().await?;

        info!("Rust client {} connected", client_id);

        Ok(client)
    }

    /// Send Connect message with subscriptions
    async fn send_connect(&mut self) -> Result<()> {
        let heads = self.db.get_heads();
        let heads_bytes: Vec<u8> = heads.into_iter().flatten().collect();

        let msg = Message::Connect {
            client_id: self.client_id.clone(),
            subscriptions: self.subscriptions.clone(),
            heads: heads_bytes,
        };

        self.ws.send(WsMessage::Binary(msg.encode())).await?;
        Ok(())
    }

    /// Wait for SubscribeAck
    async fn wait_for_subscribe_ack(&mut self) -> Result<()> {
        while let Some(msg) = self.ws.next().await {
            if let WsMessage::Binary(data) = msg? {
                if let Message::SubscribeAck { added, denied } = Message::decode(&data)? {
                    info!(
                        "SubscribeAck: {} added, {} denied",
                        added.len(),
                        denied.len()
                    );
                    return Ok(());
                }
            }
        }
        anyhow::bail!("Connection closed before SubscribeAck")
    }

    /// Wait for initial Sync
    async fn wait_for_sync(&mut self) -> Result<()> {
        while let Some(msg) = self.ws.next().await {
            if let WsMessage::Binary(data) = msg? {
                if let Message::Sync { heads: _, changes } = Message::decode(&data)? {
                    if !changes.is_empty() {
                        self.db.apply_changes(changes)?;
                        info!("Applied initial sync changes");
                    }
                    return Ok(());
                }
            }
        }
        anyhow::bail!("Connection closed before Sync")
    }

    /// Set a value in the database and push to server
    pub async fn set_path(&mut self, path: &str, value: ScalarValue) -> Result<()> {
        self.db.set_path(path, value)?;
        self.push_changes().await?;
        Ok(())
    }

    /// Get a value from the local database
    pub fn get_path(&self, path: &str) -> Option<ScalarValue> {
        self.db.get_path(path)
    }

    /// Push local changes to server
    async fn push_changes(&mut self) -> Result<()> {
        let changes = self.db.get_changes();
        let heads = self.db.get_heads();
        let heads_bytes: Vec<u8> = heads.into_iter().flatten().collect();

        let msg = Message::Push {
            heads: heads_bytes,
            changes,
        };

        self.ws.send(WsMessage::Binary(msg.encode())).await?;

        // Wait for PushAck
        self.wait_for_push_ack().await?;

        Ok(())
    }

    /// Wait for PushAck from server
    async fn wait_for_push_ack(&mut self) -> Result<()> {
        while let Some(msg) = self.ws.next().await {
            if let WsMessage::Binary(data) = msg? {
                match Message::decode(&data)? {
                    Message::PushAck { heads: _ } => {
                        return Ok(());
                    }
                    Message::Broadcast {
                        from_client_id: _,
                        changes,
                        affected_paths: _,
                    } => {
                        // Got a broadcast while waiting for ack
                        self.db.apply_changes(changes)?;
                        // Keep waiting for our ack
                    }
                    _ => {}
                }
            }
        }
        anyhow::bail!("Connection closed before PushAck")
    }

    /// Wait for a broadcast message from another client
    pub async fn wait_for_broadcast(&mut self) -> Result<Vec<Vec<u8>>> {
        while let Some(msg) = self.ws.next().await {
            match msg? {
                WsMessage::Binary(data) => {
                    match Message::decode(&data)? {
                        Message::Broadcast {
                            from_client_id,
                            changes,
                            affected_paths: _,
                        } => {
                            info!(
                                "Received broadcast from {}: {} changes",
                                from_client_id,
                                changes.len()
                            );
                            self.db.apply_changes(changes.clone())?;
                            return Ok(changes);
                        }
                        Message::Ping => {
                            // Respond to ping
                            self.ws
                                .send(WsMessage::Binary(Message::Pong.encode()))
                                .await?;
                        }
                        msg => {
                            warn!("Unexpected message while waiting for broadcast: {:?}", msg);
                        }
                    }
                }
                WsMessage::Close(_) => {
                    anyhow::bail!("Connection closed while waiting for broadcast");
                }
                _ => {}
            }
        }
        anyhow::bail!("Connection closed")
    }

    /// Wait for a broadcast with timeout
    pub async fn wait_for_broadcast_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Vec<Vec<u8>>> {
        tokio::time::timeout(timeout, self.wait_for_broadcast()).await?
    }

    /// Close the connection
    pub async fn close(mut self) -> Result<()> {
        self.ws.close(None).await?;
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

        let value = client.get_path("test.value");
        assert_eq!(value, Some(ScalarValue::Str("hello".into())));

        client.close().await.unwrap();
        server.shutdown().await.unwrap();
    }
}
