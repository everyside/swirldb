// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Native Rust client library for SwirlDB sync
//!
//! Provides a high-level API for connecting to a SwirlDB server,
//! reading/writing CRDT data, and sending/receiving ephemeral messages.
//!
//! # Features
//!
//! - Automatic WebSocket connection management with background receive loop
//! - Full CRDT sync handshake (Connect -> SubscribeAck -> Sync)
//! - Path-based reads/writes via Automerge
//! - Ephemeral pub/sub messaging (bypasses CRDT/storage for high-frequency data)
//! - Broadcast channels for change and ephemeral notifications
//!
//! # Example
//!
//! ```no_run
//! use swirldb_client::SyncClient;
//! use automerge::ScalarValue;
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = SyncClient::connect(
//!         "ws://localhost:3030/ws",
//!         vec!["**".to_string()],
//!     ).await.unwrap();
//!
//!     client.set_path("user.name", ScalarValue::Str("Alice".into())).await.unwrap();
//!     let name = client.get_path("user.name");
//! }
//! ```

use anyhow::Result;
use automerge::ScalarValue;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use swirldb_core::core::SwirlDB;
use swirldb_core::protocol::Message;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Default timeout for the connection handshake (Connect -> SubscribeAck -> Sync).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Native Rust client for SwirlDB sync
///
/// Manages a WebSocket connection to a SwirlDB server with a background
/// receive loop. CRDT changes and ephemeral messages are dispatched automatically.
///
/// The client performs a full handshake on connect:
/// 1. Sends `Connect` with client ID and subscription patterns
/// 2. Waits for `SubscribeAck` confirming which subscriptions were accepted
/// 3. Waits for initial `Sync` with the server's current CRDT state
/// 4. Spawns a background task for ongoing message dispatch
///
/// # Thread Safety
///
/// The client is safe to use from multiple tasks. The underlying CRDT database
/// is protected by an `RwLock`, and outgoing messages are sent via an `mpsc` channel.
pub struct SyncClient {
    /// Unique client identifier
    client_id: String,

    /// Local CRDT database (thread-safe)
    db: Arc<RwLock<SwirlDB>>,

    /// Channel to send WebSocket messages from any thread
    ws_tx: mpsc::Sender<Vec<u8>>,

    /// Broadcast channel for incoming ephemeral messages
    ephemeral_tx: broadcast::Sender<Vec<(String, Vec<u8>)>>,

    /// Broadcast channel for CRDT change notifications (affected paths)
    change_tx: broadcast::Sender<Vec<String>>,

    /// Handle to the background WebSocket task
    _task_handle: tokio::task::JoinHandle<()>,

    /// Subscriptions this client registered
    #[allow(dead_code)]
    subscriptions: Vec<String>,
}

impl SyncClient {
    /// Connect to a SwirlDB server with an auto-generated client ID.
    ///
    /// This is the standard entry point for creating a new client connection.
    /// A unique client ID is generated using a UUID v4.
    ///
    /// # Arguments
    ///
    /// * `url` - WebSocket URL of the SwirlDB server (e.g., `ws://localhost:3030/ws`)
    /// * `subscriptions` - Path patterns to subscribe to (e.g., `["**"]` for all paths)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The WebSocket connection fails
    /// - The handshake times out (10 seconds)
    /// - The server closes the connection before completing the handshake
    pub async fn connect(url: &str, subscriptions: Vec<String>) -> Result<Self> {
        let client_id = format!("rust-client-{}", Uuid::new_v4());
        Self::connect_with_id(url, &client_id, subscriptions).await
    }

    /// Connect to a SwirlDB server with a specific client ID.
    ///
    /// Useful for testing or when you need a deterministic client identity.
    ///
    /// # Arguments
    ///
    /// * `url` - WebSocket URL of the SwirlDB server
    /// * `client_id` - A specific client identifier to use
    /// * `subscriptions` - Path patterns to subscribe to
    pub async fn connect_with_id(
        url: &str,
        client_id: &str,
        subscriptions: Vec<String>,
    ) -> Result<Self> {
        let client_id = client_id.to_string();
        let db = Arc::new(RwLock::new(SwirlDB::new()));

        let (ws_stream, _) = connect_async(url).await?;
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        // Channel for sending messages to the WebSocket from any thread
        let (ws_tx, mut ws_rx) = mpsc::channel::<Vec<u8>>(256);

        // Ephemeral broadcast (capacity 100 for backpressure)
        let (ephemeral_tx, _) = broadcast::channel(100);

        // Change notification broadcast
        let (change_tx, _) = broadcast::channel(100);

        // Send Connect message
        let heads = {
            let db_read = db.read().await;
            let heads = db_read.get_heads();
            heads.into_iter().flatten().collect::<Vec<u8>>()
        };

        let connect_msg = Message::Connect {
            client_id: client_id.clone(),
            subscriptions: subscriptions.clone(),
            heads,
        };
        ws_sender
            .send(WsMessage::Binary(connect_msg.encode()))
            .await?;

        // Wait for SubscribeAck (with timeout)
        timeout(HANDSHAKE_TIMEOUT, async {
            loop {
                if let Some(msg) = ws_receiver.next().await {
                    if let WsMessage::Binary(data) = msg? {
                        match Message::decode(&data)? {
                            Message::SubscribeAck { added, denied } => {
                                info!(
                                    "SubscribeAck: {} added, {} denied",
                                    added.len(),
                                    denied.len()
                                );
                                return Ok::<(), anyhow::Error>(());
                            }
                            Message::Error { message } => {
                                anyhow::bail!("Server error during handshake: {}", message);
                            }
                            Message::Ping => {
                                ws_sender
                                    .send(WsMessage::Binary(Message::Pong.encode()))
                                    .await?;
                            }
                            other => {
                                warn!(
                                    "Unexpected message during SubscribeAck wait: {:?}",
                                    std::mem::discriminant(&other)
                                );
                            }
                        }
                    }
                } else {
                    anyhow::bail!("Connection closed before SubscribeAck");
                }
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("Timed out waiting for SubscribeAck"))??;

        // Wait for initial Sync (with timeout)
        timeout(HANDSHAKE_TIMEOUT, async {
            loop {
                if let Some(msg) = ws_receiver.next().await {
                    if let WsMessage::Binary(data) = msg? {
                        match Message::decode(&data)? {
                            Message::Sync { heads: _, changes } => {
                                if !changes.is_empty() {
                                    let db_write = db.write().await;
                                    db_write.apply_changes(changes)?;
                                    info!("Applied initial sync changes");
                                }
                                return Ok::<(), anyhow::Error>(());
                            }
                            Message::Error { message } => {
                                anyhow::bail!("Server error during handshake: {}", message);
                            }
                            Message::Ping => {
                                ws_sender
                                    .send(WsMessage::Binary(Message::Pong.encode()))
                                    .await?;
                            }
                            other => {
                                warn!(
                                    "Unexpected message during Sync wait: {:?}",
                                    std::mem::discriminant(&other)
                                );
                            }
                        }
                    }
                } else {
                    anyhow::bail!("Connection closed before Sync");
                }
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("Timed out waiting for initial Sync"))??;

        info!("SyncClient {} connected", client_id);

        // Spawn background task for receive loop and send forwarding
        let db_clone = Arc::clone(&db);
        let ephemeral_tx_clone = ephemeral_tx.clone();
        let change_tx_clone = change_tx.clone();

        let task_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Forward outgoing messages to WebSocket
                    msg_to_send = ws_rx.recv() => {
                        match msg_to_send {
                            Some(data) => {
                                if let Err(e) = ws_sender.send(WsMessage::Binary(data)).await {
                                    error!("Failed to send WebSocket message: {}", e);
                                    break;
                                }
                            }
                            None => {
                                // All senders dropped (SyncClient dropped), close WebSocket
                                match timeout(Duration::from_secs(5), ws_sender.close()).await {
                                    Ok(Err(e)) => warn!("WebSocket close error: {}", e),
                                    Err(_) => warn!("WebSocket close timed out after 5s"),
                                    _ => {}
                                }
                                break;
                            }
                        }
                    }

                    // Receive incoming messages from WebSocket
                    msg = ws_receiver.next() => {
                        match msg {
                            Some(Ok(WsMessage::Binary(data))) => {
                                match Message::decode(&data) {
                                    Ok(Message::Broadcast { from_client_id: _, changes, affected_paths }) => {
                                        if !changes.is_empty() {
                                            let db_write = db_clone.write().await;
                                            if let Err(e) = db_write.apply_changes(changes) {
                                                error!("Failed to apply broadcast changes: {}", e);
                                            }
                                        }
                                        let _ = change_tx_clone.send(affected_paths);
                                    }
                                    Ok(Message::PushAck { heads: _ }) => {
                                        // Push acknowledged
                                    }
                                    Ok(Message::Ephemeral { path, data }) => {
                                        let _ = ephemeral_tx_clone.send(vec![(path, data)]);
                                    }
                                    Ok(Message::EphemeralBatch { updates }) => {
                                        let _ = ephemeral_tx_clone.send(updates);
                                    }
                                    Ok(Message::Ping) => {
                                        let pong = Message::Pong.encode();
                                        if let Err(e) = ws_sender.send(WsMessage::Binary(pong)).await {
                                            error!("Failed to send pong: {}", e);
                                            break;
                                        }
                                    }
                                    Ok(Message::Error { message }) => {
                                        error!("Server error: {}", message);
                                    }
                                    Ok(_) => {}
                                    Err(e) => {
                                        warn!("Failed to decode message: {}", e);
                                    }
                                }
                            }
                            Some(Ok(WsMessage::Close(_))) | None => {
                                info!("WebSocket connection closed");
                                break;
                            }
                            Some(Err(e)) => {
                                error!("WebSocket error: {}", e);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        Ok(Self {
            client_id,
            db,
            ws_tx,
            ephemeral_tx,
            change_tx,
            _task_handle: task_handle,
            subscriptions,
        })
    }

    /// Get the client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Get a value from the local CRDT database at the given dot-notation path.
    ///
    /// Returns `None` if the path doesn't exist in the local database.
    pub async fn get_path(&self, path: &str) -> Option<ScalarValue> {
        let db = self.db.read().await;
        db.get_path(path)
    }

    /// Set a value at the given path and push the change to the server.
    ///
    /// The change is first applied to the local CRDT database, then sent
    /// to the server as a `Push` message. The server will broadcast the
    /// change to other subscribers.
    ///
    /// # Errors
    ///
    /// Returns an error if the path is invalid, the CRDT operation fails,
    /// or the WebSocket send channel is closed.
    pub async fn set_path(&self, path: &str, value: ScalarValue) -> Result<()> {
        let changes = {
            let db = self.db.write().await;
            db.set_path(path, value)?;
            let changes = db.get_changes();
            let heads = db.get_heads();
            let heads_bytes: Vec<u8> = heads.into_iter().flatten().collect();
            Message::Push {
                heads: heads_bytes,
                changes,
            }
            .encode()
        };

        self.ws_tx
            .send(changes)
            .await
            .map_err(|_| anyhow::anyhow!("WebSocket send channel closed"))?;

        Ok(())
    }

    /// Send an ephemeral message to subscribers matching the given path.
    ///
    /// Ephemeral messages bypass CRDT and storage entirely — they are
    /// pure pub/sub for high-frequency real-time data like cursor positions,
    /// DMX lighting values, or beat sync data.
    ///
    /// # Arguments
    ///
    /// * `path` - Dot-notation path that determines which subscribers receive the message
    /// * `data` - Arbitrary binary payload
    pub async fn send_ephemeral(&self, path: &str, data: &[u8]) -> Result<()> {
        let msg = Message::Ephemeral {
            path: path.to_string(),
            data: data.to_vec(),
        };
        self.ws_tx
            .send(msg.encode())
            .await
            .map_err(|_| anyhow::anyhow!("WebSocket send channel closed"))?;
        Ok(())
    }

    /// Send a batch of ephemeral messages atomically.
    ///
    /// More efficient than sending individual ephemeral messages when you
    /// have multiple updates to send at once (e.g., updating all fixture
    /// colors in a single frame).
    ///
    /// # Arguments
    ///
    /// * `updates` - Slice of (path, data) tuples to send
    pub async fn send_ephemeral_batch(&self, updates: &[(&str, &[u8])]) -> Result<()> {
        let msg = Message::EphemeralBatch {
            updates: updates
                .iter()
                .map(|(path, data)| (path.to_string(), data.to_vec()))
                .collect(),
        };
        self.ws_tx
            .send(msg.encode())
            .await
            .map_err(|_| anyhow::anyhow!("WebSocket send channel closed"))?;
        Ok(())
    }

    /// Subscribe to incoming ephemeral messages.
    ///
    /// Returns a broadcast receiver that yields batches of `(path, data)` updates.
    /// Each batch corresponds to a single `Ephemeral` or `EphemeralBatch` message
    /// from another client.
    pub fn on_ephemeral(&self) -> broadcast::Receiver<Vec<(String, Vec<u8>)>> {
        self.ephemeral_tx.subscribe()
    }

    /// Subscribe to CRDT change notifications.
    ///
    /// Returns a broadcast receiver that yields lists of affected paths
    /// whenever a `Broadcast` message is received from the server.
    pub fn on_change(&self) -> broadcast::Receiver<Vec<String>> {
        self.change_tx.subscribe()
    }

    /// Get a read lock on the underlying SwirlDB instance.
    ///
    /// Use this for advanced queries against the local CRDT state.
    pub async fn db(&self) -> tokio::sync::RwLockReadGuard<'_, SwirlDB> {
        self.db.read().await
    }
}
