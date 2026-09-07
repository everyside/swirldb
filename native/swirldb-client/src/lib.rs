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
//! - One document per client: `connect` opens the default document,
//!   `open` names one; the server may answer read-only or refuse
//! - A bearer token on the upgrade (`open_authenticated`), which is how a
//!   server with an authority learns who the client is
//! - Path-based reads/writes via Automerge
//! - Text: `set_text` creates one, `splice_text` edits it in place, and
//!   `on_text_change` reports every edit as splices with positions
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
use swirldb_core::core::{SwirlDB, TextChange};
use swirldb_core::protocol::{Access, Message, DEFAULT_DOCUMENT};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{header::AUTHORIZATION, HeaderValue};
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

    /// The document this client is on. A `SyncClient` holds exactly one;
    /// open another for another document.
    document: String,

    /// What the server's authority granted at open.
    access: Access,

    /// Local CRDT database (thread-safe)
    db: Arc<RwLock<SwirlDB>>,

    /// Channel to send WebSocket messages from any thread
    ws_tx: mpsc::Sender<Vec<u8>>,

    /// Broadcast channel for incoming ephemeral messages
    ephemeral_tx: broadcast::Sender<Vec<(String, Vec<u8>)>>,

    /// Broadcast channel for CRDT change notifications (affected paths)
    change_tx: broadcast::Sender<Vec<String>>,

    /// Broadcast channel for edits to texts, local and remote
    text_change_tx: broadcast::Sender<TextChange>,

    /// Broadcast channel for errors the server sends back, such as a refused write
    error_tx: broadcast::Sender<String>,

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

    /// Connect and open the named document with an auto-generated client ID.
    ///
    /// # Errors
    ///
    /// Besides `connect`'s errors, fails when the server's authority refuses
    /// the document; the error carries the server's reason.
    pub async fn open(url: &str, document: &str, subscriptions: Vec<String>) -> Result<Self> {
        let client_id = format!("rust-client-{}", Uuid::new_v4());
        Self::open_with_id(url, &client_id, document, subscriptions).await
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
        Self::open_with_id(url, client_id, DEFAULT_DOCUMENT, subscriptions).await
    }

    /// Connect with a specific client ID and open the named document.
    pub async fn open_with_id(
        url: &str,
        client_id: &str,
        document: &str,
        subscriptions: Vec<String>,
    ) -> Result<Self> {
        Self::establish(url, client_id, None, document, subscriptions).await
    }

    /// Connect with a bearer token and open the named document.
    ///
    /// The token goes in the `Authorization` header of the WebSocket upgrade,
    /// and the server's authority says whose it is; that, not the client id,
    /// is the subject every `may-open` is asked about. A server with an
    /// authority refuses a connection that brings no token.
    pub async fn open_authenticated(
        url: &str,
        token: &str,
        document: &str,
        subscriptions: Vec<String>,
    ) -> Result<Self> {
        let client_id = format!("rust-client-{}", Uuid::new_v4());
        Self::establish(url, &client_id, Some(token), document, subscriptions).await
    }

    /// Connect with a specific client ID and a bearer token, and open the
    /// named document.
    pub async fn open_authenticated_with_id(
        url: &str,
        client_id: &str,
        token: &str,
        document: &str,
        subscriptions: Vec<String>,
    ) -> Result<Self> {
        Self::establish(url, client_id, Some(token), document, subscriptions).await
    }

    async fn establish(
        url: &str,
        client_id: &str,
        token: Option<&str>,
        document: &str,
        subscriptions: Vec<String>,
    ) -> Result<Self> {
        let client_id = client_id.to_string();
        let document = document.to_string();
        let db = Arc::new(RwLock::new(SwirlDB::new()));

        let mut request = url.into_client_request()?;
        if let Some(token) = token {
            request.headers_mut().insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", token))
                    .map_err(|_| anyhow::anyhow!("The token is not a valid header value"))?,
            );
        }
        let (ws_stream, _) = connect_async(request).await?;
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        // Channel for sending messages to the WebSocket from any thread
        let (ws_tx, mut ws_rx) = mpsc::channel::<Vec<u8>>(256);

        // Ephemeral broadcast (capacity 100 for backpressure)
        let (ephemeral_tx, _) = broadcast::channel(100);

        // Change notification broadcast
        let (change_tx, _) = broadcast::channel(100);

        // Text edits, one message per text per batch of changes
        let (text_change_tx, _) = broadcast::channel(256);

        // Server errors (a refused write, an unknown document)
        let (error_tx, _) = broadcast::channel(16);

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
            document: document.clone(),
        };
        ws_sender
            .send(WsMessage::Binary(connect_msg.encode()))
            .await?;

        // Wait for SubscribeAck, or OpenDenied (with timeout)
        let access = timeout(HANDSHAKE_TIMEOUT, async {
            loop {
                if let Some(msg) = ws_receiver.next().await {
                    if let WsMessage::Binary(data) = msg? {
                        match Message::decode(&data)? {
                            Message::SubscribeAck {
                                added,
                                denied,
                                access,
                                ..
                            } => {
                                info!(
                                    "SubscribeAck on {}: {:?}, {} added, {} denied",
                                    document,
                                    access,
                                    added.len(),
                                    denied.len()
                                );
                                return Ok::<Access, anyhow::Error>(access);
                            }
                            Message::OpenDenied { document, reason } => {
                                anyhow::bail!("Refused to open {}: {}", document, reason);
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
                            Message::Sync {
                                heads: _, changes, ..
                            } => {
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

        info!("SyncClient {} connected on {}", client_id, document);

        // Spawn background task for receive loop and send forwarding
        let db_clone = Arc::clone(&db);
        let ephemeral_tx_clone = ephemeral_tx.clone();
        let change_tx_clone = change_tx.clone();
        let text_change_tx_clone = text_change_tx.clone();
        let error_tx_clone = error_tx.clone();
        let own_document = document.clone();

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
                                    // One connection carries one document here, so
                                    // anything about another is not ours.
                                    Ok(Message::Broadcast { document, .. })
                                    | Ok(Message::Ephemeral { document, .. })
                                    | Ok(Message::EphemeralBatch { document, .. })
                                        if document != own_document =>
                                    {
                                        warn!("Ignoring traffic for {}; this client is on {}", document, own_document);
                                    }
                                    Ok(Message::Broadcast { from_client_id: _, changes, affected_paths, document: _ }) => {
                                        if !changes.is_empty() {
                                            let db_write = db_clone.write().await;
                                            match db_write.apply_changes(changes) {
                                                Ok(applied) => {
                                                    for text_change in applied.text_changes {
                                                        let _ = text_change_tx_clone.send(text_change);
                                                    }
                                                }
                                                Err(e) => error!("Failed to apply broadcast changes: {}", e),
                                            }
                                        }
                                        let _ = change_tx_clone.send(affected_paths);
                                    }
                                    Ok(Message::PushAck { .. }) => {
                                        // Push acknowledged
                                    }
                                    Ok(Message::Ephemeral { path, data, document: _ }) => {
                                        let _ = ephemeral_tx_clone.send(vec![(path, data)]);
                                    }
                                    Ok(Message::EphemeralBatch { updates, document: _ }) => {
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
                                        let _ = error_tx_clone.send(message);
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
            document,
            access,
            db,
            ws_tx,
            ephemeral_tx,
            change_tx,
            text_change_tx,
            error_tx,
            _task_handle: task_handle,
            subscriptions,
        })
    }

    /// Get the client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// The document this client is on.
    pub fn document(&self) -> &str {
        &self.document
    }

    /// What the server granted: `Write`, or `Read` when pushes will be refused.
    pub fn access(&self) -> Access {
        self.access
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
                document: self.document.clone(),
            }
            .encode()
        };

        self.ws_tx
            .send(changes)
            .await
            .map_err(|_| anyhow::anyhow!("WebSocket send channel closed"))?;

        Ok(())
    }

    /// Put a text at a path, replacing whatever was there, and push it.
    ///
    /// A text merges: two clients splicing into one text both keep their
    /// characters, where two clients setting one string with `set_path`
    /// would each replace the other's. Read it back as a string with
    /// `get_path`. Replacing an existing text discards edits others are
    /// making to it, so this creates; [`Self::splice_text`] edits.
    pub async fn set_text(&self, path: &str, text: &str) -> Result<()> {
        let (frame, replaced_length) = {
            let db = self.db.write().await;
            let before = db.get_heads();
            let replaced_length = db.text_length(path).unwrap_or(0);
            db.set_text(path, text)?;
            (self.push_frame_since(&db, &before), replaced_length)
        };
        self.send_frame(frame).await?;
        let _ = self.text_change_tx.send(TextChange {
            path: path.to_string(),
            splices: vec![swirldb_core::core::TextSplice {
                position: 0,
                delete_count: replaced_length,
                insert: text.to_string(),
            }],
            local: true,
        });
        Ok(())
    }

    /// Edit the text at a path in place and push the edit: remove
    /// `delete_count` units at `position`, then insert `insert` there.
    /// Positions are in the local database's text encoding, code points by
    /// default. Fails when the path does not hold a text.
    pub async fn splice_text(
        &self,
        path: &str,
        position: usize,
        delete_count: usize,
        insert: &str,
    ) -> Result<()> {
        let frame = {
            let db = self.db.write().await;
            let before = db.get_heads();
            db.splice_text(path, position, delete_count, insert)?;
            self.push_frame_since(&db, &before)
        };
        self.send_frame(frame).await?;
        let _ = self.text_change_tx.send(TextChange {
            path: path.to_string(),
            splices: vec![swirldb_core::core::TextSplice {
                position,
                delete_count,
                insert: insert.to_string(),
            }],
            local: true,
        });
        Ok(())
    }

    /// A `Push` carrying only what happened since `before`. A text edit is
    /// one keystroke, and sending the whole history with each would make
    /// typing cost the length of everything typed so far.
    fn push_frame_since(&self, db: &SwirlDB, before: &[Vec<u8>]) -> Vec<u8> {
        let changes = if before.is_empty() {
            db.get_changes()
        } else {
            db.get_changes_since(before)
        };
        Message::Push {
            heads: db.get_heads().into_iter().flatten().collect(),
            changes,
            document: self.document.clone(),
        }
        .encode()
    }

    async fn send_frame(&self, frame: Vec<u8>) -> Result<()> {
        self.ws_tx
            .send(frame)
            .await
            .map_err(|_| anyhow::anyhow!("WebSocket send channel closed"))
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
            document: self.document.clone(),
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
            document: self.document.clone(),
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

    /// Subscribe to edits to texts.
    ///
    /// Yields one [`TextChange`] per text per batch: the splices that edited
    /// it, in order, with positions, and whether this client made them
    /// (`local`) or they arrived in a `Broadcast`.
    pub fn on_text_change(&self) -> broadcast::Receiver<TextChange> {
        self.text_change_tx.subscribe()
    }

    /// Subscribe to errors the server sends back — a push refused on a
    /// read-only document, for one.
    pub fn on_error(&self) -> broadcast::Receiver<String> {
        self.error_tx.subscribe()
    }

    /// Get a read lock on the underlying SwirlDB instance.
    ///
    /// Use this for advanced queries against the local CRDT state.
    pub async fn db(&self) -> tokio::sync::RwLockReadGuard<'_, SwirlDB> {
        self.db.read().await
    }
}
