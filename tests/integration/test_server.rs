// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Test server infrastructure for integration tests
//!
//! Provides an in-process SwirlDB server on a random port for testing.

use anyhow::Result;
use axum::{
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
        State as AxumState,
    },
    response::Response,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use swirldb_core::protocol::Message;
use swirldb_core::storage::InMemoryDocStorage;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};
use uuid::Uuid;

// Re-export server state types (we'll need to access server internals)
pub use swirldb_server::state::{ServerState, BroadcastMessage};

pub struct TestServer {
    pub port: u16,
    pub state: ServerState,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    server_handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestServer {
    /// Start a new test server on a random available port
    pub async fn start() -> Result<Self> {
        Self::start_with_policy(None).await
    }

    /// Start a test server with a custom policy
    pub async fn start_with_policy(policy: Option<swirldb_core::policy::PolicyEngine>) -> Result<Self> {
        // Bind to port 0 to get a random available port
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let port = addr.port();

        let storage = Arc::new(InMemoryDocStorage::new());
        let state = ServerState::new(policy, storage).await;

        let app = Router::new()
            .route("/ws", get(websocket_handler))
            .layer(CorsLayer::permissive())
            .with_state(state.clone());

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let server_handle = tokio::spawn(async move {
            let serve = axum::serve(listener, app);
            let graceful = serve.with_graceful_shutdown(async {
                shutdown_rx.await.ok();
            });

            if let Err(e) = graceful.await {
                error!("Server error: {}", e);
            }
        });

        info!("Test server started on port {}", port);

        Ok(TestServer {
            port,
            state,
            shutdown_tx: Some(shutdown_tx),
            server_handle: Some(server_handle),
        })
    }

    /// Get the WebSocket URL for this server
    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}/ws", self.port)
    }

    /// Get the number of active connections
    pub fn connection_count(&self) -> usize {
        self.state.get_connection_count()
    }

    /// Shutdown the server
    pub async fn shutdown(mut self) -> Result<()> {
        // Take ownership and send shutdown signal
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        // Server handle will be dropped, aborting the task
        Ok(())
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Send shutdown signal if still available
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        // Abort the server task if it's still running
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }
    }
}

/// WebSocket upgrade handler (same as production server)
async fn websocket_handler(
    ws: WebSocketUpgrade,
    AxumState(state): AxumState<ServerState>,
) -> Response {
    ws.on_upgrade(|socket| handle_websocket(socket, state))
}

/// Handle individual WebSocket connection (same as production server)
async fn handle_websocket(socket: WebSocket, state: ServerState) {
    let connection_id = Uuid::new_v4();
    let (mut sender, mut receiver) = socket.split();

    let mut client_info: Option<String> = None;
    let mut broadcast_rx: Option<tokio::sync::broadcast::Receiver<BroadcastMessage>> = None;

    loop {
        tokio::select! {
            msg = receiver.next() => {
                match msg {
                    Some(Ok(WsMessage::Binary(data))) => {
                        match Message::decode(&data) {
                            Ok(Message::Connect { client_id, subscriptions, heads }) => {
                                use swirldb_core::policy::{Actor, ActorType};
                                let actor = Actor {
                                    actor_type: ActorType::Anonymous,
                                    id: client_id.clone(),
                                    org_id: None,
                                    team_id: None,
                                    app_id: None,
                                    role: None,
                                    claims: std::collections::HashMap::new(),
                                };

                                let (added, denied) = match state.register_client(
                                    connection_id,
                                    client_id.clone(),
                                    actor,
                                    subscriptions.clone(),
                                    "WebSocket".to_string()
                                ).await {
                                    Ok(result) => result,
                                    Err(e) => {
                                        error!("Failed to register client: {}", e);
                                        break;
                                    }
                                };

                                client_info = Some(client_id.clone());
                                broadcast_rx = Some(state.subscribe_to_broadcasts());

                                let sub_ack = Message::SubscribeAck { added, denied };
                                if let Err(e) = sender.send(WsMessage::Binary(sub_ack.encode())).await {
                                    error!("Failed to send subscribe ack: {}", e);
                                    break;
                                }

                                let (server_heads, changes) = {
                                    let db = state.db().read().await;
                                    let server_heads = db.get_heads();

                                    let changes = if heads.is_empty() {
                                        db.get_changes()
                                    } else {
                                        let mut client_heads = Vec::new();
                                        let mut offset = 0;
                                        while offset + 32 <= heads.len() {
                                            client_heads.push(heads[offset..offset+32].to_vec());
                                            offset += 32;
                                        }
                                        db.get_changes_since(&client_heads)
                                    };

                                    (server_heads, changes)
                                };

                                let heads_bytes: Vec<u8> = server_heads.into_iter().flatten().collect();
                                let response = Message::Sync { heads: heads_bytes, changes };

                                if let Err(e) = sender.send(WsMessage::Binary(response.encode())).await {
                                    error!("Failed to send sync: {}", e);
                                    break;
                                }
                            }

                            Ok(Message::Push { heads: _client_heads, changes }) => {
                                if let Some(client_id) = &client_info {
                                    let affected_paths = {
                                        let db = state.db().read().await;
                                        db.extract_affected_paths(&changes)
                                            .unwrap_or_else(|e| {
                                                warn!("Failed to extract paths: {}. Using wildcard.", e);
                                                vec!["**".to_string()]
                                            })
                                    };

                                    match state
                                        .apply_changes(
                                            client_id.clone(),
                                            connection_id,
                                            changes,
                                            affected_paths,
                                        )
                                        .await
                                    {
                                        Ok(_) => {
                                            let server_heads = {
                                                let db = state.db().read().await;
                                                let heads = db.get_heads();
                                                heads.into_iter().flatten().collect()
                                            };

                                            let ack = Message::PushAck { heads: server_heads };
                                            if let Err(e) = sender.send(WsMessage::Binary(ack.encode())).await {
                                                error!("Failed to send push ack: {}", e);
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to apply changes: {}", e);
                                        }
                                    }
                                }
                            }

                            Ok(Message::Ping) => {
                                let pong = Message::Pong;
                                if let Err(e) = sender.send(WsMessage::Binary(pong.encode())).await {
                                    error!("Failed to send pong: {}", e);
                                    break;
                                }
                            }

                            Ok(Message::Pong) => {}

                            Ok(msg) => {
                                warn!("Unexpected message type: {:?}", msg);
                            }

                            Err(e) => {
                                error!("Failed to decode message: {}", e);
                            }
                        }
                    }

                    Some(Ok(WsMessage::Close(_))) | None => break,
                    Some(Err(e)) => {
                        error!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }

            broadcast = async {
                match &mut broadcast_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match broadcast {
                    Ok(msg) => {
                        if msg.exclude_connection == Some(connection_id) {
                            continue;
                        }

                        // Only send to clients that should receive this broadcast (based on subscriptions)
                        if let Some(ref client_id) = client_info {
                            if !msg.target_clients.contains(client_id) {
                                continue;
                            }

                            let broadcast_msg = Message::Broadcast {
                                from_client_id: msg.from_client_id,
                                changes: msg.changes,
                                affected_paths: msg.affected_paths,
                            };

                            if let Err(e) = sender.send(WsMessage::Binary(broadcast_msg.encode())).await {
                                error!("Failed to send broadcast: {}", e);
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Client lagged by {} messages", n);
                    }
                    Err(e) => {
                        error!("Broadcast receive error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    if let Err(e) = state.unregister_client(&connection_id).await {
        error!("Failed to unregister client: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_starts_and_stops() {
        let server = TestServer::start().await.unwrap();
        assert!(server.port > 0);
        assert!(server.ws_url().starts_with("ws://127.0.0.1:"));
        server.shutdown().await.unwrap();
    }
}
