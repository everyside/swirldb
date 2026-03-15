// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! WebSocket handler for the SwirlDB sync protocol.
//!
//! Shared between the production server and integration test infrastructure.

use crate::state::{BroadcastMessage, EphemeralMessage, ServerState};
use axum::extract::ws::{Message as WsMessage, WebSocket};
use futures::{SinkExt, StreamExt};
use swirldb_core::protocol::Message;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Size of an Automerge change hash (SHA-256).
const AUTOMERGE_HEAD_SIZE: usize = 32;

/// Handle an individual WebSocket connection with the SwirlDB sync protocol.
///
/// Manages the full lifecycle: Connect handshake, Push/Broadcast relay,
/// ephemeral pub/sub, and cleanup on disconnect.
pub async fn handle_websocket(socket: WebSocket, state: ServerState) {
    let connection_id = Uuid::new_v4();
    let (mut sender, mut receiver) = socket.split();

    info!("New WebSocket connection: {}", connection_id);

    let mut client_info: Option<String> = None;
    let mut broadcast_rx: Option<tokio::sync::broadcast::Receiver<BroadcastMessage>> = None;
    let mut ephemeral_rx: Option<tokio::sync::broadcast::Receiver<EphemeralMessage>> = None;

    loop {
        tokio::select! {
            // Receive messages from client
            msg = receiver.next() => {
                match msg {
                    Some(Ok(WsMessage::Binary(data))) => {
                        // Check if this is a JSON debug frame (starts with '{')
                        if !data.is_empty() && data[0] == 0x7b {
                            if let Ok(text) = String::from_utf8(data.clone()) {
                                if text.contains("\"_debug\"") {
                                    info!("Received debug frame from client");
                                    continue;
                                }
                            }
                        }

                        // Parse binary protocol message
                        match Message::decode(&data) {
                            Ok(Message::Connect { client_id, subscriptions, heads }) => {
                                info!("📱 Client {} connected ({} subscriptions)",
                                      client_id, subscriptions.len());

                                // TODO: Extract actor from JWT token instead of using anonymous
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

                                // Register client with subscriptions
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

                                if !denied.is_empty() {
                                    warn!("{} subscriptions denied by policy", denied.len());
                                }

                                client_info = Some(client_id.clone());

                                // Subscribe to broadcasts and ephemeral
                                broadcast_rx = Some(state.subscribe_to_broadcasts());
                                ephemeral_rx = Some(state.subscribe_to_ephemeral());

                                // Send SubscribeAck
                                let sub_ack = Message::SubscribeAck { added, denied };
                                if let Err(e) = sender.send(WsMessage::Binary(sub_ack.encode())).await {
                                    error!("Failed to send subscribe ack: {}", e);
                                    break;
                                }

                                // Get current server heads and changes
                                let (server_heads, changes) = {
                                    let db = state.db().read().await;
                                    let server_heads = db.get_heads();

                                    let changes = if heads.is_empty() {
                                        // Client has no heads, send everything
                                        db.get_changes()
                                    } else if heads.len() % AUTOMERGE_HEAD_SIZE != 0 {
                                        // Malformed heads — fall back to full sync
                                        warn!(
                                            "Malformed heads: length {} is not a multiple of {}",
                                            heads.len(),
                                            AUTOMERGE_HEAD_SIZE
                                        );
                                        db.get_changes()
                                    } else {
                                        // Parse client heads (each head is 32 bytes)
                                        let mut client_heads = Vec::new();
                                        let mut offset = 0;
                                        while offset + AUTOMERGE_HEAD_SIZE <= heads.len() {
                                            client_heads.push(heads[offset..offset + AUTOMERGE_HEAD_SIZE].to_vec());
                                            offset += AUTOMERGE_HEAD_SIZE;
                                        }

                                        // Send only changes the client doesn't have
                                        db.get_changes_since(&client_heads)
                                    };

                                    (server_heads, changes)
                                };

                                // Calculate total bytes for stats
                                let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
                                let sync_mode = if heads.is_empty() { "full" } else { "delta" };
                                info!("📤 SEND: {} changes ({} bytes, {}) to {}",
                                    changes.len(), total_bytes, sync_mode, client_id);

                                // Encode server heads as flat bytes (each is 32 bytes)
                                let heads_bytes: Vec<u8> = server_heads.into_iter().flatten().collect();

                                let response = Message::Sync {
                                    heads: heads_bytes,
                                    changes
                                };

                                if let Err(e) = sender.send(WsMessage::Binary(response.encode())).await {
                                    error!("Failed to send sync: {}", e);
                                    break;
                                }
                            }

                            Ok(Message::Push { heads: _client_heads, changes }) => {
                                if let Some(client_id) = &client_info {
                                    // Calculate total bytes for stats
                                    let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
                                    info!("📥 RECV: {} changes ({} bytes) from {}",
                                        changes.len(), total_bytes, client_id);

                                    // Extract affected paths from changes
                                    let affected_paths = {
                                        let db = state.db().read().await;
                                        db.extract_affected_paths(&changes)
                                            .unwrap_or_else(|e| {
                                                warn!("Failed to extract paths: {}. Using wildcard.", e);
                                                vec!["**".to_string()]
                                            })
                                    };

                                    // Apply CRDT changes and broadcast to subscribers
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
                                            // Get server's new heads after applying changes
                                            let server_heads = {
                                                let db = state.db().read().await;
                                                let heads = db.get_heads();
                                                heads.into_iter().flatten().collect()
                                            };

                                            // Send acknowledgment with server heads
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

                            Ok(Message::Pong) => {
                                // Heartbeat response, ignore
                            }

                            Ok(Message::Ephemeral { path, data }) => {
                                if let Some(client_id) = &client_info {
                                    if let Err(e) = state.route_ephemeral(
                                        client_id.clone(),
                                        connection_id,
                                        vec![(path, data)],
                                    ).await {
                                        error!("Failed to route ephemeral: {}", e);
                                    }
                                }
                            }

                            Ok(Message::EphemeralBatch { updates }) => {
                                if let Some(client_id) = &client_info {
                                    if let Err(e) = state.route_ephemeral(
                                        client_id.clone(),
                                        connection_id,
                                        updates,
                                    ).await {
                                        error!("Failed to route ephemeral batch: {}", e);
                                    }
                                }
                            }

                            Ok(msg) => {
                                warn!("Unexpected message type: {:?}", msg);
                            }

                            Err(e) => {
                                error!("Failed to decode message: {}", e);
                            }
                        }
                    }

                    Some(Ok(WsMessage::Text(_))) => {
                        // Ignore text messages (may be debug frames)
                    }

                    Some(Ok(WsMessage::Close(_))) | None => {
                        break;
                    }

                    Some(Err(e)) => {
                        error!("WebSocket error: {}", e);
                        break;
                    }

                    _ => {}
                }
            }

            // Receive broadcasts from other clients
            broadcast = async {
                match &mut broadcast_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match broadcast {
                    Ok(msg) => {
                        // Don't send back to the sender
                        if msg.exclude_connection == Some(connection_id) {
                            continue;
                        }

                        // Only send to clients that should receive this broadcast
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
                        warn!("Client {} lagged by {} messages", connection_id, n);
                    }
                    Err(e) => {
                        error!("Broadcast receive error: {}", e);
                        break;
                    }
                }
            }

            // Receive ephemeral messages from other clients
            ephemeral = async {
                match &mut ephemeral_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match ephemeral {
                    Ok(msg) => {
                        // Don't send back to the sender
                        if msg.exclude_connection == Some(connection_id) {
                            continue;
                        }

                        // Only send to targeted clients
                        if let Some(ref client_id) = client_info {
                            if !msg.target_clients.contains(client_id) {
                                continue;
                            }

                            // Send as EphemeralBatch (even for single updates, batch is superset)
                            let ephemeral_msg = Message::EphemeralBatch {
                                updates: msg.updates,
                            };

                            if let Err(e) = sender.send(WsMessage::Binary(ephemeral_msg.encode())).await {
                                error!("Failed to send ephemeral: {}", e);
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        // Silently drop - stale frames are worse than skipped frames
                        warn!("Client {} lagged by {} ephemeral messages (dropped)", connection_id, n);
                    }
                    Err(e) => {
                        error!("Ephemeral receive error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    // Cleanup
    if let Err(e) = state.unregister_client(&connection_id).await {
        error!("Failed to unregister client: {}", e);
    }
}
