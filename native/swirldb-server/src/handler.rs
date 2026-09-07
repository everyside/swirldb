// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! WebSocket handler for the SwirlDB sync protocol.
//!
//! One connection, one client, any number of open documents. `Connect`
//! registers the client and opens the document it names; `Open` opens more;
//! `Close` drops one. Every `Push`, `Broadcast` and ephemeral frame names its
//! document, and the server routes each to that document's subscribers only.
//!
//! Who the connection *is* comes from the bearer token it presented on the
//! WebSocket upgrade, verified by the server's authority at `Connect`. The
//! `client_id` in `Connect` names the connection for routing; it is not
//! believed about anything else.
//!
//! Shared between the production server and integration test infrastructure.

use crate::state::{BroadcastMessage, ControlMessage, EphemeralMessage, OpenError, ServerState};
use axum::extract::ws::{Message as WsMessage, WebSocket};
use axum::http::{header::AUTHORIZATION, HeaderMap};
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use swirldb_core::protocol::Message;
use tracing::{error, info, warn};
use uuid::Uuid;

/// The bearer token a WebSocket upgrade carried, if any.
///
/// The `Authorization: Bearer …` header is the right instrument: it is what
/// every HTTP client sends credentials in, and it never appears in a URL, so
/// it stays out of access logs, proxy logs and browser history. A browser
/// cannot set it — the WebSocket constructor takes a URL and nothing else —
/// so a `token` query parameter is accepted as well, and that is what the
/// browser bindings send. The header wins when both are present.
pub fn bearer_token(headers: &HeaderMap, query: &HashMap<String, String>) -> Option<String> {
    let from_header = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty());
    from_header.or_else(|| {
        query
            .get("token")
            .map(|token| token.trim().to_string())
            .filter(|token| !token.is_empty())
    })
}

/// Size of an Automerge change hash (SHA-256).
const AUTOMERGE_HEAD_SIZE: usize = 32;

type Sender = SplitSink<WebSocket, WsMessage>;

/// Split a flat run of heads into 32-byte hashes. A run that is not a whole
/// number of hashes is malformed and treated as no heads, which means a full
/// sync rather than a wrong delta.
fn parse_heads(flat: &[u8]) -> Vec<Vec<u8>> {
    if !flat.len().is_multiple_of(AUTOMERGE_HEAD_SIZE) {
        if !flat.is_empty() {
            warn!(
                "Malformed heads: length {} is not a multiple of {}",
                flat.len(),
                AUTOMERGE_HEAD_SIZE
            );
        }
        return Vec::new();
    }
    flat.chunks(AUTOMERGE_HEAD_SIZE)
        .map(|chunk| chunk.to_vec())
        .collect()
}

async fn send(sender: &mut Sender, message: Message) -> bool {
    if let Err(e) = sender.send(WsMessage::Binary(message.encode())).await {
        error!(
            "Failed to send {:?}: {}",
            std::mem::discriminant(&message),
            e
        );
        return false;
    }
    true
}

/// Open a document for this connection and answer the client: `SubscribeAck`
/// then `Sync` with the history it lacks, or `OpenDenied`. Returns false only
/// when the socket itself failed.
async fn open_and_answer(
    state: &ServerState,
    sender: &mut Sender,
    connection_id: Uuid,
    client_id: &str,
    document: &str,
    subscriptions: Vec<String>,
    heads: &[u8],
) -> bool {
    let opened = match state
        .open_document(&connection_id, document, subscriptions)
        .await
    {
        Ok(opened) => opened,
        Err(OpenError::Refused { subject, .. }) => {
            info!("🚫 {} refused {} on {}", subject, document, client_id);
            return send(
                sender,
                Message::OpenDenied {
                    document: document.to_string(),
                    reason: format!("{} may not open {}", subject, document),
                },
            )
            .await;
        }
        Err(OpenError::NotConnected) => {
            return send(
                sender,
                Message::Error {
                    message: "Connect before opening a document".to_string(),
                },
            )
            .await;
        }
    };

    if !opened.denied.is_empty() {
        warn!(
            "{} subscriptions on {} denied by policy",
            opened.denied.len(),
            document
        );
    }

    if !send(
        sender,
        Message::SubscribeAck {
            added: opened.added,
            denied: opened.denied,
            document: document.to_string(),
            access: opened.access,
        },
    )
    .await
    {
        return false;
    }

    let (server_heads, changes) = {
        let db = state.document(document).await;
        let db = db.read().await;
        let client_heads = parse_heads(heads);
        let changes = if client_heads.is_empty() {
            db.get_changes()
        } else {
            db.get_changes_since(&client_heads)
        };
        (db.get_heads(), changes)
    };

    let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
    let sync_mode = if heads.is_empty() { "full" } else { "delta" };
    info!(
        "📤 SEND [{}]: {} changes ({} bytes, {}) to {}",
        document,
        changes.len(),
        total_bytes,
        sync_mode,
        client_id
    );

    send(
        sender,
        Message::Sync {
            heads: server_heads.into_iter().flatten().collect(),
            changes,
            document: document.to_string(),
        },
    )
    .await
}

/// Handle an individual WebSocket connection with the SwirlDB sync protocol.
///
/// Manages the full lifecycle: Connect handshake, further opens, Push/Broadcast
/// relay, ephemeral pub/sub, and cleanup on disconnect. `token` is what
/// [`bearer_token`] found on the upgrade; the authority decides who it is.
pub async fn handle_websocket(socket: WebSocket, state: ServerState, token: Option<String>) {
    let connection_id = Uuid::new_v4();
    let (mut sender, mut receiver) = socket.split();

    info!("New WebSocket connection: {}", connection_id);

    let mut client_info: Option<String> = None;
    let mut broadcast_rx: Option<tokio::sync::broadcast::Receiver<BroadcastMessage>> = None;
    let mut ephemeral_rx: Option<tokio::sync::broadcast::Receiver<EphemeralMessage>> = None;
    let mut control_rx: Option<tokio::sync::broadcast::Receiver<ControlMessage>> = None;

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

                        match Message::decode(&data) {
                            Ok(Message::Connect { client_id, subscriptions, heads, document }) => {
                                info!("📱 Client {} connected, opening {} ({} subscriptions)",
                                      client_id, document, subscriptions.len());

                                // The subject is the authority's answer about the
                                // token, never the client's own claim. A connection
                                // the authority does not know is told so on the
                                // document it asked for, and closed.
                                let Some(actor) = state.authenticate(token.as_deref(), &client_id).await else {
                                    warn!("🚫 {} is not authenticated; closing", client_id);
                                    send(&mut sender, Message::OpenDenied {
                                        document,
                                        reason: "not authenticated".to_string(),
                                    }).await;
                                    break;
                                };
                                info!("🔑 {} is {} ({:?})", client_id, actor.id, actor.actor_type);

                                state.register_client(
                                    connection_id,
                                    client_id.clone(),
                                    actor,
                                    "WebSocket".to_string()
                                ).await;

                                client_info = Some(client_id.clone());
                                broadcast_rx = Some(state.subscribe_to_broadcasts());
                                ephemeral_rx = Some(state.subscribe_to_ephemeral());
                                control_rx = Some(state.subscribe_to_control());

                                if !open_and_answer(
                                    &state, &mut sender, connection_id, &client_id,
                                    &document, subscriptions, &heads,
                                ).await {
                                    break;
                                }
                            }

                            Ok(Message::Open { document, subscriptions, heads }) => {
                                let Some(client_id) = client_info.clone() else {
                                    if !send(&mut sender, Message::Error {
                                        message: "Connect before opening a document".to_string(),
                                    }).await {
                                        break;
                                    }
                                    continue;
                                };
                                info!("📂 {} opens {} ({} subscriptions)",
                                      client_id, document, subscriptions.len());
                                if !open_and_answer(
                                    &state, &mut sender, connection_id, &client_id,
                                    &document, subscriptions, &heads,
                                ).await {
                                    break;
                                }
                            }

                            Ok(Message::Close { document }) => {
                                if client_info.is_some() {
                                    state.close_document(&connection_id, &document).await;
                                }
                            }

                            Ok(Message::Subscribe { add, remove, document }) => {
                                if let Some(client_id) = &client_info {
                                    let Some(access) = state.document_access(&connection_id, &document) else {
                                        if !send(&mut sender, Message::Error {
                                            message: format!("{} is not open on this connection", document),
                                        }).await {
                                            break;
                                        }
                                        continue;
                                    };
                                    match state.update_subscriptions(client_id, &document, add, remove).await {
                                        Ok((added, denied)) => {
                                            if !send(&mut sender, Message::SubscribeAck {
                                                added, denied, document, access,
                                            }).await {
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            if !send(&mut sender, Message::Error { message: e.to_string() }).await {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            Ok(Message::Push { heads: _client_heads, changes, document }) => {
                                if let Some(client_id) = &client_info {
                                    let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
                                    info!("📥 RECV [{}]: {} changes ({} bytes) from {}",
                                        document, changes.len(), total_bytes, client_id);

                                    // A push on a document this connection may not
                                    // write is refused before anything is decoded
                                    // against it.
                                    if !state.document_access(&connection_id, &document)
                                        .is_some_and(|access| access.may_write())
                                    {
                                        warn!("{} may not write {}", client_id, document);
                                        if !send(&mut sender, Message::Error {
                                            message: format!("{} may not write {}", client_id, document),
                                        }).await {
                                            break;
                                        }
                                        continue;
                                    }

                                    let affected_paths = {
                                        let db = state.document(&document).await;
                                        let db = db.read().await;
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
                                            &document,
                                            changes,
                                            affected_paths,
                                        )
                                        .await
                                    {
                                        Ok(_) => {
                                            let server_heads: Vec<u8> = {
                                                let db = state.document(&document).await;
                                                let db = db.read().await;
                                                db.get_heads().into_iter().flatten().collect()
                                            };
                                            if !send(&mut sender, Message::PushAck {
                                                heads: server_heads,
                                                document,
                                            }).await {
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to apply changes: {}", e);
                                            if !send(&mut sender, Message::Error { message: e.to_string() }).await {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            Ok(Message::Ping) => {
                                if !send(&mut sender, Message::Pong).await {
                                    break;
                                }
                            }

                            Ok(Message::Pong) => {
                                // Heartbeat response, ignore
                            }

                            Ok(Message::Ephemeral { path, data, document }) => {
                                if let Some(client_id) = &client_info {
                                    if let Err(e) = state.route_ephemeral(
                                        client_id.clone(),
                                        connection_id,
                                        &document,
                                        vec![(path, data)],
                                    ).await {
                                        warn!("Ephemeral not routed: {}", e);
                                    }
                                }
                            }

                            Ok(Message::EphemeralBatch { updates, document }) => {
                                if let Some(client_id) = &client_info {
                                    if let Err(e) = state.route_ephemeral(
                                        client_id.clone(),
                                        connection_id,
                                        &document,
                                        updates,
                                    ).await {
                                        warn!("Ephemeral batch not routed: {}", e);
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

                        // Only to clients subscribed on that document
                        if let Some(ref client_id) = client_info {
                            if !msg.target_clients.contains(client_id) {
                                continue;
                            }

                            let broadcast_msg = Message::Broadcast {
                                from_client_id: msg.from_client_id,
                                changes: msg.changes,
                                affected_paths: msg.affected_paths,
                                document: msg.document,
                            };

                            if !send(&mut sender, broadcast_msg).await {
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

            // A word from the server about this connection
            control = async {
                match &mut control_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match control {
                    Ok(ControlMessage::Revoked { connection, document }) => {
                        if connection != connection_id {
                            continue;
                        }
                        // The server has already dropped the document from
                        // this connection; the client is told why. The socket
                        // stays: it may open something else.
                        if !send(&mut sender, Message::OpenDenied {
                            document,
                            reason: "revoked".to_string(),
                        }).await {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Client {} lagged by {} control messages", connection_id, n);
                    }
                    Err(e) => {
                        error!("Control receive error: {}", e);
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

                        // Only to targeted clients
                        if let Some(ref client_id) = client_info {
                            if !msg.target_clients.contains(client_id) {
                                continue;
                            }

                            // Send as EphemeralBatch (even for single updates, batch is superset)
                            let ephemeral_msg = Message::EphemeralBatch {
                                updates: msg.updates,
                                document: msg.document,
                            };

                            if !send(&mut sender, ephemeral_msg).await {
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
