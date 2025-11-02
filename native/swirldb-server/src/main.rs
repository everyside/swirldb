// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

/// SwirlDB Sync Server - High-performance CRDT synchronization
///
/// Features:
/// - Massively concurrent WebSocket connections
/// - HTTP long-polling fallback
/// - Pluggable storage (redb, memory, etc.)
/// - Binary protocol for minimal overhead
/// - Lock-free data structures for scalability

mod state;
mod storage;

use anyhow::Result;
use axum::{
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade}, State as AxumState,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use swirldb_core::protocol::Message;
use state::{ServerState, ServerStats};
use std::net::SocketAddr;
use std::sync::Arc;
use std::{env, fs, io::BufReader, time::Duration};
use tokio::time::interval;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG").unwrap_or_else(|_| "swirldb_server=info,tower_http=debug".to_string()),
        )
        .init();

    info!("🚀 SwirlDB Sync Server starting...");

    // Parse configuration from environment
    let ws_port = env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3030);

    let http_port = env::var("HTTP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3031);

    // TODO: Load policy from config file
    let policy = None;

    // Create server state
    let server_state = ServerState::new(policy);

    // Build axum router
    let app = Router::new()
        .route("/ws", get(websocket_handler))
        .route("/health", get(health_handler))
        .route("/stats", get(stats_handler))
        // TODO: Add HTTP sync endpoints with subscription-based protocol
        .layer(CorsLayer::permissive())
        .with_state(server_state.clone());

    // Spawn heartbeat task
    tokio::spawn(heartbeat_task(server_state.clone()));

    // Check for TLS certificate configuration
    let tls_cert_path = env::var("TLS_CERT_PATH").ok();
    let tls_key_path = env::var("TLS_KEY_PATH").ok();

    let addr = SocketAddr::from(([0, 0, 0, 0], ws_port));

    match (tls_cert_path, tls_key_path) {
        (Some(cert_path), Some(key_path)) => {
            // TLS mode - load certificates and start HTTPS/WSS server
            info!("🔒 Starting server with TLS enabled");
            info!("   Certificate: {}", cert_path);
            info!("   Private key: {}", key_path);

            let tls_config = load_tls_config(&cert_path, &key_path)?;

            info!("🌐 Secure WebSocket server listening on wss://{}", addr);
            info!("📊 HTTPS endpoints available on https://localhost:{}", ws_port);
            info!("✅ Server ready for secure connections");

            axum_server::bind_rustls(addr, tls_config)
                .serve(app.into_make_service())
                .await?;
        }
        _ => {
            // No TLS - start plain HTTP/WS server
            info!("⚠️  Starting server WITHOUT TLS (development mode)");
            info!("   Set TLS_CERT_PATH and TLS_KEY_PATH environment variables to enable HTTPS/WSS");

            info!("🌐 WebSocket server listening on ws://{}", addr);
            info!("📊 HTTP endpoints available on http://localhost:{}", ws_port);
            info!("✅ Server ready for connections");

            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
        }
    }

    Ok(())
}

/// Load TLS configuration from certificate and key files
fn load_tls_config(cert_path: &str, key_path: &str) -> Result<axum_server::tls_rustls::RustlsConfig> {
    use rustls::pki_types::CertificateDer;

    // Read certificate file
    let cert_file = fs::File::open(cert_path)?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()?;

    if certs.is_empty() {
        anyhow::bail!("No certificates found in {}", cert_path);
    }

    // Read private key file
    let key_file = fs::File::open(key_path)?;
    let mut key_reader = BufReader::new(key_file);

    // Try reading as PKCS8 first, then RSA
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| anyhow::anyhow!("No private key found in {}", key_path))?;

    // Build TLS config
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    // Enable HTTP/2 and HTTP/1.1
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(config)))
}

/// WebSocket upgrade handler
async fn websocket_handler(
    ws: WebSocketUpgrade,
    AxumState(state): AxumState<ServerState>,
) -> Response {
    ws.on_upgrade(|socket| handle_websocket(socket, state))
}

/// Handle individual WebSocket connection
async fn handle_websocket(socket: WebSocket, state: ServerState) {
    let connection_id = Uuid::new_v4();
    let (mut sender, mut receiver) = socket.split();

    info!("New WebSocket connection: {}", connection_id);

    let mut client_info: Option<String> = None;
    let mut broadcast_rx: Option<tokio::sync::broadcast::Receiver<state::BroadcastMessage>> = None;

    loop {
        tokio::select! {
            // Receive messages from client
            msg = receiver.next() => {
                match msg {
                    Some(Ok(WsMessage::Binary(data))) => {
                        // Check if this is a JSON debug frame (starts with '{')
                        if !data.is_empty() && data[0] == 0x7b {
                            // Skip debug JSON frames
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

                                // Subscribe to broadcasts
                                broadcast_rx = Some(state.subscribe_to_broadcasts());

                                // Send SubscribeAck to inform client which subscriptions were accepted
                                let sub_ack = Message::SubscribeAck { added, denied };
                                if let Err(e) = sender.send(WsMessage::Binary(sub_ack.encode())).await {
                                    error!("Failed to send subscribe ack: {}", e);
                                    break;
                                }

                                // Get current server heads and changes
                                let (server_heads, changes) = {
                                    let db = state.db().read().await;
                                    let server_heads = db.get_heads();

                                    // Parse client heads if present
                                    let changes = if heads.is_empty() {
                                        // Client has no changes, send everything
                                        db.get_changes()
                                    } else {
                                        // Parse client heads (each head is 32 bytes)
                                        let mut client_heads = Vec::new();
                                        let mut offset = 0;
                                        while offset + 32 <= heads.len() {
                                            client_heads.push(heads[offset..offset+32].to_vec());
                                            offset += 32;
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

                                    // TODO: Extract affected paths from changes
                                    // For now, use wildcard to broadcast to all
                                    let affected_paths = vec!["/**".to_string()];

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
                                                // Flatten Vec<Vec<u8>> to Vec<u8> (each head is 32 bytes)
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

                        if client_info.is_some() {
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
        }
    }

    // Cleanup
    if let Err(e) = state.unregister_client(&connection_id).await {
        error!("Failed to unregister client: {}", e);
    }
}

/// Health check endpoint
async fn health_handler() -> &'static str {
    "OK"
}

/// Stats endpoint
async fn stats_handler(AxumState(state): AxumState<ServerState>) -> Json<ServerStats> {
    let stats = state.get_stats().await;
    Json(stats)
}

// TODO: Add HTTP sync handlers with subscription-based protocol
// Old HTTP handlers were namespace-based and need to be rewritten

/// Heartbeat task - sends pings to all connected clients
async fn heartbeat_task(state: ServerState) {
    let mut ticker = interval(Duration::from_secs(30));

    loop {
        ticker.tick().await;
        info!("Heartbeat tick - {} active connections", state.get_connection_count());
    }
}

/// Error wrapper for Axum handlers
struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
