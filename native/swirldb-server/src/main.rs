/// SwirlDB Sync Server - High-performance CRDT synchronization
///
/// Features:
/// - Massively concurrent WebSocket connections
/// - HTTP long-polling fallback
/// - Pluggable storage (redb, memory, etc.)
/// - Binary protocol for minimal overhead
/// - Lock-free data structures for scalability

mod protocol;
mod state;
mod storage;

use anyhow::Result;
use axum::{
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
        Query, State as AxumState,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use protocol::Message;
use state::{ServerState, ServerStats};
use std::net::SocketAddr;
use std::{env, time::Duration};
use storage::{memory_adapter::MemoryAdapter, redb_adapter::RedbAdapter, StorageAdapter};
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

    let storage_type = env::var("STORAGE_TYPE").unwrap_or_else(|_| "redb".to_string());
    let data_dir = env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string());

    // Initialize storage adapter
    let storage: Box<dyn StorageAdapter> = match storage_type.as_str() {
        "memory" => {
            info!("Using in-memory storage (no persistence)");
            Box::new(MemoryAdapter::new())
        }
        "redb" => {
            let db_path = format!("{}/swirldb.redb", data_dir);
            info!("Using redb storage at: {}", db_path);
            let mut adapter = RedbAdapter::new(&db_path)?;
            adapter.init().await?;
            Box::new(adapter)
        }
        _ => {
            error!("Unknown storage type: {}", storage_type);
            std::process::exit(1);
        }
    };

    // Create server state
    let server_state = ServerState::new(storage);

    // Build axum router
    let app = Router::new()
        .route("/ws", get(websocket_handler))
        .route("/health", get(health_handler))
        .route("/stats", get(stats_handler))
        .route("/sync/connect", post(http_connect_handler))
        .route("/sync/poll", get(http_poll_handler))
        .route("/sync/push", post(http_push_handler))
        .layer(CorsLayer::permissive())
        .with_state(server_state.clone());

    // Spawn heartbeat task
    tokio::spawn(heartbeat_task(server_state.clone()));

    // Spawn admin stats publishing task
    tokio::spawn(admin_stats_task(server_state.clone()));

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], ws_port));
    info!("🌐 WebSocket server listening on ws://{}", addr);
    info!("📊 HTTP endpoints available on http://localhost:{}", http_port);
    info!("✅ Server ready for connections");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
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

    let mut client_info: Option<(String, String)> = None;
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
                            Ok(Message::Connect { client_id, namespace_id, heads }) => {
                                info!("Client {} connecting to namespace {} (client heads: {} bytes)",
                                      client_id, namespace_id, heads.len());

                                // Register client
                                if let Err(e) = state.register_client(connection_id, client_id.clone(), namespace_id.clone()).await {
                                    error!("Failed to register client: {}", e);
                                    break;
                                }

                                client_info = Some((client_id.clone(), namespace_id.clone()));

                                // Subscribe to namespace broadcasts
                                broadcast_rx = Some(state.subscribe_to_namespace(&namespace_id));

                                // Get current server heads
                                let server_heads = state.get_namespace_heads(&namespace_id).await;

                                // Get changes the client needs (incremental sync!)
                                let changes = if heads.is_empty() {
                                    // Client has no changes, send everything
                                    let all_changes = state.get_namespace_changes_since(&namespace_id, &[]).await;
                                    info!("Client has no heads, sending all {} changes", all_changes.len());
                                    all_changes
                                } else {
                                    // Parse client heads (each head is 32 bytes)
                                    let mut client_heads = Vec::new();
                                    let mut offset = 0;
                                    while offset + 32 <= heads.len() {
                                        client_heads.push(heads[offset..offset+32].to_vec());
                                        offset += 32;
                                    }

                                    // Send only changes the client doesn't have
                                    let delta_changes = state.get_namespace_changes_since(&namespace_id, &client_heads).await;
                                    info!("Client has {} heads, sending {} new changes", client_heads.len(), delta_changes.len());
                                    delta_changes
                                };

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

                            Ok(Message::Push { namespace_id, changes }) => {
                                if let Some((client_id, expected_room)) = &client_info {
                                    if &namespace_id != expected_room {
                                        warn!("Client sent push for wrong room");
                                        continue;
                                    }

                                    // Apply CRDT changes and broadcast
                                    match state
                                        .apply_and_broadcast(
                                            &namespace_id,
                                            client_id.clone(),
                                            Some(connection_id),
                                            changes,
                                        )
                                        .await
                                    {
                                        Ok(_) => {
                                            // Send acknowledgment
                                            let ack = Message::PushAck;
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
                        info!("Client {} disconnected", connection_id);
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

                        if let Some((_, _)) = &client_info {
                            let broadcast_msg = Message::Broadcast {
                                from_client_id: msg.from_client_id,
                                changes: msg.changes,
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

    info!("WebSocket connection closed: {}", connection_id);
}

/// Health check endpoint
async fn health_handler() -> &'static str {
    "OK"
}

/// Stats endpoint
async fn stats_handler(AxumState(state): AxumState<ServerState>) -> Result<Json<ServerStats>, AppError> {
    let stats = state.get_stats().await?;
    Ok(Json(stats))
}

/// HTTP connect endpoint (long-polling fallback)
#[derive(serde::Deserialize)]
struct ConnectRequest {
    client_id: String,
    namespace_id: String,
}

async fn http_connect_handler(
    AxumState(state): AxumState<ServerState>,
    Json(req): Json<ConnectRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    info!("HTTP connect request from client {} in namespace {}", req.client_id, req.namespace_id);

    // Record HTTP client activity
    state.record_http_activity(req.client_id.clone(), req.namespace_id.clone()).await;

    // Send complete CRDT state
    let room_state = state.get_namespace_state(&req.namespace_id).await?;

    // For chat demo compatibility: don't send minimal/empty Automerge docs
    let changes: Vec<Vec<u8>> = if room_state.len() <= 20 {
        vec![] // Empty namespace
    } else {
        vec![room_state] // Has data
    };

    Ok(Json(serde_json::json!({
        "changes": changes,
        "count": changes.len()
    })))
}

/// HTTP poll endpoint (long-polling - waits for new changes)
#[derive(serde::Deserialize)]
struct PollParams {
    client_id: String,
    namespace_id: String,
    #[serde(default = "default_timeout")]
    timeout: u64,
}

fn default_timeout() -> u64 {
    30000
}

async fn http_poll_handler(
    AxumState(state): AxumState<ServerState>,
    Query(params): Query<PollParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    info!("HTTP poll request from client {} in namespace {} (timeout: {}ms)",
          params.client_id, params.namespace_id, params.timeout);

    // Record HTTP client activity
    state.record_http_activity(params.client_id.clone(), params.namespace_id.clone()).await;

    // Subscribe to namespace broadcasts
    let mut broadcast_rx = state.subscribe_to_namespace(&params.namespace_id);

    // Park the connection and wait for broadcasts or timeout
    let timeout_duration = Duration::from_millis(params.timeout);

    match tokio::time::timeout(timeout_duration, broadcast_rx.recv()).await {
        // Received a broadcast before timeout
        Ok(Ok(msg)) => {
            info!("HTTP poll returning {} changes to client {}",
                  msg.changes.len(), params.client_id);

            Ok(Json(serde_json::json!({
                "changes": msg.changes,
                "from_client_id": msg.from_client_id,
                "count": msg.changes.len()
            })))
        }

        // Timeout - no new changes
        Ok(Err(_)) => {
            info!("HTTP poll timeout for client {}", params.client_id);
            Ok(Json(serde_json::json!({
                "changes": [],
                "count": 0
            })))
        }

        // Timeout elapsed
        Err(_) => {
            info!("HTTP poll timeout for client {} ({}ms elapsed)",
                  params.client_id, params.timeout);
            Ok(Json(serde_json::json!({
                "changes": [],
                "count": 0
            })))
        }
    }
}

/// HTTP push endpoint (long-polling fallback)
#[derive(serde::Deserialize)]
struct PushRequest {
    client_id: String,
    namespace_id: String,
    changes: Vec<Vec<u8>>,
}

async fn http_push_handler(
    AxumState(state): AxumState<ServerState>,
    Json(req): Json<PushRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    info!("HTTP push request from client {} in namespace {} ({} changes)",
          req.client_id, req.namespace_id, req.changes.len());

    // Record HTTP client activity
    state.record_http_activity(req.client_id.clone(), req.namespace_id.clone()).await;

    state
        .apply_and_broadcast(&req.namespace_id, req.client_id, None, req.changes)
        .await?;

    Ok(Json(serde_json::json!({
        "success": true
    })))
}

/// Heartbeat task - sends pings to all connected clients
async fn heartbeat_task(state: ServerState) {
    let mut ticker = interval(Duration::from_secs(30));

    loop {
        ticker.tick().await;
        info!("Heartbeat tick - {} active clients", state.get_stats().await.ok().map(|s| s.total_clients).unwrap_or(0));
    }
}

/// Admin stats publishing task - publishes server stats to __admin namespace every 2 seconds
async fn admin_stats_task(state: ServerState) {
    let mut ticker = interval(Duration::from_secs(2));

    loop {
        ticker.tick().await;

        match state.publish_admin_stats().await {
            Ok(_) => {
                // Log successful stats publication
                info!("Published admin stats");
            }
            Err(e) => {
                error!("Failed to publish admin stats: {}", e);
            }
        }
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
