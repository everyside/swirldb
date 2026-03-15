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
use anyhow::Result;
use axum::{
    extract::{ws::WebSocketUpgrade, State as AxumState},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::get,
    Json, Router,
};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{env, fs, io::BufReader, time::Duration};
use swirldb_core::policy::{Remote, Transport};
use swirldb_core::protocol::Message;
use swirldb_core::storage::{DocumentStorage, InMemoryDocStorage};
use swirldb_server::storage::RedbAdapter;
use swirldb_server::ServerState;
use tokio::time::interval;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG")
                .unwrap_or_else(|_| "swirldb_server=info,tower_http=debug".to_string()),
        )
        .init();

    info!("🚀 SwirlDB Sync Server starting...");

    // Parse configuration from environment
    let ws_port = env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3030);

    let _http_port = env::var("HTTP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3031);

    // TODO: Load policy from config file
    let policy = None;

    // Parse storage configuration from environment
    let storage_type = env::var("STORAGE_TYPE").unwrap_or_else(|_| "redb".to_string());
    let storage_path =
        env::var("STORAGE_PATH").unwrap_or_else(|_| "./data/swirldb.redb".to_string());

    let storage: Arc<dyn DocumentStorage> = match storage_type.as_str() {
        "memory" => {
            info!("Using in-memory storage (volatile - data lost on restart)");
            Arc::new(InMemoryDocStorage::new())
        }
        "redb" => {
            // Ensure the parent directory exists
            if let Some(parent) = std::path::Path::new(&storage_path).parent() {
                fs::create_dir_all(parent)?;
            }
            info!("Using redb persistent storage at: {}", storage_path);
            Arc::new(RedbAdapter::new(&storage_path)?)
        }
        other => {
            anyhow::bail!(
                "Unknown STORAGE_TYPE '{}'. Valid values: memory, redb",
                other
            );
        }
    };

    // Create server state with storage
    let server_state = ServerState::new(policy, storage).await;

    // Load optional config file for remotes
    let remotes = load_remotes_config();

    // Spawn peer connections for auto_connect remotes
    for remote in &remotes {
        if remote.auto_connect {
            match remote.transport {
                Transport::WebSocket => {
                    info!(
                        "Connecting to remote peer '{}' at {}",
                        remote.name, remote.endpoint
                    );
                    tokio::spawn(connect_to_peer(
                        server_state.clone(),
                        remote.name.clone(),
                        remote.endpoint.clone(),
                        remote.path_patterns.clone(),
                    ));
                }
                _ => {
                    warn!(
                        "Remote '{}' uses non-WebSocket transport, skipping",
                        remote.name
                    );
                }
            }
        }
    }

    // Build axum router
    let app = Router::new()
        .route("/ws", get(websocket_handler))
        .route("/health", get(health_handler))
        .route("/stats", get(stats_handler))
        .route("/admin/events", get(admin_sse_handler))
        .layer(CorsLayer::permissive())
        .with_state(server_state.clone());

    // Spawn heartbeat task
    tokio::spawn(heartbeat_task(server_state.clone()));

    // Start mDNS discovery if the feature is enabled
    #[cfg(feature = "mdns")]
    {
        tokio::spawn(start_mdns_discovery(server_state.clone(), ws_port));
    }

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
            info!(
                "📊 HTTPS endpoints available on https://localhost:{}",
                ws_port
            );
            info!("✅ Server ready for secure connections");

            axum_server::bind_rustls(addr, tls_config)
                .serve(app.into_make_service())
                .await?;
        }
        _ => {
            // No TLS - start plain HTTP/WS server
            info!("⚠️  Starting server WITHOUT TLS (development mode)");
            info!(
                "   Set TLS_CERT_PATH and TLS_KEY_PATH environment variables to enable HTTPS/WSS"
            );

            info!("🌐 WebSocket server listening on ws://{}", addr);
            info!(
                "📊 HTTP endpoints available on http://localhost:{}",
                ws_port
            );
            info!("✅ Server ready for connections");

            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
        }
    }

    Ok(())
}

/// Load remotes configuration from a config file.
///
/// Reads the file path from the `CONFIG_PATH` environment variable.
/// Supports two formats:
/// - Full `SwirlDBConfig` (with `policies` and `remotes` fields)
/// - Partial config with just a top-level `remotes` array
fn load_remotes_config() -> Vec<Remote> {
    use swirldb_core::policy::SwirlDBConfig;

    let config_path = match env::var("CONFIG_PATH") {
        Ok(path) => path,
        Err(_) => return Vec::new(),
    };

    let content = match fs::read_to_string(&config_path) {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to read config file {}: {}", config_path, e);
            return Vec::new();
        }
    };

    // Try full SwirlDBConfig first
    if let Ok(config) = serde_json::from_str::<SwirlDBConfig>(&content) {
        info!(
            "Loaded {} remotes from config (SwirlDBConfig format)",
            config.remotes.len()
        );
        return config.remotes;
    }

    // Try partial config with just remotes at top level
    #[derive(serde::Deserialize)]
    struct PartialConfig {
        #[serde(default)]
        remotes: Vec<Remote>,
    }

    match serde_json::from_str::<PartialConfig>(&content) {
        Ok(partial) => {
            info!(
                "Loaded {} remotes from config (partial format)",
                partial.remotes.len()
            );
            partial.remotes
        }
        Err(e) => {
            warn!("Failed to parse config file {}: {}", config_path, e);
            Vec::new()
        }
    }
}

/// Load TLS configuration from certificate and key files
fn load_tls_config(
    cert_path: &str,
    key_path: &str,
) -> Result<axum_server::tls_rustls::RustlsConfig> {
    use rustls::pki_types::CertificateDer;

    // Read certificate file
    let cert_file = fs::File::open(cert_path)?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer> =
        rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;

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

    Ok(axum_server::tls_rustls::RustlsConfig::from_config(
        Arc::new(config),
    ))
}

/// WebSocket upgrade handler
async fn websocket_handler(
    ws: WebSocketUpgrade,
    AxumState(state): AxumState<ServerState>,
) -> Response {
    ws.on_upgrade(|socket| swirldb_server::handler::handle_websocket(socket, state))
}

/// Health check endpoint
async fn health_handler() -> &'static str {
    "OK"
}

/// Stats endpoint (legacy, kept for compatibility)
async fn stats_handler(AxumState(state): AxumState<ServerState>) -> impl IntoResponse {
    let stats = state.get_stats().await;
    Json(stats)
}

/// Admin SSE endpoint - streams stats, connections, subscriptions, and activity
async fn admin_sse_handler(
    AxumState(state): AxumState<ServerState>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        let mut ticker = interval(Duration::from_secs(2));
        loop {
            ticker.tick().await;

            let stats = state.get_stats().await;
            let connections = state.get_connections().await;
            let subscriptions = state.get_subscriptions().await;
            let activity = state.get_activity().await;

            let payload = serde_json::json!({
                "stats": stats,
                "connections": connections,
                "subscriptions": subscriptions,
                "activity": activity,
            });

            yield Ok(Event::default().data(payload.to_string()));
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Heartbeat task - sends pings and cleans up stale state
async fn heartbeat_task(state: ServerState) {
    let mut ticker = interval(Duration::from_secs(30));

    loop {
        ticker.tick().await;
        info!(
            "Heartbeat tick - {} active connections",
            state.get_connection_count()
        );
        // Prune stale ephemeral_seen entries (older than 1 hour)
        state.cleanup_stale_ephemeral_seen();
    }
}

/// Connect to a peer server as a client
///
/// Establishes a WebSocket connection to a remote SwirlDB server,
/// subscribes to all paths (or a configured subset), and forwards
/// changes bidirectionally.
pub async fn connect_to_peer(
    state: ServerState,
    peer_id: String,
    endpoint: String,
    subscriptions: Vec<String>,
) {
    let mut backoff_ms = 1000u64;
    let max_backoff_ms = 30000u64;

    loop {
        info!("Connecting to peer {} at {}", peer_id, endpoint);

        match tokio_tungstenite::connect_async(&endpoint).await {
            Ok((ws_stream, _)) => {
                backoff_ms = 1000; // Reset backoff on successful connection
                info!("Connected to peer {}", peer_id);

                let (mut ws_sender, mut ws_receiver) = futures::StreamExt::split(ws_stream);

                // Send Connect message with our server ID
                let connect_msg = Message::Connect {
                    client_id: format!("peer-{}", state.server_id()),
                    subscriptions: subscriptions.clone(),
                    heads: Vec::new(),
                };

                if let Err(e) = futures::SinkExt::send(
                    &mut ws_sender,
                    tokio_tungstenite::tungstenite::Message::Binary(connect_msg.encode()),
                )
                .await
                {
                    error!("Failed to send Connect to peer {}: {}", peer_id, e);
                    // Apply backoff before retry
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
                    continue;
                }

                // Register peer only after Connect handshake succeeds
                state.register_peer(peer_id.clone(), endpoint.clone(), subscriptions.clone());

                // Subscribe to local broadcasts to forward to peer
                let mut broadcast_rx = state.subscribe_to_broadcasts();
                let mut ephemeral_rx = state.subscribe_to_ephemeral();

                // Receive loop
                loop {
                    tokio::select! {
                        msg = futures::StreamExt::next(&mut ws_receiver) => {
                            match msg {
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(data))) => {
                                    match Message::decode(&data) {
                                        Ok(Message::Broadcast { from_client_id, changes, affected_paths }) => {
                                            // Skip if from_client_id already has peer prefix
                                            // (prevents double-application in multi-hop topologies)
                                            if from_client_id.starts_with("peer-") {
                                                continue;
                                            }
                                            if let Err(e) = state.apply_peer_changes(
                                                format!("peer-{}", peer_id),
                                                changes,
                                                affected_paths,
                                            ).await {
                                                error!("Failed to apply peer changes: {}", e);
                                            }
                                        }
                                        Ok(Message::Sync { heads: _, changes }) => {
                                            if !changes.is_empty() {
                                                let db = state.db().write().await;
                                                if let Err(e) = db.apply_changes(changes) {
                                                    error!("Failed to apply peer sync: {}", e);
                                                }
                                            }
                                        }
                                        Ok(Message::EphemeralBatch { updates }) => {
                                            // Route peer ephemeral to local subscribers
                                            if let Err(e) = state.route_ephemeral(
                                                format!("peer-{}", peer_id),
                                                Uuid::nil(),
                                                updates,
                                            ).await {
                                                error!("Failed to route peer ephemeral: {}", e);
                                            }
                                        }
                                        Ok(Message::EphemeralRelay { origin, seq, path_through, updates }) => {
                                            // Atomic check-and-claim (prevents TOCTOU race)
                                            if state.try_claim_relay(&origin, seq, &path_through) {
                                                // Route to local subscribers
                                                if let Err(e) = state.route_ephemeral(
                                                    format!("peer-{}", peer_id),
                                                    Uuid::nil(),
                                                    updates,
                                                ).await {
                                                    error!("Failed to route relayed ephemeral: {}", e);
                                                    // Release claim so retries from other peers are accepted
                                                    state.release_relay_claim(&origin, seq);
                                                }
                                            }
                                        }
                                        Ok(Message::Ping) => {
                                            let _ = futures::SinkExt::send(
                                                &mut ws_sender,
                                                tokio_tungstenite::tungstenite::Message::Binary(Message::Pong.encode()),
                                            ).await;
                                        }
                                        Ok(Message::SubscribeAck { .. }) => {}
                                        Ok(Message::PushAck { .. }) => {}
                                        _ => {}
                                    }
                                }
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) | None => {
                                    info!("Peer {} disconnected", peer_id);
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!("Peer {} WebSocket error: {}", peer_id, e);
                                    break;
                                }
                                _ => {}
                            }
                        }

                        // Forward local broadcasts to peer
                        Ok(msg) = broadcast_rx.recv() => {
                            // Don't forward messages that originated from any peer
                            // (prevents broadcast storms between peers)
                            if msg.from_client_id.starts_with("peer-") {
                                continue;
                            }

                            // Read current heads from DB so the peer can do delta sync
                            let heads = {
                                let db = state.db().read().await;
                                db.get_heads().into_iter().flatten().collect()
                            };

                            let push_msg = Message::Push {
                                heads,
                                changes: msg.changes,
                            };
                            if let Err(e) = futures::SinkExt::send(
                                &mut ws_sender,
                                tokio_tungstenite::tungstenite::Message::Binary(push_msg.encode()),
                            ).await {
                                error!("Failed to forward to peer {}: {}", peer_id, e);
                                break;
                            }
                        }

                        // Forward local ephemeral to peer as EphemeralRelay
                        Ok(msg) = ephemeral_rx.recv() => {
                            // Don't forward messages that originated from any peer
                            if msg.from_client_id.starts_with("peer-") {
                                continue;
                            }

                            let relay_msg = Message::EphemeralRelay {
                                origin: state.server_id().to_string(),
                                seq: state.next_ephemeral_seq(),
                                path_through: vec![state.server_id().to_string()],
                                updates: msg.updates,
                            };
                            if let Err(e) = futures::SinkExt::send(
                                &mut ws_sender,
                                tokio_tungstenite::tungstenite::Message::Binary(relay_msg.encode()),
                            ).await {
                                error!("Failed to forward ephemeral to peer {}: {}", peer_id, e);
                                break;
                            }
                        }
                    }
                }

                state.unregister_peer(&peer_id);
            }
            Err(e) => {
                warn!(
                    "Failed to connect to peer {} at {}: {}. Retrying in {}ms...",
                    peer_id, endpoint, e, backoff_ms
                );
            }
        }

        // Exponential backoff before retry
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
    }
}

/// Start mDNS discovery and advertisement (if mdns feature is enabled)
#[cfg(feature = "mdns")]
pub async fn start_mdns_discovery(state: ServerState, port: u16) {
    use mdns_sd::{ServiceDaemon, ServiceInfo};

    let mdns = ServiceDaemon::new().expect("Failed to create mDNS daemon");
    let service_type = "_swirldb._tcp.local.";

    // Advertise our service
    let host = format!("{}.local.", state.server_id());
    let service_info = ServiceInfo::new(
        service_type,
        state.server_id(),
        &host,
        "0.0.0.0",
        port,
        None,
    )
    .expect("Failed to create service info");

    mdns.register(service_info)
        .expect("Failed to register mDNS service");
    info!("Advertising via mDNS: {}", service_type);

    // Browse for other servers
    let receiver = mdns.browse(service_type).expect("Failed to browse mDNS");

    tokio::spawn(async move {
        loop {
            match receiver.recv_async().await {
                Ok(event) => {
                    if let mdns_sd::ServiceEvent::ServiceResolved(info) = event {
                        let peer_id = info.get_fullname().to_string();
                        // Don't connect to ourselves (check for our ID followed by mDNS separator)
                        let our_prefix = format!("{}._", state.server_id());
                        if peer_id.starts_with(&our_prefix) {
                            continue;
                        }

                        let addrs: Vec<_> = info.get_addresses().iter().collect();
                        if let Some(addr) = addrs.first() {
                            let endpoint = format!("ws://{}:{}/ws", addr, info.get_port());
                            info!("Discovered peer {} at {}", peer_id, endpoint);

                            let state_clone = state.clone();
                            tokio::spawn(connect_to_peer(
                                state_clone,
                                peer_id,
                                endpoint,
                                vec!["**".to_string()],
                            ));
                        }
                    }
                }
                Err(e) => {
                    error!("mDNS browse error: {}", e);
                    break;
                }
            }
        }
    });
}

/// Error wrapper for Axum handlers
#[allow(dead_code)]
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
