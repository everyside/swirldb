/// Server state management with massive concurrency support
///
/// Design principles:
/// - Each namespace has its own SwirlDB instance (full CRDT engine)
/// - Lock-free reads where possible using Arc + DashMap
/// - Async-friendly with tokio channels for broadcasts
/// - Handles thousands of concurrent WebSocket connections
/// - Efficient namespace-based message broadcasting

use crate::protocol::Message;
use crate::storage::{Change, StorageAdapter};
use anyhow::Result;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use swirldb_core::core::SwirlDB;
use tokio::sync::{broadcast, RwLock};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Maximum number of messages to buffer per namespace broadcast channel
const BROADCAST_CHANNEL_SIZE: usize = 1000;

/// Maximum number of activity events to keep in memory
const MAX_ACTIVITY_EVENTS: usize = 100;

/// Activity event types
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActivityEvent {
    ClientConnected {
        client_id: String,
        namespace_id: String,
        transport: String,
        timestamp: i64,
    },
    ClientDisconnected {
        client_id: String,
        namespace_id: String,
        timestamp: i64,
    },
    NamespaceCreated {
        namespace_id: String,
        timestamp: i64,
    },
    NamespaceDeleted {
        namespace_id: String,
        timestamp: i64,
    },
    ChangesApplied {
        namespace_id: String,
        from_client_id: String,
        change_count: usize,
        timestamp: i64,
    },
}

/// Client connection information
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub client_id: String,
    pub namespace_id: String,
    pub connection_id: Uuid,
    pub transport: String,
    pub connected_at: i64,
    pub last_seen: i64,
}

/// Broadcast message to all clients in a namespace
#[derive(Debug, Clone)]
pub struct BroadcastMessage {
    pub from_client_id: String,
    pub changes: Vec<Vec<u8>>,
    pub exclude_connection: Option<Uuid>,
}

/// Namespace state with broadcast channel and full SwirlDB instance
///
/// A namespace is an independent SwirlDB CRDT instance that can be used for:
/// - Collaborative documents
/// - Chat rooms
/// - Game sessions
/// - Shopping carts
/// - Any multi-user data structure
pub struct Namespace {
    pub namespace_id: String,
    /// The actual CRDT database instance for this namespace
    pub db: Arc<RwLock<SwirlDB>>,
    /// Broadcast channel for real-time updates
    /// Each client subscribes to receive updates
    pub broadcast_tx: broadcast::Sender<BroadcastMessage>,
    /// Active connection count
    pub connection_count: Arc<RwLock<usize>>,
    /// Total number of changes applied to this namespace
    pub change_count: Arc<RwLock<usize>>,
    /// Timestamp of last activity in this namespace
    pub last_activity: Arc<RwLock<i64>>,
}

impl Namespace {
    fn new(namespace_id: String) -> Self {
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CHANNEL_SIZE);

        Self {
            namespace_id,
            db: Arc::new(RwLock::new(SwirlDB::new())),
            broadcast_tx,
            connection_count: Arc::new(RwLock::new(0)),
            change_count: Arc::new(RwLock::new(0)),
            last_activity: Arc::new(RwLock::new(now_timestamp())),
        }
    }

    /// Increment connection count
    pub async fn add_connection(&self) {
        let mut count = self.connection_count.write().await;
        *count += 1;
    }

    /// Decrement connection count
    pub async fn remove_connection(&self) {
        let mut count = self.connection_count.write().await;
        *count = count.saturating_sub(1);
    }

    /// Get current connection count
    pub async fn get_connection_count(&self) -> usize {
        *self.connection_count.read().await
    }

    /// Increment change count
    pub async fn add_changes(&self, count: usize) {
        let mut total = self.change_count.write().await;
        *total += count;

        // Update last activity timestamp
        let mut last = self.last_activity.write().await;
        *last = now_timestamp();
    }

    /// Get total change count
    pub async fn get_change_count(&self) -> usize {
        *self.change_count.read().await
    }

    /// Get last activity timestamp
    pub async fn get_last_activity(&self) -> i64 {
        *self.last_activity.read().await
    }
}

/// Global server state - thread-safe and highly concurrent
#[derive(Clone)]
pub struct ServerState {
    /// Namespaces indexed by namespace_id - lock-free reads
    namespaces: Arc<DashMap<String, Arc<Namespace>>>,

    /// Active clients indexed by connection_id
    clients: Arc<DashMap<Uuid, ClientInfo>>,

    /// Storage backend (pluggable)
    storage: Arc<Box<dyn StorageAdapter>>,

    /// Server start time for uptime calculation
    start_time: Arc<SystemTime>,

    /// Recent activity events (rolling buffer, limited to MAX_ACTIVITY_EVENTS)
    activity_log: Arc<RwLock<VecDeque<ActivityEvent>>>,
}

impl ServerState {
    /// Create new server state with storage adapter
    pub fn new(storage: Box<dyn StorageAdapter>) -> Self {
        Self {
            namespaces: Arc::new(DashMap::new()),
            clients: Arc::new(DashMap::new()),
            storage: Arc::new(storage),
            start_time: Arc::new(SystemTime::now()),
            activity_log: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    /// Log an activity event
    async fn log_activity(&self, event: ActivityEvent) {
        let mut log = self.activity_log.write().await;
        log.push_front(event); // Add to front (most recent first)

        // Keep only the most recent MAX_ACTIVITY_EVENTS
        if log.len() > MAX_ACTIVITY_EVENTS {
            log.truncate(MAX_ACTIVITY_EVENTS);
        }
    }

    /// Get or create a namespace (lock-free fast path for existing namespaces)
    pub fn get_or_create_namespace(&self, namespace_id: &str) -> Arc<Namespace> {
        let is_new = !self.namespaces.contains_key(namespace_id);

        let namespace = self.namespaces
            .entry(namespace_id.to_string())
            .or_insert_with(|| {
                info!("Creating new namespace: {}", namespace_id);
                Arc::new(Namespace::new(namespace_id.to_string()))
            })
            .clone();

        // Log namespace creation (async logging in background)
        if is_new && namespace_id != "__admin" {
            let self_clone = self.clone();
            let namespace_id = namespace_id.to_string();
            tokio::spawn(async move {
                self_clone.log_activity(ActivityEvent::NamespaceCreated {
                    namespace_id,
                    timestamp: now_timestamp(),
                }).await;
            });
        }

        namespace
    }

    /// Register a new client connection
    pub async fn register_client(
        &self,
        connection_id: Uuid,
        client_id: String,
        namespace_id: String,
    ) -> Result<()> {
        let namespace = self.get_or_create_namespace(&namespace_id);
        namespace.add_connection().await;

        let now = now_timestamp();
        self.clients.insert(
            connection_id,
            ClientInfo {
                client_id: client_id.clone(),
                namespace_id: namespace_id.clone(),
                connection_id,
                transport: "WebSocket".to_string(),
                connected_at: now,
                last_seen: now,
            },
        );

        info!(
            "Client registered: {} in namespace {} (connections: {})",
            connection_id,
            namespace.namespace_id,
            namespace.get_connection_count().await
        );

        // Log client connection (skip __admin to avoid recursion)
        if namespace_id != "__admin" {
            self.log_activity(ActivityEvent::ClientConnected {
                client_id,
                namespace_id,
                transport: "WebSocket".to_string(),
                timestamp: now_timestamp(),
            }).await;
        }

        Ok(())
    }

    /// Unregister a client connection
    pub async fn unregister_client(&self, connection_id: &Uuid) -> Result<()> {
        if let Some((_, client_info)) = self.clients.remove(connection_id) {
            // Log client disconnection (skip __admin to avoid recursion)
            if client_info.namespace_id != "__admin" {
                self.log_activity(ActivityEvent::ClientDisconnected {
                    client_id: client_info.client_id.clone(),
                    namespace_id: client_info.namespace_id.clone(),
                    timestamp: now_timestamp(),
                }).await;
            }

            if let Some(namespace) = self.namespaces.get(&client_info.namespace_id) {
                namespace.remove_connection().await;

                info!(
                    "Client unregistered: {} from namespace {} (connections: {})",
                    connection_id,
                    client_info.namespace_id,
                    namespace.get_connection_count().await
                );

                // Clean up empty namespaces (but never remove __admin)
                if namespace.get_connection_count().await == 0 && client_info.namespace_id != "__admin" {
                    let namespace_id = client_info.namespace_id.clone();
                    drop(namespace); // Release reference
                    self.namespaces.remove(&namespace_id);
                    info!("Namespace {} is empty, removed from memory", namespace_id);

                    // Log namespace deletion
                    self.log_activity(ActivityEvent::NamespaceDeleted {
                        namespace_id,
                        timestamp: now_timestamp(),
                    }).await;
                }
            }
        }

        Ok(())
    }

    /// Get client info
    pub fn get_client(&self, connection_id: &Uuid) -> Option<ClientInfo> {
        self.clients.get(connection_id).map(|r| r.clone())
    }

    /// Record HTTP client activity (for long-polling connections)
    /// HTTP connections are stateless, so we update last_seen instead of registering permanently
    pub async fn record_http_activity(&self, client_id: String, namespace_id: String) {
        // Create a deterministic connection ID for this HTTP client+namespace pair
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        format!("http-{}-{}", client_id, namespace_id).hash(&mut hasher);
        let hash = hasher.finish();
        let connection_id = Uuid::from_u128(hash as u128);
        let now = now_timestamp();

        // Check if this HTTP "connection" already exists
        if let Some(mut entry) = self.clients.get_mut(&connection_id) {
            // Update last_seen
            entry.last_seen = now;
        } else {
            // Create new HTTP connection entry
            self.clients.insert(
                connection_id,
                ClientInfo {
                    client_id: client_id.clone(),
                    namespace_id: namespace_id.clone(),
                    connection_id,
                    transport: "HTTP".to_string(),
                    connected_at: now,
                    last_seen: now,
                },
            );

            // Log HTTP connection (skip __admin to avoid recursion)
            if namespace_id != "__admin" {
                self.log_activity(ActivityEvent::ClientConnected {
                    client_id,
                    namespace_id,
                    transport: "HTTP".to_string(),
                    timestamp: now,
                }).await;
            }
        }
    }

    /// Get the complete CRDT state for a namespace (for initial sync)
    pub async fn get_namespace_state(&self, namespace_id: &str) -> Result<Vec<u8>> {
        let namespace = self.get_or_create_namespace(namespace_id);
        let db = namespace.db.read().await;
        let state = db.save_state();
        info!("Sending complete namespace state for {}: {} bytes", namespace_id, state.len());
        Ok(state)
    }

    /// Get the current heads for a namespace
    pub async fn get_namespace_heads(&self, namespace_id: &str) -> Vec<Vec<u8>> {
        let namespace = self.get_or_create_namespace(namespace_id);
        let db = namespace.db.read().await;
        db.get_heads()
    }

    /// Get changes since specific heads for a namespace
    pub async fn get_namespace_changes_since(&self, namespace_id: &str, heads: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let namespace = self.get_or_create_namespace(namespace_id);
        let db = namespace.db.read().await;
        db.get_changes_since(heads)
    }

    /// Apply incoming CRDT changes to the namespace and broadcast
    ///
    /// This is where the CRDT magic happens - changes are merged, not overwritten
    pub async fn apply_and_broadcast(
        &self,
        namespace_id: &str,
        from_client_id: String,
        exclude_connection: Option<Uuid>,
        changes: Vec<Vec<u8>>,
    ) -> Result<usize> {
        if changes.is_empty() {
            return Ok(0);
        }

        let namespace = self.get_or_create_namespace(namespace_id);

        // Apply changes to the CRDT (merges, doesn't replace)
        {
            let mut db = namespace.db.write().await;

            // Use apply_changes() which MERGES changes into the document
            // This is the proper CRDT sync protocol - no data loss!
            if let Err(e) = db.apply_changes(changes.clone()) {
                error!("Failed to apply CRDT changes: {}", e);
                return Err(e);
            }

            info!("Merged {} CRDT changes from {} into namespace {}",
                  changes.len(), from_client_id, namespace_id);
        }

        // Persist to storage for durability
        let timestamped_changes: Vec<Change> = changes
            .iter()
            .map(|data| Change {
                data: data.clone(),
                timestamp: now_timestamp(),
            })
            .collect();

        let change_count = timestamped_changes.len();
        self.storage.append_changes(namespace_id, timestamped_changes).await?;

        // Update namespace change counter and last activity
        namespace.add_changes(change_count).await;

        // Log changes applied (only for non-admin namespaces to avoid noise)
        if namespace_id != "__admin" {
            self.log_activity(ActivityEvent::ChangesApplied {
                namespace_id: namespace_id.to_string(),
                from_client_id: from_client_id.clone(),
                change_count,
                timestamp: now_timestamp(),
            }).await;
        }

        // Broadcast to connected clients (in-memory, fast)
        let broadcast_count = namespace.broadcast_tx.receiver_count();

        if broadcast_count > 0 {
            let msg = BroadcastMessage {
                from_client_id,
                changes,
                exclude_connection,
            };

            match namespace.broadcast_tx.send(msg) {
                Ok(_) => {
                    info!(
                        "Broadcasted {} CRDT changes to {} clients in namespace {}",
                        change_count,
                        broadcast_count,
                        namespace_id
                    );
                }
                Err(e) => {
                    warn!("Failed to broadcast: {} (no active receivers)", e);
                }
            }
        }

        Ok(broadcast_count)
    }

    /// Get all changes for a namespace from storage (legacy - for debugging)
    pub async fn get_namespace_changes(&self, namespace_id: &str) -> Result<Vec<Change>> {
        self.storage.get_namespace_changes(namespace_id).await
    }

    /// Append changes to a namespace and broadcast to connected clients (deprecated)
    ///
    /// Use apply_and_broadcast instead - this bypasses the CRDT engine
    pub async fn append_and_broadcast(
        &self,
        namespace_id: &str,
        from_client_id: String,
        exclude_connection: Option<Uuid>,
        changes: Vec<Vec<u8>>,
    ) -> Result<usize> {
        if changes.is_empty() {
            return Ok(0);
        }

        // Convert to storage format with timestamps
        let timestamped_changes: Vec<Change> = changes
            .iter()
            .map(|data| Change {
                data: data.clone(),
                timestamp: now_timestamp(),
            })
            .collect();

        let change_count = timestamped_changes.len();

        // Persist to storage
        self.storage
            .append_changes(namespace_id, timestamped_changes)
            .await?;

        // Broadcast to connected clients (in-memory, fast)
        let namespace = self.get_or_create_namespace(namespace_id);
        let broadcast_count = namespace.broadcast_tx.receiver_count();

        if broadcast_count > 0 {
            let msg = BroadcastMessage {
                from_client_id,
                changes,
                exclude_connection,
            };

            match namespace.broadcast_tx.send(msg) {
                Ok(_) => {
                    info!(
                        "Broadcasted {} changes to {} clients in namespace {}",
                        change_count,
                        broadcast_count,
                        namespace_id
                    );
                }
                Err(e) => {
                    warn!("Failed to broadcast: {} (no active receivers)", e);
                }
            }
        }

        Ok(broadcast_count)
    }

    /// Subscribe to namespace broadcasts
    pub fn subscribe_to_namespace(&self, namespace_id: &str) -> broadcast::Receiver<BroadcastMessage> {
        let namespace = self.get_or_create_namespace(namespace_id);
        namespace.broadcast_tx.subscribe()
    }

    /// Get storage statistics
    pub async fn get_stats(&self) -> Result<ServerStats> {
        let storage_stats = self.storage.stats().await?;

        Ok(ServerStats {
            total_namespaces: self.namespaces.len(),
            total_clients: self.clients.len(),
            storage_stats,
        })
    }

    /// Get storage reference
    pub fn storage(&self) -> &Arc<Box<dyn StorageAdapter>> {
        &self.storage
    }

    /// Publish admin stats to the __admin namespace for real-time monitoring
    pub async fn publish_admin_stats(&self) -> Result<()> {
        const ADMIN_NAMESPACE: &str = "__admin";

        let namespace = self.get_or_create_namespace(ADMIN_NAMESPACE);
        let mut db = namespace.db.write().await;

        // Calculate uptime
        let uptime_seconds = self.start_time
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|start| SystemTime::now().duration_since(UNIX_EPOCH).ok().map(|now| (now - start).as_secs()))
            .unwrap_or(0);

        // Collect namespace info
        let mut namespace_data = Vec::new();
        let mut total_changes: usize = 0;
        for entry in self.namespaces.iter() {
            if entry.key() == ADMIN_NAMESPACE {
                continue; // Skip the admin namespace itself
            }

            let change_count = entry.value().get_change_count().await;
            let last_activity = entry.value().get_last_activity().await;
            total_changes += change_count;

            namespace_data.push(serde_json::json!({
                "id": entry.key().clone(),
                "connection_count": entry.value().get_connection_count().await,
                "change_count": change_count,
                "last_activity": last_activity,
            }));
        }

        // Collect connection info
        let mut connection_data = Vec::new();
        for entry in self.clients.iter() {
            connection_data.push(serde_json::json!({
                "client_id": entry.value().client_id.clone(),
                "namespace_id": entry.value().namespace_id.clone(),
                "transport": entry.value().transport.clone(),
                "connected_at": entry.value().connected_at,
                "last_seen": entry.value().last_seen,
            }));
        }

        // Collect activity log
        let activity_log = self.activity_log.read().await;
        let activity_data: Vec<serde_json::Value> = activity_log
            .iter()
            .map(|event| serde_json::to_value(event).unwrap_or(serde_json::json!({})))
            .collect();
        drop(activity_log); // Release the read lock

        // Update stats in the admin CRDT
        use swirldb_core::automerge::ScalarValue;
        db.set_path("stats.active_connections", ScalarValue::Int(self.clients.len() as i64))?;
        db.set_path("stats.namespace_count", ScalarValue::Int(self.namespaces.len().saturating_sub(1) as i64))?;
        db.set_path("stats.total_changes", ScalarValue::Int(total_changes as i64))?;
        db.set_path("stats.uptime_seconds", ScalarValue::Int(uptime_seconds as i64))?;

        // Update namespaces array
        db.set_path("namespaces", ScalarValue::Str(serde_json::to_string(&namespace_data)?.into()))?;

        // Update connections array
        db.set_path("connections", ScalarValue::Str(serde_json::to_string(&connection_data)?.into()))?;

        // Update activity array
        db.set_path("activity", ScalarValue::Str(serde_json::to_string(&activity_data)?.into()))?;

        // Get only the new changes since last publish
        let current_heads = db.get_heads();
        let all_changes = db.get_changes();
        drop(db); // Release the write lock

        // Always broadcast all changes to ensure clients stay in sync
        if !all_changes.is_empty() {
            // Broadcast admin updates to all connected admin clients
            let msg = BroadcastMessage {
                from_client_id: "server".to_string(),
                changes: all_changes.clone(),
                exclude_connection: None,
            };

            let _ = namespace.broadcast_tx.send(msg);
        }

        Ok(())
    }
}

/// Server statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct ServerStats {
    pub total_namespaces: usize,
    pub total_clients: usize,
    pub storage_stats: crate::storage::StorageStats,
}

/// Get current timestamp in milliseconds
fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
