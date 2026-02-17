// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

/// Server state management with subscription-based sync
///
/// Design principles:
/// - Single global SwirlDB instance (shared CRDT)
/// - Subscription-based change filtering (path patterns)
/// - Policy-aware subscription validation
/// - Lock-free reads where possible using Arc + DashMap
/// - Async-friendly with tokio channels for broadcasts
/// - Handles thousands of concurrent WebSocket connections
use anyhow::Result;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use swirldb_core::core::SwirlDB;
use swirldb_core::policy::{Actor, PolicyEngine};
use swirldb_core::storage::DocumentStorage;
use swirldb_core::sync::SubscriptionManager;
use tokio::sync::{broadcast, Mutex, RwLock};
use tracing::info;
use uuid::Uuid;

/// Maximum number of messages to buffer in broadcast channel
const BROADCAST_CHANNEL_SIZE: usize = 1000;

/// Maximum number of activity events to keep in memory
const MAX_ACTIVITY_EVENTS: usize = 100;

/// Activity event types
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActivityEvent {
    ClientConnected {
        client_id: String,
        transport: String,
        subscriptions: Vec<String>,
        timestamp: i64,
    },
    ClientDisconnected {
        client_id: String,
        timestamp: i64,
    },
    #[allow(dead_code)]
    SubscriptionUpdated {
        client_id: String,
        added: Vec<String>,
        removed: Vec<String>,
        timestamp: i64,
    },
    ChangesApplied {
        from_client_id: String,
        change_count: usize,
        affected_paths: Vec<String>,
        timestamp: i64,
    },
}

/// Client connection information
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub client_id: String,
    #[allow(dead_code)]
    pub connection_id: Uuid,
    #[allow(dead_code)]
    pub actor: Actor,
    #[allow(dead_code)]
    pub transport: String,
    #[allow(dead_code)]
    pub connected_at: i64,
    #[allow(dead_code)]
    pub last_seen: i64,
}

/// Broadcast message to subscribers
#[derive(Debug, Clone)]
pub struct BroadcastMessage {
    pub from_client_id: String,
    pub changes: Vec<Vec<u8>>,
    pub affected_paths: Vec<String>,
    pub exclude_connection: Option<Uuid>,
    /// List of client_ids that should receive this broadcast (based on subscriptions)
    pub target_clients: Vec<String>,
}

/// Global server state - thread-safe and highly concurrent
#[derive(Clone)]
pub struct ServerState {
    /// Single global CRDT database instance (shared by all clients)
    db: Arc<RwLock<SwirlDB>>,

    /// Subscription manager (from core) for path-based filtering
    subscriptions: Arc<Mutex<SubscriptionManager>>,

    /// Global broadcast channel for real-time updates
    broadcast_tx: broadcast::Sender<BroadcastMessage>,

    /// Active clients indexed by connection_id
    clients: Arc<DashMap<Uuid, ClientInfo>>,

    /// Server start time for uptime calculation
    start_time: Arc<SystemTime>,

    /// Recent activity events (rolling buffer, limited to MAX_ACTIVITY_EVENTS)
    activity_log: Arc<RwLock<VecDeque<ActivityEvent>>>,

    /// Total number of changes applied
    change_count: Arc<RwLock<usize>>,

    /// Timestamp of last activity
    last_activity: Arc<RwLock<i64>>,
}

impl ServerState {
    /// Create new server state with optional policy and storage adapter
    ///
    /// The storage adapter determines where CRDT state is persisted.
    /// Pass a RedbAdapter for persistent storage, or InMemoryDocStorage for volatile.
    pub async fn new(policy: Option<PolicyEngine>, storage: Arc<dyn DocumentStorage>) -> Self {
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CHANNEL_SIZE);

        // Create SwirlDB with the provided storage adapter, loading any existing state
        let db = SwirlDB::with_storage(storage, "global").await;

        Self {
            db: Arc::new(RwLock::new(db)),
            subscriptions: Arc::new(Mutex::new(SubscriptionManager::new(policy))),
            broadcast_tx,
            clients: Arc::new(DashMap::new()),
            start_time: Arc::new(SystemTime::now()),
            activity_log: Arc::new(RwLock::new(VecDeque::new())),
            change_count: Arc::new(RwLock::new(0)),
            last_activity: Arc::new(RwLock::new(now_timestamp())),
        }
    }

    /// Get the global SwirlDB instance
    pub fn db(&self) -> &Arc<RwLock<SwirlDB>> {
        &self.db
    }

    /// Get a broadcast receiver for real-time updates
    pub fn subscribe_to_broadcasts(&self) -> broadcast::Receiver<BroadcastMessage> {
        self.broadcast_tx.subscribe()
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

    /// Register a client connection with subscriptions
    pub async fn register_client(
        &self,
        connection_id: Uuid,
        client_id: String,
        actor: Actor,
        subscriptions: Vec<String>,
        transport: String,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let now = now_timestamp();

        // Add subscriptions via SubscriptionManager (validates with policy)
        let (added, denied) = {
            let mut sub_mgr = self.subscriptions.lock().await;
            sub_mgr.add_client(client_id.clone(), actor.clone(), subscriptions.clone())
        };

        // Register client info
        self.clients.insert(
            connection_id,
            ClientInfo {
                client_id: client_id.clone(),
                connection_id,
                actor,
                transport: transport.clone(),
                connected_at: now,
                last_seen: now,
            },
        );

        // Connection info logged in main.rs

        // Log activity
        self.log_activity(ActivityEvent::ClientConnected {
            client_id,
            transport,
            subscriptions: added.clone(),
            timestamp: now,
        })
        .await;

        Ok((added, denied))
    }

    /// Unregister a client connection
    pub async fn unregister_client(&self, connection_id: &Uuid) -> Result<()> {
        if let Some((_, client_info)) = self.clients.remove(connection_id) {
            // Remove from subscription manager
            let mut sub_mgr = self.subscriptions.lock().await;
            sub_mgr.remove_client(&client_info.client_id);

            // Log activity
            self.log_activity(ActivityEvent::ClientDisconnected {
                client_id: client_info.client_id.clone(),
                timestamp: now_timestamp(),
            })
            .await;

            info!("❌ Client {} disconnected", client_info.client_id);
        }

        Ok(())
    }

    /// Update client subscriptions dynamically
    #[allow(dead_code)]
    pub async fn update_subscriptions(
        &self,
        client_id: &str,
        add: Vec<String>,
        remove: Vec<String>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let mut sub_mgr = self.subscriptions.lock().await;
        let (added, denied) =
            sub_mgr.update_subscriptions(client_id, add.clone(), remove.clone())?;

        // Log activity
        self.log_activity(ActivityEvent::SubscriptionUpdated {
            client_id: client_id.to_string(),
            added: added.clone(),
            removed: remove,
            timestamp: now_timestamp(),
        })
        .await;

        Ok((added, denied))
    }

    /// Apply changes from a client and broadcast to subscribers
    pub async fn apply_changes(
        &self,
        from_client_id: String,
        from_connection_id: Uuid,
        changes: Vec<Vec<u8>>,
        affected_paths: Vec<String>,
    ) -> Result<()> {
        // Apply changes to global DB and persist to storage
        {
            let db = self.db.write().await;
            db.apply_changes(changes.clone())?;
            db.persist().await?;
        }

        // Update metrics
        {
            let mut count = self.change_count.write().await;
            *count += changes.len();

            let mut last = self.last_activity.write().await;
            *last = now_timestamp();
        }

        // Get subscribers for affected paths
        let subscribers = {
            let sub_mgr = self.subscriptions.lock().await;
            sub_mgr.get_subscribers_for_paths(&affected_paths)
        };

        // Broadcast to subscribers (except sender)
        if !subscribers.is_empty() {
            let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
            info!(
                "📤 BROADCAST: {} changes ({} bytes) to {} subscribers",
                changes.len(),
                total_bytes,
                subscribers.len()
            );

            let msg = BroadcastMessage {
                from_client_id: from_client_id.clone(),
                changes: changes.clone(),
                affected_paths: affected_paths.clone(),
                exclude_connection: Some(from_connection_id),
                target_clients: subscribers,
            };

            // send may fail if no receivers, that's ok
            let _ = self.broadcast_tx.send(msg);
        }

        // Log activity
        self.log_activity(ActivityEvent::ChangesApplied {
            from_client_id,
            change_count: changes.len(),
            affected_paths,
            timestamp: now_timestamp(),
        })
        .await;

        Ok(())
    }

    /// Get client info
    #[allow(dead_code)]
    pub fn get_client(&self, connection_id: &Uuid) -> Option<ClientInfo> {
        self.clients.get(connection_id).map(|r| r.clone())
    }

    /// Get total connection count
    pub fn get_connection_count(&self) -> usize {
        self.clients.len()
    }

    /// Get total change count
    pub async fn get_change_count(&self) -> usize {
        *self.change_count.read().await
    }

    /// Get server uptime in seconds
    pub fn get_uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().unwrap_or_default().as_secs()
    }

    /// Get server stats
    pub async fn get_stats(&self) -> ServerStats {
        let subscription_count = {
            let sub_mgr = self.subscriptions.lock().await;
            sub_mgr.client_count()
        };
        ServerStats {
            active_connections: self.get_connection_count(),
            subscription_count,
            total_changes: self.get_change_count().await,
            uptime_seconds: self.get_uptime_seconds(),
            last_activity: *self.last_activity.read().await,
        }
    }

    /// Get all connection info for admin
    pub async fn get_connections(&self) -> Vec<ConnectionInfo> {
        let sub_mgr = self.subscriptions.lock().await;
        self.clients
            .iter()
            .map(|entry| {
                let client = entry.value();
                let patterns = sub_mgr
                    .get_client_subscriptions(&client.client_id)
                    .map(|s| s.patterns().to_vec())
                    .unwrap_or_default();
                ConnectionInfo {
                    client_id: client.client_id.clone(),
                    subscriptions: patterns,
                    transport: client.transport.clone(),
                    connected_at: client.connected_at,
                    last_seen: client.last_seen,
                }
            })
            .collect()
    }

    /// Get all subscription info for admin
    pub async fn get_subscriptions(&self) -> Vec<SubscriptionInfo> {
        let sub_mgr = self.subscriptions.lock().await;
        sub_mgr
            .all_clients()
            .into_iter()
            .map(|(client_id, patterns)| SubscriptionInfo {
                client_id,
                patterns,
            })
            .collect()
    }

    /// Get recent activity log
    pub async fn get_activity(&self) -> Vec<ActivityEvent> {
        self.activity_log.read().await.iter().cloned().collect()
    }
}

/// Server statistics (for /admin/stats)
#[derive(Debug, Clone, serde::Serialize)]
pub struct ServerStats {
    pub active_connections: usize,
    pub subscription_count: usize,
    pub total_changes: usize,
    pub uptime_seconds: u64,
    pub last_activity: i64,
}

/// Connection info (for /admin/connections)
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConnectionInfo {
    pub client_id: String,
    pub subscriptions: Vec<String>,
    pub transport: String,
    pub connected_at: i64,
    pub last_seen: i64,
}

/// Subscription info (for /admin/subscriptions)
#[derive(Debug, Clone, serde::Serialize)]
pub struct SubscriptionInfo {
    pub client_id: String,
    pub patterns: Vec<String>,
}

/// Get current timestamp in milliseconds
fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
