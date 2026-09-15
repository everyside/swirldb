// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Server state.
//!
//! The server holds many documents. Each is its own `SwirlDB` — its own
//! Automerge history — loaded from storage under its id the first time
//! somebody opens it, persisted under that id after every change, and
//! unloaded again once no connection has it open (`unload_if_unheld`), and
//! compacted — its history folded into one change of its state — when it is
//! loaded or unloaded with nobody holding it and too much history behind it
//! (`compact_if_due`; swirldb-core's `compaction` module says why only then).
//! Nothing
//! is shared between documents: a change graph is not partitioned by path, so
//! the only history a client can be given selectively is a whole document's,
//! and that is the unit of access as well as of sync.
//!
//! A connection carries a client and any number of open documents. Opening
//! asks the [`Authority`](crate::authority::Authority) once and remembers the
//! answer on the connection; a `Push` on a document the connection holds
//! read-only is refused here, not left to the client's good manners.
//!
//! Subscriptions — the path patterns that filter broadcasts — are kept per
//! document, so the same `SubscriptionManager` from core serves each one.
//!
//! The rest is what it was: broadcast and ephemeral channels, an activity log
//! for the admin page, peer bookkeeping for server-to-server relay. Peers work
//! on the default document only; see `db()`.

use crate::authority::{Authority, OpenToAll};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use swirldb_core::compaction::CompactionThreshold;
use swirldb_core::core::SwirlDB;
use swirldb_core::policy::{Actor, PolicyEngine};
use swirldb_core::protocol::{Access, DEFAULT_DOCUMENT};
use swirldb_core::storage::DocumentStorage;
use swirldb_core::sync::SubscriptionManager;
use tokio::sync::{broadcast, Mutex, RwLock};
use tracing::{info, warn};
use uuid::Uuid;

/// Maximum number of messages to buffer in broadcast channel
const BROADCAST_CHANNEL_SIZE: usize = 1000;

/// Maximum number of messages to buffer in ephemeral channel (smaller for backpressure)
const EPHEMERAL_CHANNEL_SIZE: usize = 100;

/// Maximum number of activity events to keep in memory
const MAX_ACTIVITY_EVENTS: usize = 100;

/// Activity event types
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActivityEvent {
    ClientConnected {
        client_id: String,
        transport: String,
        timestamp: i64,
    },
    ClientDisconnected {
        client_id: String,
        timestamp: i64,
    },
    DocumentOpened {
        client_id: String,
        document: String,
        access: Access,
        subscriptions: Vec<String>,
        timestamp: i64,
    },
    DocumentRefused {
        client_id: String,
        document: String,
        timestamp: i64,
    },
    DocumentClosed {
        client_id: String,
        document: String,
        timestamp: i64,
    },
    DocumentRevoked {
        client_id: String,
        subject: String,
        document: String,
        timestamp: i64,
    },
    SubscriptionUpdated {
        client_id: String,
        document: String,
        added: Vec<String>,
        removed: Vec<String>,
        timestamp: i64,
    },
    ChangesApplied {
        from_client_id: String,
        document: String,
        change_count: usize,
        affected_paths: Vec<String>,
        timestamp: i64,
    },
}

/// Client connection information
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub client_id: String,
    pub connection_id: Uuid,
    pub actor: Actor,
    pub transport: String,
    pub connected_at: i64,
    pub last_seen: i64,
    /// The documents this connection has open, and how.
    pub documents: HashMap<String, Access>,
}

/// What an open produced: the access granted and which subscription
/// patterns the policy accepted and refused.
#[derive(Debug, Clone)]
pub struct Opened {
    pub access: Access,
    pub added: Vec<String>,
    pub denied: Vec<String>,
}

/// Why an open did not happen.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("the authority refused {subject} access to {document}")]
    Refused { subject: String, document: String },
    #[error("no client is registered on this connection")]
    NotConnected,
}

/// A word from the server to one connection's handler, which is the only
/// thing that can write to that connection's socket.
#[derive(Debug, Clone)]
pub enum ControlMessage {
    /// The connection's access to a document was revoked; the handler tells
    /// the client and the server has already dropped the document from it.
    Revoked { connection: Uuid, document: String },
}

/// One document closed by a revocation: on which connection, by client id.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Revoked {
    pub client_id: String,
    pub document: String,
}

/// Broadcast message to subscribers
#[derive(Debug, Clone)]
pub struct BroadcastMessage {
    pub document: String,
    pub from_client_id: String,
    pub changes: Vec<Vec<u8>>,
    pub affected_paths: Vec<String>,
    pub exclude_connection: Option<Uuid>,
    /// List of client_ids that should receive this broadcast (based on subscriptions)
    pub target_clients: Vec<String>,
}

/// Ephemeral message routed to subscribers via the ephemeral broadcast channel.
///
/// These messages bypass Automerge CRDT processing and storage entirely,
/// providing a high-frequency pub/sub path for real-time data like DMX
/// lighting values, cursor positions, or beat sync data.
///
/// Routing is determined by the same per-document subscription patterns used
/// for CRDT broadcasts, but ephemeral messages are never persisted or merged.
#[derive(Debug, Clone)]
pub struct EphemeralMessage {
    pub document: String,
    /// Client that sent the ephemeral message
    pub from_client_id: String,
    /// List of (path, data) updates
    pub updates: Vec<(String, Vec<u8>)>,
    /// Connection to exclude from receiving (prevents echo to sender)
    pub exclude_connection: Option<Uuid>,
    /// List of client_ids that should receive this ephemeral (based on subscriptions)
    pub target_clients: Vec<String>,
}

/// Information about a connected peer server for server-to-server sync.
///
/// Peer connections enable multi-server topologies where CRDT changes and
/// ephemeral messages are forwarded between servers. Each peer is identified
/// by a unique peer_id derived from the remote server's server_id.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Unique peer server identifier
    pub peer_id: String,
    /// WebSocket endpoint URL (e.g., `ws://peer-host:3030/ws`)
    pub endpoint: String,
    /// Subscription patterns this peer is interested in
    pub subscriptions: Vec<String>,
    /// Whether the peer is currently connected
    pub connected: bool,
}

type Document = Arc<RwLock<SwirlDB>>;

/// Server state - thread-safe and highly concurrent
#[derive(Clone)]
pub struct ServerState {
    /// Every document loaded so far, by id. The lock is held across the
    /// load so two connections opening the same document at once get one
    /// instance rather than two that would then diverge.
    documents: Arc<Mutex<HashMap<String, Document>>>,

    /// The default document, loaded at start. Peers and anything else that
    /// predates multi-document work on this one.
    default_document: Document,

    /// Where documents are loaded from and persisted to, keyed by id.
    storage: Arc<dyn DocumentStorage>,

    /// How much history a document may carry before it is compacted.
    compaction: CompactionThreshold,

    /// Policy used to validate subscription patterns within a document.
    policy: Option<PolicyEngine>,

    /// Who decides whether a subject may open a document.
    authority: Arc<dyn Authority>,

    /// Subscription managers, one per document.
    subscriptions: Arc<Mutex<HashMap<String, SubscriptionManager>>>,

    /// Global broadcast channel for real-time CRDT updates
    broadcast_tx: broadcast::Sender<BroadcastMessage>,

    /// Ephemeral broadcast channel for high-frequency pub/sub (bypasses CRDT/storage)
    ephemeral_tx: broadcast::Sender<EphemeralMessage>,

    /// Control channel: every handler listens, each acts on what names its
    /// own connection. Revocations travel here.
    control_tx: broadcast::Sender<ControlMessage>,

    /// The secret an administrative request must present as a bearer. None
    /// keeps the administrative endpoints closed.
    admin_secret: Option<Arc<str>>,

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

    /// Connected peer servers (peer_id -> PeerInfo)
    peers: Arc<DashMap<String, PeerInfo>>,

    /// Ephemeral dedup tracking: origin server -> (last seen seq, last update time)
    ephemeral_seen: Arc<DashMap<String, (u64, Instant)>>,

    /// Monotonically increasing sequence counter for outgoing EphemeralRelay messages
    ephemeral_seq: Arc<AtomicU64>,

    /// This server's unique ID (for EphemeralRelay loop prevention)
    server_id: String,
}

impl ServerState {
    /// Create server state with an optional policy and a storage adapter,
    /// open to all: every subject may write every document. This is what
    /// the single-document demos and tests have always had.
    pub async fn new(policy: Option<PolicyEngine>, storage: Arc<dyn DocumentStorage>) -> Self {
        Self::with_authority(policy, storage, Arc::new(OpenToAll)).await
    }

    /// Create server state whose documents are opened only as `authority`
    /// allows. The policy, if any, still filters subscription patterns within
    /// a document; it does not decide access unless it is the authority too
    /// (see [`crate::authority::PolicyAuthority`]).
    pub async fn with_authority(
        policy: Option<PolicyEngine>,
        storage: Arc<dyn DocumentStorage>,
        authority: Arc<dyn Authority>,
    ) -> Self {
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CHANNEL_SIZE);
        let (ephemeral_tx, _) = broadcast::channel(EPHEMERAL_CHANNEL_SIZE);
        let (control_tx, _) = broadcast::channel(EPHEMERAL_CHANNEL_SIZE);

        let default_document: Document = Arc::new(RwLock::new(
            SwirlDB::with_storage(storage.clone(), DEFAULT_DOCUMENT).await,
        ));
        let mut documents = HashMap::new();
        documents.insert(DEFAULT_DOCUMENT.to_string(), default_document.clone());

        Self {
            documents: Arc::new(Mutex::new(documents)),
            default_document,
            storage,
            compaction: CompactionThreshold::DEFAULT,
            policy,
            authority,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            broadcast_tx,
            ephemeral_tx,
            control_tx,
            admin_secret: None,
            clients: Arc::new(DashMap::new()),
            start_time: Arc::new(SystemTime::now()),
            activity_log: Arc::new(RwLock::new(VecDeque::new())),
            change_count: Arc::new(RwLock::new(0)),
            last_activity: Arc::new(RwLock::new(now_timestamp())),
            peers: Arc::new(DashMap::new()),
            ephemeral_seen: Arc::new(DashMap::new()),
            ephemeral_seq: Arc::new(AtomicU64::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            )),
            server_id: format!("srv-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]),
        }
    }

    /// Open the administrative endpoints to requests bearing `secret`. It is
    /// the same secret the server presents to its authority, so the
    /// application that answers the server's questions is the one that may
    /// give it orders.
    pub fn with_admin_secret(mut self, secret: impl Into<Arc<str>>) -> Self {
        self.admin_secret = Some(secret.into());
        self
    }

    /// Compact documents at this threshold rather than the default. Tests
    /// use it to reach a threshold in a few changes.
    pub fn with_compaction(mut self, threshold: CompactionThreshold) -> Self {
        self.compaction = threshold;
        self
    }

    /// The secret an administrative request must present, if any.
    pub fn admin_secret(&self) -> Option<&str> {
        self.admin_secret.as_deref()
    }

    /// Get a receiver for control messages
    pub fn subscribe_to_control(&self) -> broadcast::Receiver<ControlMessage> {
        self.control_tx.subscribe()
    }

    /// The default document.
    ///
    /// Server-to-server sync (`connect_to_peer`, the peer manager) still
    /// speaks about one document, and this is it. Everything that knows a
    /// document id should call [`Self::document`] instead.
    pub fn db(&self) -> &Document {
        &self.default_document
    }

    /// The document with this id, loaded from storage on first use.
    pub async fn document(&self, id: &str) -> Document {
        let mut documents = self.documents.lock().await;
        if let Some(document) = documents.get(id) {
            return document.clone();
        }
        let document: Document = Arc::new(RwLock::new(
            SwirlDB::with_storage(self.storage.clone(), id).await,
        ));
        info!("📄 Document {} loaded", id);
        self.compact_if_due(id, &document).await;
        documents.insert(id.to_string(), document.clone());
        document
    }

    /// Fold a document's history into one change of its state, and store
    /// that, when the history has passed the threshold and nobody has the
    /// document open.
    ///
    /// **Only when nobody has it open.** A compaction replaces every change
    /// hash, and a connection that holds the document holds the old ones: its
    /// next push would depend on history that is gone. Every client in this
    /// repository opens a document from empty and is sent the whole history,
    /// so a connection that opens after the compaction is sent the snapshot and
    /// syncs with it as with any document; the connections that must not be
    /// compacted under are exactly those that have it open. So the two moments
    /// that know nobody does are where this runs: a load, which is the first
    /// thing anybody needs from a document that was not in memory, and an
    /// unload, which is the last. The load covers a document that grew
    /// before compaction existed, or while the server was down; the unload,
    /// one that grew while it was open. The default document is never
    /// compacted: peers sync it by heads.
    ///
    /// A failure is logged and changes nothing that matters: the document
    /// reads the same compacted or not, and storage keeps what it had.
    async fn compact_if_due(&self, id: &str, document: &Document) {
        if id == DEFAULT_DOCUMENT || self.is_open_anywhere(id) {
            return;
        }
        let document = document.write().await;
        let history = document.history();
        if !self.compaction.is_due(history) {
            return;
        }
        let compaction = match document.compact() {
            Ok(compaction) => compaction,
            Err(error) => {
                warn!("🗜️ Document {} could not be compacted: {}", id, error);
                return;
            }
        };
        if let Err(error) = document.persist().await {
            warn!("🗜️ Document {} was compacted but not stored: {}", id, error);
            return;
        }
        info!(
            "🗜️ Document {} compacted: {} changes ({} bytes) folded into {} ({} bytes)",
            id,
            compaction.before.changes,
            compaction.before.bytes,
            compaction.after.changes,
            compaction.after.bytes
        );
    }

    /// Whether any connection has this document open.
    fn is_open_anywhere(&self, id: &str) -> bool {
        self.clients
            .iter()
            .any(|client| client.documents.contains_key(id))
    }

    /// Ids of every document this server knows: loaded now or persisted before.
    pub async fn document_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.documents.lock().await.keys().cloned().collect();
        if let Ok(stored) = self.storage.list_keys().await {
            for key in stored {
                if !ids.contains(&key) {
                    ids.push(key);
                }
            }
        }
        ids.sort();
        ids
    }

    /// Unload a document once nothing needs it in memory: no connection has
    /// it open, and nothing is holding the loaded instance right now.
    ///
    /// **Loaded used to mean loaded for good.** Every document anybody had
    /// opened since the process started stayed in memory, so memory grew
    /// with every document ever opened rather than with the ones open — and a
    /// document is an Automerge history, which materializes to many times
    /// what it weighs on disk. That is how Studio's staging server was
    /// OOMKilled ten times on 2026-09-12.
    ///
    /// Safe to drop because a document is persisted after every change while
    /// its instance is held (`apply_changes_inner`), so storage already
    /// spells what memory does. The strong count is read under the map's
    /// lock, which is the only place an instance is handed out: a count of
    /// one is the map's own reference, so no apply or read is in flight on
    /// it, and whoever asks next loads the stored one. An open racing this —
    /// loaded, not yet recorded on its connection — loses nothing either:
    /// its sync and its pushes each ask `document` again and load it back.
    /// The default document stays, as peers work on it.
    async fn unload_if_unheld(&self, id: &str) {
        if id == DEFAULT_DOCUMENT {
            return;
        }
        if self.is_open_anywhere(id) {
            return;
        }
        let mut documents = self.documents.lock().await;
        if let Some(document) = documents
            .get(id)
            .filter(|document| Arc::strong_count(document) == 1)
            .cloned()
        {
            // Compacted while the map's lock is held, so nobody can be handed
            // the instance in the middle of it and nobody can open it.
            self.compact_if_due(id, &document).await;
            drop(document);
            documents.remove(id);
            info!("📄 Document {} unloaded; nobody has it open", id);
        }
    }

    /// How many documents are loaded in memory.
    pub async fn loaded_document_count(&self) -> usize {
        self.documents.lock().await.len()
    }

    /// Get a broadcast receiver for real-time CRDT updates
    pub fn subscribe_to_broadcasts(&self) -> broadcast::Receiver<BroadcastMessage> {
        self.broadcast_tx.subscribe()
    }

    /// Get a receiver for ephemeral messages
    pub fn subscribe_to_ephemeral(&self) -> broadcast::Receiver<EphemeralMessage> {
        self.ephemeral_tx.subscribe()
    }

    /// Route ephemeral messages on a document to its subscribers (no
    /// Automerge, no storage, no persist).
    ///
    /// A client connection must have the document open; read access is
    /// enough, because presence and cursors are not writes. A peer
    /// (`Uuid::nil()`) has nothing open and is trusted on the default document.
    pub async fn route_ephemeral(
        &self,
        from_client_id: String,
        from_connection_id: Uuid,
        document: &str,
        updates: Vec<(String, Vec<u8>)>,
    ) -> Result<()> {
        if !from_connection_id.is_nil()
            && self
                .document_access(&from_connection_id, document)
                .is_none()
        {
            return Err(anyhow!(
                "{} has not opened {} and may not send on it",
                from_client_id,
                document
            ));
        }

        // Filter out invalid paths before routing
        let updates: Vec<(String, Vec<u8>)> = updates
            .into_iter()
            .filter(|(path, _)| {
                if path.is_empty()
                    || path.starts_with('.')
                    || path.ends_with('.')
                    || path.contains("..")
                {
                    tracing::warn!("Skipping ephemeral update with invalid path: {:?}", path);
                    false
                } else {
                    true
                }
            })
            .collect();

        if updates.is_empty() {
            return Ok(());
        }

        let paths: Vec<String> = updates.iter().map(|(path, _)| path.clone()).collect();
        let subscribers = self.subscribers_for_paths(document, &paths).await;

        if !subscribers.is_empty() {
            let msg = EphemeralMessage {
                document: document.to_string(),
                from_client_id,
                updates,
                exclude_connection: Some(from_connection_id),
                target_clients: subscribers,
            };

            if self.ephemeral_tx.send(msg).is_err() {
                tracing::trace!("Ephemeral send failed (no receivers)");
            }
        }

        Ok(())
    }

    /// Get this server's unique ID
    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    /// Get the next sequence number for outgoing EphemeralRelay messages
    pub fn next_ephemeral_seq(&self) -> u64 {
        self.ephemeral_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Register a peer server connection
    pub fn register_peer(&self, peer_id: String, endpoint: String, subscriptions: Vec<String>) {
        self.peers.insert(
            peer_id.clone(),
            PeerInfo {
                peer_id,
                endpoint,
                subscriptions,
                connected: true,
            },
        );
    }

    /// Unregister a peer server
    pub fn unregister_peer(&self, peer_id: &str) {
        self.peers.remove(peer_id);
    }

    /// Atomically check and claim an ephemeral relay for processing.
    ///
    /// Returns `true` if this relay should be processed (not a duplicate and
    /// not a loop). The claim is immediately recorded so concurrent callers
    /// are rejected. If routing subsequently fails, call `release_relay_claim`
    /// to allow retries from other peers.
    pub fn try_claim_relay(&self, origin: &str, seq: u64, path_through: &[String]) -> bool {
        // Loop prevention: skip if we're already in the path
        if path_through.iter().any(|s| s == &self.server_id) {
            return false;
        }

        // Atomic dedup: check-and-claim via DashMap::entry()
        use dashmap::mapref::entry::Entry;
        match self.ephemeral_seen.entry(origin.to_string()) {
            Entry::Occupied(mut entry) => {
                if entry.get().0 >= seq {
                    return false; // Already seen
                }
                entry.insert((seq, Instant::now()));
                true
            }
            Entry::Vacant(entry) => {
                entry.insert((seq, Instant::now()));
                true
            }
        }
    }

    /// Release a relay claim after routing failure, allowing retries from other peers.
    pub fn release_relay_claim(&self, origin: &str, seq: u64) {
        use dashmap::mapref::entry::Entry;
        match self.ephemeral_seen.entry(origin.to_string()) {
            Entry::Occupied(entry) => {
                // Only release if we still hold this exact seq
                if entry.get().0 == seq {
                    entry.remove();
                }
            }
            Entry::Vacant(_) => {}
        }
    }

    /// Remove stale entries from ephemeral_seen (called by heartbeat task).
    pub fn cleanup_stale_ephemeral_seen(&self) {
        let one_hour = Duration::from_secs(3600);
        let now = Instant::now();
        self.ephemeral_seen
            .retain(|_, (_, last_update)| now.duration_since(*last_update) < one_hour);

        // Hard cap to prevent unbounded growth from spoofed origins
        const MAX_EPHEMERAL_ORIGINS: usize = 10_000;
        if self.ephemeral_seen.len() > MAX_EPHEMERAL_ORIGINS {
            // Evict oldest entries
            let mut entries: Vec<_> = self
                .ephemeral_seen
                .iter()
                .map(|r| (r.key().clone(), r.value().1))
                .collect();
            entries.sort_by_key(|(_, ts)| *ts);
            let to_remove = entries.len() - MAX_EPHEMERAL_ORIGINS;
            for (origin, _) in entries.iter().take(to_remove) {
                self.ephemeral_seen.remove(origin);
            }
        }
    }

    /// Get connected peers (for relay forwarding)
    pub fn get_peers(&self) -> Vec<PeerInfo> {
        self.peers.iter().map(|r| r.value().clone()).collect()
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

    /// Who a connection is, per the authority. `None` refuses it: nothing
    /// should be registered for a connection the authority does not know.
    pub async fn authenticate(&self, token: Option<&str>, client_id: &str) -> Option<Actor> {
        self.authority.subject(token, client_id).await
    }

    /// Register a client connection. Documents are opened separately with
    /// [`Self::open_document`].
    pub async fn register_client(
        &self,
        connection_id: Uuid,
        client_id: String,
        actor: Actor,
        transport: String,
    ) {
        let now = now_timestamp();
        self.clients.insert(
            connection_id,
            ClientInfo {
                client_id: client_id.clone(),
                connection_id,
                actor,
                transport: transport.clone(),
                connected_at: now,
                last_seen: now,
                documents: HashMap::new(),
            },
        );

        self.log_activity(ActivityEvent::ClientConnected {
            client_id,
            transport,
            timestamp: now,
        })
        .await;
    }

    /// Open a document on a connection.
    ///
    /// Asks the authority, and only if it allows: loads the document, records
    /// the access on the connection, and adds the subscription patterns
    /// (policy-checked) for it. The connection then receives the document's
    /// broadcasts and ephemeral traffic.
    pub async fn open_document(
        &self,
        connection_id: &Uuid,
        document: &str,
        subscriptions: Vec<String>,
    ) -> Result<Opened, OpenError> {
        let (client_id, actor) = {
            let client = self
                .clients
                .get(connection_id)
                .ok_or(OpenError::NotConnected)?;
            (client.client_id.clone(), client.actor.clone())
        };

        let Some(access) = self.authority.may_open(&actor, document).await else {
            self.log_activity(ActivityEvent::DocumentRefused {
                client_id: client_id.clone(),
                document: document.to_string(),
                timestamp: now_timestamp(),
            })
            .await;
            return Err(OpenError::Refused {
                subject: actor.id,
                document: document.to_string(),
            });
        };

        // Make sure it is loaded before the client's Sync is computed.
        self.document(document).await;

        let (added, denied) = {
            let mut managers = self.subscriptions.lock().await;
            let manager = managers
                .entry(document.to_string())
                .or_insert_with(|| SubscriptionManager::new(self.policy.clone()));
            manager.add_client(client_id.clone(), actor, subscriptions)
        };

        if let Some(mut client) = self.clients.get_mut(connection_id) {
            client.documents.insert(document.to_string(), access);
            client.last_seen = now_timestamp();
        }

        self.log_activity(ActivityEvent::DocumentOpened {
            client_id,
            document: document.to_string(),
            access,
            subscriptions: added.clone(),
            timestamp: now_timestamp(),
        })
        .await;

        Ok(Opened {
            access,
            added,
            denied,
        })
    }

    /// Close one document on a connection; the connection stays.
    pub async fn close_document(&self, connection_id: &Uuid, document: &str) {
        let Some(client_id) = self.drop_document(connection_id, document).await else {
            return;
        };
        self.log_activity(ActivityEvent::DocumentClosed {
            client_id,
            document: document.to_string(),
            timestamp: now_timestamp(),
        })
        .await;
    }

    /// Take a document off a connection: no more broadcasts, no more
    /// pushes. Returns the connection's client id, or nothing when there is
    /// no such connection.
    async fn drop_document(&self, connection_id: &Uuid, document: &str) -> Option<String> {
        let client_id = {
            let mut client = self.clients.get_mut(connection_id)?;
            client.documents.remove(document);
            client.client_id.clone()
        };
        {
            let mut managers = self.subscriptions.lock().await;
            if let Some(manager) = managers.get_mut(document) {
                manager.remove_client(&client_id);
            }
        }
        self.unload_if_unheld(document).await;
        Some(client_id)
    }

    /// Revoke a subject's access: close `document` on every connection the
    /// subject holds — every document the subject has open, when none is
    /// named — tell each such connection with an `OpenDenied` naming the
    /// reason `revoked`, and have the authority forget what it remembered
    /// about the subject, so a reopen is asked afresh. The connections
    /// themselves stay up; a client may open something else on one.
    /// Returns what was closed, by client id.
    ///
    /// The subject is the authority's name for a connection, the `id` of
    /// the actor its `whoami` answered — never a client id, which a
    /// connection chooses for itself.
    pub async fn revoke(&self, subject: &str, document: Option<&str>) -> Vec<Revoked> {
        let held: Vec<(Uuid, Vec<String>)> = self
            .clients
            .iter()
            .filter(|client| client.actor.id == subject)
            .map(|client| {
                let documents = client
                    .documents
                    .keys()
                    .filter(|held| document.is_none_or(|named| named == held.as_str()))
                    .cloned()
                    .collect();
                (client.connection_id, documents)
            })
            .collect();

        let mut revoked = Vec::new();
        for (connection, documents) in held {
            for document in documents {
                let Some(client_id) = self.drop_document(&connection, &document).await else {
                    continue;
                };
                let _ = self.control_tx.send(ControlMessage::Revoked {
                    connection,
                    document: document.clone(),
                });
                self.log_activity(ActivityEvent::DocumentRevoked {
                    client_id: client_id.clone(),
                    subject: subject.to_string(),
                    document: document.clone(),
                    timestamp: now_timestamp(),
                })
                .await;
                info!("🚫 {} revoked on {} ({})", subject, document, client_id);
                revoked.push(Revoked {
                    client_id,
                    document,
                });
            }
        }

        self.authority.forget(subject, document).await;
        revoked
    }

    /// What this connection may do with the document, if it has it open.
    pub fn document_access(&self, connection_id: &Uuid, document: &str) -> Option<Access> {
        self.clients
            .get(connection_id)
            .and_then(|client| client.documents.get(document).copied())
    }

    /// Unregister a client connection, closing everything it had open.
    pub async fn unregister_client(&self, connection_id: &Uuid) -> Result<()> {
        if let Some((_, client_info)) = self.clients.remove(connection_id) {
            {
                let mut managers = self.subscriptions.lock().await;
                for document in client_info.documents.keys() {
                    if let Some(manager) = managers.get_mut(document) {
                        manager.remove_client(&client_info.client_id);
                    }
                }
            }

            for document in client_info.documents.keys() {
                self.unload_if_unheld(document).await;
            }

            self.log_activity(ActivityEvent::ClientDisconnected {
                client_id: client_info.client_id.clone(),
                timestamp: now_timestamp(),
            })
            .await;

            info!("❌ Client {} disconnected", client_info.client_id);
        }

        Ok(())
    }

    /// Update a client's subscriptions within a document it has open.
    pub async fn update_subscriptions(
        &self,
        client_id: &str,
        document: &str,
        add: Vec<String>,
        remove: Vec<String>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let (added, denied) = {
            let mut managers = self.subscriptions.lock().await;
            let manager = managers
                .get_mut(document)
                .ok_or_else(|| anyhow!("Document {} has no subscribers", document))?;
            manager.update_subscriptions(client_id, add, remove.clone())?
        };

        self.log_activity(ActivityEvent::SubscriptionUpdated {
            client_id: client_id.to_string(),
            document: document.to_string(),
            added: added.clone(),
            removed: remove,
            timestamp: now_timestamp(),
        })
        .await;

        Ok((added, denied))
    }

    async fn subscribers_for_paths(&self, document: &str, paths: &[String]) -> Vec<String> {
        let managers = self.subscriptions.lock().await;
        managers
            .get(document)
            .map(|manager| manager.get_subscribers_for_paths(paths))
            .unwrap_or_default()
    }

    /// Apply changes a client pushed to a document and broadcast them to the
    /// document's subscribers. Refused unless the connection holds the
    /// document with write access.
    pub async fn apply_changes(
        &self,
        from_client_id: String,
        from_connection_id: Uuid,
        document: &str,
        changes: Vec<Vec<u8>>,
        affected_paths: Vec<String>,
    ) -> Result<()> {
        match self.document_access(&from_connection_id, document) {
            Some(access) if access.may_write() => {}
            Some(_) => {
                return Err(anyhow!(
                    "{} holds {} read-only and may not write to it",
                    from_client_id,
                    document
                ))
            }
            None => {
                return Err(anyhow!(
                    "{} has not opened {} and may not write to it",
                    from_client_id,
                    document
                ))
            }
        }
        self.apply_changes_inner(
            from_client_id,
            Some(from_connection_id),
            document,
            changes,
            affected_paths,
        )
        .await
    }

    /// Apply changes from a peer server to the default document.
    ///
    /// Similar to `apply_changes` but marks the broadcast with the peer's client_id
    /// so other peer connections can filter it out (preventing broadcast storms).
    /// Peers have no local connection and no per-document access; they are
    /// trusted on the default document.
    pub async fn apply_peer_changes(
        &self,
        from_peer_id: String,
        changes: Vec<Vec<u8>>,
        affected_paths: Vec<String>,
    ) -> Result<()> {
        self.apply_changes_inner(
            from_peer_id,
            None,
            DEFAULT_DOCUMENT,
            changes,
            affected_paths,
        )
        .await
    }

    /// Internal: Apply changes, persist, and broadcast to subscribers
    async fn apply_changes_inner(
        &self,
        from_client_id: String,
        exclude_connection: Option<Uuid>,
        document: &str,
        changes: Vec<Vec<u8>>,
        affected_paths: Vec<String>,
    ) -> Result<()> {
        // Apply changes to the document and persist it under its id
        {
            let db = self.document(document).await;
            let db = db.write().await;
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

        let subscribers = self.subscribers_for_paths(document, &affected_paths).await;

        // Broadcast to subscribers (except sender)
        if !subscribers.is_empty() {
            let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
            info!(
                "📤 BROADCAST [{}]: {} changes ({} bytes) to {} subscribers",
                document,
                changes.len(),
                total_bytes,
                subscribers.len()
            );

            let msg = BroadcastMessage {
                document: document.to_string(),
                from_client_id: from_client_id.clone(),
                changes: changes.clone(),
                affected_paths: affected_paths.clone(),
                exclude_connection,
                target_clients: subscribers,
            };

            if self.broadcast_tx.send(msg).is_err() {
                tracing::trace!("Broadcast send failed (no receivers)");
            }
        }

        self.log_activity(ActivityEvent::ChangesApplied {
            from_client_id,
            document: document.to_string(),
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
            let managers = self.subscriptions.lock().await;
            managers
                .values()
                .map(|manager| manager.client_count())
                .sum()
        };
        ServerStats {
            active_connections: self.get_connection_count(),
            subscription_count,
            document_count: self.loaded_document_count().await,
            total_changes: self.get_change_count().await,
            uptime_seconds: self.get_uptime_seconds(),
            last_activity: *self.last_activity.read().await,
        }
    }

    /// Get all connection info for admin
    pub async fn get_connections(&self) -> Vec<ConnectionInfo> {
        let managers = self.subscriptions.lock().await;
        self.clients
            .iter()
            .map(|entry| {
                let client = entry.value();
                let mut documents: Vec<DocumentInfo> = client
                    .documents
                    .iter()
                    .map(|(document, access)| DocumentInfo {
                        document: document.clone(),
                        access: *access,
                        subscriptions: managers
                            .get(document)
                            .and_then(|manager| manager.get_client_subscriptions(&client.client_id))
                            .map(|set| set.patterns().to_vec())
                            .unwrap_or_default(),
                    })
                    .collect();
                documents.sort_by(|a, b| a.document.cmp(&b.document));
                let subscriptions = documents
                    .iter()
                    .flat_map(|info| info.subscriptions.iter().cloned())
                    .collect();
                ConnectionInfo {
                    client_id: client.client_id.clone(),
                    documents,
                    subscriptions,
                    transport: client.transport.clone(),
                    connected_at: client.connected_at,
                    last_seen: client.last_seen,
                }
            })
            .collect()
    }

    /// Get all subscription info for admin
    pub async fn get_subscriptions(&self) -> Vec<SubscriptionInfo> {
        let managers = self.subscriptions.lock().await;
        let mut infos: Vec<SubscriptionInfo> = managers
            .iter()
            .flat_map(|(document, manager)| {
                manager
                    .all_clients()
                    .into_iter()
                    .map(|(client_id, patterns)| SubscriptionInfo {
                        client_id,
                        document: document.clone(),
                        patterns,
                    })
            })
            .collect();
        infos.sort_by(|a, b| (&a.document, &a.client_id).cmp(&(&b.document, &b.client_id)));
        infos
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
    pub document_count: usize,
    pub total_changes: usize,
    pub uptime_seconds: u64,
    pub last_activity: i64,
}

/// One open document on a connection (for /admin/connections)
#[derive(Debug, Clone, serde::Serialize)]
pub struct DocumentInfo {
    pub document: String,
    pub access: Access,
    pub subscriptions: Vec<String>,
}

/// Connection info (for /admin/connections)
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConnectionInfo {
    pub client_id: String,
    pub documents: Vec<DocumentInfo>,
    /// Every pattern across every open document; kept for the admin page.
    pub subscriptions: Vec<String>,
    pub transport: String,
    pub connected_at: i64,
    pub last_seen: i64,
}

/// Subscription info (for /admin/subscriptions)
#[derive(Debug, Clone, serde::Serialize)]
pub struct SubscriptionInfo {
    pub client_id: String,
    pub document: String,
    pub patterns: Vec<String>,
}

/// Get current timestamp in milliseconds
fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::PolicyAuthority;
    use swirldb_core::automerge::ScalarValue;
    use swirldb_core::policy::ActorType;
    use swirldb_core::storage::InMemoryDocStorage;

    fn actor(id: &str) -> Actor {
        Actor {
            actor_type: ActorType::User,
            id: id.to_string(),
            org_id: None,
            team_id: None,
            app_id: None,
            role: None,
            claims: Default::default(),
        }
    }

    async fn changes_setting(db: &Document, path: &str, value: &str) -> Vec<Vec<u8>> {
        let db = db.read().await;
        let before = db.get_heads();
        db.set_path(path, ScalarValue::Str(value.into())).unwrap();
        db.get_changes_since(&before)
    }

    #[tokio::test]
    async fn documents_are_persisted_under_their_own_ids() {
        let storage = Arc::new(InMemoryDocStorage::new());
        let state = ServerState::new(None, storage.clone()).await;
        let connection = Uuid::new_v4();
        state
            .register_client(connection, "alice".into(), actor("alice"), "test".into())
            .await;

        for id in ["alpha", "beta"] {
            state
                .open_document(&connection, id, vec!["**".into()])
                .await
                .unwrap();
            let scratch: Document = Arc::new(RwLock::new(SwirlDB::new()));
            let changes = changes_setting(&scratch, "name", id).await;
            state
                .apply_changes("alice".into(), connection, id, changes, vec!["name".into()])
                .await
                .unwrap();
        }

        let mut keys = storage.list_keys().await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["alpha", "beta"]);

        let alpha = state.document("alpha").await;
        assert_eq!(
            alpha.read().await.get_path("name"),
            Some(ScalarValue::Str("alpha".into()))
        );
        let beta = state.document("beta").await;
        assert_eq!(
            beta.read().await.get_path("name"),
            Some(ScalarValue::Str("beta".into()))
        );

        // A fresh server over the same storage finds both by id, beside the
        // default document it always holds.
        let again = ServerState::new(None, storage).await;
        assert_eq!(
            again.document_ids().await,
            vec!["alpha", "beta", DEFAULT_DOCUMENT]
        );
        assert_eq!(
            again.document("beta").await.read().await.get_path("name"),
            Some(ScalarValue::Str("beta".into()))
        );
    }

    #[tokio::test]
    async fn a_document_nobody_has_open_is_unloaded_and_loses_nothing() {
        let storage = Arc::new(InMemoryDocStorage::new());
        let state = ServerState::new(None, storage.clone()).await;
        let alice = Uuid::new_v4();
        let bob = Uuid::new_v4();
        for (connection, name) in [(alice, "alice"), (bob, "bob")] {
            state
                .register_client(connection, name.into(), actor(name), "test".into())
                .await;
            state
                .open_document(&connection, "piece", vec!["**".into()])
                .await
                .unwrap();
        }
        let scratch: Document = Arc::new(RwLock::new(SwirlDB::new()));
        let changes = changes_setting(&scratch, "name", "kept").await;
        state
            .apply_changes("alice".into(), alice, "piece", changes, vec!["name".into()])
            .await
            .unwrap();
        // The default document, and this one.
        assert_eq!(state.loaded_document_count().await, 2);

        // Bob still has it open.
        state.close_document(&alice, "piece").await;
        assert_eq!(state.loaded_document_count().await, 2);

        // Somebody is holding the instance: a read in flight.
        let held = state.document("piece").await;
        state.unregister_client(&bob).await.unwrap();
        assert_eq!(state.loaded_document_count().await, 2);
        drop(held);

        // Nobody: the next close of it lets it go, and the default stays.
        state
            .open_document(&alice, "piece", vec!["**".into()])
            .await
            .unwrap();
        state.close_document(&alice, "piece").await;
        assert_eq!(state.loaded_document_count().await, 1);

        // And what it held comes back from storage.
        assert_eq!(
            state.document("piece").await.read().await.get_path("name"),
            Some(ScalarValue::Str("kept".into()))
        );
    }

    /// Stores `id` with `writes` changes, each its own.
    async fn store_long_history(storage: &InMemoryDocStorage, id: &str, writes: usize) {
        let db = SwirlDB::new();
        for index in 0..writes {
            db.set_path("count", ScalarValue::Int(index as i64))
                .unwrap();
            db.get_heads();
        }
        storage.save(id, &db.save_state()).await.unwrap();
    }

    const SMALL: CompactionThreshold = CompactionThreshold {
        changes: 10,
        bytes: usize::MAX,
    };

    #[tokio::test]
    async fn a_load_compacts_unless_a_connection_already_has_the_document_open() {
        let storage = Arc::new(InMemoryDocStorage::new());
        store_long_history(&storage, "held", 20).await;
        store_long_history(&storage, "free", 20).await;
        let state = ServerState::new(None, storage.clone())
            .await
            .with_compaction(SMALL);
        let connection = Uuid::new_v4();
        state
            .register_client(connection, "alice".into(), actor("alice"), "test".into())
            .await;

        // Recorded as open without being in memory: the race `unload_if_unheld`
        // describes, where the next ask loads it back. Not compacted under her.
        state
            .clients
            .get_mut(&connection)
            .unwrap()
            .documents
            .insert("held".into(), Access::Write);
        let held = state.document("held").await;
        assert_eq!(held.read().await.history().changes, 20);
        assert_eq!(
            held.read().await.get_path("count"),
            Some(ScalarValue::Int(19))
        );

        let free = state.document("free").await;
        assert_eq!(free.read().await.history().changes, 1);
        assert_eq!(
            free.read().await.get_path("count"),
            Some(ScalarValue::Int(19))
        );
        let stored = SwirlDB::with_storage(storage.clone(), "free").await;
        assert!(stored.is_compacted());
    }

    #[tokio::test]
    async fn the_default_document_is_never_compacted() {
        let storage = Arc::new(InMemoryDocStorage::new());
        store_long_history(&storage, DEFAULT_DOCUMENT, 20).await;
        let state = ServerState::new(None, storage).await.with_compaction(SMALL);
        let document = state.document(DEFAULT_DOCUMENT).await;
        assert_eq!(document.read().await.history().changes, 20);
        state.unload_if_unheld(DEFAULT_DOCUMENT).await;
        assert_eq!(state.db().read().await.history().changes, 20);
    }

    #[tokio::test]
    async fn a_push_on_an_unopened_or_read_only_document_is_refused() {
        let engine = PolicyEngine::from_json(
            r#"{"policies":{"rules":[
                {"priority":10,"actor":{"type":"Any"},"action":"Read","path_pattern":"shared","effect":"Allow"}
            ]}}"#,
        )
        .unwrap();
        let state = ServerState::with_authority(
            None,
            Arc::new(InMemoryDocStorage::new()),
            Arc::new(PolicyAuthority::new(engine)),
        )
        .await;
        let connection = Uuid::new_v4();
        state
            .register_client(connection, "alice".into(), actor("alice"), "test".into())
            .await;

        let opened = state
            .open_document(&connection, "shared", vec!["**".into()])
            .await
            .unwrap();
        assert_eq!(opened.access, Access::Read);

        let refused = state
            .open_document(&connection, "private", vec!["**".into()])
            .await;
        assert!(matches!(refused, Err(OpenError::Refused { .. })));

        let scratch: Document = Arc::new(RwLock::new(SwirlDB::new()));
        let changes = changes_setting(&scratch, "name", "x").await;
        let read_only = state
            .apply_changes(
                "alice".into(),
                connection,
                "shared",
                changes.clone(),
                vec![],
            )
            .await;
        assert!(read_only.unwrap_err().to_string().contains("read-only"));
        let unopened = state
            .apply_changes("alice".into(), connection, "private", changes, vec![])
            .await;
        assert!(unopened.unwrap_err().to_string().contains("has not opened"));
        assert!(state
            .document("shared")
            .await
            .read()
            .await
            .get_path("name")
            .is_none());
    }

    #[tokio::test]
    async fn revoking_closes_the_subjects_documents_and_nobody_elses() {
        let state = ServerState::new(None, Arc::new(InMemoryDocStorage::new())).await;
        let mut control = state.subscribe_to_control();
        let alice = Uuid::new_v4();
        let bob = Uuid::new_v4();
        // Alice's client calls itself "not-alice": the subject is the actor.
        state
            .register_client(alice, "not-alice".into(), actor("alice"), "test".into())
            .await;
        state
            .register_client(bob, "bob".into(), actor("bob"), "test".into())
            .await;
        for connection in [alice, bob] {
            for document in ["alpha", "beta"] {
                state
                    .open_document(&connection, document, vec!["**".into()])
                    .await
                    .unwrap();
            }
        }

        let closed = state.revoke("alice", Some("alpha")).await;
        assert_eq!(
            closed,
            vec![Revoked {
                client_id: "not-alice".into(),
                document: "alpha".into()
            }]
        );
        assert!(state.document_access(&alice, "alpha").is_none());
        assert_eq!(state.document_access(&alice, "beta"), Some(Access::Write));
        assert_eq!(state.document_access(&bob, "alpha"), Some(Access::Write));
        assert_eq!(
            state.subscribers_for_paths("alpha", &["name".into()]).await,
            vec!["bob".to_string()]
        );
        match control.try_recv().unwrap() {
            ControlMessage::Revoked {
                connection,
                document,
            } => {
                assert_eq!(connection, alice);
                assert_eq!(document, "alpha");
            }
        }

        // No document named: everything the subject still holds.
        let closed = state.revoke("alice", None).await;
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].document, "beta");
        assert!(state.document_access(&alice, "beta").is_none());
        assert_eq!(state.document_access(&bob, "beta"), Some(Access::Write));
        assert!(state.get_client(&alice).is_some(), "the connection stays");

        // A subject nobody is: nothing to close, nothing wrong.
        assert!(state.revoke("carol", None).await.is_empty());
        assert!(control.try_recv().is_ok());
        assert!(control.try_recv().is_err());
    }

    #[tokio::test]
    async fn closing_a_document_stops_its_broadcasts_only() {
        let state = ServerState::new(None, Arc::new(InMemoryDocStorage::new())).await;
        let connection = Uuid::new_v4();
        state
            .register_client(connection, "alice".into(), actor("alice"), "test".into())
            .await;
        state
            .open_document(&connection, "alpha", vec!["**".into()])
            .await
            .unwrap();
        state
            .open_document(&connection, "beta", vec!["**".into()])
            .await
            .unwrap();
        state.close_document(&connection, "alpha").await;

        assert!(state.document_access(&connection, "alpha").is_none());
        assert_eq!(
            state.document_access(&connection, "beta"),
            Some(Access::Write)
        );
        assert!(state
            .subscribers_for_paths("alpha", &["name".into()])
            .await
            .is_empty());
        assert_eq!(
            state.subscribers_for_paths("beta", &["name".into()]).await,
            vec!["alice".to_string()]
        );
    }
}
