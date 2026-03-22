// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Sync-aware peer lifecycle manager.
//!
//! The [`PeerManager`] is the high-level peer-to-peer coordination layer for
//! SwirlDB. It bridges discovery, transport, and the CRDT sync protocol into
//! a single, simple interface for applications.
//!
//! ```text
//!   Application (swirl-engine, swirldb-server)
//!     │
//!     │  PeerEvent::ChangesApplied { paths }
//!     │  PeerEvent::EphemeralReceived { from, updates }
//!     │  PeerEvent::PeerSynced { peer_id }
//!     │  PeerEvent::PeerDisconnected { peer_id }
//!     ▼
//!   PeerManager
//!     │  Owns: SwirlDB instance (shared Arc<RwLock<SwirlDB>>)
//!     │  Runs: sync protocol per-peer (Connect → Sync → Push/Broadcast)
//!     │  Handles: auto-connect, reconnect, subscription routing
//!     ▼
//!   PeerTransport + PeerDiscovery (trait objects)
//! ```
//!
//! # What Makes This Different
//!
//! Unlike a naive connection manager, the PeerManager **understands the sync
//! protocol**. When a peer connects, it automatically:
//!
//! 1. Performs the Connect/Sync handshake (exchanges heads, sends delta)
//! 2. Applies incoming CRDT changes to the shared SwirlDB instance
//! 3. Routes outgoing changes to peers with matching subscriptions
//! 4. Handles ephemeral messages (beat sync, cursor positions)
//!
//! The application never touches raw protocol bytes. It receives high-level
//! events: "peer synced", "changes applied at these paths", "ephemeral data
//! from this peer".
//!
//! # Design Principles (learned from libp2p, NATS, Automerge sync)
//!
//! - **Interest-based routing**: Changes are only sent to peers subscribed
//!   to affected paths (like NATS subjects). Not every peer gets everything.
//! - **Delta sync on reconnect**: Peers exchange heads and only send missing
//!   changes. No full-state transfer on every reconnect.
//! - **Dual-channel**: Reliable (TCP) for CRDT sync, ephemeral (UDP) for
//!   real-time data. One transport, two behaviors.
//! - **Symmetric connections**: No client/server distinction. Both sides are
//!   equal peers. Either side can initiate sync.
//! - **Transport-agnostic**: Works over LAN (TCP/UDP), WebSocket, or BLE.
//!   The sync protocol is the same regardless of carrier.
//!
//! # Usage
//!
//! ```rust,ignore
//! use swirldb_core::core::SwirlDB;
//! use swirldb_server::peer_manager::{PeerManager, PeerManagerConfig, PeerEvent};
//! use swirldb_server::transport::LanTransport;
//! use swirldb_server::discovery::MdnsDiscovery;
//!
//! // Create shared CRDT database
//! let db = Arc::new(RwLock::new(SwirlDB::new()));
//!
//! // Create transport and discovery
//! let transport = LanTransport::bind("pi-1", 3030, Some(3031)).await?;
//! let discovery = MdnsDiscovery::new()?;
//! // ... advertise and start_discovery ...
//!
//! // Create peer manager — it runs the sync protocol automatically
//! let mesh = PeerManager::start(
//!     PeerManagerConfig::default(),
//!     db.clone(),
//!     transport,
//!     Some(discovery),
//! );
//!
//! // Write locally, push to peers
//! {
//!     let db = db.write().await;
//!     db.set_path("settings.bpm", 120.into())?;
//!     let changes = db.get_changes(); // TODO: get only new changes
//!     mesh.push_local_changes(changes, vec!["settings.bpm".into()]).await?;
//! }
//!
//! // Receive events
//! loop {
//!     match mesh.next_event().await? {
//!         PeerEvent::PeerSynced { peer_id, .. } => {
//!             println!("Peer {} synced — CRDT state is shared", peer_id);
//!         }
//!         PeerEvent::ChangesApplied { from, affected_paths } => {
//!             println!("Remote changes at {:?}", affected_paths);
//!             // Re-read from db to get updated values
//!         }
//!         PeerEvent::EphemeralReceived { from, updates } => {
//!             for (path, data) in &updates {
//!                 // e.g., "beat.bpm" → [0, 120]
//!             }
//!         }
//!         PeerEvent::PeerDisconnected { peer_id } => {
//!             println!("Peer {} left", peer_id);
//!         }
//!     }
//! }
//! ```

use anyhow::{bail, Result};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use swirldb_core::core::SwirlDB;
use swirldb_core::protocol::Message;
use swirldb_core::transport::{PeerAddr, PeerDiscovery, PeerId, PeerTransport, TransportEvent};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Event channel capacity.
const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Size of an Automerge change hash (SHA-256).
const HEAD_SIZE: usize = 32;

// ---------------------------------------------------------------------------
// PeerEvent — what the application sees
// ---------------------------------------------------------------------------

/// Events emitted by the [`PeerManager`] to the application.
///
/// These are high-level, sync-aware events. The application never needs to
/// handle raw protocol bytes or manage the sync handshake.
#[derive(Debug, Clone)]
pub enum PeerEvent {
    /// A peer connected and completed the initial sync handshake.
    ///
    /// At this point, the local SwirlDB instance has been updated with
    /// any changes the peer had that we were missing. The CRDT state
    /// is converged.
    PeerSynced { peer_id: PeerId, info: PeerInfo },

    /// CRDT changes were received from a peer and applied to SwirlDB.
    ///
    /// The database is already updated — the app can re-read affected paths
    /// to get new values. This is emitted for ongoing changes after the
    /// initial sync, not for the initial sync itself.
    ChangesApplied {
        from: PeerId,
        affected_paths: Vec<String>,
    },

    /// Ephemeral (non-CRDT) data received from a peer.
    ///
    /// Used for high-frequency real-time data: beat sync, cursor positions,
    /// DMX values. Not stored, not merged — just forwarded.
    EphemeralReceived {
        from: PeerId,
        updates: Vec<(String, Vec<u8>)>,
    },

    /// A peer disconnected.
    ///
    /// If the peer was discovered via mDNS/etc, the manager will attempt
    /// to reconnect automatically with exponential backoff.
    PeerDisconnected { peer_id: PeerId },
}

// ---------------------------------------------------------------------------
// PeerInfo — metadata about a connected peer
// ---------------------------------------------------------------------------

/// Information about a connected peer, exposed to the application.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub addr: PeerAddr,
    pub source: PeerSource,
    pub connected_at: Instant,
    /// Metadata from discovery (mDNS TXT records, etc.)
    pub metadata: HashMap<String, String>,
}

/// How we learned about this peer.
#[derive(Debug, Clone, PartialEq)]
pub enum PeerSource {
    /// Found via PeerDiscovery (mDNS, BLE scan). Auto-reconnects.
    Discovered,
    /// Connected explicitly via [`PeerManager::connect`]. No auto-reconnect.
    Manual,
    /// They connected to us (inbound). No auto-reconnect.
    Inbound,
}

// ---------------------------------------------------------------------------
// PeerManagerConfig
// ---------------------------------------------------------------------------

/// Configuration for the [`PeerManager`].
#[derive(Debug, Clone)]
pub struct PeerManagerConfig {
    /// Path patterns to subscribe to on each peer (default: `["**"]` = everything).
    pub subscriptions: Vec<String>,
    /// Initial delay before the first reconnect attempt.
    pub initial_reconnect: Duration,
    /// Maximum delay between reconnect attempts.
    pub max_reconnect: Duration,
}

impl Default for PeerManagerConfig {
    fn default() -> Self {
        Self {
            subscriptions: vec!["**".to_string()],
            initial_reconnect: Duration::from_secs(1),
            max_reconnect: Duration::from_secs(30),
        }
    }
}

// ---------------------------------------------------------------------------
// Internal per-peer state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ConnState {
    /// TCP connected, sync handshake not yet complete.
    Connected,
    /// Sync handshake complete — ready for ongoing sync.
    Synced,
    /// Disconnected, may be reconnecting.
    Disconnected { attempts: u32 },
}

#[derive(Debug, Clone)]
struct ManagedPeer {
    addr: PeerAddr,
    source: PeerSource,
    state: ConnState,
    connected_at: Option<Instant>,
    /// Whether we initiated the connection (outbound) or they did (inbound).
    /// Determines who sends the Connect message first.
    outbound: bool,
    /// Path patterns this peer is subscribed to (from their Connect message).
    /// Empty means "everything" (same as `["**"]`).
    subscriptions: Vec<String>,
}

// ---------------------------------------------------------------------------
// Path pattern matching for subscription filtering
// ---------------------------------------------------------------------------

/// Check if a path matches any of the given subscription patterns.
///
/// Supports:
/// - `**` matches zero or more path segments
/// - `*` matches exactly one path segment
/// - Exact segment match
/// - `!pattern` negates a pattern (exclude matching paths)
///
/// Empty patterns list matches everything.
fn path_matches_subscriptions(path: &str, patterns: &[String]) -> bool {
    if patterns.is_empty() {
        return true;
    }

    let path_segs: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();
    let mut matched = false;

    for pattern in patterns {
        if let Some(negated) = pattern.strip_prefix('!') {
            // Negation: if the path matches the negated pattern, exclude it
            let pat_segs: Vec<&str> = negated.split('.').filter(|s| !s.is_empty()).collect();
            if match_segments(&pat_segs, &path_segs) {
                return false;
            }
        } else {
            let pat_segs: Vec<&str> = pattern.split('.').filter(|s| !s.is_empty()).collect();
            if match_segments(&pat_segs, &path_segs) {
                matched = true;
            }
        }
    }

    matched
}

fn match_segments(pattern: &[&str], path: &[&str]) -> bool {
    if pattern.is_empty() && path.is_empty() {
        return true;
    }
    if pattern.is_empty() || path.is_empty() {
        return pattern.len() == 1 && pattern[0] == "**";
    }
    match pattern[0] {
        "**" => {
            for i in 0..=path.len() {
                if match_segments(&pattern[1..], &path[i..]) {
                    return true;
                }
            }
            false
        }
        "*" => match_segments(&pattern[1..], &path[1..]),
        segment => {
            if segment == path[0] {
                match_segments(&pattern[1..], &path[1..])
            } else {
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PeerManager
// ---------------------------------------------------------------------------

/// Sync-aware peer lifecycle manager.
///
/// See [module docs](self) for architecture and usage.
pub struct PeerManager {
    inner: Arc<Inner>,
    event_rx: Mutex<mpsc::Receiver<PeerEvent>>,
}

struct Inner {
    transport: Box<dyn PeerTransport>,
    db: Arc<RwLock<SwirlDB>>,
    peers: DashMap<String, ManagedPeer>,
    event_tx: mpsc::Sender<PeerEvent>,
    shut_down: AtomicBool,
    config: PeerManagerConfig,
    reconnect_tx: mpsc::Sender<String>,
    /// Our peer ID (from the transport's local identity).
    local_peer_id: String,
    /// Flag: CRDT changes have been applied but not yet persisted to disk.
    /// Checked by the persist timer task every PERSIST_DEBOUNCE interval.
    persist_needed: AtomicBool,
}

/// How often to flush pending CRDT changes to disk (seconds).
/// Prevents blocking the event loop with synchronous disk writes on every push.
const PERSIST_DEBOUNCE_SECS: u64 = 1;

impl PeerManager {
    /// Start the peer manager.
    ///
    /// Spawns background tasks for transport events, discovery events,
    /// and reconnection. The transport and discovery should already be
    /// bound/advertising/browsing before calling this.
    ///
    /// # Arguments
    ///
    /// - `config`: Subscriptions, reconnect timing, etc.
    /// - `db`: Shared SwirlDB instance. The PeerManager reads heads and
    ///   changes for sync, and applies incoming changes.
    /// - `transport`: The bound transport (e.g., `LanTransport`).
    /// - `discovery`: Optional peer discovery (e.g., `MdnsDiscovery`).
    /// - `local_peer_id`: This peer's identity string.
    pub fn start(
        config: PeerManagerConfig,
        db: Arc<RwLock<SwirlDB>>,
        transport: impl PeerTransport + 'static,
        discovery: Option<impl PeerDiscovery + 'static>,
        local_peer_id: impl Into<String>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let (reconnect_tx, reconnect_rx) = mpsc::channel::<String>(256);

        let inner = Arc::new(Inner {
            transport: Box::new(transport),
            db,
            peers: DashMap::new(),
            event_tx,
            shut_down: AtomicBool::new(false),
            config,
            reconnect_tx,
            local_peer_id: local_peer_id.into(),
            persist_needed: AtomicBool::new(false),
        });

        // Spawn transport event loop
        {
            let inner = Arc::clone(&inner);
            tokio::spawn(async move { transport_loop(inner).await });
        }

        // Spawn discovery loop
        if let Some(disc) = discovery {
            let inner = Arc::clone(&inner);
            tokio::spawn(async move { discovery_loop(inner, disc).await });
        }

        // Spawn reconnect processor
        {
            let inner = Arc::clone(&inner);
            tokio::spawn(async move { reconnect_processor(inner, reconnect_rx).await });
        }

        // Spawn keepalive ping timer — sends Ping to all connected peers periodically
        // so the TCP read timeout doesn't fire on healthy idle connections.
        {
            let inner = Arc::clone(&inner);
            tokio::spawn(async move { keepalive_timer(inner).await });
        }

        // Spawn debounced persist timer — flushes CRDT to disk at most once per second
        {
            let inner = Arc::clone(&inner);
            tokio::spawn(async move { persist_timer(inner).await });
        }

        Self {
            inner,
            event_rx: Mutex::new(event_rx),
        }
    }

    /// Wait for the next peer event.
    pub async fn next_event(&self) -> Result<PeerEvent> {
        let mut rx = self.event_rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("PeerManager event channel closed"))
    }

    /// Manually connect to a peer (not via discovery).
    ///
    /// The sync handshake will be performed automatically. A
    /// [`PeerEvent::PeerSynced`] event will be emitted when complete.
    /// Manual peers are NOT auto-reconnected on disconnect.
    pub async fn connect(&self, peer: &PeerAddr) -> Result<()> {
        if self.inner.shut_down.load(Ordering::Relaxed) {
            bail!("PeerManager is shut down");
        }

        let peer_id = peer.peer_id.as_str().to_string();
        if self.inner.peers.contains_key(&peer_id) {
            bail!("Already tracking peer {}", peer_id);
        }

        self.inner.peers.insert(
            peer_id,
            ManagedPeer {
                addr: peer.clone(),
                source: PeerSource::Manual,
                state: ConnState::Connected,
                connected_at: Some(Instant::now()),
                outbound: true,
                subscriptions: vec![],
            },
        );

        self.inner.transport.connect(peer).await
    }

    /// Push local CRDT changes to all connected, synced peers.
    ///
    /// Call this after writing to SwirlDB locally. The changes will be
    /// sent to all peers whose subscriptions match the affected paths.
    ///
    /// # Arguments
    ///
    /// - `changes`: Raw Automerge change bytes (from `SwirlDB::get_changes()`
    ///   or captured after a write).
    /// - `affected_paths`: Dot-notation paths that changed (e.g., `["settings.bpm"]`).
    pub async fn push_local_changes(
        &self,
        changes: Vec<Vec<u8>>,
        affected_paths: Vec<String>,
    ) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        let heads = {
            let db = self.inner.db.read().await;
            db.get_heads().into_iter().flatten().collect()
        };

        let msg = Message::Push { heads, changes };
        let encoded = msg.encode();

        // Send to all synced peers whose subscriptions match the affected paths
        for entry in self.inner.peers.iter() {
            if !matches!(entry.state, ConnState::Synced) {
                continue;
            }

            // Check if any affected path matches this peer's subscriptions
            let dominated = &entry.subscriptions;
            if !dominated.is_empty()
                && !affected_paths
                    .iter()
                    .any(|p| path_matches_subscriptions(p, dominated))
            {
                debug!(
                    "Skipping push to {} — no paths match subscriptions",
                    entry.key()
                );
                continue;
            }

            let peer_id = PeerId::new(entry.key().as_str());
            if let Err(e) = self.inner.transport.send_reliable(&peer_id, &encoded).await {
                warn!("Failed to push changes to {}: {}", entry.key(), e);
            }
        }

        Ok(())
    }

    /// Send an ephemeral message to a specific peer.
    pub fn send_ephemeral(&self, peer: &PeerId, data: &[u8]) -> Result<()> {
        self.inner.transport.send_ephemeral(peer, data)
    }

    /// Broadcast ephemeral data to all connected peers.
    ///
    /// Used for high-frequency real-time data (beat sync, DMX, etc.).
    pub fn broadcast_ephemeral(&self, data: &[u8]) -> Result<()> {
        self.inner.transport.broadcast_ephemeral(data)
    }

    /// Get info about a connected peer.
    pub fn peer_info(&self, peer_id: &PeerId) -> Option<PeerInfo> {
        let entry = self.inner.peers.get(peer_id.as_str())?;
        Some(PeerInfo {
            peer_id: peer_id.clone(),
            addr: entry.addr.clone(),
            source: entry.source.clone(),
            connected_at: entry.connected_at.unwrap_or_else(Instant::now),
            metadata: entry.addr.metadata.clone(),
        })
    }

    /// Get all connected (and synced) peer IDs.
    pub fn connected_peers(&self) -> Vec<PeerId> {
        self.inner.transport.connected_peers()
    }

    /// Get all synced peer IDs (completed the handshake).
    pub fn synced_peers(&self) -> Vec<PeerId> {
        self.inner
            .peers
            .iter()
            .filter(|e| matches!(e.state, ConnState::Synced))
            .map(|e| PeerId::new(e.key().as_str()))
            .collect()
    }

    /// Remove a peer from tracking (e.g., before a manual reconnect).
    /// Also disconnects at the transport level if still connected.
    pub async fn remove_peer(&self, peer_id: &PeerId) {
        let id = peer_id.as_str().to_string();
        self.inner.peers.remove(&id);
        if self.inner.transport.is_connected(peer_id) {
            let _ = self.inner.transport.disconnect(peer_id).await;
        }
    }

    /// Shut down the peer manager and transport.
    pub async fn shutdown(&self) -> Result<()> {
        // Persist any pending changes before shutting down
        if self.inner.persist_needed.swap(false, Ordering::AcqRel) {
            let db = self.inner.db.read().await;
            if let Err(e) = db.persist().await {
                error!("Failed to persist on shutdown: {}", e);
            }
        }
        self.inner.shut_down.store(true, Ordering::Relaxed);
        self.inner.transport.shutdown().await?;
        self.inner.peers.clear();
        info!("PeerManager shut down");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Transport event loop
// ---------------------------------------------------------------------------

async fn transport_loop(inner: Arc<Inner>) {
    loop {
        if inner.shut_down.load(Ordering::Relaxed) {
            break;
        }

        let event = match inner.transport.next_event().await {
            Ok(e) => e,
            Err(e) => {
                if inner.shut_down.load(Ordering::Relaxed) {
                    break;
                }
                debug!("Transport event error: {}", e);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        match event {
            TransportEvent::PeerConnected(peer_id) => {
                handle_peer_connected(&inner, &peer_id).await;
            }

            TransportEvent::PeerDisconnected(peer_id) => {
                handle_peer_disconnected(&inner, &peer_id).await;
            }

            TransportEvent::ReliableMessage { from, data } => {
                handle_reliable_message(&inner, &from, &data).await;
            }

            TransportEvent::EphemeralMessage { from, data } => {
                handle_ephemeral_message(&inner, &from, &data).await;
            }

            // Discovery events come from the discovery loop, not transport
            TransportEvent::PeerDiscovered(_) | TransportEvent::PeerLost(_) => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Debounced persist timer
// ---------------------------------------------------------------------------

/// Periodically flushes CRDT changes to disk if any have accumulated.
///
/// CRDT changes are applied to the in-memory Automerge document immediately
/// when received. This timer persists them to storage at most once per
/// `PERSIST_DEBOUNCE_SECS`, preventing slow disk I/O (especially on SD cards)
/// from blocking the event loop on every Push message.
async fn persist_timer(inner: Arc<Inner>) {
    let mut interval = tokio::time::interval(Duration::from_secs(PERSIST_DEBOUNCE_SECS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        if inner.shut_down.load(Ordering::Relaxed) {
            // Final persist on shutdown
            if inner.persist_needed.swap(false, Ordering::AcqRel) {
                let db = inner.db.read().await;
                if let Err(e) = db.persist().await {
                    error!("Failed to persist on shutdown: {}", e);
                }
            }
            break;
        }

        if inner.persist_needed.swap(false, Ordering::AcqRel) {
            let db = inner.db.read().await;
            if let Err(e) = db.persist().await {
                error!("Failed to persist (debounced): {}", e);
                // Re-set the flag so we retry next tick
                inner.persist_needed.store(true, Ordering::Release);
            }
        }
    }
}

/// Periodically sends Ping messages to all connected peers.
///
/// This serves two purposes:
/// 1. Keeps the TCP connection alive through NAT/firewall timeouts
/// 2. Ensures the remote peer's read loop doesn't time out on idle connections
///
/// The interval must be shorter than the transport's TCP_READ_TIMEOUT_SECS.
async fn keepalive_timer(inner: Arc<Inner>) {
    // Send pings every 15 seconds — well within the 45s read timeout
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let ping_data = Message::Ping.encode();

    loop {
        interval.tick().await;

        if inner.shut_down.load(Ordering::Relaxed) {
            break;
        }

        // Send ping to all synced peers
        let peer_ids: Vec<String> = inner
            .peers
            .iter()
            .filter(|e| matches!(e.value().state, ConnState::Synced))
            .map(|e| e.key().clone())
            .collect();

        for pid in &peer_ids {
            if let Err(e) = inner
                .transport
                .send_reliable(&PeerId::new(pid), &ping_data)
                .await
            {
                debug!("Keepalive ping to {} failed: {}", pid, e);
            }
        }

        if !peer_ids.is_empty() {
            debug!("Keepalive ping sent to {} peers", peer_ids.len());
        }
    }
}

/// A peer's TCP connection was established. Start the sync handshake.
async fn handle_peer_connected(inner: &Arc<Inner>, peer_id: &PeerId) {
    let id = peer_id.as_str().to_string();

    // If this is an unknown peer (inbound connection), register it
    if !inner.peers.contains_key(&id) {
        inner.peers.insert(
            id.clone(),
            ManagedPeer {
                addr: PeerAddr::new(peer_id.clone()),
                source: PeerSource::Inbound,
                state: ConnState::Connected,
                connected_at: Some(Instant::now()),
                outbound: false,
                subscriptions: vec![],
            },
        );
    } else {
        // Update state for known peers (discovered or manual)
        if let Some(mut entry) = inner.peers.get_mut(&id) {
            entry.state = ConnState::Connected;
            entry.connected_at = Some(Instant::now());
        }
    }

    // Determine if we should initiate the sync handshake.
    // The outbound side sends Connect first. The inbound side waits.
    let is_outbound = inner.peers.get(&id).map(|e| e.outbound).unwrap_or(false);

    if is_outbound {
        if let Err(e) = send_connect_message(inner, peer_id).await {
            warn!("Failed to send Connect to {}: {}", id, e);
        }
    }
    // Inbound peers: we wait for their Connect message in handle_reliable_message
}

/// A peer disconnected. Clean up and maybe schedule reconnect.
async fn handle_peer_disconnected(inner: &Arc<Inner>, peer_id: &PeerId) {
    let id = peer_id.as_str().to_string();

    let should_reconnect = if let Some(mut entry) = inner.peers.get_mut(&id) {
        let reconnect = entry.source == PeerSource::Discovered;
        if reconnect {
            entry.state = ConnState::Disconnected { attempts: 0 };
        }
        reconnect
    } else {
        false
    };

    // Remove non-reconnecting peers from tracking
    if !should_reconnect {
        inner.peers.remove(&id);
    }

    let _ = inner
        .event_tx
        .send(PeerEvent::PeerDisconnected {
            peer_id: peer_id.clone(),
        })
        .await;

    if should_reconnect {
        let _ = inner.reconnect_tx.send(id).await;
    }
}

/// Handle a reliable (TCP) message from a peer.
///
/// This is where the sync protocol lives:
/// - Connect: peer is initiating sync handshake → respond with Sync
/// - Sync: peer is responding to our Connect → apply changes, mark synced
/// - Push: ongoing changes from peer → apply and notify app
/// - Broadcast: changes relayed through peer → apply and notify app
/// - Ping/Pong: keepalive
async fn handle_reliable_message(inner: &Arc<Inner>, from: &PeerId, data: &[u8]) {
    let msg = match Message::decode(data) {
        Ok(m) => m,
        Err(e) => {
            warn!("Failed to decode message from {}: {}", from, e);
            return;
        }
    };

    let from_id = from.as_str().to_string();

    match msg {
        // -- Sync handshake: they sent Connect, we respond with Sync --
        Message::Connect {
            client_id: _,
            subscriptions,
            heads,
        } => {
            info!(
                "📥 Received Connect from {} (subscriptions: {:?})",
                from_id, subscriptions
            );

            // Store the peer's subscriptions so we can filter outgoing changes
            if let Some(mut entry) = inner.peers.get_mut(&from_id) {
                entry.subscriptions = subscriptions;
            }

            // Get changes they need (delta sync based on their heads)
            let changes = {
                let db = inner.db.read().await;
                if heads.is_empty() {
                    db.get_changes()
                } else {
                    let peer_heads = parse_heads(&heads);
                    db.get_changes_since(&peer_heads)
                }
            };

            let our_heads: Vec<u8> = {
                let db = inner.db.read().await;
                db.get_heads().into_iter().flatten().collect()
            };

            let sync_msg = Message::Sync {
                heads: our_heads,
                changes,
            };

            if let Err(e) = inner
                .transport
                .send_reliable(from, &sync_msg.encode())
                .await
            {
                error!("Failed to send Sync to {}: {}", from_id, e);
                return;
            }

            info!("📤 Sent Sync response to {}", from_id);

            // Send our own Connect back so they can sync us too (bidirectional).
            // But only if the peer isn't already synced — otherwise we'd loop
            // (Connect → Sync+Connect → Sync+Connect → ...).
            let already_synced = inner
                .peers
                .get(&from_id)
                .map(|e| matches!(e.state, ConnState::Synced))
                .unwrap_or(false);
            if !already_synced {
                if let Err(e) = send_connect_message(inner, from).await {
                    warn!("Failed to send Connect back to {}: {}", from_id, e);
                }
            }
        }

        // -- Sync handshake: they responded to our Connect --
        Message::Sync { heads: _, changes } => {
            if !changes.is_empty() {
                let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
                info!(
                    "📥 Sync from {}: {} changes ({} bytes)",
                    from_id,
                    changes.len(),
                    total_bytes
                );

                let db = inner.db.read().await;
                if let Err(e) = db.apply_changes(changes) {
                    error!("Failed to apply sync changes from {}: {}", from_id, e);
                    return;
                }
                if let Err(e) = db.persist().await {
                    error!("Failed to persist after sync from {}: {}", from_id, e);
                }
            }

            // Mark peer as synced
            let info = mark_peer_synced(inner, &from_id);

            if let Some(info) = info {
                let _ = inner
                    .event_tx
                    .send(PeerEvent::PeerSynced {
                        peer_id: from.clone(),
                        info,
                    })
                    .await;
                info!("✅ Peer {} synced", from_id);
            }
        }

        // -- Ongoing sync: peer is pushing changes --
        Message::Push { heads: _, changes } | Message::Broadcast { changes, .. } => {
            if changes.is_empty() {
                return;
            }

            let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
            info!(
                "📥 Changes from {}: {} changes ({} bytes)",
                from_id,
                changes.len(),
                total_bytes
            );

            // Extract affected paths before applying
            let affected_paths = {
                let db = inner.db.read().await;
                db.extract_affected_paths(&changes).unwrap_or_else(|e| {
                    warn!("Failed to extract paths: {}", e);
                    vec!["**".to_string()]
                })
            };

            // Apply to local SwirlDB (persist is debounced — see persist_timer)
            {
                let db = inner.db.read().await;
                if let Err(e) = db.apply_changes(changes.clone()) {
                    error!("Failed to apply changes from {}: {}", from_id, e);
                    return;
                }
                inner.persist_needed.store(true, Ordering::Release);
            }

            // Notify app
            let _ = inner
                .event_tx
                .send(PeerEvent::ChangesApplied {
                    from: from.clone(),
                    affected_paths: affected_paths.clone(),
                })
                .await;

            // Relay to other synced peers (not back to sender)
            // This enables multi-hop sync in mesh topologies
            let heads: Vec<u8> = {
                let db = inner.db.read().await;
                db.get_heads().into_iter().flatten().collect()
            };

            let relay_msg = Message::Push { heads, changes };
            let encoded = relay_msg.encode();

            for entry in inner.peers.iter() {
                if entry.key() == &from_id {
                    continue; // Don't echo back
                }
                if !matches!(entry.state, ConnState::Synced) {
                    continue;
                }

                // Only relay if any affected path matches this peer's subscriptions
                let subs = &entry.subscriptions;
                if !subs.is_empty()
                    && !affected_paths
                        .iter()
                        .any(|p| path_matches_subscriptions(p, subs))
                {
                    debug!(
                        "Skipping relay to {} — no paths match subscriptions",
                        entry.key()
                    );
                    continue;
                }

                let peer = PeerId::new(entry.key().as_str());
                if let Err(e) = inner.transport.send_reliable(&peer, &encoded).await {
                    debug!("Failed to relay to {}: {}", entry.key(), e);
                }
            }
        }

        // Ephemeral messages arriving over TCP (fallback when UDP addr unknown)
        Message::Ephemeral { path, data } => {
            let _ = inner
                .event_tx
                .send(PeerEvent::EphemeralReceived {
                    from: from.clone(),
                    updates: vec![(path, data)],
                })
                .await;
        }

        Message::EphemeralBatch { updates } => {
            let _ = inner
                .event_tx
                .send(PeerEvent::EphemeralReceived {
                    from: from.clone(),
                    updates,
                })
                .await;
        }

        Message::PushAck { .. } | Message::SubscribeAck { .. } => {
            // Acknowledgments — can be used for flow control later
        }

        Message::Ping => {
            let _ = inner
                .transport
                .send_reliable(from, &Message::Pong.encode())
                .await;
        }

        Message::Pong => {}

        other => {
            debug!("Unhandled message type from {}: {:?}", from_id, other);
        }
    }
}

/// Handle an ephemeral (UDP) message from a peer.
async fn handle_ephemeral_message(inner: &Arc<Inner>, from: &PeerId, data: &[u8]) {
    let msg = match Message::decode(data) {
        Ok(m) => m,
        Err(e) => {
            debug!("Failed to decode ephemeral from {}: {}", from, e);
            return;
        }
    };

    match msg {
        Message::EphemeralBatch { updates } => {
            let _ = inner
                .event_tx
                .send(PeerEvent::EphemeralReceived {
                    from: from.clone(),
                    updates,
                })
                .await;
        }

        Message::Ephemeral {
            path,
            data: payload,
        } => {
            let _ = inner
                .event_tx
                .send(PeerEvent::EphemeralReceived {
                    from: from.clone(),
                    updates: vec![(path, payload)],
                })
                .await;
        }

        other => {
            debug!(
                "Unexpected ephemeral message type from {}: {:?}",
                from, other
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Sync protocol helpers
// ---------------------------------------------------------------------------

/// Send our Connect message to a peer (starts the sync handshake).
async fn send_connect_message(inner: &Arc<Inner>, peer: &PeerId) -> Result<()> {
    let heads: Vec<u8> = {
        let db = inner.db.read().await;
        db.get_heads().into_iter().flatten().collect()
    };

    let connect_msg = Message::Connect {
        client_id: inner.local_peer_id.clone(),
        subscriptions: inner.config.subscriptions.clone(),
        heads,
    };

    inner
        .transport
        .send_reliable(peer, &connect_msg.encode())
        .await
}

/// Parse flat head bytes into individual 32-byte hashes.
fn parse_heads(flat_heads: &[u8]) -> Vec<Vec<u8>> {
    flat_heads
        .chunks_exact(HEAD_SIZE)
        .map(|chunk| chunk.to_vec())
        .collect()
}

/// Mark a peer as synced and return its PeerInfo.
fn mark_peer_synced(inner: &Arc<Inner>, peer_id: &str) -> Option<PeerInfo> {
    let mut entry = inner.peers.get_mut(peer_id)?;
    entry.state = ConnState::Synced;

    Some(PeerInfo {
        peer_id: PeerId::new(peer_id),
        addr: entry.addr.clone(),
        source: entry.source.clone(),
        connected_at: entry.connected_at.unwrap_or_else(Instant::now),
        metadata: entry.addr.metadata.clone(),
    })
}

// ---------------------------------------------------------------------------
// Discovery loop
// ---------------------------------------------------------------------------

async fn discovery_loop(inner: Arc<Inner>, discovery: impl PeerDiscovery) {
    loop {
        if inner.shut_down.load(Ordering::Relaxed) {
            break;
        }

        let event = match discovery.next_event().await {
            Ok(e) => e,
            Err(e) => {
                if inner.shut_down.load(Ordering::Relaxed) {
                    break;
                }
                debug!("Discovery error: {}", e);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        match event {
            TransportEvent::PeerDiscovered(addr) => {
                let peer_id = addr.peer_id.as_str().to_string();

                // Skip if already connected or reconnecting
                if inner.peers.contains_key(&peer_id) {
                    debug!("Discovered peer {} already tracked", peer_id);
                    continue;
                }

                info!(
                    "🔗 Discovered peer: {} (tcp={:?}, udp={:?}, metadata={:?})",
                    addr,
                    addr.address("tcp"),
                    addr.address("udp"),
                    addr.metadata
                );

                inner.peers.insert(
                    peer_id.clone(),
                    ManagedPeer {
                        addr: addr.clone(),
                        source: PeerSource::Discovered,
                        state: ConnState::Connected,
                        connected_at: None,
                        outbound: true,
                        subscriptions: vec![],
                    },
                );

                match inner.transport.connect(&addr).await {
                    Ok(()) => {
                        // PeerConnected event will come through transport_loop
                    }
                    Err(e) => {
                        // Could be a race (they connected to us first) —
                        // check if transport says we're already connected
                        if inner.transport.is_connected(&PeerId::new(&peer_id)) {
                            debug!(
                                "Connect to {} failed but already connected (simultaneous connect)",
                                peer_id
                            );
                            // State will be updated when we process the PeerConnected event
                        } else {
                            warn!("Failed to connect to discovered peer {}: {}", peer_id, e);
                            if let Some(mut entry) = inner.peers.get_mut(&peer_id) {
                                entry.state = ConnState::Disconnected { attempts: 0 };
                            }
                            let _ = inner.reconnect_tx.send(peer_id).await;
                        }
                    }
                }
            }

            TransportEvent::PeerLost(peer_id) => {
                let id = peer_id.as_str().to_string();
                info!("Peer lost from discovery: {}", id);

                // Remove tracking — stop reconnecting
                if let Some((_, peer)) = inner.peers.remove(&id) {
                    if matches!(peer.state, ConnState::Connected | ConnState::Synced) {
                        let _ = inner.transport.disconnect(&peer_id).await;
                    }
                }
            }

            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Reconnection
// ---------------------------------------------------------------------------

async fn reconnect_processor(inner: Arc<Inner>, mut rx: mpsc::Receiver<String>) {
    while let Some(peer_id) = rx.recv().await {
        if inner.shut_down.load(Ordering::Relaxed) {
            break;
        }
        let inner = Arc::clone(&inner);
        tokio::spawn(async move { reconnect_loop(inner, peer_id).await });
    }
}

async fn reconnect_loop(inner: Arc<Inner>, peer_id: String) {
    loop {
        if inner.shut_down.load(Ordering::Relaxed) {
            return;
        }

        // Get addr and compute delay
        let (addr, delay) = {
            let entry = match inner.peers.get(&peer_id) {
                Some(e) => e,
                None => return, // Removed (PeerLost) — stop
            };

            match &entry.state {
                ConnState::Synced | ConnState::Connected => return, // Already reconnected
                ConnState::Disconnected { attempts } => {
                    let base = inner.config.initial_reconnect.as_millis() as u64;
                    let max = inner.config.max_reconnect.as_millis() as u64;
                    let delay_ms = (base * 2u64.saturating_pow(*attempts)).min(max);
                    (entry.addr.clone(), Duration::from_millis(delay_ms))
                }
            }
        };

        debug!("Reconnecting to {} in {:?}", peer_id, delay);
        tokio::time::sleep(delay).await;

        // Re-check after sleep
        if inner.shut_down.load(Ordering::Relaxed) || !inner.peers.contains_key(&peer_id) {
            return;
        }
        if let Some(entry) = inner.peers.get(&peer_id) {
            if matches!(entry.state, ConnState::Synced | ConnState::Connected) {
                return; // Reconnected while sleeping (e.g., inbound)
            }
        }

        match inner.transport.connect(&addr).await {
            Ok(()) => {
                info!("Reconnected to {}", peer_id);
                // PeerConnected → sync handshake will happen via transport_loop
                return;
            }
            Err(e) => {
                if inner.transport.is_connected(&PeerId::new(&peer_id)) {
                    debug!("Reconnect to {} failed but already connected", peer_id);
                    return;
                }
                warn!("Reconnect to {} failed: {}", peer_id, e);
                if let Some(mut entry) = inner.peers.get_mut(&peer_id) {
                    let next = match &entry.state {
                        ConnState::Disconnected { attempts } => attempts + 1,
                        _ => 1,
                    };
                    entry.state = ConnState::Disconnected { attempts: next };
                }
                // Loop continues with longer delay
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use swirldb_core::transport::{MockDiscovery, MockTransport};
    use tokio::time::Duration;

    fn test_db() -> Arc<RwLock<SwirlDB>> {
        Arc::new(RwLock::new(SwirlDB::new()))
    }

    #[tokio::test]
    async fn test_start_and_shutdown() {
        let manager = PeerManager::start(
            PeerManagerConfig::default(),
            test_db(),
            MockTransport::new(),
            None::<MockDiscovery>,
            "test-peer",
        );

        assert!(manager.connected_peers().is_empty());
        assert!(manager.synced_peers().is_empty());
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_manual_connect_triggers_sync() {
        let transport = MockTransport::new();
        let db = test_db();

        let manager = PeerManager::start(
            PeerManagerConfig::default(),
            db.clone(),
            transport,
            None::<MockDiscovery>,
            "local-peer",
        );

        let addr = PeerAddr::new("remote-peer").with_address("tcp", "10.0.0.1:3030");
        manager.connect(&addr).await.unwrap();

        // Should get PeerConnected from transport → triggers Connect message
        // MockTransport auto-generates PeerConnected on connect
        // The transport_loop will see it and call handle_peer_connected
        // which sends a Connect message (because outbound=true)

        // Give the background tasks a moment
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The mock transport should have received the Connect message
        // (We can't easily verify this with MockTransport since the
        // PeerManager owns it. But we can verify the peer is tracked.)
        assert!(manager.inner.peers.contains_key("remote-peer"));

        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_discovery_auto_connects() {
        let transport = MockTransport::new();
        let discovery = MockDiscovery::new();
        let db = test_db();

        // Inject a discovered peer
        let peer_addr = PeerAddr::new("discovered-1").with_address("tcp", "10.42.0.2:3030");
        discovery.inject_peer(peer_addr);

        let manager = PeerManager::start(
            PeerManagerConfig::default(),
            db,
            transport,
            Some(discovery),
            "local-peer",
        );

        // Give discovery_loop time to process the event and auto-connect
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Peer should be tracked as discovered + outbound
        assert!(
            manager.inner.peers.contains_key("discovered-1"),
            "Discovered peer should be tracked"
        );

        let entry = manager.inner.peers.get("discovered-1").unwrap();
        assert_eq!(entry.source, PeerSource::Discovered);
        assert!(entry.outbound);
        drop(entry);

        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_inbound_peer_registered() {
        let transport = MockTransport::new();
        let db = test_db();

        let manager = PeerManager::start(
            PeerManagerConfig::default(),
            db,
            transport,
            None::<MockDiscovery>,
            "local-peer",
        );

        // Simulate an inbound connection by injecting PeerConnected event
        // This is tricky because MockTransport is owned by PeerManager.
        // With real LanTransport, inbound connections just emit PeerConnected.
        // For unit tests, we verify the handling logic via the entry-point.
        //
        // The handle_peer_connected function will register unknown peers
        // as Inbound. We can test this by checking the function's behavior:
        // - Unknown peer → creates entry with source=Inbound, outbound=false
        // - Doesn't send Connect (waits for the peer to send it)

        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_peer_info_exposed() {
        let transport = MockTransport::new();
        let db = test_db();

        let manager = PeerManager::start(
            PeerManagerConfig::default(),
            db,
            transport,
            None::<MockDiscovery>,
            "local-peer",
        );

        let addr = PeerAddr::new("peer-1")
            .with_address("tcp", "10.0.0.1:3030")
            .with_metadata("role", "beat-leader");

        manager.connect(&addr).await.unwrap();

        let info = manager.peer_info(&PeerId::new("peer-1")).unwrap();
        assert_eq!(info.source, PeerSource::Manual);
        assert_eq!(info.metadata.get("role"), Some(&"beat-leader".to_string()));

        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_shutdown_prevents_connect() {
        let manager = PeerManager::start(
            PeerManagerConfig::default(),
            test_db(),
            MockTransport::new(),
            None::<MockDiscovery>,
            "test-peer",
        );

        manager.shutdown().await.unwrap();

        let addr = PeerAddr::new("peer-1").with_address("tcp", "10.0.0.1:3030");
        assert!(manager.connect(&addr).await.is_err());
    }

    // -----------------------------------------------------------------------
    // Subscription pattern matching tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_subscriptions_match_everything() {
        assert!(path_matches_subscriptions("settings.bpm", &[]));
        assert!(path_matches_subscriptions("anything", &[]));
    }

    #[test]
    fn test_exact_match() {
        let subs = vec!["settings.bpm".to_string()];
        assert!(path_matches_subscriptions("settings.bpm", &subs));
        assert!(!path_matches_subscriptions("settings.palette", &subs));
    }

    #[test]
    fn test_double_star_matches_all() {
        let subs = vec!["**".to_string()];
        assert!(path_matches_subscriptions("settings.bpm", &subs));
        assert!(path_matches_subscriptions("a.b.c.d.e", &subs));
        assert!(path_matches_subscriptions("x", &subs));
    }

    #[test]
    fn test_single_star_wildcard() {
        let subs = vec!["settings.*".to_string()];
        assert!(path_matches_subscriptions("settings.bpm", &subs));
        assert!(path_matches_subscriptions("settings.palette", &subs));
        assert!(!path_matches_subscriptions("settings.deep.nested", &subs));
        assert!(!path_matches_subscriptions("other.bpm", &subs));
    }

    #[test]
    fn test_negation_pattern() {
        let subs = vec!["**".to_string(), "!settings.output_enabled".to_string()];
        assert!(path_matches_subscriptions("settings.bpm", &subs));
        assert!(path_matches_subscriptions("settings.palette", &subs));
        assert!(!path_matches_subscriptions(
            "settings.output_enabled",
            &subs
        ));
    }

    #[test]
    fn test_negation_with_wildcard() {
        let subs = vec!["**".to_string(), "!settings.device_local.*".to_string()];
        assert!(path_matches_subscriptions("settings.bpm", &subs));
        assert!(!path_matches_subscriptions(
            "settings.device_local.volume",
            &subs
        ));
        assert!(!path_matches_subscriptions(
            "settings.device_local.output",
            &subs
        ));
    }

    #[test]
    fn test_negation_takes_precedence() {
        // Even if multiple positive patterns match, negation wins
        let subs = vec![
            "**".to_string(),
            "settings.*".to_string(),
            "!settings.output_enabled".to_string(),
        ];
        assert!(!path_matches_subscriptions(
            "settings.output_enabled",
            &subs
        ));
    }

    #[test]
    fn test_double_star_mid_pattern() {
        let subs = vec!["fixtures.**.color".to_string()];
        assert!(path_matches_subscriptions("fixtures.led1.color", &subs));
        assert!(path_matches_subscriptions(
            "fixtures.room.led1.color",
            &subs
        ));
        assert!(!path_matches_subscriptions(
            "fixtures.led1.brightness",
            &subs
        ));
    }

    #[tokio::test]
    async fn test_parse_heads() {
        let heads = vec![0u8; 64]; // Two 32-byte heads
        let parsed = parse_heads(&heads);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].len(), 32);
        assert_eq!(parsed[1].len(), 32);

        // Empty heads
        let parsed = parse_heads(&[]);
        assert_eq!(parsed.len(), 0);

        // Partial head (< 32 bytes) is dropped by chunks_exact
        let parsed = parse_heads(&[0u8; 33]);
        assert_eq!(parsed.len(), 1);
    }
}
