// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Transport-agnostic peer networking for SwirlDB.
//!
//! This module defines the [`PeerTransport`] trait that abstracts over different
//! network carriers (LAN TCP/UDP, WebSocket, BLE). All transports speak the
//! same SwirlDB binary [`Message`](crate::protocol::Message) protocol —
//! callers encode/decode messages, transports just move bytes.
//!
//! # Design
//!
//! Follows the same plugin pattern as [`DocumentStorage`](crate::storage::DocumentStorage)
//! and [`EncryptionProvider`](crate::encryption::EncryptionProvider):
//!
//! - Trait defined here in core (platform-agnostic)
//! - Implementations live in platform crates (swirldb-server, swirl-engine)
//! - WASM-safe via conditional `Send + Sync` marker trait
//!
//! The trait separates two concerns:
//!
//! - **[`PeerTransport`]**: Connection lifecycle and byte-level I/O. Implementations
//!   handle the mechanics of connecting, sending, and receiving over a specific
//!   carrier (TCP, WebSocket, BLE).
//! - **[`PeerDiscovery`]**: Finding peers on the network. Implementations handle
//!   carrier-specific discovery (mDNS for LAN, configured URLs for WebSocket,
//!   BLE scanning). Discovery is optional — some transports connect to
//!   known addresses directly.
//!
//! # Reliable vs Ephemeral
//!
//! - **Reliable**: Ordered, guaranteed delivery. Used for CRDT sync (Push,
//!   Broadcast, Sync messages). Backed by TCP or WebSocket.
//! - **Ephemeral**: Best-effort, unordered. Used for high-frequency data
//!   (beat sync, DMX values). Backed by UDP or lossy channels. Falls back
//!   to the reliable channel if the transport has no separate ephemeral path.
//!
//! # Architecture
//!
//! ```text
//!                          swirldb-core (traits + types)
//! ┌──────────────┐    ┌────────────────┐    ┌───────────────┐
//! │ PeerTransport│    │ PeerDiscovery  │    │ TransportEvent│
//! │   (trait)    │    │   (trait)      │    │   (enum)      │
//! └──────┬───────┘    └───────┬────────┘    └───────────────┘
//!        │                    │
//!        │         swirldb-server / swirl-engine (implementations)
//! ┌──────┴───────┐    ┌───────┴────────┐
//! │LanTransport  │    │ MdnsDiscovery  │
//! │ TCP+UDP      │    │ mDNS browse    │
//! ├──────────────┤    ├────────────────┤
//! │WsTransport   │    │ StaticDiscovery│
//! │ WebSocket    │    │ config URLs    │
//! ├──────────────┤    ├────────────────┤
//! │BleTransport  │    │ BleDiscovery   │
//! │ BLE GATT     │    │ BLE scan       │
//! └──────────────┘    └────────────────┘
//! ```

use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt;

// ---------------------------------------------------------------------------
// WASM-safe marker traits (follows DocumentStorage / EncryptionProvider pattern)
// ---------------------------------------------------------------------------

/// Marker trait for [`PeerTransport`] — adds `Send + Sync` on native only.
#[cfg(not(target_arch = "wasm32"))]
pub trait PeerTransportMarker: Send + Sync {}

/// Marker trait for [`PeerTransport`] — no thread-safety bounds on WASM.
#[cfg(target_arch = "wasm32")]
pub trait PeerTransportMarker {}

/// Marker trait for [`PeerDiscovery`] — adds `Send + Sync` on native only.
#[cfg(not(target_arch = "wasm32"))]
pub trait PeerDiscoveryMarker: Send + Sync {}

/// Marker trait for [`PeerDiscovery`] — no thread-safety bounds on WASM.
#[cfg(target_arch = "wasm32")]
pub trait PeerDiscoveryMarker {}

// ---------------------------------------------------------------------------
// PeerId — unique identifier for a peer on the network
// ---------------------------------------------------------------------------

/// Unique identifier for a peer.
///
/// Typically a UUID or server-id string (e.g., `"srv-a1b2c3d4"`).
/// Must be stable across reconnections so the sync coordinator can
/// track connection state and dedup.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerId(pub String);

impl PeerId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for PeerId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for PeerId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

// ---------------------------------------------------------------------------
// PeerAddr — discovered peer address information
// ---------------------------------------------------------------------------

/// Network address and metadata for a discovered peer.
///
/// Produced by [`PeerDiscovery`] and consumed by [`PeerTransport::connect`].
/// Contains all the information needed to establish a connection.
#[derive(Debug, Clone)]
pub struct PeerAddr {
    /// The peer's unique identifier.
    pub peer_id: PeerId,

    /// Reachable addresses keyed by protocol.
    ///
    /// Examples:
    /// - `"tcp"` → `"10.42.0.1:3030"` (LAN transport)
    /// - `"udp"` → `"10.42.0.1:3031"` (LAN ephemeral)
    /// - `"ws"`  → `"ws://demo.swirldb.org:3030/ws"` (WebSocket)
    /// - `"ble"` → `"AA:BB:CC:DD:EE:FF"` (BLE MAC address)
    pub addresses: HashMap<String, String>,

    /// Metadata from discovery (e.g., mDNS TXT records, BLE advertisement data).
    ///
    /// Examples:
    /// - `"name"`    → `"swirl-pi-living-room"`
    /// - `"version"` → `"0.2.0"`
    /// - `"role"`    → `"beat-leader"`
    pub metadata: HashMap<String, String>,
}

impl PeerAddr {
    /// Create a new PeerAddr with no addresses or metadata.
    pub fn new(peer_id: impl Into<PeerId>) -> Self {
        Self {
            peer_id: peer_id.into(),
            addresses: HashMap::new(),
            metadata: HashMap::new(),
        }
    }

    /// Builder: add a protocol address.
    pub fn with_address(mut self, protocol: impl Into<String>, addr: impl Into<String>) -> Self {
        self.addresses.insert(protocol.into(), addr.into());
        self
    }

    /// Builder: add metadata.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Get an address for a specific protocol.
    pub fn address(&self, protocol: &str) -> Option<&str> {
        self.addresses.get(protocol).map(|s| s.as_str())
    }
}

impl fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Peer({})", self.peer_id)?;
        if !self.addresses.is_empty() {
            let addrs: Vec<String> = self
                .addresses
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            write!(f, " [{}]", addrs.join(", "))?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ServiceInfo — what to advertise about this peer
// ---------------------------------------------------------------------------

/// Information about this peer for network advertising.
///
/// Passed to [`PeerDiscovery::advertise`] to make this peer discoverable
/// by other peers on the network.
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    /// This peer's unique identifier.
    pub peer_id: PeerId,

    /// Service type for discovery protocols (e.g., `"_swirldb._tcp"` for mDNS).
    pub service_type: String,

    /// Port for reliable (TCP/WebSocket) connections.
    pub port: u16,

    /// Optional port for ephemeral (UDP) messages.
    /// If `None`, ephemeral messages share the reliable channel.
    pub ephemeral_port: Option<u16>,

    /// Additional metadata to advertise.
    pub metadata: HashMap<String, String>,
}

impl ServiceInfo {
    /// Create service info with the default SwirlDB service type.
    pub fn new(peer_id: impl Into<PeerId>, port: u16) -> Self {
        Self {
            peer_id: peer_id.into(),
            service_type: "_swirldb._tcp".to_string(),
            port,
            ephemeral_port: None,
            metadata: HashMap::new(),
        }
    }

    /// Set the ephemeral (UDP) port.
    pub fn with_ephemeral_port(mut self, port: u16) -> Self {
        self.ephemeral_port = Some(port);
        self
    }

    /// Add metadata to advertise.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

// ---------------------------------------------------------------------------
// TransportEvent — events emitted by transport and discovery
// ---------------------------------------------------------------------------

/// Events from the transport and discovery layers.
///
/// The sync coordinator (Phase 3) polls these to drive connection lifecycle
/// and message routing. Events from both [`PeerTransport`] and
/// [`PeerDiscovery`] are funneled through the same enum.
#[derive(Debug, Clone)]
pub enum TransportEvent {
    // -- Discovery events --
    /// A new peer was discovered on the network (e.g., via mDNS).
    PeerDiscovered(PeerAddr),

    /// A previously discovered peer is no longer available
    /// (e.g., mDNS TTL expired, BLE out of range).
    PeerLost(PeerId),

    // -- Connection events --
    /// A reliable connection to a peer was established
    /// (outbound via [`PeerTransport::connect`] or inbound accepted).
    PeerConnected(PeerId),

    /// A reliable connection to a peer was lost.
    PeerDisconnected(PeerId),

    // -- Message events --
    /// A reliable (ordered, guaranteed) message was received.
    ///
    /// Payload is an encoded [`Message`](crate::protocol::Message) —
    /// typically Connect, Sync, Push, Broadcast, PushAck, Subscribe, etc.
    ReliableMessage { from: PeerId, data: Vec<u8> },

    /// An ephemeral (best-effort, unordered) message was received.
    ///
    /// Payload is an encoded [`Message`](crate::protocol::Message) —
    /// typically Ephemeral, EphemeralBatch, or EphemeralRelay.
    EphemeralMessage { from: PeerId, data: Vec<u8> },
}

impl fmt::Display for TransportEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerDiscovered(addr) => write!(f, "PeerDiscovered({})", addr.peer_id),
            Self::PeerLost(id) => write!(f, "PeerLost({})", id),
            Self::PeerConnected(id) => write!(f, "PeerConnected({})", id),
            Self::PeerDisconnected(id) => write!(f, "PeerDisconnected({})", id),
            Self::ReliableMessage { from, data } => {
                write!(f, "ReliableMessage(from={}, {} bytes)", from, data.len())
            }
            Self::EphemeralMessage { from, data } => {
                write!(f, "EphemeralMessage(from={}, {} bytes)", from, data.len())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PeerTransport trait — connection and I/O
// ---------------------------------------------------------------------------

/// Transport-agnostic peer connection and message I/O.
///
/// Handles the mechanics of connecting to peers and moving bytes.
/// Does **not** handle discovery — that's [`PeerDiscovery`]'s job.
/// Does **not** interpret messages — callers use
/// [`Message::encode`](crate::protocol::Message::encode) /
/// [`Message::decode`](crate::protocol::Message::decode).
///
/// # Implementations
///
/// | Impl | Reliable | Ephemeral | Notes |
/// |------|----------|-----------|-------|
/// | `LanTransport` | TCP | UDP | Primary for swirl-engine mesh |
/// | `WsTransport` | WebSocket | WebSocket | Existing server refactored |
/// | `BleTransport` | BLE GATT write | BLE notify | Future, 2-device only |
///
/// # Lifecycle
///
/// 1. Create the transport
/// 2. Call [`connect`](Self::connect) for outbound peers, or accept inbound
/// 3. Poll [`next_event`](Self::next_event) in a loop for messages and state changes
/// 4. Send via [`send_reliable`](Self::send_reliable) or [`send_ephemeral`](Self::send_ephemeral)
/// 5. Call [`shutdown`](Self::shutdown) to tear down
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PeerTransport: PeerTransportMarker {
    /// Connect to a peer at the given address.
    ///
    /// Initiates an outbound connection. On success, a
    /// [`TransportEvent::PeerConnected`] will be emitted via [`next_event`](Self::next_event).
    /// The transport may also accept inbound connections (e.g., TCP listener),
    /// which also emit `PeerConnected`.
    async fn connect(&self, peer: &PeerAddr) -> Result<()>;

    /// Disconnect from a peer, closing the reliable channel.
    async fn disconnect(&self, peer: &PeerId) -> Result<()>;

    /// Send a reliable (ordered, guaranteed delivery) message to a connected peer.
    ///
    /// The `data` should be an encoded [`Message`](crate::protocol::Message).
    /// Used for CRDT sync: Push, Broadcast, Sync, Connect, Subscribe, etc.
    ///
    /// # Errors
    /// Returns an error if the peer is not connected or the send fails.
    async fn send_reliable(&self, peer: &PeerId, data: &[u8]) -> Result<()>;

    /// Send an ephemeral (best-effort, unordered) message to a connected peer.
    ///
    /// The `data` should be an encoded [`Message`](crate::protocol::Message).
    /// Used for high-frequency real-time data: beat sync, DMX values.
    /// May be silently dropped under load — callers must tolerate loss.
    ///
    /// If the transport has no separate ephemeral channel (e.g., WebSocket),
    /// this may fall back to the reliable channel.
    fn send_ephemeral(&self, peer: &PeerId, data: &[u8]) -> Result<()>;

    /// Send an ephemeral message to all connected peers.
    ///
    /// Convenience for broadcasting beat sync or similar high-frequency data.
    fn broadcast_ephemeral(&self, data: &[u8]) -> Result<()>;

    /// Wait for the next transport event.
    ///
    /// Blocks (async) until a connection, disconnection, or message event occurs.
    /// This is the main event source for the sync coordinator.
    ///
    /// # Errors
    /// Returns an error if the transport has been shut down.
    async fn next_event(&self) -> Result<TransportEvent>;

    /// Get the IDs of all currently connected peers.
    fn connected_peers(&self) -> Vec<PeerId>;

    /// Check if a specific peer is currently connected.
    fn is_connected(&self, peer: &PeerId) -> bool {
        self.connected_peers().iter().any(|p| p == peer)
    }

    /// Shut down the transport, closing all connections and freeing resources.
    async fn shutdown(&self) -> Result<()>;
}

// ---------------------------------------------------------------------------
// PeerDiscovery trait — finding peers on the network
// ---------------------------------------------------------------------------

/// Peer discovery — finding other SwirlDB instances on the network.
///
/// Separated from [`PeerTransport`] because:
/// - Not all transports need discovery (WebSocket uses configured URLs)
/// - Discovery mechanisms vary independently of transport (mDNS, BLE scan, DNS-SD)
/// - Some setups use one discovery mechanism with multiple transports
///
/// Discovered peers are reported via [`TransportEvent::PeerDiscovered`] and
/// [`TransportEvent::PeerLost`] through [`next_event`](Self::next_event).
///
/// # Implementations
///
/// | Impl | Mechanism | Use case |
/// |------|-----------|----------|
/// | `MdnsDiscovery` | mDNS/DNS-SD browsing | LAN peer discovery |
/// | `StaticDiscovery` | Configured URL list | WebSocket remotes from config |
/// | `BleDiscovery` | BLE scanning | Close-range device pairing |
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PeerDiscovery: PeerDiscoveryMarker {
    /// Start advertising this peer so others can discover it.
    ///
    /// For mDNS: registers a DNS-SD service record.
    /// For BLE: starts GATT advertising.
    /// For static/config: no-op.
    async fn advertise(&self, info: &ServiceInfo) -> Result<()>;

    /// Stop advertising this peer.
    async fn stop_advertising(&self) -> Result<()>;

    /// Start discovering peers on the network.
    ///
    /// Discovered peers will be reported via [`TransportEvent::PeerDiscovered`]
    /// from [`next_event`](Self::next_event).
    async fn start_discovery(&self) -> Result<()>;

    /// Stop discovering peers.
    async fn stop_discovery(&self) -> Result<()>;

    /// Wait for the next discovery event.
    ///
    /// Returns [`TransportEvent::PeerDiscovered`] or [`TransportEvent::PeerLost`].
    ///
    /// # Errors
    /// Returns an error if discovery has been shut down.
    async fn next_event(&self) -> Result<TransportEvent>;

    /// Get all currently known (discovered but not necessarily connected) peers.
    fn discovered_peers(&self) -> Vec<PeerAddr>;
}

// ---------------------------------------------------------------------------
// Mock implementations (for testing)
// ---------------------------------------------------------------------------

/// A recorded message sent via the mock transport.
#[derive(Debug, Clone)]
pub struct SentMessage {
    /// The peer the message was sent to, or `None` for broadcasts.
    pub to: Option<PeerId>,
    /// The message bytes.
    pub data: Vec<u8>,
    /// Whether this was sent as reliable (`true`) or ephemeral (`false`).
    pub reliable: bool,
}

/// In-memory mock transport for testing.
///
/// Records all sent messages and lets tests inject events.
/// Follows the same pattern as [`InMemoryDocStorage`](crate::storage::InMemoryDocStorage) —
/// a test-friendly implementation that lives in core.
///
/// # Usage
///
/// ```rust
/// use swirldb_core::transport::*;
///
/// # futures::executor::block_on(async {
/// let transport = MockTransport::new();
///
/// // Simulate a peer connecting
/// let addr = PeerAddr::new("peer-1").with_address("tcp", "10.0.0.1:3030");
/// transport.connect(&addr).await.unwrap();
/// assert!(transport.is_connected(&PeerId::new("peer-1")));
///
/// // Send a message
/// transport.send_reliable(&PeerId::new("peer-1"), b"hello").await.unwrap();
/// assert_eq!(transport.sent_messages().len(), 1);
///
/// // Inject an event (simulates receiving a message from the network)
/// transport.inject_event(TransportEvent::ReliableMessage {
///     from: PeerId::new("peer-1"),
///     data: b"world".to_vec(),
/// });
/// let event = transport.next_event().await.unwrap();
/// # });
/// ```
pub struct MockTransport {
    connected: std::sync::Mutex<Vec<PeerAddr>>,
    sent: std::sync::Mutex<Vec<SentMessage>>,
    events: std::sync::Mutex<std::collections::VecDeque<TransportEvent>>,
    shut_down: std::sync::Mutex<bool>,
}

impl MockTransport {
    /// Create a new mock transport with no connections or events.
    pub fn new() -> Self {
        Self {
            connected: std::sync::Mutex::new(Vec::new()),
            sent: std::sync::Mutex::new(Vec::new()),
            events: std::sync::Mutex::new(std::collections::VecDeque::new()),
            shut_down: std::sync::Mutex::new(false),
        }
    }

    /// Inject an event into the mock's event queue.
    ///
    /// The next call to [`next_event`](PeerTransport::next_event) will return this event.
    pub fn inject_event(&self, event: TransportEvent) {
        self.events.lock().unwrap().push_back(event);
    }

    /// Get all messages sent through this transport.
    pub fn sent_messages(&self) -> Vec<SentMessage> {
        self.sent.lock().unwrap().clone()
    }

    /// Get messages sent to a specific peer.
    pub fn sent_to(&self, peer: &PeerId) -> Vec<SentMessage> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.to.as_ref() == Some(peer))
            .cloned()
            .collect()
    }

    /// Get all broadcast ephemeral messages (sent to all peers).
    pub fn sent_broadcasts(&self) -> Vec<SentMessage> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.to.is_none())
            .cloned()
            .collect()
    }

    /// Clear the sent message log.
    pub fn clear_sent(&self) {
        self.sent.lock().unwrap().clear();
    }

    /// Check if the transport has been shut down.
    pub fn is_shut_down(&self) -> bool {
        *self.shut_down.lock().unwrap()
    }

    /// Check if there are pending events.
    pub fn has_pending_events(&self) -> bool {
        !self.events.lock().unwrap().is_empty()
    }
}

impl Default for MockTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerTransportMarker for MockTransport {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PeerTransport for MockTransport {
    async fn connect(&self, peer: &PeerAddr) -> Result<()> {
        if *self.shut_down.lock().unwrap() {
            return Err(anyhow::anyhow!("Transport is shut down"));
        }
        let mut connected = self.connected.lock().unwrap();
        if connected.iter().any(|p| p.peer_id == peer.peer_id) {
            return Err(anyhow::anyhow!("Already connected to {}", peer.peer_id));
        }
        connected.push(peer.clone());
        // Auto-inject a PeerConnected event
        self.events
            .lock()
            .unwrap()
            .push_back(TransportEvent::PeerConnected(peer.peer_id.clone()));
        Ok(())
    }

    async fn disconnect(&self, peer: &PeerId) -> Result<()> {
        let mut connected = self.connected.lock().unwrap();
        let before = connected.len();
        connected.retain(|p| &p.peer_id != peer);
        if connected.len() == before {
            return Err(anyhow::anyhow!("Not connected to {}", peer));
        }
        // Auto-inject a PeerDisconnected event
        self.events
            .lock()
            .unwrap()
            .push_back(TransportEvent::PeerDisconnected(peer.clone()));
        Ok(())
    }

    async fn send_reliable(&self, peer: &PeerId, data: &[u8]) -> Result<()> {
        if *self.shut_down.lock().unwrap() {
            return Err(anyhow::anyhow!("Transport is shut down"));
        }
        if !self.is_connected(peer) {
            return Err(anyhow::anyhow!("Not connected to {}", peer));
        }
        self.sent.lock().unwrap().push(SentMessage {
            to: Some(peer.clone()),
            data: data.to_vec(),
            reliable: true,
        });
        Ok(())
    }

    fn send_ephemeral(&self, peer: &PeerId, data: &[u8]) -> Result<()> {
        if *self.shut_down.lock().unwrap() {
            return Err(anyhow::anyhow!("Transport is shut down"));
        }
        if !self.is_connected(peer) {
            return Err(anyhow::anyhow!("Not connected to {}", peer));
        }
        self.sent.lock().unwrap().push(SentMessage {
            to: Some(peer.clone()),
            data: data.to_vec(),
            reliable: false,
        });
        Ok(())
    }

    fn broadcast_ephemeral(&self, data: &[u8]) -> Result<()> {
        if *self.shut_down.lock().unwrap() {
            return Err(anyhow::anyhow!("Transport is shut down"));
        }
        self.sent.lock().unwrap().push(SentMessage {
            to: None,
            data: data.to_vec(),
            reliable: false,
        });
        Ok(())
    }

    async fn next_event(&self) -> Result<TransportEvent> {
        self.events
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("No pending events"))
    }

    fn connected_peers(&self) -> Vec<PeerId> {
        self.connected
            .lock()
            .unwrap()
            .iter()
            .map(|p| p.peer_id.clone())
            .collect()
    }

    async fn shutdown(&self) -> Result<()> {
        *self.shut_down.lock().unwrap() = true;
        self.connected.lock().unwrap().clear();
        Ok(())
    }
}

/// In-memory mock discovery for testing.
///
/// Lets tests inject discovered peers and verify advertising behavior.
pub struct MockDiscovery {
    advertising: std::sync::Mutex<Option<ServiceInfo>>,
    discovering: std::sync::Mutex<bool>,
    known_peers: std::sync::Mutex<Vec<PeerAddr>>,
    events: std::sync::Mutex<std::collections::VecDeque<TransportEvent>>,
}

impl MockDiscovery {
    /// Create a new mock discovery with no known peers.
    pub fn new() -> Self {
        Self {
            advertising: std::sync::Mutex::new(None),
            discovering: std::sync::Mutex::new(false),
            known_peers: std::sync::Mutex::new(Vec::new()),
            events: std::sync::Mutex::new(std::collections::VecDeque::new()),
        }
    }

    /// Simulate a peer appearing on the network.
    ///
    /// Adds the peer to the known list and queues a `PeerDiscovered` event.
    pub fn inject_peer(&self, addr: PeerAddr) {
        self.events
            .lock()
            .unwrap()
            .push_back(TransportEvent::PeerDiscovered(addr.clone()));
        self.known_peers.lock().unwrap().push(addr);
    }

    /// Simulate a peer disappearing from the network.
    ///
    /// Removes the peer from the known list and queues a `PeerLost` event.
    pub fn inject_peer_lost(&self, peer_id: &PeerId) {
        self.events
            .lock()
            .unwrap()
            .push_back(TransportEvent::PeerLost(peer_id.clone()));
        self.known_peers
            .lock()
            .unwrap()
            .retain(|p| &p.peer_id != peer_id);
    }

    /// Check if this mock is currently advertising.
    pub fn is_advertising(&self) -> bool {
        self.advertising.lock().unwrap().is_some()
    }

    /// Get the current advertising info, if any.
    pub fn advertising_info(&self) -> Option<ServiceInfo> {
        self.advertising.lock().unwrap().clone()
    }

    /// Check if this mock is currently discovering.
    pub fn is_discovering(&self) -> bool {
        *self.discovering.lock().unwrap()
    }
}

impl Default for MockDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerDiscoveryMarker for MockDiscovery {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PeerDiscovery for MockDiscovery {
    async fn advertise(&self, info: &ServiceInfo) -> Result<()> {
        *self.advertising.lock().unwrap() = Some(info.clone());
        Ok(())
    }

    async fn stop_advertising(&self) -> Result<()> {
        *self.advertising.lock().unwrap() = None;
        Ok(())
    }

    async fn start_discovery(&self) -> Result<()> {
        *self.discovering.lock().unwrap() = true;
        Ok(())
    }

    async fn stop_discovery(&self) -> Result<()> {
        *self.discovering.lock().unwrap() = false;
        Ok(())
    }

    async fn next_event(&self) -> Result<TransportEvent> {
        self.events
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("No pending discovery events"))
    }

    fn discovered_peers(&self) -> Vec<PeerAddr> {
        self.known_peers.lock().unwrap().clone()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn test_peer_id_equality() {
        let a = PeerId::new("peer-1");
        let b = PeerId::new("peer-1");
        let c = PeerId::new("peer-2");

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_peer_id_display() {
        let id = PeerId::new("swirl-pi-1");
        assert_eq!(format!("{}", id), "swirl-pi-1");
    }

    #[test]
    fn test_peer_id_from() {
        let id: PeerId = "test".into();
        assert_eq!(id.as_str(), "test");

        let id: PeerId = String::from("test2").into();
        assert_eq!(id.as_str(), "test2");
    }

    #[test]
    fn test_peer_addr_builder() {
        let addr = PeerAddr::new("peer-1")
            .with_address("tcp", "10.42.0.1:3030")
            .with_address("udp", "10.42.0.1:3031")
            .with_metadata("name", "living-room-pi");

        assert_eq!(addr.peer_id, PeerId::new("peer-1"));
        assert_eq!(addr.address("tcp"), Some("10.42.0.1:3030"));
        assert_eq!(addr.address("udp"), Some("10.42.0.1:3031"));
        assert_eq!(addr.address("ws"), None);
        assert_eq!(
            addr.metadata.get("name"),
            Some(&"living-room-pi".to_string())
        );
    }

    #[test]
    fn test_peer_addr_display() {
        let addr = PeerAddr::new("peer-1").with_address("tcp", "10.42.0.1:3030");
        let display = format!("{}", addr);
        assert!(display.contains("peer-1"));
        assert!(display.contains("tcp=10.42.0.1:3030"));
    }

    #[test]
    fn test_peer_addr_no_addresses() {
        let addr = PeerAddr::new("lonely-peer");
        assert_eq!(format!("{}", addr), "Peer(lonely-peer)");
    }

    #[test]
    fn test_service_info_defaults() {
        let info = ServiceInfo::new("peer-1", 3030);
        assert_eq!(info.peer_id, PeerId::new("peer-1"));
        assert_eq!(info.service_type, "_swirldb._tcp");
        assert_eq!(info.port, 3030);
        assert_eq!(info.ephemeral_port, None);
        assert!(info.metadata.is_empty());
    }

    #[test]
    fn test_service_info_builder() {
        let info = ServiceInfo::new("peer-1", 3030)
            .with_ephemeral_port(3031)
            .with_metadata("version", "0.2.0")
            .with_metadata("role", "beat-leader");

        assert_eq!(info.ephemeral_port, Some(3031));
        assert_eq!(info.metadata.get("version"), Some(&"0.2.0".to_string()));
        assert_eq!(info.metadata.get("role"), Some(&"beat-leader".to_string()));
    }

    #[test]
    fn test_transport_event_display() {
        let events = vec![
            TransportEvent::PeerDiscovered(PeerAddr::new("p1")),
            TransportEvent::PeerLost(PeerId::new("p2")),
            TransportEvent::PeerConnected(PeerId::new("p3")),
            TransportEvent::PeerDisconnected(PeerId::new("p4")),
            TransportEvent::ReliableMessage {
                from: PeerId::new("p5"),
                data: vec![1, 2, 3],
            },
            TransportEvent::EphemeralMessage {
                from: PeerId::new("p6"),
                data: vec![4, 5],
            },
        ];

        assert!(format!("{}", events[0]).contains("PeerDiscovered"));
        assert!(format!("{}", events[1]).contains("PeerLost"));
        assert!(format!("{}", events[2]).contains("PeerConnected"));
        assert!(format!("{}", events[3]).contains("PeerDisconnected"));
        assert!(format!("{}", events[4]).contains("3 bytes"));
        assert!(format!("{}", events[5]).contains("2 bytes"));
    }

    #[test]
    fn test_peer_id_hash_map_key() {
        let mut map = HashMap::new();
        map.insert(PeerId::new("peer-1"), "hello");
        map.insert(PeerId::new("peer-2"), "world");

        assert_eq!(map.get(&PeerId::new("peer-1")), Some(&"hello"));
        assert_eq!(map.get(&PeerId::new("peer-2")), Some(&"world"));
        assert_eq!(map.get(&PeerId::new("peer-3")), None);
    }

    // -----------------------------------------------------------------------
    // MockTransport tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_mock_connect_disconnect() {
        block_on(async {
            let transport = MockTransport::new();
            let addr = PeerAddr::new("peer-1").with_address("tcp", "10.0.0.1:3030");

            // Connect
            transport.connect(&addr).await.unwrap();
            assert!(transport.is_connected(&PeerId::new("peer-1")));
            assert_eq!(transport.connected_peers().len(), 1);

            // Should have emitted PeerConnected
            let event = transport.next_event().await.unwrap();
            assert!(matches!(event, TransportEvent::PeerConnected(id) if id.as_str() == "peer-1"));

            // Duplicate connect fails
            assert!(transport.connect(&addr).await.is_err());

            // Disconnect
            transport.disconnect(&PeerId::new("peer-1")).await.unwrap();
            assert!(!transport.is_connected(&PeerId::new("peer-1")));
            assert_eq!(transport.connected_peers().len(), 0);

            // Should have emitted PeerDisconnected
            let event = transport.next_event().await.unwrap();
            assert!(
                matches!(event, TransportEvent::PeerDisconnected(id) if id.as_str() == "peer-1")
            );

            // Duplicate disconnect fails
            assert!(transport.disconnect(&PeerId::new("peer-1")).await.is_err());
        });
    }

    #[test]
    fn test_mock_send_reliable() {
        block_on(async {
            let transport = MockTransport::new();
            let addr = PeerAddr::new("peer-1").with_address("tcp", "10.0.0.1:3030");
            transport.connect(&addr).await.unwrap();

            // Send reliable
            transport
                .send_reliable(&PeerId::new("peer-1"), b"crdt-changes")
                .await
                .unwrap();

            let sent = transport.sent_messages();
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0].to, Some(PeerId::new("peer-1")));
            assert_eq!(sent[0].data, b"crdt-changes");
            assert!(sent[0].reliable);

            // Send to disconnected peer fails
            assert!(transport
                .send_reliable(&PeerId::new("peer-2"), b"data")
                .await
                .is_err());
        });
    }

    #[test]
    fn test_mock_send_ephemeral() {
        block_on(async {
            let transport = MockTransport::new();
            let addr = PeerAddr::new("peer-1").with_address("tcp", "10.0.0.1:3030");
            transport.connect(&addr).await.unwrap();

            // Send ephemeral to specific peer
            transport
                .send_ephemeral(&PeerId::new("peer-1"), b"beat-sync")
                .unwrap();

            let sent = transport.sent_to(&PeerId::new("peer-1"));
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0].data, b"beat-sync");
            assert!(!sent[0].reliable);

            // Send to disconnected peer fails
            assert!(transport
                .send_ephemeral(&PeerId::new("peer-2"), b"data")
                .is_err());
        });
    }

    #[test]
    fn test_mock_broadcast_ephemeral() {
        block_on(async {
            let transport = MockTransport::new();

            // Broadcast works even with no peers (it's fire-and-forget)
            transport.broadcast_ephemeral(b"beat-sync").unwrap();

            let broadcasts = transport.sent_broadcasts();
            assert_eq!(broadcasts.len(), 1);
            assert_eq!(broadcasts[0].data, b"beat-sync");
            assert!(broadcasts[0].to.is_none());
            assert!(!broadcasts[0].reliable);
        });
    }

    #[test]
    fn test_mock_inject_events() {
        block_on(async {
            let transport = MockTransport::new();

            // Inject a message event
            transport.inject_event(TransportEvent::ReliableMessage {
                from: PeerId::new("peer-1"),
                data: b"hello".to_vec(),
            });
            transport.inject_event(TransportEvent::EphemeralMessage {
                from: PeerId::new("peer-1"),
                data: b"beat".to_vec(),
            });

            assert!(transport.has_pending_events());

            // Events come out in order
            let event = transport.next_event().await.unwrap();
            assert!(
                matches!(event, TransportEvent::ReliableMessage { from, data }
                if from.as_str() == "peer-1" && data == b"hello")
            );

            let event = transport.next_event().await.unwrap();
            assert!(
                matches!(event, TransportEvent::EphemeralMessage { from, data }
                if from.as_str() == "peer-1" && data == b"beat")
            );

            // No more events
            assert!(!transport.has_pending_events());
            assert!(transport.next_event().await.is_err());
        });
    }

    #[test]
    fn test_mock_shutdown() {
        block_on(async {
            let transport = MockTransport::new();
            let addr = PeerAddr::new("peer-1").with_address("tcp", "10.0.0.1:3030");
            transport.connect(&addr).await.unwrap();

            transport.shutdown().await.unwrap();

            assert!(transport.is_shut_down());
            assert_eq!(transport.connected_peers().len(), 0);
            // Operations fail after shutdown
            assert!(transport.connect(&addr).await.is_err());
            assert!(transport
                .send_reliable(&PeerId::new("peer-1"), b"data")
                .await
                .is_err());
            assert!(transport.broadcast_ephemeral(b"data").is_err());
        });
    }

    #[test]
    fn test_mock_clear_sent() {
        block_on(async {
            let transport = MockTransport::new();
            let addr = PeerAddr::new("peer-1").with_address("tcp", "10.0.0.1:3030");
            transport.connect(&addr).await.unwrap();

            transport
                .send_reliable(&PeerId::new("peer-1"), b"msg1")
                .await
                .unwrap();
            assert_eq!(transport.sent_messages().len(), 1);

            transport.clear_sent();
            assert_eq!(transport.sent_messages().len(), 0);
        });
    }

    #[test]
    fn test_mock_multiple_peers() {
        block_on(async {
            let transport = MockTransport::new();
            let addr1 = PeerAddr::new("peer-1").with_address("tcp", "10.0.0.1:3030");
            let addr2 = PeerAddr::new("peer-2").with_address("tcp", "10.0.0.2:3030");

            transport.connect(&addr1).await.unwrap();
            transport.connect(&addr2).await.unwrap();
            assert_eq!(transport.connected_peers().len(), 2);

            // Send to each
            transport
                .send_reliable(&PeerId::new("peer-1"), b"to-1")
                .await
                .unwrap();
            transport
                .send_reliable(&PeerId::new("peer-2"), b"to-2")
                .await
                .unwrap();

            assert_eq!(transport.sent_to(&PeerId::new("peer-1")).len(), 1);
            assert_eq!(transport.sent_to(&PeerId::new("peer-2")).len(), 1);

            // Disconnect one, other still works
            transport.disconnect(&PeerId::new("peer-1")).await.unwrap();
            assert!(!transport.is_connected(&PeerId::new("peer-1")));
            assert!(transport.is_connected(&PeerId::new("peer-2")));
        });
    }

    // -----------------------------------------------------------------------
    // MockDiscovery tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_mock_discovery_advertise() {
        block_on(async {
            let discovery = MockDiscovery::new();
            assert!(!discovery.is_advertising());

            let info = ServiceInfo::new("my-peer", 3030).with_metadata("version", "0.2.0");
            discovery.advertise(&info).await.unwrap();

            assert!(discovery.is_advertising());
            let ad = discovery.advertising_info().unwrap();
            assert_eq!(ad.peer_id, PeerId::new("my-peer"));
            assert_eq!(ad.port, 3030);

            discovery.stop_advertising().await.unwrap();
            assert!(!discovery.is_advertising());
        });
    }

    #[test]
    fn test_mock_discovery_find_peers() {
        block_on(async {
            let discovery = MockDiscovery::new();
            discovery.start_discovery().await.unwrap();
            assert!(discovery.is_discovering());

            // Simulate peers appearing
            let addr1 = PeerAddr::new("peer-1").with_address("tcp", "10.42.0.2:3030");
            let addr2 = PeerAddr::new("peer-2").with_address("tcp", "10.42.0.3:3030");
            discovery.inject_peer(addr1);
            discovery.inject_peer(addr2);

            assert_eq!(discovery.discovered_peers().len(), 2);

            // Events come out in order
            let event = discovery.next_event().await.unwrap();
            assert!(
                matches!(event, TransportEvent::PeerDiscovered(addr) if addr.peer_id.as_str() == "peer-1")
            );

            let event = discovery.next_event().await.unwrap();
            assert!(
                matches!(event, TransportEvent::PeerDiscovered(addr) if addr.peer_id.as_str() == "peer-2")
            );

            // Peer disappears
            discovery.inject_peer_lost(&PeerId::new("peer-1"));
            assert_eq!(discovery.discovered_peers().len(), 1);

            let event = discovery.next_event().await.unwrap();
            assert!(matches!(event, TransportEvent::PeerLost(id) if id.as_str() == "peer-1"));

            discovery.stop_discovery().await.unwrap();
            assert!(!discovery.is_discovering());
        });
    }

    #[test]
    fn test_mock_transport_with_protocol_messages() {
        use crate::protocol::Message;

        block_on(async {
            let transport = MockTransport::new();
            let addr = PeerAddr::new("peer-1").with_address("tcp", "10.0.0.1:3030");
            transport.connect(&addr).await.unwrap();
            // Drain the PeerConnected event
            let _ = transport.next_event().await;

            // Send a real protocol message (Push) via reliable
            let push = Message::Push {
                heads: vec![1, 2, 3],
                changes: vec![vec![4, 5, 6]],
            };
            let encoded = push.encode();
            transport
                .send_reliable(&PeerId::new("peer-1"), &encoded)
                .await
                .unwrap();

            // Simulate receiving a Broadcast via injected event
            let broadcast = Message::Broadcast {
                from_client_id: "peer-1".to_string(),
                changes: vec![vec![7, 8, 9]],
                affected_paths: vec!["settings.brightness".to_string()],
            };
            transport.inject_event(TransportEvent::ReliableMessage {
                from: PeerId::new("peer-1"),
                data: broadcast.encode(),
            });

            // Verify sent message can be decoded
            let sent = transport.sent_messages();
            let decoded = Message::decode(&sent[0].data).unwrap();
            assert!(matches!(decoded, Message::Push { .. }));

            // Verify received event can be decoded
            let event = transport.next_event().await.unwrap();
            if let TransportEvent::ReliableMessage { data, .. } = event {
                let decoded = Message::decode(&data).unwrap();
                assert!(matches!(decoded, Message::Broadcast { .. }));
            } else {
                panic!("Expected ReliableMessage");
            }

            // Send ephemeral beat sync
            let beat = Message::EphemeralBatch {
                updates: vec![("beat.bpm".to_string(), vec![0, 120])],
            };
            transport.broadcast_ephemeral(&beat.encode()).unwrap();

            let broadcasts = transport.sent_broadcasts();
            let decoded = Message::decode(&broadcasts[0].data).unwrap();
            assert!(matches!(decoded, Message::EphemeralBatch { .. }));
        });
    }
}
