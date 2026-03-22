// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! LAN transport — TCP (reliable) + UDP (ephemeral) peer communication.
//!
//! Primary transport for swirl-engine mesh setups: multiple Pis and Macs on
//! the same WiFi network. Implements [`PeerTransport`] from swirldb-core.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │              LanTransport               │
//! │                                         │
//! │  TCP listener ──→ accept task           │
//! │  TCP connections ──→ per-peer read task  │
//! │  UDP socket ──→ single recv task        │
//! │                                         │
//! │  peers: { PeerId → PeerConn }           │
//! │  event_tx ──→ event_rx (next_event)     │
//! └─────────────────────────────────────────┘
//! ```
//!
//! # TCP Framing
//!
//! Length-prefixed: `[4-byte big-endian length][payload]`.
//! Max frame size: 16 MiB (sanity limit).
//!
//! # Handshake
//!
//! On connect (both directions), the initiator sends its PeerId as the
//! first framed message (raw UTF-8 string, not a protocol Message).
//! The acceptor reads it, registers the peer, and sends its own PeerId back.
//!
//! # UDP
//!
//! Single bound socket. Peer UDP addresses learned from [`PeerAddr`] metadata
//! (populated by mDNS discovery's `eph_port` TXT record). Incoming UDP
//! packets are prefixed with `[1-byte peer_id_len][peer_id_bytes]` for demuxing.

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use swirldb_core::transport::{
    PeerAddr, PeerId, PeerTransport, PeerTransportMarker, TransportEvent,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

/// Maximum TCP frame size: 16 MiB.
const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// Maximum UDP packet size. Our ephemeral messages are small (~20-200 bytes).
const MAX_UDP_PACKET: usize = 4096;

/// TCP keepalive interval — how often to probe idle connections.
const TCP_KEEPALIVE_SECS: u64 = 10;

/// TCP read timeout — if no data arrives for this long, consider the peer dead.
/// Must be significantly longer than keepalive to allow for retries.
const TCP_READ_TIMEOUT_SECS: u64 = 45;

/// Event channel capacity.
const EVENT_CHANNEL_CAPACITY: usize = 1024;

// ---------------------------------------------------------------------------
// PeerConn — per-peer connection state
// ---------------------------------------------------------------------------

/// State for a single connected peer.
struct PeerConn {
    /// Write half of the TCP stream. Read half is owned by the read task.
    tcp_writer: Mutex<tokio::net::tcp::OwnedWriteHalf>,
    /// Peer's UDP address for ephemeral sends. `None` if unknown yet
    /// (falls back to TCP).
    udp_addr: Option<SocketAddr>,
}

// ---------------------------------------------------------------------------
// Shared inner state (held by Arc in both LanTransport and spawned tasks)
// ---------------------------------------------------------------------------

/// Shared state for the transport, extracted so both the struct and
/// spawned tasks can hold `Arc<Inner>` without needing `Arc<LanTransport>`.
/// Shared byte counters for transport throughput monitoring.
/// Created by `LanTransport::bind()` and accessible via `LanTransport::stats()`.
/// The `Arc` references remain valid even after the transport is moved.
#[derive(Debug, Clone)]
pub struct TransportByteCounters {
    pub tcp_bytes_sent: Arc<AtomicU64>,
    pub tcp_bytes_recv: Arc<AtomicU64>,
    pub udp_bytes_sent: Arc<AtomicU64>,
    pub udp_bytes_recv: Arc<AtomicU64>,
}

impl Default for TransportByteCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportByteCounters {
    pub fn new() -> Self {
        Self {
            tcp_bytes_sent: Arc::new(AtomicU64::new(0)),
            tcp_bytes_recv: Arc::new(AtomicU64::new(0)),
            udp_bytes_sent: Arc::new(AtomicU64::new(0)),
            udp_bytes_recv: Arc::new(AtomicU64::new(0)),
        }
    }
}

struct Inner {
    local_peer_id: String,
    peers: DashMap<String, PeerConn>,
    udp_socket: UdpSocket,
    event_tx: mpsc::Sender<TransportEvent>,
    shut_down: AtomicBool,
    /// Reverse map: UDP source addr → PeerId for demuxing incoming packets.
    udp_addr_to_peer: DashMap<SocketAddr, String>,
    /// Our TCP listener address (for callers and tests).
    tcp_local_addr: SocketAddr,
    /// Our UDP local address.
    udp_local_addr: SocketAddr,
    /// Shared byte counters.
    counters: TransportByteCounters,
}

impl Inner {
    /// Remove a peer from all maps.
    fn remove_peer(&self, peer_id: &str) {
        if let Some((_, conn)) = self.peers.remove(peer_id) {
            if let Some(addr) = conn.udp_addr {
                self.udp_addr_to_peer.remove(&addr);
            }
        }
    }

    /// Build the UDP packet: [1-byte id_len][peer_id_bytes][payload]
    fn build_udp_packet(&self, data: &[u8]) -> Vec<u8> {
        let id_bytes = self.local_peer_id.as_bytes();
        let id_len = id_bytes.len().min(255) as u8;
        let mut packet = Vec::with_capacity(1 + id_len as usize + data.len());
        packet.push(id_len);
        packet.extend_from_slice(&id_bytes[..id_len as usize]);
        packet.extend_from_slice(data);
        packet
    }
}

// ---------------------------------------------------------------------------
// LanTransport
// ---------------------------------------------------------------------------

/// LAN transport: TCP for reliable messages, UDP for ephemeral.
///
/// Created via [`LanTransport::bind`], which starts the TCP listener and
/// UDP socket. Use [`connect`](PeerTransport::connect) for outbound connections;
/// inbound connections are accepted automatically by the listener task.
///
/// # Example
///
/// ```rust,ignore
/// use swirldb_server::transport::LanTransport;
/// use swirldb_core::transport::{PeerTransport, PeerAddr, TransportEvent};
///
/// let transport = LanTransport::bind("my-peer", 3030, Some(3031)).await?;
///
/// // Connect to a discovered peer
/// let addr = PeerAddr::new("other-peer")
///     .with_address("tcp", "10.42.0.2:3030")
///     .with_address("udp", "10.42.0.2:3031");
/// transport.connect(&addr).await?;
///
/// // Poll events
/// loop {
///     match transport.next_event().await? {
///         TransportEvent::PeerConnected(id) => println!("Connected: {}", id),
///         TransportEvent::ReliableMessage { from, data } => { /* handle */ }
///         TransportEvent::EphemeralMessage { from, data } => { /* handle */ }
///         _ => {}
///     }
/// }
/// ```
pub struct LanTransport {
    inner: Arc<Inner>,
    event_rx: Mutex<mpsc::Receiver<TransportEvent>>,
}

impl LanTransport {
    /// Bind the TCP listener and UDP socket, start accept/recv tasks.
    ///
    /// - `local_peer_id`: This peer's unique ID (sent during handshake).
    /// - `tcp_port`: Port for the TCP listener (0 = OS-assigned).
    /// - `udp_port`: Optional port for the UDP socket. If `None`, uses port 0.
    pub async fn bind(
        local_peer_id: impl Into<String>,
        tcp_port: u16,
        udp_port: Option<u16>,
    ) -> Result<Self> {
        let local_peer_id = local_peer_id.into();

        // Bind TCP listener
        let tcp_addr: SocketAddr = format!("0.0.0.0:{}", tcp_port).parse()?;
        let tcp_listener = TcpListener::bind(tcp_addr)
            .await
            .context("Failed to bind TCP listener")?;
        let tcp_local_addr = tcp_listener.local_addr()?;
        info!("🔌 LAN transport TCP listening on {}", tcp_local_addr);

        // Bind UDP socket
        let udp_bind_port = udp_port.unwrap_or(0);
        let udp_addr: SocketAddr = format!("0.0.0.0:{}", udp_bind_port).parse()?;
        let udp_socket = UdpSocket::bind(udp_addr)
            .await
            .context("Failed to bind UDP socket")?;
        let udp_local_addr = udp_socket.local_addr()?;
        info!("🔌 LAN transport UDP bound on {}", udp_local_addr);

        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

        let counters = TransportByteCounters::new();

        let inner = Arc::new(Inner {
            local_peer_id,
            peers: DashMap::new(),
            udp_socket,
            event_tx,
            shut_down: AtomicBool::new(false),
            udp_addr_to_peer: DashMap::new(),
            tcp_local_addr,
            udp_local_addr,
            counters,
        });

        // Spawn TCP accept task
        {
            let inner = Arc::clone(&inner);
            tokio::spawn(async move {
                tcp_accept_loop(inner, tcp_listener).await;
            });
        }

        // Spawn UDP recv task
        {
            let inner = Arc::clone(&inner);
            tokio::spawn(async move {
                udp_recv_loop(inner).await;
            });
        }

        Ok(Self {
            inner,
            event_rx: Mutex::new(event_rx),
        })
    }

    /// The local TCP address we're listening on.
    pub fn tcp_addr(&self) -> SocketAddr {
        self.inner.tcp_local_addr
    }

    /// The local UDP address we're bound to.
    pub fn udp_addr(&self) -> SocketAddr {
        self.inner.udp_local_addr
    }

    /// Our peer ID.
    pub fn local_peer_id(&self) -> &str {
        &self.inner.local_peer_id
    }

    /// Cumulative byte counters: (tcp_sent, tcp_recv, udp_sent, udp_recv).
    pub fn byte_counts(&self) -> (u64, u64, u64, u64) {
        (
            self.inner.counters.tcp_bytes_sent.load(Ordering::Relaxed),
            self.inner.counters.tcp_bytes_recv.load(Ordering::Relaxed),
            self.inner.counters.udp_bytes_sent.load(Ordering::Relaxed),
            self.inner.counters.udp_bytes_recv.load(Ordering::Relaxed),
        )
    }

    /// Get a clone of the shared byte counters.
    /// These remain valid even after the transport is moved into PeerManager.
    pub fn stats(&self) -> TransportByteCounters {
        self.inner.counters.clone()
    }
}

impl PeerTransportMarker for LanTransport {}

#[async_trait]
impl PeerTransport for LanTransport {
    async fn connect(&self, peer: &PeerAddr) -> Result<()> {
        if self.inner.shut_down.load(Ordering::Relaxed) {
            bail!("Transport is shut down");
        }

        let peer_id = peer.peer_id.as_str();
        if self.inner.peers.contains_key(peer_id) {
            bail!("Already connected to {}", peer_id);
        }

        connect_outbound(Arc::clone(&self.inner), peer).await
    }

    async fn disconnect(&self, peer: &PeerId) -> Result<()> {
        let peer_id = peer.as_str();
        if !self.inner.peers.contains_key(peer_id) {
            bail!("Not connected to {}", peer_id);
        }
        self.inner.remove_peer(peer_id);
        let _ = self
            .inner
            .event_tx
            .send(TransportEvent::PeerDisconnected(peer.clone()))
            .await;
        info!("Disconnected from {}", peer_id);
        Ok(())
    }

    async fn send_reliable(&self, peer: &PeerId, data: &[u8]) -> Result<()> {
        if self.inner.shut_down.load(Ordering::Relaxed) {
            bail!("Transport is shut down");
        }

        let conn = self
            .inner
            .peers
            .get(peer.as_str())
            .ok_or_else(|| anyhow::anyhow!("Not connected to {}", peer))?;

        let mut writer = conn.tcp_writer.lock().await;
        let len = data.len() as u64;
        let result = write_frame(&mut writer, data).await;
        if result.is_ok() {
            self.inner
                .counters
                .tcp_bytes_sent
                .fetch_add(len + 4, Ordering::Relaxed); // +4 for length prefix
        }
        result
    }

    fn send_ephemeral(&self, peer: &PeerId, data: &[u8]) -> Result<()> {
        if self.inner.shut_down.load(Ordering::Relaxed) {
            bail!("Transport is shut down");
        }

        let conn = self
            .inner
            .peers
            .get(peer.as_str())
            .ok_or_else(|| anyhow::anyhow!("Not connected to {}", peer))?;

        if let Some(udp_addr) = conn.udp_addr {
            let packet = self.inner.build_udp_packet(data);
            let pkt_len = packet.len() as u64;
            let inner = Arc::clone(&self.inner);
            let pkt = packet;
            tokio::spawn(async move {
                match inner.udp_socket.send_to(&pkt, udp_addr).await {
                    Ok(_) => {
                        inner
                            .counters
                            .udp_bytes_sent
                            .fetch_add(pkt_len, Ordering::Relaxed);
                    }
                    Err(e) => {
                        debug!("UDP send error to {}: {}", udp_addr, e);
                    }
                }
            });
            Ok(())
        } else {
            // No UDP addr — fall back to TCP
            let inner = Arc::clone(&self.inner);
            let peer_id = peer.as_str().to_string();
            let data_len = data.len() as u64;
            let data = data.to_vec();
            tokio::spawn(async move {
                if let Some(conn) = inner.peers.get(&peer_id) {
                    let mut writer = conn.tcp_writer.lock().await;
                    if let Err(e) = write_frame(&mut writer, &data).await {
                        debug!("TCP fallback send error to {}: {}", peer_id, e);
                    } else {
                        inner
                            .counters
                            .tcp_bytes_sent
                            .fetch_add(data_len + 4, Ordering::Relaxed);
                    }
                }
            });
            Ok(())
        }
    }

    fn broadcast_ephemeral(&self, data: &[u8]) -> Result<()> {
        if self.inner.shut_down.load(Ordering::Relaxed) {
            bail!("Transport is shut down");
        }

        let packet = self.inner.build_udp_packet(data);

        let mut udp_targets = Vec::new();
        let mut tcp_fallback_peers = Vec::new();

        for entry in self.inner.peers.iter() {
            if let Some(addr) = entry.value().udp_addr {
                udp_targets.push(addr);
            } else {
                tcp_fallback_peers.push(entry.key().clone());
            }
        }

        if !udp_targets.is_empty() {
            let inner = Arc::clone(&self.inner);
            let pkt = packet;
            let pkt_len = pkt.len() as u64;
            tokio::spawn(async move {
                for addr in &udp_targets {
                    match inner.udp_socket.send_to(&pkt, addr).await {
                        Ok(_) => {
                            inner
                                .counters
                                .udp_bytes_sent
                                .fetch_add(pkt_len, Ordering::Relaxed);
                        }
                        Err(e) => {
                            debug!("UDP broadcast error to {}: {}", addr, e);
                        }
                    }
                }
            });
        }

        if !tcp_fallback_peers.is_empty() {
            let inner = Arc::clone(&self.inner);
            let data_len = data.len() as u64;
            let data = data.to_vec();
            tokio::spawn(async move {
                for peer_id in tcp_fallback_peers {
                    if let Some(conn) = inner.peers.get(&peer_id) {
                        let mut writer = conn.tcp_writer.lock().await;
                        if let Err(e) = write_frame(&mut writer, &data).await {
                            debug!("TCP fallback broadcast error to {}: {}", peer_id, e);
                        } else {
                            inner
                                .counters
                                .tcp_bytes_sent
                                .fetch_add(data_len + 4, Ordering::Relaxed);
                        }
                    }
                }
            });
        }

        Ok(())
    }

    async fn next_event(&self) -> Result<TransportEvent> {
        let mut rx = self.event_rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("Event channel closed (transport shut down)"))
    }

    fn connected_peers(&self) -> Vec<PeerId> {
        self.inner
            .peers
            .iter()
            .map(|entry| PeerId::new(entry.key().as_str()))
            .collect()
    }

    async fn shutdown(&self) -> Result<()> {
        self.inner.shut_down.store(true, Ordering::Relaxed);

        let peer_ids: Vec<String> = self.inner.peers.iter().map(|e| e.key().clone()).collect();
        for pid in peer_ids {
            self.inner.remove_peer(&pid);
        }

        info!("LAN transport shut down");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Free functions for spawned tasks (operate on Arc<Inner>)
// ---------------------------------------------------------------------------

/// Configure TCP keepalive and nodelay on a stream.
///
/// Keepalive probes detect dead connections that would otherwise block the
/// read loop forever (common on WiFi when a peer disappears without FIN).
fn configure_tcp_socket(stream: &TcpStream) {
    // Set TCP_NODELAY for low-latency small messages
    if let Err(e) = stream.set_nodelay(true) {
        debug!("Failed to set TCP_NODELAY: {}", e);
    }

    // Set OS-level TCP keepalive
    let sock = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(TCP_KEEPALIVE_SECS))
        .with_interval(std::time::Duration::from_secs(TCP_KEEPALIVE_SECS));

    // On Linux, also set the retry count
    #[cfg(target_os = "linux")]
    let keepalive = keepalive.with_retries(3);

    if let Err(e) = sock.set_tcp_keepalive(&keepalive) {
        debug!("Failed to set TCP keepalive: {}", e);
    }
}

/// TCP accept loop — runs in a spawned task.
async fn tcp_accept_loop(inner: Arc<Inner>, listener: TcpListener) {
    loop {
        if inner.shut_down.load(Ordering::Relaxed) {
            break;
        }

        match listener.accept().await {
            Ok((stream, addr)) => {
                debug!("TCP inbound connection from {}", addr);
                let inner = Arc::clone(&inner);
                tokio::spawn(async move {
                    if let Err(e) = handle_inbound(inner, stream).await {
                        warn!("Inbound connection from {} failed: {}", addr, e);
                    }
                });
            }
            Err(e) => {
                if inner.shut_down.load(Ordering::Relaxed) {
                    break;
                }
                error!("TCP accept error: {}", e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

/// Handle an inbound TCP connection: read peer's ID, send ours, register.
async fn handle_inbound(inner: Arc<Inner>, stream: TcpStream) -> Result<()> {
    configure_tcp_socket(&stream);
    let (mut reader, mut writer) = stream.into_split();

    // Read the peer's ID (first framed message)
    let peer_id = read_handshake(&mut reader).await?;

    // Send our ID back
    write_handshake(&mut writer, &inner.local_peer_id).await?;

    // Duplicate connection tiebreak: lower peer ID keeps the connection
    if inner.peers.contains_key(&peer_id) {
        if inner.local_peer_id < peer_id {
            bail!(
                "Duplicate inbound from {}, keeping existing (we won tiebreak)",
                peer_id
            );
        }
        info!(
            "Duplicate inbound from {}, replacing (they won tiebreak)",
            peer_id
        );
        inner.remove_peer(&peer_id);
    }

    let peer_conn = PeerConn {
        tcp_writer: Mutex::new(writer),
        udp_addr: None, // Learned when we receive their first UDP packet
    };
    inner.peers.insert(peer_id.clone(), peer_conn);

    let _ = inner
        .event_tx
        .send(TransportEvent::PeerConnected(PeerId::new(&peer_id)))
        .await;

    info!("✅ Peer connected (inbound): {}", peer_id);

    // Read loop (runs until connection drops)
    tcp_read_loop(inner, peer_id, reader).await;

    Ok(())
}

/// Outbound TCP connect: handshake, register, spawn read loop.
async fn connect_outbound(inner: Arc<Inner>, peer: &PeerAddr) -> Result<()> {
    let tcp_addr_str = peer
        .address("tcp")
        .ok_or_else(|| anyhow::anyhow!("PeerAddr has no TCP address"))?;

    let tcp_addr: SocketAddr = tcp_addr_str
        .parse()
        .context("Invalid TCP address in PeerAddr")?;

    // Extract the TCP port from the primary address
    let tcp_port = tcp_addr.port();

    // Build list of candidate addresses: primary first, then all IPs from metadata
    let mut candidates: Vec<SocketAddr> = vec![tcp_addr];
    for (key, val) in &peer.metadata {
        if key.starts_with("ip_") {
            if let Ok(ip) = val.parse::<std::net::IpAddr>() {
                let candidate = SocketAddr::new(ip, tcp_port);
                if !candidates.contains(&candidate) {
                    candidates.push(candidate);
                }
            }
        }
    }

    // Try each candidate address with a short timeout
    let mut stream = None;
    let mut last_err = None;
    for addr in &candidates {
        debug!("Trying TCP connect to {} ...", addr);
        match tokio::time::timeout(std::time::Duration::from_secs(2), TcpStream::connect(addr))
            .await
        {
            Ok(Ok(s)) => {
                info!(
                    "TCP connected to {} (of {} candidates)",
                    addr,
                    candidates.len()
                );
                stream = Some((s, *addr));
                break;
            }
            Ok(Err(e)) => {
                debug!("TCP connect to {} failed: {}", addr, e);
                last_err = Some(e.into());
            }
            Err(_) => {
                debug!("TCP connect to {} timed out", addr);
                last_err = Some(anyhow::anyhow!("TCP connect to {} timed out", addr));
            }
        }
    }

    let (stream, connected_addr) = stream
        .ok_or_else(|| last_err.unwrap_or_else(|| anyhow::anyhow!("All TCP addresses failed")))?;

    configure_tcp_socket(&stream);
    let (mut reader, mut writer) = stream.into_split();

    // Send our ID
    write_handshake(&mut writer, &inner.local_peer_id).await?;

    // Read their ID
    let remote_peer_id = read_handshake(&mut reader).await?;

    // Verify it matches what discovery told us
    let expected_id = peer.peer_id.as_str();
    if remote_peer_id != expected_id {
        bail!(
            "Peer ID mismatch: expected '{}', got '{}'",
            expected_id,
            remote_peer_id
        );
    }

    // Parse UDP address from PeerAddr, preferring the IP that actually connected
    let udp_addr = peer.address("udp").and_then(|s| {
        let parsed: SocketAddr = s.parse().ok()?;
        // If the connected IP differs from the advertised UDP IP, use the
        // connected IP with the advertised ephemeral port (multi-homed fix)
        if parsed.ip() != connected_addr.ip() {
            info!(
                "Rewriting UDP addr from {} to {}:{} (matched connected IP)",
                parsed,
                connected_addr.ip(),
                parsed.port()
            );
            Some(SocketAddr::new(connected_addr.ip(), parsed.port()))
        } else {
            Some(parsed)
        }
    });

    let peer_conn = PeerConn {
        tcp_writer: Mutex::new(writer),
        udp_addr,
    };

    // Register UDP addr → peer mapping for recv demux
    if let Some(addr) = udp_addr {
        inner.udp_addr_to_peer.insert(addr, remote_peer_id.clone());
    }

    inner.peers.insert(remote_peer_id.clone(), peer_conn);

    let _ = inner
        .event_tx
        .send(TransportEvent::PeerConnected(PeerId::new(&remote_peer_id)))
        .await;

    info!("✅ Peer connected (outbound): {}", remote_peer_id);

    // Send a UDP "ping" to the peer so they learn our UDP address.
    // The packet is just our peer ID header with an empty payload.
    if let Some(addr) = udp_addr {
        let ping = inner.build_udp_packet(&[]);
        match inner.udp_socket.send_to(&ping, addr).await {
            Ok(_) => info!("UDP ping sent to {} ({})", remote_peer_id, addr),
            Err(e) => debug!("UDP ping to {} failed: {}", addr, e),
        }
    }

    // Spawn read loop
    let inner_clone = Arc::clone(&inner);
    let pid = remote_peer_id.clone();
    tokio::spawn(async move {
        tcp_read_loop(inner_clone, pid, reader).await;
    });

    Ok(())
}

/// TCP read loop — reads framed messages until the connection drops or times out.
///
/// The read timeout ensures we detect dead connections even if TCP keepalive
/// doesn't trigger (e.g., the remote host is unreachable but the local OS
/// hasn't given up yet). For idle connections with no CRDT traffic, the
/// PeerManager sends periodic Ping messages to keep the connection alive.
async fn tcp_read_loop(
    inner: Arc<Inner>,
    peer_id: String,
    mut reader: tokio::net::tcp::OwnedReadHalf,
) {
    let timeout_duration = std::time::Duration::from_secs(TCP_READ_TIMEOUT_SECS);

    loop {
        match tokio::time::timeout(timeout_duration, read_frame(&mut reader)).await {
            Ok(Ok(data)) => {
                inner
                    .counters
                    .tcp_bytes_recv
                    .fetch_add(data.len() as u64 + 4, Ordering::Relaxed);
                let _ = inner
                    .event_tx
                    .send(TransportEvent::ReliableMessage {
                        from: PeerId::new(&peer_id),
                        data,
                    })
                    .await;
            }
            Ok(Err(e)) => {
                if !inner.shut_down.load(Ordering::Relaxed) {
                    debug!("TCP read from {} ended: {}", peer_id, e);
                }
                break;
            }
            Err(_) => {
                // Read timeout — connection is likely dead
                if !inner.shut_down.load(Ordering::Relaxed) {
                    warn!("TCP read timeout from {} ({}s) — disconnecting", peer_id, TCP_READ_TIMEOUT_SECS);
                }
                break;
            }
        }
    }

    // Peer disconnected — clean up and notify
    inner.remove_peer(&peer_id);
    let _ = inner
        .event_tx
        .send(TransportEvent::PeerDisconnected(PeerId::new(&peer_id)))
        .await;
    info!("❌ Peer disconnected: {}", peer_id);
}

/// UDP recv loop — runs in a spawned task, demuxes by sender peer ID.
async fn udp_recv_loop(inner: Arc<Inner>) {
    let mut buf = vec![0u8; MAX_UDP_PACKET];

    loop {
        if inner.shut_down.load(Ordering::Relaxed) {
            break;
        }

        match inner.udp_socket.recv_from(&mut buf).await {
            Ok((len, src_addr)) => {
                inner
                    .counters
                    .udp_bytes_recv
                    .fetch_add(len as u64, Ordering::Relaxed);

                if len < 2 {
                    continue; // Need at least [id_len][1 byte id]
                }

                // Parse: [1-byte peer_id_len][peer_id bytes][payload]
                let id_len = buf[0] as usize;
                if len < 1 + id_len {
                    continue; // Malformed
                }

                let peer_id = match std::str::from_utf8(&buf[1..1 + id_len]) {
                    Ok(s) => s.to_string(),
                    Err(_) => continue, // Invalid UTF-8
                };

                let payload = buf[1 + id_len..len].to_vec();

                // Learn/update the peer's UDP addr
                if let Some(mut conn) = inner.peers.get_mut(&peer_id) {
                    if conn.udp_addr.is_none() || conn.udp_addr != Some(src_addr) {
                        info!("Learned UDP addr for {}: {}", peer_id, src_addr);
                        conn.udp_addr = Some(src_addr);
                        inner.udp_addr_to_peer.insert(src_addr, peer_id.clone());
                    }
                } else {
                    debug!(
                        "UDP from unknown peer '{}' at {} (not yet in peers map)",
                        peer_id, src_addr
                    );
                    // Store the mapping anyway — when the peer connects via TCP,
                    // we can look it up
                    inner.udp_addr_to_peer.insert(src_addr, peer_id.clone());
                }

                let _ = inner
                    .event_tx
                    .send(TransportEvent::EphemeralMessage {
                        from: PeerId::new(&peer_id),
                        data: payload,
                    })
                    .await;
            }
            Err(e) => {
                if inner.shut_down.load(Ordering::Relaxed) {
                    break;
                }
                warn!("UDP recv error: {}", e);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TCP framing helpers
// ---------------------------------------------------------------------------

/// Write a length-prefixed frame: [4-byte BE length][payload].
async fn write_frame(writer: &mut tokio::net::tcp::OwnedWriteHalf, data: &[u8]) -> Result<()> {
    let len = data.len() as u32;
    if len > MAX_FRAME_SIZE {
        bail!("Frame too large: {} bytes (max {})", len, MAX_FRAME_SIZE);
    }
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(data).await?;
    writer.flush().await?;
    Ok(())
}

/// Read a length-prefixed frame: [4-byte BE length][payload].
async fn read_frame(reader: &mut tokio::net::tcp::OwnedReadHalf) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);

    if len > MAX_FRAME_SIZE {
        bail!(
            "Frame too large: {} bytes (max {}). Possibly corrupted.",
            len,
            MAX_FRAME_SIZE
        );
    }

    let mut data = vec![0u8; len as usize];
    reader.read_exact(&mut data).await?;
    Ok(data)
}

/// Send our peer ID as the handshake frame.
async fn write_handshake(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    peer_id: &str,
) -> Result<()> {
    write_frame(writer, peer_id.as_bytes()).await
}

/// Read the remote peer's ID from the handshake frame.
async fn read_handshake(reader: &mut tokio::net::tcp::OwnedReadHalf) -> Result<String> {
    let data = read_frame(reader)
        .await
        .context("Failed to read handshake")?;
    String::from_utf8(data).context("Handshake peer ID is not valid UTF-8")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use swirldb_core::protocol::Message;
    use tokio::time::{timeout, Duration};

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn test_bind_and_shutdown() {
        let transport = LanTransport::bind("test-peer", 0, Some(0)).await.unwrap();

        assert_eq!(transport.local_peer_id(), "test-peer");
        assert!(transport.connected_peers().is_empty());
        assert!(transport.tcp_addr().port() > 0);
        assert!(transport.udp_addr().port() > 0);

        transport.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_two_peers_connect_and_exchange_reliable() {
        let a = LanTransport::bind("peer-a", 0, Some(0)).await.unwrap();
        let b = LanTransport::bind("peer-b", 0, Some(0)).await.unwrap();

        // peer-b connects to peer-a
        let a_addr = PeerAddr::new("peer-a")
            .with_address("tcp", loopback(a.tcp_addr()))
            .with_address("udp", loopback(a.udp_addr()));

        b.connect(&a_addr).await.unwrap();

        // peer-b should see PeerConnected
        let event = timeout(TEST_TIMEOUT, b.next_event())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(&event, TransportEvent::PeerConnected(id) if id.as_str() == "peer-a"),
            "Expected PeerConnected(peer-a), got {:?}",
            event
        );

        // peer-a should also see PeerConnected (from the inbound accept)
        let event = timeout(TEST_TIMEOUT, a.next_event())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(&event, TransportEvent::PeerConnected(id) if id.as_str() == "peer-b"),
            "Expected PeerConnected(peer-b), got {:?}",
            event
        );

        // Both should see each other as connected
        assert_eq!(a.connected_peers().len(), 1);
        assert_eq!(b.connected_peers().len(), 1);

        // Send a reliable message from b → a
        let msg = Message::Push {
            heads: vec![1, 2, 3],
            changes: vec![vec![4, 5, 6]],
        };
        let encoded = msg.encode();
        b.send_reliable(&PeerId::new("peer-a"), &encoded)
            .await
            .unwrap();

        // peer-a receives it
        let event = timeout(TEST_TIMEOUT, a.next_event())
            .await
            .unwrap()
            .unwrap();
        match event {
            TransportEvent::ReliableMessage { from, data } => {
                assert_eq!(from.as_str(), "peer-b");
                let decoded = Message::decode(&data).unwrap();
                assert!(matches!(decoded, Message::Push { .. }));
            }
            other => panic!("Expected ReliableMessage, got {:?}", other),
        }

        // Send a reliable message from a → b
        let msg2 = Message::Broadcast {
            from_client_id: "peer-a".to_string(),
            changes: vec![vec![7, 8, 9]],
            affected_paths: vec!["settings.bpm".to_string()],
        };
        a.send_reliable(&PeerId::new("peer-b"), &msg2.encode())
            .await
            .unwrap();

        let event = timeout(TEST_TIMEOUT, b.next_event())
            .await
            .unwrap()
            .unwrap();
        match event {
            TransportEvent::ReliableMessage { from, data } => {
                assert_eq!(from.as_str(), "peer-a");
                let decoded = Message::decode(&data).unwrap();
                assert!(matches!(decoded, Message::Broadcast { .. }));
            }
            other => panic!("Expected ReliableMessage, got {:?}", other),
        }

        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }

    /// Convert a 0.0.0.0 bind addr to 127.0.0.1 for loopback tests.
    fn loopback(addr: SocketAddr) -> String {
        format!("127.0.0.1:{}", addr.port())
    }

    #[tokio::test]
    async fn test_ephemeral_udp_exchange() {
        let a = LanTransport::bind("peer-a", 0, Some(0)).await.unwrap();
        let b = LanTransport::bind("peer-b", 0, Some(0)).await.unwrap();

        // peer-b connects to peer-a (with UDP address)
        let a_addr = PeerAddr::new("peer-a")
            .with_address("tcp", loopback(a.tcp_addr()))
            .with_address("udp", loopback(a.udp_addr()));
        b.connect(&a_addr).await.unwrap();

        // Drain PeerConnected events
        let _ = timeout(TEST_TIMEOUT, b.next_event())
            .await
            .unwrap()
            .unwrap();
        let _ = timeout(TEST_TIMEOUT, a.next_event())
            .await
            .unwrap()
            .unwrap();

        // Send ephemeral from b → a
        let beat_msg = Message::EphemeralBatch {
            updates: vec![("beat.bpm".to_string(), vec![0, 120])],
        };
        b.send_ephemeral(&PeerId::new("peer-a"), &beat_msg.encode())
            .unwrap();

        // peer-a should receive it as EphemeralMessage
        let event = timeout(TEST_TIMEOUT, a.next_event())
            .await
            .unwrap()
            .unwrap();
        match event {
            TransportEvent::EphemeralMessage { from, data } => {
                assert_eq!(from.as_str(), "peer-b");
                let decoded = Message::decode(&data).unwrap();
                assert!(matches!(decoded, Message::EphemeralBatch { .. }));
            }
            other => panic!("Expected EphemeralMessage, got {:?}", other),
        }

        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_broadcast_ephemeral() {
        let a = LanTransport::bind("peer-a", 0, Some(0)).await.unwrap();
        let b = LanTransport::bind("peer-b", 0, Some(0)).await.unwrap();
        let c = LanTransport::bind("peer-c", 0, Some(0)).await.unwrap();

        // b and c connect to a
        let a_addr = PeerAddr::new("peer-a")
            .with_address("tcp", loopback(a.tcp_addr()))
            .with_address("udp", loopback(a.udp_addr()));

        b.connect(&a_addr).await.unwrap();
        c.connect(&a_addr).await.unwrap();

        // Drain connect events (2 on a, 1 each on b and c)
        for _ in 0..2 {
            let _ = timeout(TEST_TIMEOUT, a.next_event()).await.unwrap();
        }
        let _ = timeout(TEST_TIMEOUT, b.next_event()).await.unwrap();
        let _ = timeout(TEST_TIMEOUT, c.next_event()).await.unwrap();

        // Also: a needs UDP addrs for b and c. Those are learned on first
        // receive, but for broadcast_ephemeral from a, a needs to know where
        // to send. Since b and c connected outbound to a, a only has their
        // TCP. The UDP addr is learned when a receives a UDP packet from them.
        //
        // So let's have b and c send an ephemeral to a first (to teach a
        // their UDP addresses), then a broadcasts.
        b.send_ephemeral(&PeerId::new("peer-a"), b"ping").unwrap();
        c.send_ephemeral(&PeerId::new("peer-a"), b"ping").unwrap();

        // Drain the two ephemeral messages on a
        let _ = timeout(TEST_TIMEOUT, a.next_event()).await.unwrap();
        let _ = timeout(TEST_TIMEOUT, a.next_event()).await.unwrap();

        // Now a broadcasts
        a.broadcast_ephemeral(b"beat-sync-data").unwrap();

        // Both b and c should receive it
        let event_b = timeout(TEST_TIMEOUT, b.next_event())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(&event_b, TransportEvent::EphemeralMessage { from, data }
                if from.as_str() == "peer-a" && data == b"beat-sync-data"),
            "peer-b got: {:?}",
            event_b
        );

        let event_c = timeout(TEST_TIMEOUT, c.next_event())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(&event_c, TransportEvent::EphemeralMessage { from, data }
                if from.as_str() == "peer-a" && data == b"beat-sync-data"),
            "peer-c got: {:?}",
            event_c
        );

        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
        c.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_disconnect() {
        let a = LanTransport::bind("peer-a", 0, Some(0)).await.unwrap();
        let b = LanTransport::bind("peer-b", 0, Some(0)).await.unwrap();

        let a_addr = PeerAddr::new("peer-a").with_address("tcp", loopback(a.tcp_addr()));
        b.connect(&a_addr).await.unwrap();

        // Drain connect events
        let _ = timeout(TEST_TIMEOUT, a.next_event()).await.unwrap();
        let _ = timeout(TEST_TIMEOUT, b.next_event()).await.unwrap();

        assert_eq!(a.connected_peers().len(), 1);
        assert_eq!(b.connected_peers().len(), 1);

        // b disconnects from a
        b.disconnect(&PeerId::new("peer-a")).await.unwrap();

        // b emits PeerDisconnected locally
        let event = timeout(TEST_TIMEOUT, b.next_event())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(&event, TransportEvent::PeerDisconnected(id) if id.as_str() == "peer-a"));

        assert_eq!(b.connected_peers().len(), 0);

        // a should detect the disconnect when its TCP read loop notices the closed connection
        let event = timeout(TEST_TIMEOUT, a.next_event())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(&event, TransportEvent::PeerDisconnected(id) if id.as_str() == "peer-b"));

        assert_eq!(a.connected_peers().len(), 0);

        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_duplicate_connect_rejected() {
        let a = LanTransport::bind("peer-a", 0, Some(0)).await.unwrap();
        let b = LanTransport::bind("peer-b", 0, Some(0)).await.unwrap();

        let a_addr = PeerAddr::new("peer-a").with_address("tcp", loopback(a.tcp_addr()));

        b.connect(&a_addr).await.unwrap();

        // Drain connect events
        let _ = timeout(TEST_TIMEOUT, a.next_event()).await.unwrap();
        let _ = timeout(TEST_TIMEOUT, b.next_event()).await.unwrap();

        // Second connect should fail
        let result = b.connect(&a_addr).await;
        assert!(result.is_err());

        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_to_disconnected_peer_fails() {
        let a = LanTransport::bind("peer-a", 0, Some(0)).await.unwrap();

        let result = a.send_reliable(&PeerId::new("nobody"), b"hello").await;
        assert!(result.is_err());

        let result = a.send_ephemeral(&PeerId::new("nobody"), b"hello");
        assert!(result.is_err());

        a.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_shutdown_prevents_operations() {
        let a = LanTransport::bind("peer-a", 0, Some(0)).await.unwrap();
        a.shutdown().await.unwrap();

        let addr = PeerAddr::new("peer-b").with_address("tcp", "127.0.0.1:9999");
        assert!(a.connect(&addr).await.is_err());
        assert!(a.send_reliable(&PeerId::new("x"), b"data").await.is_err());
        assert!(a.send_ephemeral(&PeerId::new("x"), b"data").is_err());
        assert!(a.broadcast_ephemeral(b"data").is_err());
    }

    #[tokio::test]
    async fn test_ephemeral_fallback_to_tcp() {
        // When no UDP address is known, ephemeral falls back to TCP
        let a = LanTransport::bind("peer-a", 0, Some(0)).await.unwrap();
        let b = LanTransport::bind("peer-b", 0, Some(0)).await.unwrap();

        // Connect without UDP address
        let a_addr = PeerAddr::new("peer-a").with_address("tcp", loopback(a.tcp_addr()));
        // Note: no .with_address("udp", ...)

        b.connect(&a_addr).await.unwrap();

        // Drain connect events
        let _ = timeout(TEST_TIMEOUT, a.next_event()).await.unwrap();
        let _ = timeout(TEST_TIMEOUT, b.next_event()).await.unwrap();

        // Send ephemeral — should fall back to TCP
        b.send_ephemeral(&PeerId::new("peer-a"), b"beat-data")
            .unwrap();

        // Give the spawned TCP fallback task a moment
        tokio::time::sleep(Duration::from_millis(50)).await;

        // a receives it as ReliableMessage (since it came over TCP)
        let event = timeout(TEST_TIMEOUT, a.next_event())
            .await
            .unwrap()
            .unwrap();
        match event {
            TransportEvent::ReliableMessage { from, data } => {
                assert_eq!(from.as_str(), "peer-b");
                assert_eq!(data, b"beat-data");
            }
            other => panic!("Expected ReliableMessage (TCP fallback), got {:?}", other),
        }

        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}
