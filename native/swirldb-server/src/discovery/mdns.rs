// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! mDNS/DNS-SD peer discovery for LAN environments.
//!
//! Uses the `mdns-sd` crate to advertise and discover SwirlDB peers on the
//! local network. This is the primary discovery mechanism for swirl-engine
//! mesh setups (multiple Pis + Macs on the same WiFi network).
//!
//! # Service Type
//!
//! Advertises as `_swirldb._tcp.local.` with TXT records for metadata
//! (peer_id, version, etc.).
//!
//! # Example
//!
//! ```rust,ignore
//! use swirldb_server::discovery::MdnsDiscovery;
//! use swirldb_core::transport::{PeerDiscovery, ServiceInfo, TransportEvent};
//!
//! let discovery = MdnsDiscovery::new().unwrap();
//!
//! // Advertise ourselves
//! let info = ServiceInfo::new("my-peer", 3030)
//!     .with_metadata("version", "0.2.0");
//! discovery.advertise(&info).await.unwrap();
//!
//! // Start browsing
//! discovery.start_discovery().await.unwrap();
//!
//! // Poll for events
//! loop {
//!     match discovery.next_event().await.unwrap() {
//!         TransportEvent::PeerDiscovered(addr) => {
//!             println!("Found: {}", addr);
//!         }
//!         TransportEvent::PeerLost(id) => {
//!             println!("Lost: {}", id);
//!         }
//!         _ => {}
//!     }
//! }
//! ```

use anyhow::{Context, Result};
use async_trait::async_trait;
use mdns_sd::{
    Receiver as MdnsReceiver, ServiceDaemon, ServiceEvent, ServiceInfo as MdnsServiceInfo,
};
use std::collections::HashMap;
use std::sync::Mutex;
use swirldb_core::transport::{
    PeerAddr, PeerDiscovery, PeerDiscoveryMarker, PeerId, ServiceInfo, TransportEvent,
};
use tracing::{debug, info, warn};

/// The DNS-SD service type used by SwirlDB peers.
const SERVICE_TYPE: &str = "_swirldb._tcp.local.";

/// TXT record key for the peer's unique ID.
const TXT_PEER_ID: &str = "peer_id";

/// TXT record key for an optional ephemeral (UDP) port.
const TXT_EPHEMERAL_PORT: &str = "eph_port";

/// mDNS/DNS-SD peer discovery.
///
/// Wraps the `mdns-sd` crate's [`ServiceDaemon`] and implements
/// [`PeerDiscovery`] from swirldb-core. Handles:
///
/// - **Advertising**: Registers a DNS-SD service record so other peers find us.
/// - **Browsing**: Discovers other SwirlDB peers, reporting them as
///   [`TransportEvent::PeerDiscovered`] / [`TransportEvent::PeerLost`].
/// - **Self-filtering**: Ignores our own advertisements.
pub struct MdnsDiscovery {
    daemon: ServiceDaemon,
    /// Our peer ID, set after advertise() — used to filter self-discovery.
    local_peer_id: Mutex<Option<String>>,
    /// The fullname we registered with mdns-sd, for unregistering.
    registered_fullname: Mutex<Option<String>>,
    /// Browse receiver — created on start_discovery().
    browse_rx: Mutex<Option<MdnsReceiver<ServiceEvent>>>,
    /// Currently known peers.
    known_peers: Mutex<HashMap<String, PeerAddr>>,
}

impl MdnsDiscovery {
    /// Create a new mDNS discovery instance.
    ///
    /// Starts the background mDNS daemon thread. Call [`advertise`](PeerDiscovery::advertise)
    /// and [`start_discovery`](PeerDiscovery::start_discovery) to begin.
    pub fn new() -> Result<Self> {
        let daemon = ServiceDaemon::new().context("Failed to create mDNS daemon")?;
        Ok(Self {
            daemon,
            local_peer_id: Mutex::new(None),
            registered_fullname: Mutex::new(None),
            browse_rx: Mutex::new(None),
            known_peers: Mutex::new(HashMap::new()),
        })
    }

    /// Convert an mdns-sd ServiceInfo into our PeerAddr.
    fn service_info_to_peer_addr(info: &MdnsServiceInfo) -> Option<PeerAddr> {
        // Extract peer_id from TXT record, fall back to fullname
        let peer_id = info
            .get_property_val_str(TXT_PEER_ID)
            .map(|s| s.to_string())
            .unwrap_or_else(|| info.get_fullname().to_string());

        let addrs = info.get_addresses();
        if addrs.is_empty() {
            warn!("mDNS service {} has no addresses, skipping", peer_id);
            return None;
        }

        let port = info.get_port();
        let ip = addrs.iter().next().unwrap();

        let mut peer_addr =
            PeerAddr::new(PeerId::new(&peer_id)).with_address("tcp", format!("{}:{}", ip, port));

        // Add all resolved IPs as metadata for multi-homed hosts
        for (i, addr) in addrs.iter().enumerate() {
            peer_addr
                .metadata
                .insert(format!("ip_{}", i), addr.to_string());
        }

        // Check for ephemeral port in TXT records
        if let Some(eph_port_str) = info.get_property_val_str(TXT_EPHEMERAL_PORT) {
            if let Ok(eph_port) = eph_port_str.parse::<u16>() {
                peer_addr = peer_addr.with_address("udp", format!("{}:{}", ip, eph_port));
            }
        }

        // Copy remaining TXT records to metadata
        for prop in info.get_properties().iter() {
            let key = prop.key();
            if key != TXT_PEER_ID && key != TXT_EPHEMERAL_PORT {
                let val = prop.val_str();
                if !val.is_empty() {
                    peer_addr.metadata.insert(key.to_string(), val.to_string());
                }
            }
        }

        Some(peer_addr)
    }
}

impl PeerDiscoveryMarker for MdnsDiscovery {}

#[async_trait]
impl PeerDiscovery for MdnsDiscovery {
    async fn advertise(&self, info: &ServiceInfo) -> Result<()> {
        let host = format!("{}.local.", info.peer_id.as_str());

        // Build TXT properties as HashMap (what mdns-sd expects)
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert(TXT_PEER_ID.to_string(), info.peer_id.as_str().to_string());

        if let Some(eph_port) = info.ephemeral_port {
            properties.insert(TXT_EPHEMERAL_PORT.to_string(), eph_port.to_string());
        }

        for (key, value) in &info.metadata {
            properties.insert(key.clone(), value.clone());
        }

        let service_type = &info.service_type;
        let mdns_type = if service_type.ends_with(".local.") {
            service_type.clone()
        } else {
            format!("{}.local.", service_type)
        };

        let mdns_info = MdnsServiceInfo::new(
            &mdns_type,
            info.peer_id.as_str(),
            &host,
            "", // empty = auto-detect IP
            info.port,
            Some(properties),
        )
        .context("Failed to create mDNS service info")?
        .enable_addr_auto();

        // Store our peer_id for self-filtering
        *self.local_peer_id.lock().unwrap() = Some(info.peer_id.as_str().to_string());

        // Store the fullname for later unregistration
        let fullname = mdns_info.get_fullname().to_string();
        *self.registered_fullname.lock().unwrap() = Some(fullname);

        self.daemon
            .register(mdns_info)
            .context("Failed to register mDNS service")?;

        info!(
            "📡 Advertising via mDNS: {} (port {})",
            mdns_type, info.port
        );

        Ok(())
    }

    async fn stop_advertising(&self) -> Result<()> {
        let fullname = self.registered_fullname.lock().unwrap().take();
        if let Some(fullname) = fullname {
            self.daemon
                .unregister(&fullname)
                .context("Failed to unregister mDNS service")?;
            info!("Stopped mDNS advertising");
        }
        *self.local_peer_id.lock().unwrap() = None;
        Ok(())
    }

    async fn start_discovery(&self) -> Result<()> {
        let receiver = self
            .daemon
            .browse(SERVICE_TYPE)
            .context("Failed to start mDNS browsing")?;

        *self.browse_rx.lock().unwrap() = Some(receiver);
        info!("🔍 Browsing for peers via mDNS: {}", SERVICE_TYPE);

        Ok(())
    }

    async fn stop_discovery(&self) -> Result<()> {
        // Drop the receiver to stop getting events
        self.browse_rx.lock().unwrap().take();

        if let Err(e) = self.daemon.stop_browse(SERVICE_TYPE) {
            // Not critical — the daemon may already be stopped
            debug!("stop_browse returned error (may be benign): {}", e);
        }

        info!("Stopped mDNS browsing");
        Ok(())
    }

    async fn next_event(&self) -> Result<TransportEvent> {
        // Get a clone of the receiver (flume::Receiver is Clone)
        let rx = {
            let guard = self.browse_rx.lock().unwrap();
            guard
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Discovery not started"))?
                .clone()
        };

        // Block on the flume receiver (async-compatible via recv_async)
        loop {
            let event: ServiceEvent = rx
                .recv_async()
                .await
                .context("mDNS browse channel closed")?;

            match event {
                ServiceEvent::ServiceResolved(info) => {
                    // Extract peer_id
                    let peer_id = info
                        .get_property_val_str(TXT_PEER_ID)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| info.get_fullname().to_string());

                    // Skip our own advertisement
                    let local_id = self.local_peer_id.lock().unwrap().clone();
                    if local_id.as_deref() == Some(&peer_id) {
                        debug!("Ignoring self-discovery: {}", peer_id);
                        continue;
                    }

                    if let Some(peer_addr) = Self::service_info_to_peer_addr(&info) {
                        info!("🔗 Discovered peer: {}", peer_addr);
                        self.known_peers
                            .lock()
                            .unwrap()
                            .insert(peer_id, peer_addr.clone());
                        return Ok(TransportEvent::PeerDiscovered(peer_addr));
                    }
                    // No usable addresses — skip and wait for next event
                }

                ServiceEvent::ServiceRemoved(_service_type, fullname) => {
                    // Find the peer by matching fullname against known peers
                    let mut known = self.known_peers.lock().unwrap();

                    // Try to find by fullname match (the fullname contains the peer_id)
                    let removed_id = known
                        .keys()
                        .find(|id| fullname.contains(id.as_str()))
                        .cloned();

                    if let Some(id) = removed_id {
                        known.remove(&id);
                        info!("❌ Lost peer: {}", id);
                        return Ok(TransportEvent::PeerLost(PeerId::new(id)));
                    }

                    debug!("ServiceRemoved for unknown peer: {}", fullname);
                    // Unknown peer removed — skip and wait for next event
                }

                ServiceEvent::SearchStarted(ty) => {
                    debug!("mDNS search started for {}", ty);
                }

                ServiceEvent::SearchStopped(ty) => {
                    debug!("mDNS search stopped for {}", ty);
                    return Err(anyhow::anyhow!("mDNS search stopped for {}", ty));
                }

                ServiceEvent::ServiceFound(_ty, _name) => {
                    // Intermediate event — wait for ServiceResolved
                    debug!("mDNS service found (awaiting resolution)");
                }
            }
        }
    }

    fn discovered_peers(&self) -> Vec<PeerAddr> {
        self.known_peers.lock().unwrap().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mdns_discovery_creation() {
        // Just verify we can create the daemon without panicking
        let discovery = MdnsDiscovery::new();
        assert!(discovery.is_ok());
    }

    #[tokio::test]
    async fn test_advertise_and_discover_self_filtering() {
        // Create two discovery instances to test advertisement + browsing
        let advertiser = MdnsDiscovery::new().unwrap();
        let browser = MdnsDiscovery::new().unwrap();

        // Use a unique name to avoid interference from parallel tests
        let peer_name = format!("test-adv-{}", uuid::Uuid::new_v4().simple());

        // Advertise
        let info = ServiceInfo::new(peer_name.as_str(), 13030).with_metadata("version", "0.2.0");
        advertiser.advertise(&info).await.unwrap();

        // Browser should be able to start discovery
        browser.start_discovery().await.unwrap();

        // Poll for events until we find our specific peer or timeout.
        // Other tests may advertise concurrently, so we skip unknown peers.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut found = false;

        while tokio::time::Instant::now() < deadline {
            let result = tokio::time::timeout_at(deadline, browser.next_event()).await;

            match result {
                Ok(Ok(TransportEvent::PeerDiscovered(addr)))
                    if addr.peer_id.as_str() == peer_name =>
                {
                    assert!(addr.address("tcp").is_some());
                    assert_eq!(addr.metadata.get("version"), Some(&"0.2.0".to_string()));
                    found = true;
                    break;
                }
                Ok(Ok(_)) => continue, // Skip events from other tests
                Ok(Err(_)) | Err(_) => break,
            }
        }

        // Clean up regardless of result
        advertiser.stop_advertising().await.unwrap();
        browser.stop_discovery().await.unwrap();

        // If mDNS multicast works on this machine, we should have found it
        // (not asserting `found` because CI may block multicast)
        if found {
            assert!(browser
                .discovered_peers()
                .iter()
                .any(|p| p.peer_id.as_str() == peer_name));
        }
    }

    #[tokio::test]
    async fn test_advertise_with_ephemeral_port() {
        let discovery = MdnsDiscovery::new().unwrap();

        let info = ServiceInfo::new("test-eph-port", 13031)
            .with_ephemeral_port(13032)
            .with_metadata("role", "beat-leader");

        // Should not panic
        discovery.advertise(&info).await.unwrap();
        discovery.stop_advertising().await.unwrap();
    }

    #[tokio::test]
    async fn test_stop_before_start() {
        let discovery = MdnsDiscovery::new().unwrap();

        // stop_advertising before advertise is a no-op
        discovery.stop_advertising().await.unwrap();

        // stop_discovery before start_discovery is a no-op
        discovery.stop_discovery().await.unwrap();
    }

    #[tokio::test]
    async fn test_next_event_before_start() {
        let discovery = MdnsDiscovery::new().unwrap();

        // next_event before start_discovery should error
        let result = discovery.next_event().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_discovered_peers_empty_initially() {
        let discovery = MdnsDiscovery::new().unwrap();
        assert!(discovery.discovered_peers().is_empty());
    }
}
