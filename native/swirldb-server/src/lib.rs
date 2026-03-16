// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! SwirlDB Server library
//!
//! This library exposes server components for use in integration tests.

pub mod discovery;
pub mod handler;
pub mod peer_manager;
pub mod state;
pub mod storage;
pub mod transport;

// Re-export commonly used types
pub use state::{
    ActivityEvent, BroadcastMessage, ClientInfo, ConnectionInfo, EphemeralMessage, PeerInfo,
    ServerState, ServerStats, SubscriptionInfo,
};

#[cfg(feature = "mdns")]
pub use discovery::MdnsDiscovery;

pub use peer_manager::{PeerEvent, PeerManager, PeerManagerConfig, PeerSource};
pub use transport::LanTransport;
