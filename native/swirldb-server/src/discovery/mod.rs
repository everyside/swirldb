// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Discovery implementations for SwirlDB server.
//!
//! Implements the [`PeerDiscovery`](swirldb_core::transport::PeerDiscovery) trait
//! from core for different discovery mechanisms.

#[cfg(feature = "mdns")]
pub mod mdns;

#[cfg(feature = "mdns")]
pub use self::mdns::MdnsDiscovery;
