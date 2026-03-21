// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Transport implementations for SwirlDB server.
//!
//! Implements the [`PeerTransport`](swirldb_core::transport::PeerTransport) trait
//! from core for different network carriers.

pub mod lan;

pub use self::lan::{LanTransport, TransportByteCounters};
