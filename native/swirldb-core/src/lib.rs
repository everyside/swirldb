// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

// Core CRDT engine - platform-agnostic
pub mod core;

// Storage traits - pluggable storage backends
pub mod storage;

// Encryption providers - pluggable encryption strategies
pub mod encryption;

// Sync protocol module
pub mod sync;

// Network protocol - binary message encoding/decoding
pub mod protocol;

// Policy engine - platform-agnostic authorization
pub mod policy;

// Auth providers - pluggable authentication
pub mod auth;

// Re-export automerge types for convenience
pub use automerge;

// Re-export core SwirlDB for convenience
pub use core::SwirlDB;
