/// Sync protocol for upstream/downstream synchronization
///
/// This module implements efficient delta syncing using Automerge's built-in
/// change tracking. The protocol is peer-to-peer aware but supports
/// upstream/downstream topology for client-server architectures.

use anyhow::{Result, anyhow};
use serde::{Serialize, Deserialize};

/// Sync message types for the protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SyncMessage {
    /// Client → Server: Initial connection with current sync state
    Connect {
        client_id: String,
        heads: Vec<u8>, // Serialized Automerge heads
    },

    /// Server → Client: Response with missing changes
    Sync {
        changes: Vec<u8>, // Serialized Automerge changes
        heads: Vec<u8>,   // New heads after applying these changes
    },

    /// Client → Server: Push local changes
    Push {
        client_id: String,
        changes: Vec<u8>,
        heads: Vec<u8>,
    },

    /// Server → Client: Broadcast changes from other clients
    Broadcast {
        from_client_id: String,
        changes: Vec<u8>,
        heads: Vec<u8>,
    },

    /// Bidirectional: Heartbeat to keep connection alive
    Ping,
    Pong,

    /// Error messages
    Error {
        message: String,
    },
}

/// Sync configuration
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// WebSocket URL for upstream server
    pub upstream_url: String,

    /// Client identifier (should be unique per browser/device)
    pub client_id: String,

    /// Reconnection settings
    pub reconnect_delay_ms: u64,
    pub max_reconnect_attempts: u32,

    /// Batching settings for performance
    pub batch_changes: bool,
    pub batch_delay_ms: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            upstream_url: String::new(),
            client_id: String::new(),
            reconnect_delay_ms: 1000,
            max_reconnect_attempts: 10,
            batch_changes: true,
            batch_delay_ms: 100,
        }
    }
}

/// Sync state tracking
#[derive(Debug, Clone)]
pub struct SyncState {
    /// Current sync heads (what changes we have)
    pub local_heads: Vec<u8>,

    /// Last known upstream heads
    pub upstream_heads: Vec<u8>,

    /// Pending changes to send
    pub pending_changes: Vec<Vec<u8>>,

    /// Connection state
    pub connected: bool,

    /// Last successful sync timestamp
    pub last_sync: Option<u64>,
}

impl SyncState {
    pub fn new() -> Self {
        Self {
            local_heads: Vec::new(),
            upstream_heads: Vec::new(),
            pending_changes: Vec::new(),
            connected: false,
            last_sync: None,
        }
    }

    pub fn has_pending_changes(&self) -> bool {
        !self.pending_changes.is_empty()
    }

    pub fn add_pending_change(&mut self, change: Vec<u8>) {
        self.pending_changes.push(change);
    }

    pub fn drain_pending_changes(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.pending_changes)
    }
}

/// Trait for sync adapters (WebSocket, WebRTC, HTTP polling, etc.)
#[cfg(feature = "wasm")]
use async_trait::async_trait;

#[cfg(feature = "wasm")]
#[async_trait(?Send)]
pub trait SyncAdapter {
    /// Connect to upstream
    async fn connect(&mut self) -> Result<()>;

    /// Disconnect from upstream
    async fn disconnect(&mut self) -> Result<()>;

    /// Send a message to upstream
    async fn send(&mut self, message: SyncMessage) -> Result<()>;

    /// Receive next message (non-blocking)
    async fn recv(&mut self) -> Result<Option<SyncMessage>>;

    /// Check if connected
    fn is_connected(&self) -> bool;
}
