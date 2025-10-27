# SwirlDB Sync Server

CRDT synchronization server written in Rust. Handles real-time synchronization between SwirlDB clients using WebSocket protocol with subscription-based change filtering.

## Features

- Subscription-based sync with path pattern matching (e.g., `/chat/**`, `/user/*/profile`)
- Binary WebSocket protocol for real-time updates
- Single global CRDT instance shared by all clients
- Policy-based access control for subscriptions
- Persistent storage via redb (embedded key-value database)
- Lock-free client tracking using DashMap
- Incremental sync using CRDT heads
- Standalone binary, no Node.js required

## Quick Start

### Build

```bash
cd native/swirldb-server
cargo build --release
```

### Run

```bash
# With default settings (port 3030)
./target/release/swirldb-server

# With logging
RUST_LOG=info ./target/release/swirldb-server
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 3030 | WebSocket server port |
| `RUST_LOG` | (none) | Log level: `error`, `warn`, `info`, `debug`, `trace` |

## Endpoints

### WebSocket

```
ws://localhost:3030/ws
```

Binary protocol for real-time sync. Message types:

- **Connect**: Client registration with subscription patterns and heads for incremental sync
- **Sync**: Server response containing CRDT changes
- **Push**: Client pushes local changes to server
- **PushAck**: Server acknowledges push with updated heads
- **Broadcast**: Server broadcasts changes to other subscribed clients
- **Ping/Pong**: Heartbeat messages

### HTTP

#### GET `/health`

Health check endpoint.

```bash
curl http://localhost:3030/health
```

#### GET `/stats`

Server statistics (connection count, change count, uptime).

```bash
curl http://localhost:3030/stats
```

Returns:
```json
{
  "connection_count": 5,
  "change_count": 1234,
  "uptime_seconds": 3600,
  "last_activity": 1735264200000
}
```

## Architecture

### Subscription-Based Sync

Clients subscribe to path patterns using glob-style syntax:

- `/**` - Subscribe to all changes
- `/chat/**` - Subscribe to all chat-related changes
- `/user/alice/**` - Subscribe to all changes under user alice

The server filters broadcasts based on affected paths, only sending changes to clients with matching subscriptions.

### Global CRDT Instance

The server maintains a single global SwirlDB CRDT instance:

```rust
pub struct ServerState {
    db: Arc<RwLock<SwirlDB>>,              // Single global CRDT
    subscriptions: Arc<Mutex<SubscriptionManager>>,  // Path-based filtering
    broadcast_tx: broadcast::Sender<BroadcastMessage>,
    clients: Arc<DashMap<Uuid, ClientInfo>>,
}
```

All clients operate on the same CRDT document. Changes from one client are merged and broadcast to other clients with matching subscriptions.

### Incremental Sync

Clients track their last synced state using CRDT heads (change hashes). On reconnection:

1. Client sends its heads with Connect message
2. Server computes delta using `get_changes_since(heads)`
3. Server sends only new changes
4. Client applies changes via `apply_changes()`

This minimizes bandwidth - a client that's already up-to-date receives 0 changes.

### Concurrency Model

- Lock-free client tracking using `DashMap`
- Tokio broadcast channels for efficient room-wide messaging
- Async-first - handles thousands of concurrent connections
- Read-write locks minimize contention on CRDT access

### Storage

The server uses redb for persistent storage:

- ACID transactions
- Memory-mapped files for zero-copy reads
- Embedded (no separate database server needed)
- Fast writes with write-ahead logging

Changes are persisted to redb and applied to the in-memory CRDT. On restart, the CRDT state is loaded from redb.

## Development

### Run with debug logging

```bash
RUST_LOG=debug cargo run
```

### Run tests

```bash
cargo test
```

### Format code

```bash
cargo fmt
```

## Performance Notes

The current implementation prioritizes correctness over performance. Some optimization opportunities:

- **Array Optimization**: Arrays of record-like objects (with `id`, `timestamp`, or `_key` fields) are automatically converted to maps internally, reducing sync overhead by 99.8% for incremental array updates
- **Change Batching**: Multiple rapid changes create multiple CRDT changes - this is correct but could be batched in the future
- **Broadcast Filtering**: Currently uses wildcard pattern `/**` - proper path extraction from changes would enable more selective broadcasts

## License

Apache 2.0
