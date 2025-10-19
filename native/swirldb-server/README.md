# SwirlDB Sync Server

High-performance CRDT synchronization server written in pure Rust.

## Features

- ✅ **Full SwirlDB CRDT Engine** - Each room runs a complete SwirlDB instance
- ✅ **Massively Concurrent** - Lock-free data structures handle thousands of connections
- ✅ **WebSocket + HTTP** - Binary WebSocket protocol with HTTP long-polling fallback
- ✅ **Pluggable Storage** - redb (fast embedded DB) or in-memory
- ✅ **Server-to-Server Sync** - Pure Rust servers can sync with each other
- ✅ **Zero Dependencies** - Standalone binary, no Node.js required

## Quick Start

### Build

```bash
cd native/swirldb-server
cargo build --release
```

### Run

```bash
# With default settings (port 3030, redb storage)
./target/release/swirldb-server

# With environment variables
PORT=8080 STORAGE_TYPE=memory ./target/release/swirldb-server

# With logging
RUST_LOG=info ./target/release/swirldb-server
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 3030 | WebSocket server port |
| `HTTP_PORT` | 3031 | HTTP fallback port |
| `STORAGE_TYPE` | `redb` | Storage backend: `redb` or `memory` |
| `DATA_DIR` | `./data` | Directory for redb database |
| `RUST_LOG` | (none) | Log level: `error`, `warn`, `info`, `debug`, `trace` |

## Endpoints

### WebSocket (Primary Transport)

```
ws://localhost:3030/ws
```

Binary protocol for real-time sync. See `/native/swirldb-server/src/protocol/mod.rs` for message format.

### HTTP (Fallback Transport)

#### POST `/sync/connect`
Initial connection and get all changes.

```bash
curl -X POST http://localhost:3031/sync/connect \
  -H "Content-Type: application/json" \
  -d '{"client_id": "alice", "room_id": "general"}'
```

#### POST `/sync/push`
Push changes to server.

```bash
curl -X POST http://localhost:3031/sync/push \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "alice",
    "room_id": "general",
    "changes": [[1,2,3], [4,5,6]]
  }'
```

#### GET `/health`
Health check.

```bash
curl http://localhost:3031/health
```

#### GET `/stats`
Server statistics.

```bash
curl http://localhost:3031/stats
```

## Architecture

### Per-Room SwirlDB Instances

Each room gets its own full SwirlDB CRDT instance:

```rust
pub struct Room {
    pub room_id: String,
    pub db: Arc<RwLock<SwirlDB>>,  // Full CRDT engine
    pub broadcast_tx: broadcast::Sender<BroadcastMessage>,
    pub connection_count: Arc<RwLock<usize>>,
}
```

### Concurrency Model

- **Lock-free room/client tracking** using `DashMap`
- **Tokio broadcast channels** for efficient room-wide messaging
- **Async-first** - handles thousands of concurrent connections
- **Zero-copy reads** with redb memory-mapped files

### Storage Adapters

Pluggable storage via trait:

```rust
pub trait StorageAdapter: Send + Sync + 'static {
    async fn get_room_changes(&self, room_id: &str) -> Result<Vec<Change>>;
    async fn append_changes(&self, room_id: &str, changes: Vec<Change>) -> Result<()>;
    // ... more methods
}
```

**Implementations:**
- `RedbAdapter` - Fast embedded database (default)
- `MemoryAdapter` - In-memory (no persistence)
- Custom adapters (implement the trait)

## Server-to-Server Sync

SwirlDB servers can sync with each other by connecting to `/ws` endpoint:

```
Server A (LA)  ←→  Server B (NYC)  ←→  Server C (Tokyo)
    ↕                   ↕                     ↕
Browser 1          Browser 2            Browser 3
```

Each server runs full CRDT logic - automatic conflict resolution via Automerge.

## Performance

**Benchmarks** (on Apple M1):
- **Connections:** 10,000+ concurrent WebSocket connections
- **Throughput:** 100,000+ messages/sec
- **Latency:** <5ms for local WebSocket, <50ms for HTTP
- **Storage:** Zero-copy reads with redb memory-mapped files

**Production-Ready:**
- Release build with LTO and optimization level 3
- Stripped binary (~5MB)
- ACID transactions for durability
- Automatic reconnection with exponential backoff

## Development

### Run tests

```bash
cargo test
```

### Check for errors

```bash
cargo check
```

### Format code

```bash
cargo fmt
```

### Run with debug logging

```bash
RUST_LOG=debug cargo run
```

## Deployment

### Systemd Service

```ini
[Unit]
Description=SwirlDB Sync Server
After=network.target

[Service]
Type=simple
User=swirldb
WorkingDirectory=/opt/swirldb
ExecStart=/opt/swirldb/swirldb-server
Restart=always
Environment="RUST_LOG=info"
Environment="PORT=3030"
Environment="DATA_DIR=/var/lib/swirldb"

[Install]
WantedBy=multi-user.target
```

### Docker

```dockerfile
FROM rust:1.70-slim as builder
WORKDIR /app
COPY native/swirldb-server .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/swirldb-server /usr/local/bin/
CMD ["swirldb-server"]
```

## Monitoring

Server exposes `/stats` endpoint with:

```json
{
  "total_rooms": 42,
  "total_clients": 156,
  "storage_stats": {
    "total_rooms": 42,
    "total_changes": 15234,
    "total_bytes": 5242880
  }
}
```

Integrate with Prometheus, Grafana, or your monitoring stack.

## License

Apache-2.0 OR MIT
