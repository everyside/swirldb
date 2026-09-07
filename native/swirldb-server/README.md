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

### Run in Docker

The repository root carries a `Dockerfile` for this server — the root, not
this directory, because the server is a workspace member and the build uses
the workspace's `Cargo.lock` (`--locked`):

```bash
# from the repository root
docker build -t swirldb-server:local .

docker run --rm -p 3030:3030 \
  -v swirldb-data:/data \
  -e AUTHORITY_URL=http://studio-server:8080/authority \
  -e AUTHORITY_SECRET=... \
  swirldb-server:local
curl http://localhost:3030/health   # OK
```

Two stages: a builder on `rust:1.98-bookworm` and a `debian:bookworm-slim`
runtime, both pinned by digest, holding the binary, `tini` as PID 1, and
`curl` for the container's own `HEALTHCHECK` against `/health`. The server
runs as `swirldb` (uid 10001) and keeps its documents on the `/data` volume,
which is where `STORAGE_PATH` points; run it without a volume there and a
restart loses every document's history. The image sets `PORT=3030`,
`STORAGE_TYPE=redb`, `STORAGE_PATH=/data/swirldb.redb` and
`RUST_LOG=swirldb_server=info`; `AUTHORITY_URL` and `AUTHORITY_SECRET` are
deliberately not defaulted and come from the deployment. The server binds
`0.0.0.0` on `PORT`, and the health check reads `PORT` too, so changing the
port is one variable.
The image is about 120 MB, most of it Debian; the server binary is a few
megabytes of it.

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 3030 | WebSocket server port |
| `RUST_LOG` | (none) | Log level: `error`, `warn`, `info`, `debug`, `trace` |
| `AUTHORITY_URL` | (none) | Base URL of the application that answers `POST /whoami` (whose token is this) and `POST /may-open` (may this subject open this document). Without it every connection is whoever it says it is and every document is open to it, and the log says so |
| `AUTHORITY_SECRET` | (none) | Shared secret sent to the authority as `Authorization: Bearer <secret>` on both questions, so the application can refuse to answer anyone else, and required as the bearer on `POST /admin/revoke`, so nobody else can give the server orders. Without it the only credential a request can carry is user-info in `AUTHORITY_URL` (`http://swirldb:<secret>@host/authority`, sent as `Basic`); that form is kept for one release and dropped from the URL whenever `AUTHORITY_SECRET` is set — and `/admin/revoke` stays closed |

## Endpoints

### WebSocket

```
ws://localhost:3030/ws
ws://localhost:3030/ws?token=<bearer token>
```

A connection identifies itself on the upgrade, with `Authorization: Bearer <token>`
or, from a browser that cannot set a header, the `token` query parameter. With
`AUTHORITY_URL` set the authority is asked whose token it is, and that subject —
not the `client_id` the connection names in `Connect` — is what every access
decision is about; a connection without a token the authority knows is answered
`OpenDenied` and closed.

The authority's two endpoints answer questions about other people's access, so
the server proves itself on every request with `AUTHORITY_SECRET` as a bearer;
an authority that is down, or that refuses the secret, refuses every
connection and every open rather than admitting them. The log names the
authority's URL with any user-info removed and never prints the secret.

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

#### POST `/admin/revoke`

Close what a subject has open, now. The authority is asked once, at open,
and a connection holds that answer for as long as the document is open; the
ten seconds an answer is cached only decides how soon a *reopen* is refused.
This is the other direction of the seam: the application that owns
membership says a subject's access ended, and the server acts on it.

```bash
curl -X POST http://localhost:3030/admin/revoke \
  -H "Authorization: Bearer $AUTHORITY_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"subject": "alice", "document": "palette.3"}'
```

`subject` is the `id` of the actor the authority's `/whoami` named for the
connection — never a `client_id`, which a connection chooses for itself.
`document` is the id to close on the subject's connections, or `null` for
every document the subject has open.

What happens: the server drops the document from every connection the
subject holds and sends each an `OpenDenied` naming the reason `revoked`
(the browser handle's `onDenied` fires and its `access` reads `null`; the
Rust client's `on_denied` yields and its connection ends); the connections
themselves stay up, so a client may open something else on one; and the
authority forgets what it cached about the subject — the one document's
answer, or with `null` every answer and whose token it is — so a reopen is
asked afresh. The order is important: the cache is cleared after the close,
so a reopen that races the revocation cannot be admitted out of it.

Returns what was closed, by client id; empty when the subject held nothing,
which is not an error:
```json
{ "closed": [{ "client_id": "alice-laptop", "document": "palette.3" }] }
```

`401` for a missing or wrong bearer, `400` for an empty subject, and `403`
when the server has no `AUTHORITY_SECRET`: an order anyone can give is not
an order, so without a secret the endpoint is closed rather than open.

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
