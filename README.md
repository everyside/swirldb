# SwirlDB

Cross-platform CRDT database built on Automerge. Runs in browsers via WebAssembly and as a native Rust sync server.

> ⚠️ **UNDER ACTIVE DEVELOPMENT** ⚠️
>
> SwirlDB is in early development and not ready for production use.
> The API is unstable and subject to breaking changes.

📚 **[Full Documentation](https://docs.swirldb.org)**

## Design

- **CRDT-based**: Built on Automerge for automatic conflict resolution
- **Cross-platform**: Browser WASM (~489KB gzipped) and native Rust server with different optimizations
- **Pluggable storage**: In-memory, LocalStorage, IndexedDB, or redb
- **Real-time sync**: WebSocket-based synchronization server
- **Many documents**: a server holds any number of documents, each its own Automerge history, synced whole to whoever has it open; a browser holds several over one connection
- **Access from an authority**: who may open a document is asked of the application that owns membership, not authored a second time in SwirlDB
- **Observable**: Field-level change tracking via observers
- **Policy engine**: Access control for subscriptions within a document

## Architecture

### Browser and Server Builds

- **Browser**: WebAssembly module with JavaScript bindings
- **Server**: Native Rust binary for WebSocket/HTTP sync

### Crate Structure

- **swirldb-core**: Platform-agnostic CRDT engine and storage traits
- **swirldb-browser**: WASM bindings with localStorage and IndexedDB adapters
- **swirldb-server**: Sync server with redb storage and subscription management

See [BUILD.md](./BUILD.md) for build instructions.

## Prerequisites

- **Rust** - Install from [rustup.rs](https://rustup.rs)
- **pnpm** - Install with `npm install -g pnpm` or `brew install pnpm`

## Quick Start

### Browser (WASM)

**1. Build the WASM package:**
```bash
# From repository root
pnpm run build:wasm
```

**2. Use in your application:**
```javascript
import { SwirlDB } from '@swirldb/js';

// Configure with LocalStorage adapter (data persists automatically)
const db = await SwirlDB.withLocalStorage('my-app');

// Use natural property access via Proxies
db.data.user.name = 'Alice';
db.data.user.age = 30;

// Read values
console.log(db.data.user.name.$value); // 'Alice'

// Observe changes reactively
db.data.user.name.$observe((newValue) => {
  console.log('Name changed:', newValue);
});

// Data is automatically persisted to localStorage
// No manual save/load needed!
```

**Or with in-memory storage:**
```javascript
// Volatile storage (nothing persists)
const db = await SwirlDB.create();
```

## Documents, and who may open them

A document is the unit of sync and the unit of access, and the two are the
same thing on purpose. Automerge's change graph is not partitioned by path, so
the only history a server can hand a client selectively is a whole document's;
a "global document with per-user branches" leaks the whole history to every
client on its first sync. So SwirlDB holds many documents, each with its own
id, its own history, its own row in storage and its own subscribers.

Every protocol message that touches a document names it. A connection opens
its first document in `Connect` and more with `Open`; each is answered with
`SubscribeAck` (carrying the access granted) and `Sync` (carrying only that
document's history), or `OpenDenied`. A client that never names a document is
on the default one, which is how the single-document demos and tests kept
working unchanged.

```javascript
import { SwirlDBConnection } from '@swirldb/js';

const connection = await SwirlDBConnection.open('wss://example/ws', 'alice');
const pattern = await connection.openDocument('pattern.7');   // rejects if refused
const palette = await connection.openDocument('palette.3');   // same socket
pattern.data.source = '...';
pattern.syncChanges();
pattern.sendPresence('alice', { cursor: 42 });               // per document, ephemeral
```

**Who may open a document is decided by an `Authority`**, a server trait with
one question: `may_open(subject, document) -> Read | Write | None`. Three
implementations ship:

| Authority | When |
|---|---|
| `OpenToAll` | The default. Every document open to every client — a laptop, a demo, the test suite |
| `PolicyAuthority` | The policy file *is* where access is decided; rules are written over document ids |
| `HttpAuthority` | An application owns membership. `POST <AUTHORITY_URL>/may-open` with `{"subject": <actor>, "document": "<id>"}` is answered `{"access": "read" \| "write" \| "none"}`; answers are cached ten seconds; an unreachable authority refuses |

The rule this protects is that access is **derived here and authored there**.
Two places that both know who may read a document disagree eventually, and
when they do the disagreement is a disclosure. So SwirlDB never carries a copy
of an application's membership; it asks, briefly remembers, and enforces: a
reader receives the document and may send presence, but its `Push` is
refused; a refused subject never receives the history. See
`native/swirldb-server/src/authority.rs`.

What stays single-document: server-to-server peer sync (`connect_to_peer`,
the peer manager, the LAN transport) speaks about the default document only.

## Development

### Building from Source

**Build Browser WASM:**
```bash
cd native/swirldb-browser
wasm-pack build --target web --out-dir pkg
```

**Build Server:**
```bash
cd native/swirldb-server
cargo build --release
```

### Running Tests

**Prerequisites for Integration Tests:**
```bash
# Install Node.js dependencies for browser tests
cd tests/integration
npm install

# Install Playwright for headless browser testing
npx playwright install chromium
```

**Run All Integration Tests:**
```bash
cd tests
cargo test --test integration
```

**Run Specific Test Suites:**
```bash
# Browser WebAssembly ↔ Server sync tests
cargo test --test integration browser_sync

# Subscription filtering tests
cargo test --test integration subscription_filtering

# Multi-client sync tests
cargo test --test integration multi_client_sync

# Network resilience tests
cargo test --test integration network_resilience
```

**Run a Single Test:**
```bash
cargo test --test integration test_browser_to_server_sync -- --nocapture
```

The integration test suite includes:
- 32 passing tests covering Browser WASM and Rust native clients
- Real headless browser testing with Playwright
- Cross-platform CRDT synchronization validation
- Subscription filtering and policy enforcement
- Network disconnect/reconnect scenarios

See [BUILD.md](./BUILD.md) for detailed build instructions.

## Storage and Sync Adapters

SwirlDB is built entirely from swappable adapters:

### Storage Adapters

**Implemented:**
- ✅ In-memory (volatile, fast)
- ✅ LocalStorage (browser, 5-10MB)
- ✅ IndexedDB (browser, 50MB-1GB)
- ✅ redb (server, embedded, persistent)
- ✅ Memory adapter (server, multi-threaded)

**Planned:**
- 🔜 SQLite (portable, queryable)
- 🔜 Sharded files (large datasets)
- 🔜 S3 (cloud-native)


### Sync Adapters

**Implemented:**
- ✅ WebSocket (real-time, bi-directional)
- ✅ HTTP long-polling (fallback)

**Planned:**
- 🔜 WebRTC (peer-to-peer)
- 🔜 Custom protocols

### Auth & Policy Adapters

**Planned:**
- 🔜 JWT validation
- 🔜 OAuth integration
- 🔜 ABAC (attribute-based access control)
- 🔜 Custom policy engines

### Encryption Adapters

**Planned:**
- 🔜 AES-GCM (document-level)
- 🔜 Field-level encryption
- 🔜 Custom crypto implementations

## License

Apache 2.0

Copyright 2025 Everyside Innovations, LLC
