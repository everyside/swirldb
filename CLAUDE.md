# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SwirlDB is a cross-platform, pluggable, real-time CRDT-based embedded database engine designed to be **modular-first**:

- **Pluggable everything**: Storage, encryption, sync, and observability are all swappable adapters
- **Path-level granularity**: Fine-grained control over which keys are memory-only, persisted, or synced
- **Observable state**: Reactive change tracking at the dot-path level
- **Multi-environment**: Browser WASM and pure Rust server with target-optimized implementations
- **Runtime adaptable**: Configure behavior per-deployment without recompilation

The core implementation uses Automerge for CRDT functionality with path-based key-value operations and reactive observers. The architecture prioritizes modularity - there is no single "primary" storage layer. Instead, you compose the system from pluggable adapters to match your use case.

## Architecture

### Design Philosophy

**Browser = WASM, Server = Pure Rust**

- **Browser:** Compiled to WASM via `wasm-bindgen`, runs in all modern browsers
- **Server:** Pure Rust binary with no Node.js dependency, optimized for performance and native I/O
- **No shared WASM/Node.js layer:** Each target has its own optimized implementation

### Architectural Layers

**1. Core Engine** (`native/swirldb-core/src/core.rs`) — Pure Rust, platform-agnostic:
- Built on `automerge::AutoCommit` for CRDT state management
- Path-based APIs: `getPath`, `setPath` for dot-notation access (e.g., `user.name`)
- Observer system: Thread-safe reactive change tracking
- Snapshots: `saveState`, `loadState` for serialization
- No binding attributes - pure Rust types and logic
- Planned: `getChangesSince`, `applyChanges` for delta syncing

**2. Browser Target** (`native/swirldb-core/src/browser.rs`) — WASM bindings:
- Thin wrapper around core with `#[wasm_bindgen]` attributes
- JS↔Rust type conversions via `js-sys` and `serde-wasm-bindgen`
- Browser storage integration: IndexedDB, localStorage via `web-sys`
- Compiled with `wasm-pack --target web`

**3. Server Target** (future: `server/` workspace) — Pure Rust HTTP server:
- Native binary with no Node.js/npm dependency
- Async runtime: `tokio`
- HTTP framework: `axum` (or `actix-web`)
- Native storage: `redb` (pure Rust embedded DB) or `rusqlite`
- Direct file system access for sharded storage
- Production-ready with `tracing` for observability

### Module Structure

```
native/swirldb-core/src/
├── lib.rs              # Feature-gated module exports
├── core.rs             # Pure Rust core (no bindings)
│                       # - Automerge CRDT operations
│                       # - Path resolution logic
│                       # - Thread-safe observer management
│                       # - StorageAdapter trait + InMemoryStorage
│                       # - Storage hints (MemoryOnly, Persisted, Synced)
├── storage.rs          # Browser storage adapters
│                       # - LocalStorageAdapter (browser)
│                       # - IndexedDBAdapter (browser, future)
├── browser.rs          # WASM bindings via wasm-bindgen
└── types.rs            # Shared type definitions (optional)
```

### Key Implementation Details

**Core Engine** (`core.rs`):
- Uses `Arc<Mutex<AutoCommit>>` for thread-safe CRDT access
- Path resolution: `"user.name"` → nested map traversal with auto-creation
- Observers: `Arc<Mutex<Vec<Observer>>>` with path-based callbacks
- Type system: Internal conversions between Rust types and Automerge `ScalarValue`
- Storage: `StorageAdapter` trait with `InMemoryStorage` default implementation
- Storage hints: Per-path policies (MemoryOnly, Persisted, Synced)

**Path Resolution:**
- Paths are dot-separated strings (e.g., `"user.profile.email"`)
- `resolve_path()` creates intermediate maps when `create=true`
- `resolve_path_read()` traverses without mutation (read-only access)

**CRDT Internals:**
- Powered by `automerge::AutoCommit` for conflict-free replicated data
- Subtree addressing via dot-paths enables fine-grained updates
- Mergeable state allows distributed sync
- Snapshotting via `saveState`/`loadState` for persistence
- Planned: `getChangesSince`/`applyChanges` for incremental sync

**Observability:**
- `observe(path, callback)` enables field-level change tracking
- Observers check cached values and fire on changes
- Automatically triggered after mutations (`setPath`, `loadState`)
- Thread-safe implementation allows concurrent access

### Plugin Architecture (Core Design Principle)

**Everything is pluggable** - SwirlDB is composed from runtime-swappable adapters:

```rust
SwirlDB::new(Config {
    storage: Box::new(RedbAdapter::new("./data")),
    encryption: Box::new(AesAdapter::new(key)),
    sync: Box::new(WebSocketSync::new(url)),
})
```

#### Adapter Types

**StorageAdapter** - Equal priority implementations:
- ✅ **InMemoryStorage** (volatile, fast) - Default, implemented in `core.rs`
- ✅ **LocalStorageAdapter** (browser, ~5-10MB) - Implemented in `storage.rs`
- 🔜 **IndexedDBAdapter** (browser, ~50MB-1GB) - Planned in `storage.rs`
- 🔜 **redb** (native, embedded, persistent) - Future
- 🔜 **SQLite** (SQL queries, portable) - Future
- 🔜 **Sharded files** (large datasets, streaming) - Future

**Storage Hints** - Per-path storage policies:
```rust
// Rust API
db.set_storage_hint("session.temp", StorageHint::MemoryOnly);
db.set_storage_hint("user.profile", StorageHint::Persisted);
db.set_storage_hint("shared.doc", StorageHint::Synced);

// TypeScript API
db.setStorageHint('session.temp', 'memory-only');
db.setStorageHint('user.profile', 'persisted');
db.setStorageHint('shared.doc', 'synced');
```

**Creating Storage Instances:**
```rust
// Rust - In-memory (default)
let db = SwirlDB::new();

// Rust - Custom storage adapter
let storage = Arc::new(LocalStorageAdapter::new("my-app")?);
let db = SwirlDB::with_storage(storage, "db-key");

// Rust - Enable auto-persist
let mut db = SwirlDB::with_storage(storage, "db-key");
db.set_auto_persist(true);
```

```typescript
// TypeScript/JavaScript - In-memory (default)
const db = new SwirlDB(new WasmDB());

// TypeScript/JavaScript - LocalStorage
const db = await SwirlDB.withLocalStorage('my-app');

// TypeScript/JavaScript - Auto-persist with debouncing
db.enableAutoPersist(500); // 500ms debounce
db.data.user.name = 'Alice'; // Auto-saves after 500ms
db.disableAutoPersist();

// Manual persist
db.persist();
```

**EncryptionAdapter** - Optional security layer:
- Plaintext (no encryption)
- AES-GCM (document-level)
- Field-level (selective encryption by path pattern)

**SyncAdapter** - Bi-directional sync strategies:
- HTTP REST (polling)
- WebSocket (real-time push/pull)
- WebRTC (peer-to-peer, no server)
- Custom (implement your own protocol)

**Sync control per-path**:
```rust
db.setPath("local.draft", value).with_sync(SyncHint::NoSync);
db.setPath("shared.doc", value).with_sync(SyncHint::Upstream | SyncHint::Downstream);
```

## Dependencies & Library Choices

### Core Dependencies (Shared)
```toml
automerge = "0.6.1"              # CRDT engine
serde = { version = "1.0", features = ["derive"] }
anyhow = "1.0"                   # Error handling
base64 = "0.21"                  # Base64 encoding for storage
```

### Browser-Specific Dependencies
```toml
wasm-bindgen = "0.2"             # Rust↔JS FFI
js-sys = "0.3"                   # JavaScript types
web-sys = "0.3"                  # Browser APIs (IndexedDB, Storage)
console_error_panic_hook = "0.1" # Better panic messages
serde-wasm-bindgen = "0.6"       # Efficient serialization
```

### Server-Specific Dependencies
```toml
tokio = { version = "1", features = ["full"] }
axum = "0.7"                     # HTTP framework
redb = "2"                       # Pure Rust embedded database
tracing = "0.1"                  # Structured logging
tracing-subscriber = "0.3"       # Log formatting
tower-http = "0.5"               # HTTP middleware
```

### Why These Choices?

- **No `once_cell`**: Removed in favor of `std::sync::OnceLock` (stabilized in Rust 1.70+)
- **`redb` over `sled`**: More actively maintained, zero-copy design, ACID guarantees
- **`axum` over `actix-web`**: Better async ergonomics, built on `tokio`, type-safe
- **No Node.js/NAPI**: Eliminates FFI overhead, simplifies deployment, better performance

## Development Commands

### Browser WASM Build

From `native/swirldb-core/`:
```bash
wasm-pack build --target web --features wasm --out-dir ../../packages/browser-wasm
```

Outputs to `packages/browser-wasm/`:
- `index_bg.wasm` - WASM binary (optimized for size)
- `index.js` - JS glue code
- `index_bg.js` - Internal bindings
- `index.d.ts` - TypeScript definitions

### Server Binary Build

From repository root (future):
```bash
cargo build --release --features native-server --bin swirldb-server
```

Cross-compile for Linux deployment:
```bash
cargo install cross
cross build --release --target x86_64-unknown-linux-musl
```

### Development Workflow

**Browser development:**
1. Edit Rust in `native/swirldb-core/src/core.rs` or `browser.rs`
2. Rebuild: `wasm-pack build --target web --features wasm`
3. Test in browser example: `examples/browser/index.html`

**Server development:**
1. Edit Rust in `server/src/` or `native/swirldb-core/src/core.rs`
2. Run: `cargo run --features native-server`
3. Test with `curl` or HTTP client

**Core logic changes:**
- Edit `core.rs` (pure Rust, no bindings)
- Changes automatically available to both browser and server after rebuild

## Package Structure

```
swirldb/
├── native/swirldb-core/      # Core Rust library + WASM bindings
├── packages/browser-wasm/    # Auto-generated WASM output (never edit)
├── server/                   # Pure Rust HTTP server (future)
├── examples/
│   ├── browser/              # Browser HTML/JS example
│   └── cli/                  # Command-line example
└── CLAUDE.md                 # This file
```

**Never manually edit:**
- `packages/browser-wasm/*` - Auto-generated by `wasm-pack`

## Requirements

- **Rust**: Version 1.70+ (for `OnceLock`)
- **wasm-pack**: For browser builds (`cargo install wasm-pack`)
- **cross** (optional): For cross-platform server builds

## Browser Integration Example

```html
<!DOCTYPE html>
<html>
<head>
  <script type="module">
    import init, { SwirlDB } from './pkg/index.js';

    await init();
    const db = new SwirlDB();

    db.observe('user.name', (newVal) => {
      console.log('Name changed:', newVal);
    });

    db.setPath('user.name', 'Alice');
    db.setPath('user.age', 30);

    console.log('Name:', db.getPath('user.name'));

    // Save to localStorage
    const state = db.saveState();
    localStorage.setItem('db-state', state);
  </script>
</head>
</html>
```

## Server API Example (Planned)

```rust
use axum::{Router, routing::post, Json};
use swirldb_core::SwirlDB;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    let db = Arc::new(Mutex::new(SwirlDB::new()));

    let app = Router::new()
        .route("/set", post(set_value))
        .route("/get", post(get_value))
        .with_state(db);

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
```

## Current Status (October 2025)

### ✅ Completed Features

**Core CRDT Engine:**
- ✅ Path-based key-value operations (dot-notation: `user.name`)
- ✅ Automerge CRDT integration with `AutoCommit`
- ✅ Observer system for reactive change tracking
- ✅ Delta sync with `getChanges()` and `applyChanges()`
- ✅ State snapshotting with `saveState()` / `loadState()`
- ✅ Storage adapter trait with pluggable backends

**Browser WASM:**
- ✅ WASM bindings via `wasm-bindgen`
- ✅ TypeScript wrapper with Proxy-based API (`db.data.user.name = 'Alice'`)
- ✅ LocalStorage persistence adapter
- ✅ IndexedDB persistence adapter
- ✅ Auto-persist with debouncing
- ✅ Storage backend selection (memory/localStorage/IndexedDB)

**Sync Server (native/swirldb-server):**
- ✅ Pure Rust HTTP/WebSocket server with `axum` and `tokio`
- ✅ redb storage backend for persistent namespaces
- ✅ WebSocket real-time sync protocol
- ✅ HTTP REST API with long-polling
- ✅ Binary protocol for CRDT changes
- ✅ Multi-client broadcast in namespaces
- ✅ Incremental sync with client heads
- ✅ Structured logging with `tracing`

**Demo Application:**
- ✅ Real-time chat client (`docs/src/pages/chat-client.astro`)
- ✅ Multi-user sync (tested with Alice & Bob)
- ✅ Message discovery via CRDT message index
- ✅ Transport switching (WebSocket ↔ HTTP)
- ✅ Settings persistence (per-client preferences)
- ✅ Debug mode with mutation tracking

### 🔜 Planned Features

| Area | Task | Priority |
|------|------|----------|
| Performance | Batch CRDT mutations into transactions | High |
| Server | Namespace cleanup/GC for idle namespaces | Medium |
| Browser Storage | Storage quota management & warnings | Medium |
| Encryption | Field-level encryption adapter | Low |
| Sync | Upstream-only hint (settings sync) | Medium |
| WebRTC | Peer-to-peer sync (no server) | Low |
| Testing | Integration tests for sync scenarios | High |
| Benchmarks | Performance comparison: native vs WASM | Medium |
| Publishing | Publish to crates.io & npm | Low |

## Current Implementation Details

### Server Architecture (`native/swirldb-server/`)

**State Management:**
- In-memory namespace store with `Arc<RwLock<HashMap<String, Namespace>>>`
- Each namespace contains:
  - Automerge CRDT document
  - Connected clients (WebSocket connections)
  - Pending HTTP poll requests (long-polling)
- Persistent storage via redb (embedded key-value store)
- Namespaces auto-load from disk on first access
- Namespaces removed from memory when all clients disconnect

**Protocol:**
- Binary WebSocket protocol with message types:
  - `MSG_CONNECT (0x01)`: Client registration with heads for incremental sync
  - `MSG_SYNC (0x02)`: Server response with CRDT changes
  - `MSG_PUSH (0x03)`: Client pushes local changes
  - `MSG_BROADCAST (0x04)`: Server broadcasts changes to other clients
- HTTP REST API:
  - `POST /sync/connect`: Initial connection, returns namespace state
  - `POST /sync/push`: Push CRDT changes
  - `GET /sync/poll`: Long-polling for new changes (25s timeout)

**Ports:**
- WebSocket: `ws://localhost:3030/ws`
- HTTP API: `http://localhost:3030/sync/*`

### Browser Client Architecture

**TypeScript Wrapper (`docs/public/swirldb.js`):**
- Proxy-based API for natural JavaScript syntax
- Special properties:
  - `$value`: Get actual value (avoids returning proxy)
  - `$observe(callback)`: Watch for changes
  - `$delete()`: Delete property
- Auto-persist with debouncing (configurable)
- Storage backend selection (memory/localStorage/IndexedDB)

**Message Discovery Pattern:**
- Each message stored as `msg_{clientId}-{timestamp}` in CRDT
- Message IDs tracked in:
  1. localStorage: `chat-{roomId}-all-ids` (client-side cache)
  2. CRDT: `message_index` (synced JSON string array)
- On receiving CRDT changes, clients:
  1. Apply changes via `applyChanges()`
  2. Read `message_index` from CRDT
  3. Update localStorage cache
  4. Render all known messages

**Debug Mode:**
- Enable in Settings panel → "Debug Mode (GLOBAL)"
- Adds `_debug` metadata to network requests
- Shows `recent_mutations` array with field-level changes
- Logs detailed sync information to console
- For WebSocket: Sends JSON text frame before binary frame

### CRDT Behavior

**Change Granularity:**
- Each field assignment creates a separate Automerge change
- Example: One message = 5 changes (id, from, text, timestamp, message_index)
- This is **normal and correct** for CRDTs
- Enables:
  - Fine-grained conflict resolution
  - Granular history tracking
  - Incremental sync efficiency

**Sync Flow:**
1. Client mutates local CRDT
2. Client sends all changes via `db.getChanges()`
3. Server merges changes into namespace CRDT
4. Server broadcasts to all connected clients
5. Clients apply changes via `db.applyChanges()` (merges, doesn't replace)

## Development Setup

### Running the Demo

1. **Start the sync server:**
   ```bash
   cd native/swirldb-server
   cargo run --release
   # Server listens on ws://localhost:3030 and http://localhost:3030
   ```

2. **Start the docs dev server:**
   ```bash
   cd docs
   npm install
   npm run dev
   # Vite dev server runs on http://localhost:4321
   ```

3. **Open multiple chat clients:**
   - Alice: http://localhost:4321/chat-client?id=alice
   - Bob: http://localhost:4321/chat-client?id=bob
   - Messages sync in real-time between all clients

4. **Enable debug mode:**
   - Open Settings panel in any client
   - Check "Debug Mode (GLOBAL)"
   - Watch console for detailed sync logs
   - Inspect Network tab for `_debug` metadata

## Important Notes

- **No TypeScript server**: Server is pure Rust for maximum performance
- **No Node.js dependency**: Server runs as standalone binary
- **WASM is browser-only**: No WASM in Node.js, no `--experimental-wasm-modules` flag needed
- **Thread-safe core**: Can be used in multi-threaded server contexts
- **Auto-generated packages**: Never edit `packages/*` directories manually
- **Feature flags**: Use `--features wasm` for browser, `--features native-server` for server
- **Size optimization**: Browser WASM uses `opt-level = "z"` for smaller bundles
- **Speed optimization**: Server uses `opt-level = 3` + LTO for maximum performance
- **CRDT changes are normal**: Many changes per operation is expected behavior, not a bug
