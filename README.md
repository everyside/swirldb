# SwirlDB

**A modular-first, cross-platform, CRDT-based embedded database engine**

SwirlDB is designed around pluggability and fine-grained control. Every aspect—storage, encryption, sync—is swappable via runtime adapters. You can mark individual paths as memory-only, persisted, or synced, giving you complete control over your data architecture.

---

## 🎯 Design Philosophy

- **Modular-first**: Everything is pluggable—storage, encryption, sync
- **Path-level control**: Mark specific keys as memory-only, persisted, or synced
- **CRDT-powered**: Built on Automerge for conflict-free distributed data
- **Cross-platform**: Browser WASM and pure Rust server (no Node.js)
- **Observable**: Reactive change tracking at the field level

---

## 🏗️ Architecture

### Browser = WASM | Server = Pure Rust

- **Browser**: Compiled to WebAssembly, runs in all modern browsers
- **Server**: Pure Rust binary, no Node.js dependency, optimized for native I/O

### Core Structure

```
native/swirldb-core/src/
├── core.rs       # Pure Rust core (thread-safe, platform-agnostic)
├── browser.rs    # WASM bindings (thin wrapper around core)
└── lib.rs        # Feature-gated exports
```

---

## 🚀 Quick Start

### Browser (WASM)

**1. Build the WASM package:**
```bash
cd native/swirldb-core
npm run build:wasm
```

**2. Use in your HTML:**
```html
<script type="module">
  import init, { SwirlDB } from './packages/browser-wasm/index.js';

  await init();
  const db = new SwirlDB();

  // Set values with dot-notation paths
  db.setPath('user.name', 'Alice');
  db.setPath('user.age', 30);

  // Get values
  console.log(db.getPath('user.name')); // "Alice"

  // Observe changes
  db.observe('user.name', (newValue) => {
    console.log('Name changed:', newValue);
  });

  // Persist to localStorage
  const state = db.saveState();
  localStorage.setItem('db', state);

  // Restore later
  db.loadState(localStorage.getItem('db'));
</script>
```

**3. Try the example:**
```bash
cd examples/browser
python3 -m http.server 8000
# Open http://localhost:8000
```

---

## 📦 Project Structure

```
swirldb/
├── native/swirldb-core/       # Rust core + WASM bindings
│   ├── src/
│   │   ├── core.rs            # Pure Rust implementation
│   │   ├── browser.rs         # WASM wrapper
│   │   └── lib.rs             # Feature gates
│   └── Cargo.toml
├── packages/browser-wasm/     # Auto-generated WASM output
├── examples/
│   └── browser/               # Browser example
├── CLAUDE.md                  # Development guide
└── README.md                  # This file
```

---

## 🔧 Development

### Build WASM for Browser

```bash
cd native/swirldb-core
npm run build:wasm
```

Outputs to `packages/browser-wasm/`:
- `index_bg.wasm` - WASM binary
- `index.js` - JavaScript glue code
- `index.d.ts` - TypeScript definitions

### Run Tests

```bash
cd native/swirldb-core
cargo test
```

---

## 🧩 Planned Features (Pluggable Architecture)

### Storage Adapters
- In-memory (volatile, fast)
- redb (embedded, persistent)
- SQLite (portable, queryable)
- Sharded files (large datasets)
- IndexedDB (browser only)

**Per-path storage control:**
```rust
db.setPath("session.temp", value).with_storage(StorageHint::MemoryOnly);
db.setPath("user.profile", value).with_storage(StorageHint::Persisted);
```

### Encryption Adapters
- Plaintext (default)
- AES-GCM (document-level)
- Field-level (selective encryption)

### Sync Adapters
- HTTP REST (polling)
- WebSocket (real-time)
- WebRTC (peer-to-peer)

**Per-path sync control:**
```rust
db.setPath("local.draft", value).with_sync(SyncHint::NoSync);
db.setPath("shared.doc", value).with_sync(SyncHint::Bidirectional);
```

---

## 🛠️ Technology Stack

**Core:**
- Rust 1.70+ (for `std::sync::OnceLock`)
- Automerge (CRDT engine)
- wasm-bindgen (Rust ↔ JS FFI)

**Browser:**
- WebAssembly
- web-sys (browser APIs)
- IndexedDB/localStorage support

**Server (Planned):**
- Pure Rust binary
- tokio (async runtime)
- axum (HTTP framework)
- redb (embedded storage)

---

## 📖 API Reference

### Core Methods

```typescript
// Create instance
const db = new SwirlDB();

// Set value at path
db.setPath('user.name', 'Alice');

// Get value at path
const name = db.getPath('user.name');

// Observe changes
db.observe('user.name', (newValue) => {
  console.log('Changed:', newValue);
});

// Manually check observers
db.checkObservers();

// Save state to bytes
const state = db.saveState();

// Load state from bytes
db.loadState(state);
```

---

## 🤝 Contributing

SwirlDB is in active development. The current focus is on:

1. ✅ Core CRDT implementation
2. ✅ Browser WASM support
3. ⏳ Storage adapter architecture
4. ⏳ Pure Rust HTTP server
5. ⏳ Sync protocols

See [CLAUDE.md](./CLAUDE.md) for detailed development guidance.

---

## 📄 License

MIT

---

## 🔗 Resources

- **Documentation**: See [CLAUDE.md](./CLAUDE.md) for architecture details
- **Examples**: Check `examples/browser/` for working code
- **Automerge**: [automerge.org](https://automerge.org)
