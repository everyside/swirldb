# SwirlDB Build Guide

This document explains how to build all components of SwirlDB correctly.

## Quick Start

```bash
# Build everything (WASM + Server)
./build.sh

# Build and deploy WASM to admin site
./build.sh --admin

# Build and deploy WASM to both admin and docs sites
./build.sh --admin --docs

# Build only WASM
./build.sh --wasm-only --admin

# Build only server
./build.sh --server-only
```

## Architecture Overview

SwirlDB has **two completely separate build targets**:

```
┌─────────────────────────────────────────────────────────────┐
│                        SwirlDB                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────────┐      ┌──────────────────────┐   │
│  │   Browser (WASM)     │      │   Server (Native)    │   │
│  ├──────────────────────┤      ├──────────────────────┤   │
│  │ Target: wasm32       │      │ Target: native       │   │
│  │ Features: wasm       │      │ Features: none       │   │
│  │ Entry: browser.rs    │      │ Entry: main.rs       │   │
│  │ Output: .wasm files  │      │ Output: binary       │   │
│  └──────────────────────┘      └──────────────────────┘   │
│           ↓                              ↓                  │
│  ┌──────────────────────┐      ┌──────────────────────┐   │
│  │  admin/public/wasm/  │      │ target/release/      │   │
│  │  docs/public/wasm/   │      │ swirldb-server       │   │
│  └──────────────────────┘      └──────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Critical Build Requirements

### ⚠️ WASM Build MUST Use `--features wasm`

**CRITICAL:** When building WASM, you **must** use `--features wasm` or the browser bindings won't be included!

```bash
# ✅ CORRECT - Includes browser bindings
wasm-pack build --target web --features wasm

# ❌ WRONG - No browser bindings, only exports init()
wasm-pack build --target web
```

**Why this matters:**

Without `--features wasm`, the `browser.rs` module is not compiled, resulting in:
- No `SwirlDB` class exported
- Only `init()` and `initSync()` functions available
- Error: `WasmSwirlDB is not a constructor`
- Error: `LinkError: WebAssembly.instantiate(): Import #X requires a callable`

This was the root cause of our painful debugging session on 2025-10-18.

## Build Targets

### 1. Browser WASM

**Purpose:** Runs in web browsers (Chrome, Firefox, Safari, Edge)

**Build command:**
```bash
cd native/swirldb-core
wasm-pack build --target web --features wasm
```

**Output location:**
```
native/swirldb-core/pkg/
├── swirldb_core.js           # JavaScript glue code
├── swirldb_core_bg.wasm      # WebAssembly binary
├── swirldb_core.d.ts         # TypeScript definitions
├── swirldb_core_bg.wasm.d.ts # WASM TypeScript definitions
└── package.json              # npm package metadata
```

**Deploy to sites:**
```bash
# Admin site
cp -r native/swirldb-core/pkg/* admin/public/wasm/

# Docs site
cp -r native/swirldb-core/pkg/* docs/public/wasm/
```

**What gets compiled:**
- `src/core.rs` - Core CRDT engine (pure Rust)
- `src/browser.rs` - WASM bindings with `#[wasm_bindgen]` attributes
- `src/storage.rs` - LocalStorage and IndexedDB adapters

**Dependencies used:**
- `wasm-bindgen` - Rust ↔ JavaScript FFI
- `js-sys` - JavaScript standard types
- `web-sys` - Browser Web APIs (localStorage, IndexedDB)
- `automerge` - CRDT library (with wasm feature)

### 2. Native Server

**Purpose:** Standalone HTTP/WebSocket sync server (no Node.js, no WASM)

**Build command:**
```bash
cd native/swirldb-server
cargo build --release
```

**Output location:**
```
native/swirldb-server/target/release/swirldb-server
```

**Run:**
```bash
cd native/swirldb-server
RUST_LOG=info cargo run --release
# Or run the binary directly:
./target/release/swirldb-server
```

**What gets compiled:**
- Pure Rust with no WASM or Node.js dependencies
- `tokio` async runtime
- `axum` HTTP framework
- `redb` embedded database
- WebSocket server on port 3030
- HTTP API on port 3031

## Development Workflow

### Initial Setup

```bash
# 1. Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 2. Add WASM target
rustup target add wasm32-unknown-unknown

# 3. Install wasm-pack
cargo install wasm-pack

# 4. Build everything
./build.sh --admin --docs
```

### Common Development Tasks

**Working on browser features:**
```bash
# 1. Edit code in native/swirldb-core/src/browser.rs or core.rs
# 2. Rebuild WASM
./build.sh --wasm-only --admin
# 3. Browser will auto-reload (Vite HMR)
```

**Working on server:**
```bash
# 1. Edit code in native/swirldb-server/src/
# 2. Server auto-recompiles (cargo run watches for changes)
# No manual rebuild needed
```

**Testing admin interface:**
```bash
# Terminal 1: Run sync server
cd native/swirldb-server
RUST_LOG=info cargo run --release

# Terminal 2: Run admin dev server
cd admin
npm run dev

# Open: http://localhost:4321/data
```

**Testing docs/chat demo:**
```bash
# Terminal 1: Run sync server
cd native/swirldb-server
RUST_LOG=info cargo run --release

# Terminal 2: Run docs dev server
cd docs
npm run dev

# Open: http://localhost:4321/chat-client
```

## Troubleshooting

### "WasmSwirlDB is not a constructor"

**Problem:** WASM was built without `--features wasm`

**Solution:**
```bash
./build.sh --wasm-only --admin
```

### "LinkError: WebAssembly.instantiate()"

**Problem:** WASM file and JavaScript glue code are out of sync

**Solution:** Rebuild WASM completely
```bash
cd native/swirldb-core
rm -rf pkg/
wasm-pack build --target web --features wasm
cp -r pkg/* ../../admin/public/wasm/
```

### "Connection Failed" in admin interface

**Checklist:**
1. Is the sync server running? (`lsof -ti:3030`)
2. Is the admin dev server running? (`lsof -ti:4321`)
3. Check browser console for errors
4. Check server logs for connection attempts

**Fix:**
```bash
# Start sync server
cd native/swirldb-server
RUST_LOG=info cargo run --release

# In another terminal, start admin
cd admin
npm run dev
```

### CSP blocking WASM

**Problem:** Content Security Policy blocking `eval` in JavaScript

**Note:** This was a red herring in our debugging session. The real issue was missing `--features wasm`, but CSP warnings can be confusing.

**If you actually need to fix CSP:**
Admin's `astro.config.mjs` has a Vite middleware that sets a permissive CSP for development:
```javascript
res.setHeader('Content-Security-Policy',
  "default-src 'self'; script-src 'self' 'unsafe-eval' 'unsafe-inline'; ..."
);
```

## File Structure

```
swirldb/
├── build.sh                          # Master build script
├── BUILD.md                          # This file
│
├── native/
│   ├── swirldb-core/                 # Core library (WASM + Rust)
│   │   ├── src/
│   │   │   ├── lib.rs                # Feature-gated exports
│   │   │   ├── core.rs               # Pure Rust CRDT engine
│   │   │   ├── browser.rs            # WASM bindings (feature = "wasm")
│   │   │   ├── storage.rs            # Storage adapters
│   │   │   └── sync.rs               # Sync protocols
│   │   ├── Cargo.toml                # Features: wasm, native-server
│   │   └── pkg/                      # WASM output (auto-generated)
│   │
│   └── swirldb-server/               # Pure Rust sync server
│       ├── src/
│       │   ├── main.rs               # Server entry point
│       │   ├── state.rs              # Namespace state management
│       │   ├── protocol/             # WebSocket protocol
│       │   └── storage/              # Persistent storage (redb)
│       ├── Cargo.toml
│       └── target/release/           # Binary output
│           └── swirldb-server
│
├── admin/                            # Admin interface
│   ├── public/
│   │   ├── wasm/                     # WASM files (deployed here)
│   │   └── swirldb.js                # TypeScript wrapper
│   ├── src/pages/
│   │   └── data.astro                # Data browser
│   └── astro.config.mjs              # CSP configuration
│
└── docs/                             # Documentation + demos
    ├── public/
    │   ├── wasm/                     # WASM files (deployed here)
    │   └── swirldb.js                # TypeScript wrapper
    └── src/pages/
        └── chat-client.astro         # Chat demo
```

## Feature Flags Reference

### swirldb-core (native/swirldb-core/Cargo.toml)

```toml
[features]
default = []

# Browser WASM target - REQUIRED for wasm-pack builds
wasm = [
    "dep:wasm-bindgen",
    "dep:wasm-bindgen-futures",
    "dep:js-sys",
    "dep:web-sys",
    "dep:console_error_panic_hook",
    "dep:serde-wasm-bindgen",
]

# Native server target (future)
native-server = [
    "dep:tokio",
    "dep:axum",
    "dep:tracing",
    "dep:tracing-subscriber",
]
```

**Usage:**
- WASM build: `wasm-pack build --features wasm` ✅
- Server (future): `cargo build --features native-server`
- Core only: `cargo build` (no features)

## Best Practices

1. **Always use the build script** - Don't manually run wasm-pack or cargo
2. **Never edit generated files** - Don't touch anything in `pkg/` or `target/`
3. **WASM requires --features wasm** - This is non-negotiable
4. **Test both targets** - WASM and server are independent
5. **Check feature flags** - Wrong flags = broken builds

## Quick Reference

| Task | Command |
|------|---------|
| Build everything | `./build.sh` |
| Build WASM only | `./build.sh --wasm-only --admin` |
| Build server only | `./build.sh --server-only` |
| Deploy WASM to admin | `./build.sh --admin` |
| Deploy WASM to docs | `./build.sh --docs` |
| Run sync server | `cd native/swirldb-server && cargo run --release` |
| Run admin dev | `cd admin && npm run dev` |
| Run docs dev | `cd docs && npm run dev` |
| Clean WASM | `cd native/swirldb-core && rm -rf pkg/` |
| Clean server | `cd native/swirldb-server && cargo clean` |

## Help

If you encounter issues:

1. Check this BUILD.md file
2. Run `./build.sh --help` for script options
3. Verify feature flags in Cargo.toml
4. Check browser console for errors
5. Check server logs with `RUST_LOG=info`

**Common mistakes to avoid:**
- ❌ Building WASM without `--features wasm`
- ❌ Manually copying WASM files instead of using build script
- ❌ Editing generated files in `pkg/`
- ❌ Forgetting to restart dev servers after rebuild
- ❌ Running wasm-pack from wrong directory
