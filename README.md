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
- **Identity and access from an authority**: who a connection is, and which documents it may open, are asked of the application that owns sessions and membership, not authored a second time in SwirlDB
- **Text that merges**: a text at a path is a sequence of characters, not a string; two people splicing into one both keep their characters, and observers hear the edits as splices with positions
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

const connection = await SwirlDBConnection.open('wss://example/ws', 'alice', sessionToken);
const pattern = await connection.openDocument('pattern.7');   // rejects if refused
const palette = await connection.openDocument('palette.3');   // same socket
pattern.data.source = '...';
pattern.syncChanges();
pattern.sendPresence('alice', { cursor: 42 });               // per document, ephemeral
```

**Who a connection is, and what it may open, is decided by an `Authority`**,
a server trait with two questions. `subject(token, client_id) -> Actor | None`
is asked once per connection, about the bearer token it presented on the
WebSocket upgrade; `may_open(subject, document) -> Read | Write | None` is
asked once per open, about the subject the first answer named. Three
implementations ship:

| Authority | When |
|---|---|
| `OpenToAll` | The default. Every connection is whoever it says it is and every document is open to it — a laptop, a demo, the test suite. It says so in the log |
| `PolicyAuthority` | The policy file *is* where access is decided; rules are written over document ids. A policy file knows no sessions, so connections under it are anonymous and self-named, and its rules are written for `Anonymous` or `Any` |
| `HttpAuthority` | An application owns sessions and membership. `POST <AUTHORITY_URL>/whoami` with `{"token": "…"}` is answered `{"subject": {"actor_type": "User", "id": "…"}}` or `401`; `POST <AUTHORITY_URL>/may-open` with `{"subject": <actor>, "document": "<id>"}` is answered `{"access": "read" \| "write" \| "none"}`. Both answers are cached ten seconds; a connection without a token is refused; an unreachable authority refuses everything |

The token travels on the upgrade, not in a message: `Authorization: Bearer …`
from the Rust client (`SyncClient::open_authenticated`), or a `token` query
parameter from the browser, whose `WebSocket` cannot set a header. The header
is the right instrument and wins when both are present; the query parameter
is the concession a browser needs. The `client_id` in `Connect` names the
connection for routing and is believed about nothing else: a connection
calling itself `alice` on `bob`'s token is `bob` to every `may-open`.

The rule this protects is that identity and access are **derived here and
authored there**. Two places that both know who may read a document disagree
eventually, and when they do the disagreement is a disclosure; a subject the
client named for itself is a subject anyone can name, and a decision about it
protects nothing. So SwirlDB never carries a copy of an application's sessions
or membership; it asks, briefly remembers, and enforces: a reader receives the
document and may send presence, but its `Push` is refused; a refused subject
never receives the history; an unauthenticated connection is told
`OpenDenied` on the document it asked for and closed. See
`native/swirldb-server/src/authority.rs`.

What stays single-document: server-to-server peer sync (`connect_to_peer`,
the peer manager, the LAN transport) speaks about the default document only.

## Text, and two people in it

A string set with `setPath` is one value: the next `setPath` replaces it, and
two people typing into the same string replace each other's work a keystroke
at a time. A **text** is different. It is an Automerge sequence of characters
under a path, and an edit to it is a *splice* — so many units removed at a
position, this string inserted there — which merges with everyone else's
splices instead of overwriting them. It still reads as a string.

```javascript
const pattern = await connection.openDocument('pattern.7');
pattern.setText('source', 'hue = t');           // create; replaces what was there
pattern.spliceText('source', 4, 0, '2 * ');     // insert at 4
pattern.spliceText('source', 0, 3, 'sat');      // delete 3 at 0, insert "sat"
pattern.syncChanges();
pattern.getPath('source');                       // 'sat = 2 * t'
pattern.data.source.$value;                      // the same string

pattern.observeText('source', ({ splices, local }) => {
  if (local) return;                             // this handle's own edits
  for (const { position, deleteCount, insert } of splices) {
    editor.dispatch({ changes: { from: position, to: position + deleteCount, insert } });
  }
});
```

`observeText` is the half that makes an editor possible. Where `observe`
hands a callback the new value, this hands it the edits, in order, each
against the text as the ones before left it, so an editor applies them to
what it is showing rather than replacing it and losing the caret. `local` is
true for the handle's own `spliceText` and `setText`, which the editor that
made them does not need to hear twice.

**Positions are counted in the units of whoever is looking.** In the browser
that is UTF-16 code units — what a JavaScript string index is and what an
editor offset is — so nothing is converted on the way in or out. The Rust
client and server count code points. The same edit is reported to each in
its own units; `"😀"` is two in one place and one in the other, and the
documents still agree. In Rust, `SwirlDB::new_with_text_encoding` chooses.

The Rust client has the same three: `set_text`, `splice_text`, and
`on_text_change`, a broadcast receiver of `TextChange { path, splices, local }`.
A `splice_text` pushes only the change it made, not the history, so typing
costs a keystroke each time rather than everything typed so far.

`setText` on a path that already holds a text replaces the whole text, which
throws away edits others are making to it at that moment; it is for creating
a text, and `spliceText` is for changing it. A text observer sees such a
replacement honestly, as a deletion of all of the old followed by an
insertion of all of the new.

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
