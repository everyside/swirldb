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
- **Lists that merge**: a list at a path is edited in place — insert, splice, delete — so two people adding to one both keep their items, and a delete is a delete rather than a tombstone
- **Observable**: an observer on a path hears every write to it, under it or above it, local or remote, and is told which
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

**Revocation while a document is open.** The authority is asked once, at
open, and a connection holds that answer as long as the document is open —
so taking somebody off a team never closed a document they already had, and
the cache only decided how soon a reopen would be refused. `POST
/admin/revoke` on the server, bearing `AUTHORITY_SECRET`, with `{"subject":
"<id>", "document": "<id>" | null}`, closes what the subject holds now: the
server drops the document (or every document) from the subject's
connections, tells each with an `OpenDenied` naming the reason `revoked`,
and has the authority forget what it cached about the subject, so a reopen
is asked afresh. In the browser the handle's `onDenied(reason)` fires and
its `access` reads `null`; the Rust client's `on_denied()` yields a `Denied`
and its connection ends. Everyone else on the document is untouched. See
`native/swirldb-server/README.md`, "POST /admin/revoke".

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

**Every write the Rust client makes pushes what it wrote since its last
push, and nothing more.** The client remembers the document's heads as they
stood when it last pushed, and a `set_path`, `set_text`, `splice_text`,
`insert_list_item`, `splice_list` or `delete_path` sends the changes since
those heads that the client's own actor made — not the history it was given
at open, and not a change it heard from the server in between, which is the
server's already. So typing costs a keystroke each time rather than
everything typed so far, and a scalar on a long document costs the scalar.
Writes made through `client.db()` directly — several scalars under one lock
— ride with the next write that pushes, or with `client.push()`, which sends
what is owed and nothing when nothing is.

`setText` on a path that already holds a text replaces the whole text, which
throws away edits others are making to it at that moment; it is for creating
a text, and `spliceText` is for changing it. A text observer sees such a
replacement honestly, as a deletion of all of the old followed by an
insertion of all of the new.

## Lists, and a delete

An array assigned with `setValue` is one value, replaced whole by the next
assignment; two people each adding a stop to the same palette that way keep
one stop between them. A **list** is edited in place. `insertListItem` puts
one item at an index and `spliceList` removes a run and inserts another, and
both merge: two people inserting at the same index at the same moment both
keep their items, in an order every copy agrees on. An item may be anything —
a color, a record, another list — and a record becomes a map whose keys
merge in turn, so two people changing two fields of one stop both land. The
list reads back as an array, and an item as `<path>.<index>`.

```javascript
const palette = await connection.openDocument('palette.3');
palette.insertListItem('stops', 0, { color: '#ff0000' });   // creates the list
palette.insertListItem('stops', 1, { color: '#0000ff' });
palette.spliceList('stops', 1, 0, [{ color: '#00ff00' }]);  // insert between
palette.spliceList('stops', 2, 1, []);                       // remove one
palette.setPath('stops.0.color', '#ff8800');                 // one key of one item
palette.deletePath('stops.1');                               // and it is gone
palette.syncChanges();
palette.getValue('stops');                                   // [{ color: '#ff8800' }]
```

**A reorder is a removal and an insertion**, never a replacement of the
list, which is what keeps it safe: an item somebody else inserts beside the
moving one at the same moment stays where they put it. A splice removes the
items that are there now; what arrives concurrently is not among them.

`deletePath` removes a key from its map or an item from its list, with
everything under it, and is the same call as `db.data.<path>.$delete()`.
It is a delete on the wire too — the other side's observer on the path is
handed `null` (`None` in Rust), and an observer above it fires because
something under it changed — so a removed stop needs no tombstone for
readers to skip. Deleting a path that holds nothing does nothing, sends
nothing, and is not an error. A delete that meets a concurrent write
resolves as Automerge resolves it: a value put in the deleted one's place
survives, because the delete removed only what it had seen; an edit inside
a deleted map or list item goes with it. Both copies agree either way.

The Rust client has the same three, `insert_list_item`, `splice_list` and
`delete_path`, each pushing only the edit it made; the core has them
underneath, and its `set_path`, `set_text` and `set_value` accept an index
as the last segment of a path, so `stops.0.color` and `stops.0` are
writable like any other path. Changed paths name a list item by index with
a dot — `stops.2`, the same notation `getPath` takes and a subscription
`stops.**` covers.

## Observers, and who made the change

An observer on a path hears every write to it, under it, or above it. A
`setPath` at `user.name` fires an observer on `user.name` and one on `user`;
a `setValue` replacing `user` fires one on `user.name`. Segments are
compared whole, so `user` and `username` are strangers. It is handed the
value at its path — a scalar, an object for a map, an array for a list,
`null` for nothing — and beside it a change saying what happened.

```javascript
palette.observe('stops', (stops, { path, changedPaths, local }) => {
  if (local) return;      // this handle wrote it and already knows
  render(stops);          // ['#ff0000', ...] — the list, as an array
});
palette.data.stops.$observe((stops, change) => { /* the same */ });
```

**Local writes and remote ones fire an observer alike, and `local` says
which.** This handle's own `setPath`, `setValue`, `setText`, `spliceText`,
`insertListItem`, `spliceList` and `deletePath` fire its observers with
`local` true, the way `observeText` has always marked the handle's own
splices; a write that arrives from the server, or through `applyChanges`,
fires them with `local` false. `changedPaths` names what was written —
`stops.2` for a list item, `user.name` for a key, the text's own path for a
splice — and `['**']` when the whole document arrived. A panel that reads
back what it wrote can pass its own by; a panel that renders the document
need not know who wrote it.

Before this, a map observer fired only from a socket frame, because the glue
compared the scalar at the observed path and a map has none. Every write now
reports the paths it touched, and a remote change reports the paths its
patches touched, so nothing depends on a scalar being there. In Rust,
`ChangeNotification` carries the same `local`, and `Applied` — what
`apply_changes` returns — carries `changed_paths` for a layer that keeps
observers of its own.

The Rust client's `on_change` hears the same. It yields a `Change` — the
`changed_paths` and `local` of the browser's change, without the observer's
`path`, since the channel is for the whole document — for every write this
client makes, through its own methods or through `db()`, and for every
`Broadcast` the server sends, with the paths the server named. Before, the
channel carried only broadcasts, so a server holding a document could not
hear its own writes the way a browser tab now can, and could not tell whose
a change was. A server that projects every change it hears passes its own
by; the projection it would make of its own write is the one it just made,
and a stamp written after a projection would otherwise wake the next.

```rust
let mut changes = client.on_change();
while let Ok(change) = changes.recv().await {
    if change.local { continue; }   // this client wrote it and already knows
    project(&client).await;         // change.changed_paths says what moved
}
```

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
