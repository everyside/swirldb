// src/wrapper.ts
import init from "./wasm/swirldb_browser.js";
var DEFAULT_DOCUMENT = "default";
var wasmInitialized = false;
var wasmInitPromise = null;
async function ensureWasmInit() {
  if (wasmInitialized) return;
  if (wasmInitPromise) return wasmInitPromise;
  wasmInitPromise = (async () => {
    await init();
    wasmInitialized = true;
  })();
  return wasmInitPromise;
}
var SwirlDBProxy = class _SwirlDBProxy {
  constructor(db, path = [], swirlDB) {
    this.db = db;
    this.path = path;
    this.swirlDB = swirlDB;
  }
  get(target, prop) {
    if (typeof prop === "symbol") {
      return void 0;
    }
    if (prop === "toJSON") {
      return () => this.db.getValue(this.path.join("."));
    }
    if (prop === "valueOf") {
      return () => this.db.getValue(this.path.join("."));
    }
    if (prop === "$value") {
      return this.db.getValue(this.path.join("."));
    }
    if (prop === "$observe") {
      return (callback) => {
        this.db.observe(this.path.join("."), callback);
      };
    }
    if (prop === "$delete") {
      return () => {
        this.db.deletePath(this.path.join("."));
        if (this.swirlDB) {
          this.swirlDB.triggerAutoPersist();
        }
      };
    }
    return new Proxy({}, new _SwirlDBProxy(this.db, [...this.path, prop], this.swirlDB));
  }
  set(target, prop, value) {
    if (typeof prop === "symbol") {
      return false;
    }
    const fullPath = [...this.path, prop].join(".");
    this.db.setValue(fullPath, value);
    if (this.swirlDB) {
      this.swirlDB.triggerAutoPersist();
    }
    return true;
  }
};
var SwirlDB = class _SwirlDB {
  constructor(wasmDB) {
    this.autoPersist = false;
    this.persistDebounceMs = 500;
    this.persistTimeout = null;
    this.wasmDB = wasmDB;
    this.proxy = new Proxy({}, new SwirlDBProxy(this.wasmDB, [], this));
  }
  /**
   * Create a new in-memory SwirlDB instance (automatically initializes WASM)
   *
   * @example
   * const db = await SwirlDB.create();
   * db.data.user.name = 'Alice';
   */
  static async create() {
    await ensureWasmInit();
    const { SwirlDB: WasmSwirlDB } = await import("./wasm/swirldb_browser.js");
    const wasmDB = new WasmSwirlDB();
    return new _SwirlDB(wasmDB);
  }
  /**
   * Create a SwirlDB instance with LocalStorage persistence (automatically initializes WASM)
   *
   * @example
   * const db = await SwirlDB.withLocalStorage('my-app');
   * db.data.user.name = 'Alice'; // Automatically persisted
   */
  static async withLocalStorage(storageKey) {
    await ensureWasmInit();
    const { SwirlDB: WasmSwirlDB } = await import("./wasm/swirldb_browser.js");
    const wasmDB = await WasmSwirlDB.withLocalStorage(storageKey);
    return new _SwirlDB(wasmDB);
  }
  /**
   * Create a SwirlDB instance with IndexedDB persistence (automatically initializes WASM)
   *
   * IndexedDB supports much larger storage (~50MB-1GB+) compared to localStorage (~5-10MB)
   *
   * @example
   * const db = await SwirlDB.withIndexedDB('my-app');
   * db.data.user.name = 'Alice';
   */
  static async withIndexedDB(dbName) {
    await ensureWasmInit();
    const { SwirlDB: WasmSwirlDB } = await import("./wasm/swirldb_browser.js");
    const wasmDB = await WasmSwirlDB.withIndexedDB(dbName);
    return new _SwirlDB(wasmDB);
  }
  /**
   * Create a SwirlDB instance with policy configuration
   *
   * @example
   * const policyJson = JSON.stringify({
   *   policies: {
   *     rules: [...]
   *   }
   * });
   * const db = await SwirlDB.withPolicy(policyJson);
   */
  static async withPolicy(policyJson) {
    await ensureWasmInit();
    const { SwirlDB: WasmSwirlDB } = await import("./wasm/swirldb_browser.js");
    const wasmDB = WasmSwirlDB.withPolicy(policyJson);
    return new _SwirlDB(wasmDB);
  }
  /**
   * Enable auto-persist: automatically save to storage after mutations
   *
   * @param debounceMs - Debounce period in milliseconds (default: 500ms)
   */
  enableAutoPersist(debounceMs = 500) {
    this.autoPersist = true;
    this.persistDebounceMs = debounceMs;
  }
  /**
   * Disable auto-persist
   */
  disableAutoPersist() {
    this.autoPersist = false;
    if (this.persistTimeout !== null) {
      clearTimeout(this.persistTimeout);
      this.persistTimeout = null;
    }
  }
  /**
   * Trigger a debounced persist if auto-persist is enabled
   * @internal
   */
  triggerAutoPersist() {
    if (!this.autoPersist) return;
    if (this.persistTimeout !== null) {
      clearTimeout(this.persistTimeout);
    }
    this.persistTimeout = setTimeout(() => {
      this.persist();
    }, this.persistDebounceMs);
  }
  /**
   * Manually persist to storage
   */
  async persist() {
    if (typeof this.wasmDB.persist === "function") {
      await this.wasmDB.persist();
    }
  }
  /**
   * Access the database with native property syntax
   *
   * @example
   * db.data.user.name = 'Alice';
   * console.log(db.data.user.name.$value); // 'Alice'
   */
  get data() {
    return this.proxy;
  }
  /**
   * The document this handle is on: `'default'` unless it came from
   * `SwirlDBConnection.openDocument`.
   */
  get document() {
    return this.wasmDB.document;
  }
  /**
   * What the server granted on this document — `'read'`, `'write'` — or
   * `null` before the server has answered or when not connected. A handle
   * with `'read'` receives every change and may send presence, but its own
   * changes are refused by the server.
   */
  get access() {
    return this.wasmDB.access ?? null;
  }
  /**
   * Stop receiving this document. On a connection with other documents open
   * the socket stays up for them.
   */
  close() {
    this.wasmDB.close();
  }
  /**
   * Send an ephemeral message on this document: not stored, not merged,
   * routed to whoever has the document open and subscribes to the path.
   */
  sendEphemeral(path, data) {
    this.wasmDB.sendEphemeral(path, data);
  }
  /**
   * Receive ephemeral messages on this document whose path matches the
   * pattern. Returns a handler id for `offEphemeral`.
   */
  onEphemeral(pattern, callback) {
    return this.wasmDB.onEphemeral(pattern, callback);
  }
  offEphemeral(handlerId) {
    this.wasmDB.offEphemeral(handlerId);
  }
  /**
   * Presence: who is in this document and where. Rides the ephemeral channel
   * under `presence.<clientId>`, so it is per document, never stored, and
   * gone when the sender goes. `state` is any JSON — a cursor, a selection,
   * a name and a color.
   */
  sendPresence(clientId, state) {
    this.sendEphemeral(`presence.${clientId}`, new TextEncoder().encode(JSON.stringify(state)));
  }
  /**
   * Hear presence from the others in this document. The callback gets the
   * sender's client id and the state they sent.
   */
  onPresence(callback) {
    return this.onEphemeral("presence.*", (path, data) => {
      const clientId = path.slice("presence.".length);
      try {
        callback(clientId, JSON.parse(new TextDecoder().decode(data)));
      } catch (error) {
        console.warn("Presence from", clientId, "was not JSON:", error);
      }
    });
  }
  /**
   * Traditional path-based access (for compatibility)
   */
  setPath(path, value) {
    this.wasmDB.setPath(path, value);
  }
  getPath(path) {
    return this.wasmDB.getPath(path);
  }
  /**
   * Put a text at a path, replacing whatever was there.
   *
   * A text merges: two people splicing into one text both keep their
   * characters, where two people assigning one string each replace the
   * other's. It reads back as a string through `getPath`, `getValue` and
   * `db.data.<path>.$value`. Replacing an existing text discards edits others
   * are making to it, so this creates; `spliceText` edits.
   *
   * @example
   * db.setText('source', 'hue = t');
   */
  setText(path, text) {
    this.wasmDB.setText(path, text);
  }
  /**
   * Edit the text at a path in place: remove `deleteCount` UTF-16 code units
   * at `position`, then insert `insert` there. Throws when the path holds no
   * text. Call `syncChanges` to push it.
   *
   * @example
   * db.spliceText('source', 4, 0, 'black ');   // insert
   * db.spliceText('source', 0, 3, '');         // delete
   * db.syncChanges();
   */
  spliceText(path, position, deleteCount, insert) {
    this.wasmDB.spliceText(path, position, deleteCount, insert);
  }
  /**
   * The length of the text at a path in UTF-16 code units, or `null` when the
   * path holds no text.
   */
  textLength(path) {
    const length = this.wasmDB.textLength(path);
    return length === void 0 ? null : length;
  }
  /**
   * Observe edits to the text at a path. Where `observe` hands a callback the
   * new value, this hands it the edits, with positions, so an editor can
   * apply them to what it is showing rather than replace it.
   *
   * @example
   * db.observeText('source', ({ splices, local }) => {
   *   if (local) return;
   *   for (const { position, deleteCount, insert } of splices) {
   *     view.dispatch({ changes: { from: position, to: position + deleteCount, insert } });
   *   }
   * });
   */
  observeText(path, callback) {
    this.wasmDB.observeText(path, callback);
  }
  /**
   * Insert a value into the list at a path so that it sits at `index`;
   * `index` equal to the length appends. The list is created when the path
   * holds nothing, and `value` may be anything — an object becomes a map
   * inside the list. Two people inserting at once both keep their items,
   * where two assignments of whole arrays would each replace the other's.
   * The list reads back as an array through `getValue` and
   * `db.data.<path>.$value`, and an item as `<path>.<index>`. Call
   * `syncChanges` to push it.
   *
   * @example
   * db.insertListItem('stops', 1, { color: '#00ff00' });
   * db.syncChanges();
   */
  insertListItem(path, index, value) {
    this.wasmDB.insertListItem(path, index, value);
    this.triggerAutoPersist();
  }
  /**
   * Edit the list at a path in place: remove `deleteCount` items at `index`,
   * then insert `values` there. A reorder is a removal and an insertion; a
   * splice never replaces the list, so an item somebody else inserts beside
   * it at the same moment stays. Throws when the range is outside the list.
   *
   * @example
   * db.spliceList('stops', 2, 1, []);                  // remove one
   * db.spliceList('stops', 0, 0, [{ color: '#000' }]);  // insert at the front
   * db.syncChanges();
   */
  spliceList(path, index, deleteCount, values) {
    this.wasmDB.spliceList(path, index, deleteCount, values);
    this.triggerAutoPersist();
  }
  /**
   * Remove whatever is at a path — a key from its map, an item from its list
   * by index — and everything under it. A path that holds nothing is left
   * alone. An observer on the path is handed `null`; an observer above it
   * fires because something under it changed. `db.data.<path>.$delete()` is
   * the same call.
   *
   * @example
   * db.deletePath('stops.2');
   * db.syncChanges();
   */
  deletePath(path) {
    this.wasmDB.deletePath(path);
    this.triggerAutoPersist();
  }
  /**
   * Set any JavaScript value at a path
   */
  setValue(path, value) {
    this.wasmDB.setValue(path, value);
  }
  /**
   * Get any JavaScript value at a path
   */
  getValue(path) {
    return this.wasmDB.getValue(path);
  }
  /**
   * Get all root-level keys
   */
  getRootKeys() {
    return this.wasmDB.getRootKeys();
  }
  /**
   * Observe changes to a path
   */
  observe(path, callback) {
    this.wasmDB.observe(path, callback);
  }
  /**
   * Manually trigger observer checks
   */
  checkObservers() {
    this.wasmDB.checkObservers();
  }
  /**
   * Save state to Uint8Array
   */
  saveState() {
    return this.wasmDB.saveState();
  }
  /**
   * Load state from Uint8Array
   */
  loadState(bytes) {
    this.wasmDB.loadState(bytes);
  }
  /**
   * Get all changes from the document
   */
  getChanges() {
    return this.wasmDB.getChanges();
  }
  /**
   * Get changes since the given heads (incremental sync)
   */
  getChangesSince(heads) {
    return this.wasmDB.getChangesSince(heads);
  }
  /**
   * Get current heads for incremental sync (flat byte array)
   */
  getHeads() {
    return this.wasmDB.getHeads();
  }
  /**
   * Get current heads as an array (compatible with getChangesSince)
   */
  getHeadsArray() {
    return this.wasmDB.getHeadsArray();
  }
  /**
   * Apply changes (merges instead of replacing)
   */
  applyChanges(changes) {
    this.wasmDB.applyChanges(changes);
  }
  /**
   * Authenticate with a JWT token
   *
   * This decodes the JWT token and extracts the actor information from the claims.
   * The actor will then be used for all policy evaluations.
   *
   * **Important**: This only DECODES the token, it does NOT validate the signature!
   * The JWT should be validated server-side before being passed to the client.
   */
  authenticateJWT(token) {
    this.wasmDB.authenticateJWT(token);
  }
  /**
   * Get the current actor as a JavaScript object
   */
  getActor() {
    return this.wasmDB.getActor();
  }
  /**
   * Connect to sync server via WebSocket (managed internally by WASM)
   *
   * @param url - WebSocket URL (e.g., 'ws://demo.swirldb.org:3030/ws')
   * @param clientId - Unique client identifier
   * @param subscriptions - Array of subscription patterns (e.g., ['/**'])
   * @param token - Bearer token the server's authority verifies to learn who
   *   this connection is. Sent as a `token` query parameter, since a browser
   *   WebSocket cannot carry a header. A server with an authority refuses a
   *   connection without one.
   *
   * @example
   * db.connect('ws://demo.swirldb.org:3030/ws', 'alice', ['/**'], sessionToken);
   */
  connect(url, clientId, subscriptions, token) {
    if (typeof this.wasmDB.connect === "function") {
      this.wasmDB.connect(url, clientId, subscriptions, token);
    } else {
      throw new Error("connect() not available in WASM layer");
    }
  }
  /**
   * Sync local changes to server (WebSocket only)
   *
   * Sends incremental changes since last sync to the server via WebSocket.
   * This is automatically called by WASM when using the internal WebSocket connection.
   *
   * @example
   * db.data.message = 'Hello';
   * db.syncChanges(); // Push to server
   */
  syncChanges() {
    if (typeof this.wasmDB.syncChanges === "function") {
      this.wasmDB.syncChanges();
    } else {
      console.warn("syncChanges() not available in WASM layer");
    }
  }
  /**
   * Get a proxy at a specific path
   *
   * @example
   * const user = db.at('user');
   * user.name = 'Alice';
   * console.log(user.name.$value);
   */
  at(path) {
    const segments = path.split(".");
    return new Proxy({}, new SwirlDBProxy(this.wasmDB, segments, this));
  }
  /**
   * Query the database
   */
  query(pattern) {
    return [this.getPath(pattern)];
  }
  /**
   * Batch operations
   */
  batch(fn) {
    fn(this);
    this.checkObservers();
  }
  /**
   * Subscribe to changes with unsubscribe function
   */
  subscribe(path, callback) {
    this.observe(path, callback);
    return () => {
    };
  }
};
var SwirlDBConnection = class _SwirlDBConnection {
  constructor(connection) {
    this.connection = connection;
  }
  /**
   * Open a socket to `url` as `clientId`. Nothing is sent until the first
   * document. `token` is the bearer token the server's authority verifies to
   * learn who this connection is — the subject every `may-open` is asked
   * about — sent as a `token` query parameter because a browser WebSocket
   * cannot carry a header. A server with an authority refuses a connection
   * without one; `clientId` is only a name.
   */
  static async open(url, clientId, token) {
    await ensureWasmInit();
    const { Connection: WasmConnection } = await import("./wasm/swirldb_browser.js");
    return new _SwirlDBConnection(new WasmConnection(url, clientId, token));
  }
  /**
   * Open a document. Resolves once the server has sent its history; rejects
   * with the server's reason when the authority refuses. `subscriptions`
   * defaults to the whole document.
   */
  async openDocument(document, subscriptions) {
    const handle = await this.connection.openDocument(document, subscriptions);
    return new SwirlDB(handle);
  }
  /** Stop receiving one document; the socket stays open for the others. */
  closeDocument(document) {
    this.connection.closeDocument(document);
  }
  /** The documents currently open on this connection. */
  get openDocuments() {
    return this.connection.openDocuments();
  }
  get clientId() {
    return this.connection.clientId();
  }
  /** Close the socket and every document on it. */
  close() {
    this.connection.close();
  }
};
function createStore(db, basePath = "") {
  return db.at(basePath);
}
function createPersistedStore(db, storageKey, basePath = "") {
  const saved = localStorage.getItem(storageKey);
  if (saved) {
    try {
      const bytes = Uint8Array.from(atob(saved), (c) => c.charCodeAt(0));
      db.loadState(bytes);
    } catch (e) {
      console.warn("Failed to load persisted state:", e);
    }
  }
  const store = db.at(basePath);
  let saveTimeout;
  const autoSave = () => {
    clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {
      const state = db.saveState();
      const base64 = btoa(String.fromCharCode(...state));
      localStorage.setItem(storageKey, base64);
    }, 500);
  };
  return new Proxy(store, {
    set(target, prop, value) {
      const result = Reflect.set(target, prop, value);
      autoSave();
      return result;
    }
  });
}
export {
  DEFAULT_DOCUMENT,
  SwirlDB,
  SwirlDBConnection,
  createPersistedStore,
  createStore
};
