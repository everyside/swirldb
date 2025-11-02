// src/wrapper.ts
import init from "./wasm/swirldb_browser.js";
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
        this.db.setValue(this.path.join("."), null);
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
   * Traditional path-based access (for compatibility)
   */
  setPath(path, value) {
    this.wasmDB.setPath(path, value);
  }
  getPath(path) {
    return this.wasmDB.getPath(path);
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
   *
   * @example
   * db.connect('ws://demo.swirldb.org:3030/ws', 'alice', ['/**']);
   */
  connect(url, clientId, subscriptions) {
    if (typeof this.wasmDB.connect === "function") {
      this.wasmDB.connect(url, clientId, subscriptions);
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
  SwirlDB,
  createPersistedStore,
  createStore
};
