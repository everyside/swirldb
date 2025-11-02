/**
 * TypeScript wrapper for SwirlDB with native property access via Proxies
 *
 * This provides the primary, first-class API for SwirlDB in the browser.
 * Instead of: db.setPath('user.name', 'Alice')
 * You can do: db.data.user.name = 'Alice'
 */

import type { SwirlDB as WasmSwirlDB } from './wasm/swirldb_browser';
import init from './wasm/swirldb_browser.js';

// WASM initialization state
let wasmInitialized = false;
let wasmInitPromise: Promise<void> | null = null;

async function ensureWasmInit(): Promise<void> {
  if (wasmInitialized) return;
  if (wasmInitPromise) return wasmInitPromise;

  wasmInitPromise = (async () => {
    await init();
    wasmInitialized = true;
  })();

  return wasmInitPromise;
}

/**
 * Proxy handler for nested object access
 */
class SwirlDBProxy implements ProxyHandler<object> {
  constructor(
    private db: WasmSwirlDB,
    private path: string[] = [],
    private swirlDB?: SwirlDB
  ) {}

  get(target: object, prop: string | symbol): any {
    if (typeof prop === 'symbol') {
      return undefined;
    }

    // Handle special methods
    if (prop === 'toJSON') {
      return () => this.db.getValue(this.path.join('.'));
    }

    if (prop === 'valueOf') {
      return () => this.db.getValue(this.path.join('.'));
    }

    if (prop === '$value') {
      return this.db.getValue(this.path.join('.'));
    }

    if (prop === '$observe') {
      return (callback: (value: any) => void) => {
        this.db.observe(this.path.join('.'), callback);
      };
    }

    if (prop === '$delete') {
      return () => {
        // Delete by setting to null (Automerge doesn't have true delete)
        this.db.setValue(this.path.join('.'), null);
      };
    }

    // Return a new proxy for nested access
    return new Proxy({}, new SwirlDBProxy(this.db, [...this.path, prop], this.swirlDB));
  }

  set(target: object, prop: string | symbol, value: any): boolean {
    if (typeof prop === 'symbol') {
      return false;
    }

    const fullPath = [...this.path, prop].join('.');
    this.db.setValue(fullPath, value);

    // Trigger auto-persist if configured
    if (this.swirlDB) {
      this.swirlDB.triggerAutoPersist();
    }

    return true;
  }
}

/**
 * Enhanced SwirlDB with TypeScript magic - the primary API for browser usage
 */
export class SwirlDB {
  private wasmDB: WasmSwirlDB;
  private proxy: any;
  private autoPersist = false;
  private persistDebounceMs = 500;
  private persistTimeout: number | null = null;

  constructor(wasmDB: WasmSwirlDB) {
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
  static async create(): Promise<SwirlDB> {
    await ensureWasmInit();
    const { SwirlDB: WasmSwirlDB } = await import('./wasm/swirldb_browser.js');
    const wasmDB = new WasmSwirlDB();
    return new SwirlDB(wasmDB);
  }

  /**
   * Create a SwirlDB instance with LocalStorage persistence (automatically initializes WASM)
   *
   * @example
   * const db = await SwirlDB.withLocalStorage('my-app');
   * db.data.user.name = 'Alice'; // Automatically persisted
   */
  static async withLocalStorage(storageKey: string): Promise<SwirlDB> {
    await ensureWasmInit();
    const { SwirlDB: WasmSwirlDB } = await import('./wasm/swirldb_browser.js');
    const wasmDB = await WasmSwirlDB.withLocalStorage(storageKey);
    return new SwirlDB(wasmDB);
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
  static async withIndexedDB(dbName: string): Promise<SwirlDB> {
    await ensureWasmInit();
    const { SwirlDB: WasmSwirlDB } = await import('./wasm/swirldb_browser.js');
    const wasmDB = await WasmSwirlDB.withIndexedDB(dbName);
    return new SwirlDB(wasmDB);
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
  static async withPolicy(policyJson: string): Promise<SwirlDB> {
    await ensureWasmInit();
    const { SwirlDB: WasmSwirlDB } = await import('./wasm/swirldb_browser.js');
    const wasmDB = WasmSwirlDB.withPolicy(policyJson);
    return new SwirlDB(wasmDB);
  }

  /**
   * Enable auto-persist: automatically save to storage after mutations
   *
   * @param debounceMs - Debounce period in milliseconds (default: 500ms)
   */
  enableAutoPersist(debounceMs = 500): void {
    this.autoPersist = true;
    this.persistDebounceMs = debounceMs;
  }

  /**
   * Disable auto-persist
   */
  disableAutoPersist(): void {
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
  triggerAutoPersist(): void {
    if (!this.autoPersist) return;

    if (this.persistTimeout !== null) {
      clearTimeout(this.persistTimeout);
    }

    this.persistTimeout = setTimeout(() => {
      this.persist();
    }, this.persistDebounceMs) as unknown as number;
  }

  /**
   * Manually persist to storage
   */
  async persist(): Promise<void> {
    if (typeof this.wasmDB.persist === 'function') {
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
  get data(): any {
    return this.proxy;
  }

  /**
   * Traditional path-based access (for compatibility)
   */
  setPath(path: string, value: any): void {
    this.wasmDB.setPath(path, value);
  }

  getPath(path: string): any {
    return this.wasmDB.getPath(path);
  }

  /**
   * Set any JavaScript value at a path
   */
  setValue(path: string, value: any): void {
    this.wasmDB.setValue(path, value);
  }

  /**
   * Get any JavaScript value at a path
   */
  getValue(path: string): any {
    return this.wasmDB.getValue(path);
  }

  /**
   * Get all root-level keys
   */
  getRootKeys(): string[] {
    return this.wasmDB.getRootKeys();
  }

  /**
   * Observe changes to a path
   */
  observe(path: string, callback: (value: any) => void): void {
    this.wasmDB.observe(path, callback);
  }

  /**
   * Manually trigger observer checks
   */
  checkObservers(): void {
    this.wasmDB.checkObservers();
  }

  /**
   * Save state to Uint8Array
   */
  saveState(): Uint8Array {
    return this.wasmDB.saveState();
  }

  /**
   * Load state from Uint8Array
   */
  loadState(bytes: Uint8Array): void {
    this.wasmDB.loadState(bytes);
  }

  /**
   * Get all changes from the document
   */
  getChanges(): Uint8Array[] {
    return this.wasmDB.getChanges();
  }

  /**
   * Get changes since the given heads (incremental sync)
   */
  getChangesSince(heads: Uint8Array[]): Uint8Array[] {
    return this.wasmDB.getChangesSince(heads);
  }

  /**
   * Get current heads for incremental sync (flat byte array)
   */
  getHeads(): Uint8Array {
    return this.wasmDB.getHeads();
  }

  /**
   * Get current heads as an array (compatible with getChangesSince)
   */
  getHeadsArray(): Uint8Array[] {
    return this.wasmDB.getHeadsArray();
  }

  /**
   * Apply changes (merges instead of replacing)
   */
  applyChanges(changes: Uint8Array[]): void {
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
  authenticateJWT(token: string): void {
    this.wasmDB.authenticateJWT(token);
  }

  /**
   * Get the current actor as a JavaScript object
   */
  getActor(): any {
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
  connect(url: string, clientId: string, subscriptions: string[]): void {
    if (typeof this.wasmDB.connect === 'function') {
      this.wasmDB.connect(url, clientId, subscriptions);
    } else {
      throw new Error('connect() not available in WASM layer');
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
  syncChanges(): void {
    if (typeof this.wasmDB.syncChanges === 'function') {
      this.wasmDB.syncChanges();
    } else {
      console.warn('syncChanges() not available in WASM layer');
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
  at(path: string): any {
    const segments = path.split('.');
    return new Proxy({}, new SwirlDBProxy(this.wasmDB, segments, this));
  }

  /**
   * Query the database
   */
  query(pattern: string): any[] {
    return [this.getPath(pattern)];
  }

  /**
   * Batch operations
   */
  batch(fn: (db: SwirlDB) => void): void {
    fn(this);
    this.checkObservers();
  }

  /**
   * Subscribe to changes with unsubscribe function
   */
  subscribe(path: string, callback: (value: any) => void): () => void {
    this.observe(path, callback);
    return () => {};
  }
}

/**
 * Utility: Create a reactive store from a SwirlDB instance
 *
 * @example
 * const store = createStore(db, 'app.state');
 * store.count = 0;
 * store.$observe((value) => console.log('State changed:', value));
 * store.count++; // Observer fires
 */
export function createStore(db: SwirlDB, basePath = ''): any {
  return db.at(basePath);
}

/**
 * Utility: Create a reactive object that syncs with localStorage
 */
export function createPersistedStore(db: SwirlDB, storageKey: string, basePath = ''): any {
  // Try to load from localStorage
  const saved = localStorage.getItem(storageKey);
  if (saved) {
    try {
      const bytes = Uint8Array.from(atob(saved), c => c.charCodeAt(0));
      db.loadState(bytes);
    } catch (e) {
      console.warn('Failed to load persisted state:', e);
    }
  }

  // Auto-save on changes
  const store = db.at(basePath);

  // Setup auto-save (debounced)
  let saveTimeout: number;
  const autoSave = () => {
    clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {
      const state = db.saveState();
      const base64 = btoa(String.fromCharCode(...state));
      localStorage.setItem(storageKey, base64);
    }, 500) as unknown as number; // Debounce 500ms
  };

  // Wrap setters to trigger auto-save
  return new Proxy(store, {
    set(target, prop, value) {
      const result = Reflect.set(target, prop, value);
      autoSave();
      return result;
    }
  });
}
