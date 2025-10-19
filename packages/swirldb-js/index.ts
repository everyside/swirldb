/**
 * TypeScript wrapper for SwirlDB with native property access via Proxies
 *
 * Instead of: db.setPath('user.name', 'Alice')
 * You can do: db.user.name = 'Alice'
 */

import type { SwirlDB as WasmSwirlDB } from '../browser-wasm/swirldb_core';

type ObserverCallback = (value: any) => void;

// WASM initialization state
let wasmInitialized = false;
let wasmInitPromise: Promise<void> | null = null;

async function ensureWasmInit(): Promise<void> {
  if (wasmInitialized) return;
  if (wasmInitPromise) return wasmInitPromise;

  wasmInitPromise = (async () => {
    const { default: init } = await import('./wasm/swirldb_core.js');
    await init();
    wasmInitialized = true;
  })();

  return wasmInitPromise;
}

/**
 * Proxy handler for nested object access
 */
class SwirlDBProxy {
  constructor(
    private db: WasmSwirlDB,
    private path: string[] = [],
    private swirlDB?: SwirlDB
  ) {}

  get(target: any, prop: string | symbol): any {
    if (typeof prop === 'symbol') {
      return undefined;
    }

    // Handle special methods
    if (prop === 'toJSON') {
      return () => this.db.getPath(this.path.join('.'));
    }

    if (prop === 'valueOf') {
      return () => this.db.getPath(this.path.join('.'));
    }

    if (prop === '$value') {
      return this.db.getPath(this.path.join('.'));
    }

    if (prop === '$observe') {
      return (callback: ObserverCallback) => {
        this.db.observe(this.path.join('.'), callback);
      };
    }

    if (prop === '$delete') {
      return () => {
        // Delete by setting to null (Automerge doesn't have true delete)
        this.db.setPath(this.path.join('.'), null);
      };
    }

    // Return a new proxy for nested access
    return new Proxy(
      {},
      new SwirlDBProxy(this.db, [...this.path, prop], this.swirlDB)
    );
  }

  set(target: any, prop: string | symbol, value: any): boolean {
    if (typeof prop === 'symbol') {
      return false;
    }

    const fullPath = [...this.path, prop].join('.');
    this.db.setPath(fullPath, value);

    // Trigger auto-persist if configured
    if (this.swirlDB) {
      (this.swirlDB as any).triggerAutoPersist();
    }

    return true;
  }
}

/**
 * Enhanced SwirlDB with TypeScript magic
 */
export class SwirlDB {
  private wasmDB: WasmSwirlDB;
  private proxy: any;
  private autoPersist: boolean = false;
  private persistDebounceMs: number = 500;
  private persistTimeout: any = null;

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
    const { SwirlDB: WasmSwirlDB } = await import('./wasm/swirldb_core.js');
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
    const { SwirlDB: WasmSwirlDB } = await import('./wasm/swirldb_core.js');
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
    const { SwirlDB: WasmSwirlDB } = await import('./wasm/swirldb_core.js');
    const wasmDB = await WasmSwirlDB.withIndexedDB(dbName);
    return new SwirlDB(wasmDB);
  }

  /**
   * Enable auto-persist: automatically save to storage after mutations
   *
   * @param debounceMs - Debounce period in milliseconds (default: 500ms)
   */
  enableAutoPersist(debounceMs: number = 500): void {
    this.autoPersist = true;
    this.persistDebounceMs = debounceMs;
  }

  /**
   * Disable auto-persist
   */
  disableAutoPersist(): void {
    this.autoPersist = false;
    if (this.persistTimeout) {
      clearTimeout(this.persistTimeout);
      this.persistTimeout = null;
    }
  }

  /**
   * Trigger a debounced persist if auto-persist is enabled
   */
  private triggerAutoPersist(): void {
    if (!this.autoPersist) return;

    if (this.persistTimeout) {
      clearTimeout(this.persistTimeout);
    }

    this.persistTimeout = setTimeout(() => {
      this.persist();
    }, this.persistDebounceMs);
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
   * Set storage hint for a path
   *
   * @example
   * db.setStorageHint('session.temp', 'memory-only');
   * db.setStorageHint('user.profile', 'persisted');
   * db.setStorageHint('shared.doc', 'synced');
   */
  setStorageHint(path: string, hint: 'memory-only' | 'persisted' | 'synced'): void {
    if (typeof this.wasmDB.setStorageHint === 'function') {
      this.wasmDB.setStorageHint(path, hint);
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
   * Observe changes to a path
   */
  observe(path: string, callback: ObserverCallback): void {
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
   * Get a proxy at a specific path
   *
   * @example
   * const user = db.at('user');
   * user.name = 'Alice';
   * console.log(user.name.$value);
   */
  at(path: string): any {
    const segments = path.split('.');
    return new Proxy(
      {},
      new SwirlDBProxy(this.wasmDB, segments, this)
    );
  }

  /**
   * Query the database (future: more advanced queries)
   */
  query(pattern: string): any[] {
    // TODO: Implement pattern matching
    // For now, just return the value at the path
    return [this.getPath(pattern)];
  }

  /**
   * Batch operations
   */
  batch(fn: (db: this) => void): void {
    fn(this);
    this.checkObservers();
  }

  /**
   * Subscribe to changes with unsubscribe function
   */
  subscribe(path: string, callback: ObserverCallback): () => void {
    this.observe(path, callback);
    // TODO: Implement actual unsubscribe
    // For now, return a no-op
    return () => {
      console.warn('Unsubscribe not yet implemented');
    };
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
export function createStore(db: SwirlDB, basePath: string = ''): any {
  return db.at(basePath);
}

/**
 * Utility: Create a reactive object that syncs with localStorage
 */
export function createPersistedStore(
  db: SwirlDB,
  storageKey: string,
  basePath: string = ''
): any {
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
  let saveTimeout: any;
  const autoSave = () => {
    clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {
      const state = db.saveState();
      const base64 = btoa(String.fromCharCode(...state));
      localStorage.setItem(storageKey, base64);
    }, 500); // Debounce 500ms
  };

  // Wrap setters to trigger auto-save
  return new Proxy(store, {
    set(target: any, prop: string | symbol, value: any) {
      const result = Reflect.set(target, prop, value);
      autoSave();
      return result;
    }
  });
}
