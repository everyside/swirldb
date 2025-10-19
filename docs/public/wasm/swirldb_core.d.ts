/* tslint:disable */
/* eslint-disable */
/**
 * Browser-specific WASM wrapper around core SwirlDB
 *
 * This is a thin binding layer that delegates to the core implementation
 */
export class SwirlDB {
  free(): void;
  /**
   * Create a new SwirlDB instance with default in-memory storage
   */
  constructor();
  /**
   * Create a new SwirlDB instance with LocalStorage persistence
   *
   * Example:
   * ```javascript
   * const db = await SwirlDB.withLocalStorage('my-app');
   * ```
   */
  static withLocalStorage(storage_key: string): Promise<any>;
  /**
   * Create a new SwirlDB instance with IndexedDB persistence
   *
   * Example:
   * ```javascript
   * const db = await SwirlDB.withIndexedDB('my-app');
   * ```
   */
  static withIndexedDB(db_name: string): Promise<any>;
  /**
   * Set a value at the given dot-separated path
   */
  setPath(path: string, value: any): void;
  /**
   * Get a value at the given dot-separated path
   */
  getPath(path: string): any;
  /**
   * Save the current state to a Uint8Array
   */
  saveState(): Uint8Array;
  /**
   * Load state from a Uint8Array
   */
  loadState(input: Uint8Array): void;
  /**
   * Observe changes to a specific path
   *
   * The callback will be invoked with the new value whenever it changes
   */
  observe(path: string, callback: Function): void;
  /**
   * Set storage hint for a path
   *
   * Example:
   * ```javascript
   * db.setStorageHint('session.temp', 'memory-only');
   * db.setStorageHint('user.profile', 'persisted');
   * db.setStorageHint('shared.doc', 'synced');
   * ```
   */
  setStorageHint(path: string, hint: string): void;
  /**
   * Enable auto-persistence (saves after every mutation)
   */
  enableAutoPersist(): void;
  /**
   * Manually persist current state to storage
   */
  persist(): Promise<any>;
  /**
   * Manually trigger observer checks
   */
  checkObservers(): void;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly __wbg_swirldb_free: (a: number, b: number) => void;
  readonly swirldb_new: () => number;
  readonly swirldb_withLocalStorage: (a: number, b: number) => number;
  readonly swirldb_withIndexedDB: (a: number, b: number) => number;
  readonly swirldb_setPath: (a: number, b: number, c: number, d: number, e: number) => void;
  readonly swirldb_getPath: (a: number, b: number, c: number) => number;
  readonly swirldb_saveState: (a: number) => number;
  readonly swirldb_loadState: (a: number, b: number, c: number) => void;
  readonly swirldb_observe: (a: number, b: number, c: number, d: number, e: number) => void;
  readonly swirldb_setStorageHint: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
  readonly swirldb_enableAutoPersist: (a: number) => void;
  readonly swirldb_persist: (a: number) => number;
  readonly swirldb_checkObservers: (a: number) => void;
  readonly __wbindgen_export_0: (a: number) => void;
  readonly __wbindgen_export_1: (a: number, b: number, c: number) => void;
  readonly __wbindgen_export_2: (a: number, b: number) => number;
  readonly __wbindgen_export_3: (a: number, b: number, c: number, d: number) => number;
  readonly __wbindgen_export_4: WebAssembly.Table;
  readonly __wbindgen_add_to_stack_pointer: (a: number) => number;
  readonly __wbindgen_export_5: (a: number, b: number, c: number) => void;
  readonly __wbindgen_export_6: (a: number, b: number, c: number) => void;
  readonly __wbindgen_export_7: (a: number, b: number, c: number, d: number) => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;
/**
* Instantiates the given `module`, which can either be bytes or
* a precompiled `WebAssembly.Module`.
*
* @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
*
* @returns {InitOutput}
*/
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
*
* @returns {Promise<InitOutput>}
*/
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
