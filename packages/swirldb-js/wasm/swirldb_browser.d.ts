/* tslint:disable */
/* eslint-disable */
/**
 * Browser-specific WASM wrapper around core SwirlDB
 *
 * This is a thin binding layer that delegates to the core implementation
 */
export class SwirlDB {
  free(): void;
  [Symbol.dispose](): void;
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
   * Set any JavaScript value (scalar, array, or object) at the given path
   *
   * This method accepts any JavaScript value and recursively converts it to native Automerge types:
   * - Arrays become Automerge Lists (element-level CRDT)
   * - Objects become Automerge Maps (key-level CRDT)
   * - Scalars are stored as ScalarValue types
   *
   * Example:
   * ```javascript
   * db.setValue('messages', [
   *   {id: '1', from: 'alice', text: 'Hello'},
   *   {id: '2', from: 'bob', text: 'Hi!'}
   * ]);
   * ```
   */
  setValue(path: string, value: any): void;
  /**
   * Get any JavaScript value (scalar, array, or object) at the given path
   *
   * Returns the value as a native JavaScript type:
   * - Automerge Lists become JavaScript arrays
   * - Automerge Maps become JavaScript objects
   * - Scalars become JavaScript primitives
   *
   * Example:
   * ```javascript
   * const messages = db.getValue('messages');
   * // Returns: [{id: '1', from: 'alice', text: 'Hello'}, ...]
   * ```
   */
  getValue(path: string): any;
  /**
   * Get all root-level keys in the document
   *
   * Returns an array of strings representing all top-level keys
   *
   * Example:
   * ```javascript
   * const keys = db.getRootKeys();
   * console.log('Root keys:', keys); // ['chat', 'user', 'settings', ...]
   * ```
   */
  getRootKeys(): string[];
  /**
   * Save the current state to a Uint8Array
   */
  saveState(): Uint8Array;
  /**
   * Load state from a Uint8Array (REPLACES current state)
   */
  loadState(input: Uint8Array): void;
  /**
   * Apply CRDT changes (MERGES with current state)
   *
   * This is the correct way to sync CRDT state - it merges changes
   * rather than replacing the entire document like loadState() does.
   *
   * Example:
   * ```javascript
   * // Receive changes from server
   * const changes = [change1Bytes, change2Bytes];
   * db.applyChanges(changes);
   * ```
   */
  applyChanges(changes: Uint8Array[]): void;
  /**
   * Get all changes from the document as an array of Uint8Array
   *
   * This returns the complete change history that can be sent to other peers
   */
  getChanges(): Uint8Array[];
  /**
   * Get changes since the given heads (incremental sync)
   *
   * This returns only the changes that have been made since the given heads,
   * enabling efficient incremental synchronization.
   *
   * Example:
   * ```javascript
   * // Get only new changes since last sync
   * const newChanges = db.getChangesSince(lastSyncedHeads);
   * ```
   */
  getChangesSince(heads: Uint8Array[]): Uint8Array[];
  /**
   * Get the current heads (tips of the change graph) as a flat Uint8Array
   *
   * Returns a Uint8Array containing all heads concatenated (each head is 32 bytes)
   * These can be sent to the server for incremental sync
   */
  getHeads(): Uint8Array;
  /**
   * Get the current heads as an array of Uint8Array
   *
   * Returns an array where each element is a single head (32 bytes)
   * This format is compatible with getChangesSince()
   *
   * Example:
   * ```javascript
   * const heads = db.getHeadsArray();
   * const newChanges = db.getChangesSince(heads);
   * ```
   */
  getHeadsArray(): Uint8Array[];
  /**
   * Observe changes to a specific path
   *
   * The callback will be invoked with the new value whenever it changes
   */
  observe(path: string, callback: Function): void;
  /**
   * Enable auto-persistence (saves after every mutation)
   */
  enableAutoPersist(): void;
  /**
   * Load policy configuration from JSON string
   *
   * Example:
   * ```javascript
   * const policyJson = JSON.stringify({
   *   policies: {
   *     rules: [
   *       {
   *         priority: 10,
   *         actor: { type: "User" },
   *         action: "Write",
   *         path_pattern: "/user/{actor.id}/**",
   *         effect: "Allow"
   *       }
   *     ]
   *   }
   * });
   * db.loadPolicyConfig(policyJson);
   * ```
   */
  loadPolicyConfig(json_str: string): void;
  /**
   * Create a new SwirlDB instance with policy configuration
   *
   * Example:
   * ```javascript
   * const policyJson = JSON.stringify({
   *   policies: {
   *     rules: [...]
   *   }
   * });
   * const db = SwirlDB.withPolicy(policyJson);
   * ```
   */
  static withPolicy(json_str: string): SwirlDB;
  /**
   * Authenticate with a JWT token
   *
   * This decodes the JWT token and extracts the actor information from the claims.
   * The actor will then be used for all policy evaluations.
   *
   * **Important**: This only DECODES the token, it does NOT validate the signature!
   * The JWT should be validated server-side before being passed to the client.
   *
   * Example:
   * ```javascript
   * // After receiving a JWT from your auth server
   * const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...";
   * db.authenticateJWT(token);
   *
   * // Now all operations use the authenticated actor
   * db.setPath('/user/alice/profile.name', 'Alice'); // Uses actor from JWT
   * ```
   */
  authenticateJWT(token: string): void;
  /**
   * Get the current actor as a JavaScript object
   *
   * Example:
   * ```javascript
   * const actor = db.getActor();
   * console.log('Current actor:', actor.id, actor.type);
   * ```
   */
  getActor(): any;
  /**
   * Manually persist current state to storage
   */
  persist(): Promise<any>;
  /**
   * Manually trigger observer checks
   */
  checkObservers(): void;
  /**
   * Connect to sync server with WebSocket (managed internally)
   *
   * WebSocket connection is managed entirely in WASM. TypeScript doesn't need to handle
   * any protocol logic - just use the Proxy API for data access.
   *
   * Example:
   * ```javascript
   * db.connect('ws://localhost:3030/ws', 'alice', ['/**']);
   * // That's it! Now mutations automatically sync:
   * db.data.messages = [...messages, newMessage];
   * ```
   */
  connect(url: string, client_id: string, subscriptions: string[]): void;
  /**
   * Send local changes to server (called after mutations)
   */
  syncChanges(): void;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly __wbg_swirldb_free: (a: number, b: number) => void;
  readonly swirldb_new: () => number;
  readonly swirldb_withLocalStorage: (a: number, b: number) => any;
  readonly swirldb_withIndexedDB: (a: number, b: number) => any;
  readonly swirldb_setPath: (a: number, b: number, c: number, d: any) => [number, number];
  readonly swirldb_getPath: (a: number, b: number, c: number) => any;
  readonly swirldb_setValue: (a: number, b: number, c: number, d: any) => [number, number];
  readonly swirldb_getValue: (a: number, b: number, c: number) => any;
  readonly swirldb_getRootKeys: (a: number) => [number, number];
  readonly swirldb_saveState: (a: number) => any;
  readonly swirldb_loadState: (a: number, b: any) => [number, number];
  readonly swirldb_applyChanges: (a: number, b: number, c: number) => [number, number];
  readonly swirldb_getChanges: (a: number) => [number, number];
  readonly swirldb_getChangesSince: (a: number, b: number, c: number) => [number, number];
  readonly swirldb_getHeads: (a: number) => any;
  readonly swirldb_getHeadsArray: (a: number) => [number, number];
  readonly swirldb_observe: (a: number, b: number, c: number, d: any) => [number, number];
  readonly swirldb_enableAutoPersist: (a: number) => void;
  readonly swirldb_loadPolicyConfig: (a: number, b: number, c: number) => [number, number];
  readonly swirldb_withPolicy: (a: number, b: number) => [number, number, number];
  readonly swirldb_authenticateJWT: (a: number, b: number, c: number) => [number, number];
  readonly swirldb_getActor: (a: number) => any;
  readonly swirldb_persist: (a: number) => any;
  readonly swirldb_checkObservers: (a: number) => void;
  readonly swirldb_connect: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => [number, number];
  readonly swirldb_syncChanges: (a: number) => void;
  readonly wasm_bindgen__convert__closures_____invoke__h9a932ac99699e6ea: (a: number, b: number, c: any) => void;
  readonly wasm_bindgen__closure__destroy__h09128478747f473b: (a: number, b: number) => void;
  readonly wasm_bindgen__convert__closures_____invoke__h391af020e6f42ee5: (a: number, b: number, c: any) => void;
  readonly wasm_bindgen__closure__destroy__hd034537d4d4dd52d: (a: number, b: number) => void;
  readonly wasm_bindgen__convert__closures_____invoke__h7c0d50c30086be17: (a: number, b: number, c: any, d: any) => void;
  readonly __wbindgen_malloc: (a: number, b: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
  readonly __wbindgen_exn_store: (a: number) => void;
  readonly __externref_table_alloc: () => number;
  readonly __wbindgen_externrefs: WebAssembly.Table;
  readonly __wbindgen_free: (a: number, b: number, c: number) => void;
  readonly __externref_table_dealloc: (a: number) => void;
  readonly __externref_drop_slice: (a: number, b: number) => void;
  readonly __wbindgen_start: () => void;
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
