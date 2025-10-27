// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

use wasm_bindgen::prelude::*;
use wasm_bindgen::closure::Closure;
use wasm_bindgen_futures::future_to_promise;
use js_sys::{Function, Uint8Array, Promise};
use automerge::ScalarValue;
use swirldb_core::core::SwirlDB as CoreSwirlDB;
use swirldb_core::policy::PolicyEngine;
use swirldb_core::protocol::Message;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use serde_wasm_bindgen::from_value;
use web_sys::{WebSocket, MessageEvent, CloseEvent, ErrorEvent, BinaryType};

mod storage;
use storage::{LocalDocumentStorage, IndexedDBAdapter};

thread_local! {
    static OBSERVERS: RefCell<Vec<(usize, String, Function, Option<ScalarValue>)>> = RefCell::new(Vec::new());
    static NEXT_ID: RefCell<usize> = RefCell::new(0);
    static CONNECTIONS: RefCell<std::collections::HashMap<usize, ConnectionState>> = RefCell::new(std::collections::HashMap::new());
}

/// Connection state for protocol handling
struct ConnectionState {
    client_id: String,
    subscriptions: Vec<String>,
    last_synced_heads: Vec<Vec<u8>>,
    websocket: Option<WebSocket>,
    #[allow(dead_code)]
    onopen_closure: Option<Closure<dyn FnMut(web_sys::Event)>>,
    #[allow(dead_code)]
    onmessage_closure: Option<Closure<dyn FnMut(MessageEvent)>>,
    #[allow(dead_code)]
    onclose_closure: Option<Closure<dyn FnMut(CloseEvent)>>,
    #[allow(dead_code)]
    onerror_closure: Option<Closure<dyn FnMut(ErrorEvent)>>,
}

impl ConnectionState {
    fn new(client_id: String, subscriptions: Vec<String>) -> Self {
        Self {
            client_id,
            subscriptions,
            last_synced_heads: Vec::new(),
            websocket: None,
            onopen_closure: None,
            onmessage_closure: None,
            onclose_closure: None,
            onerror_closure: None,
        }
    }
}

/// Fire observers for paths affected by remote changes
/// Only fires observers whose paths match the affected paths
fn fire_observers_for_paths(db_id: usize, core: &swirldb_core::SwirlDB, affected_paths: &[String]) {
    OBSERVERS.with(|observers| {
        let mut observers_ref = observers.borrow_mut();

        for (id, path, callback, last_value) in observers_ref.iter_mut() {
            if *id != db_id {
                continue;
            }

            // Check if this observer's path is affected by the changes
            let is_affected = affected_paths.iter().any(|affected| {
                // Match if observer path is a prefix of affected path, or vice versa
                // e.g., observer="messages" matches affected="messages.msg_123"
                // Also handles glob patterns like "/**"
                affected.starts_with(path.as_str()) ||
                path.starts_with(affected.as_str()) ||
                affected == "/**" ||
                path == "/**"
            });

            if is_affected {
                // Get current value and fire observer
                let current = core.get_path(path);
                let js_value = match &current {
                    Some(v) => scalar_to_js(v),
                    None => JsValue::NULL,
                };

                let _ = callback.call1(&JsValue::NULL, &js_value);
                *last_value = current;
            }
        }
    });
}

/// Fire all observers for a database (used for Sync messages without affected_paths)
fn fire_all_observers(db_id: usize, core: &swirldb_core::SwirlDB) {
    OBSERVERS.with(|observers| {
        let mut observers_ref = observers.borrow_mut();

        for (id, path, callback, last_value) in observers_ref.iter_mut() {
            if *id != db_id {
                continue;
            }

            // Get current value and fire observer
            let current = core.get_path(path);
            let js_value = match &current {
                Some(v) => scalar_to_js(v),
                None => JsValue::NULL,
            };

            let _ = callback.call1(&JsValue::NULL, &js_value);
            *last_value = current;
        }
    });
}

/// Browser-specific WASM wrapper around core SwirlDB
///
/// This is a thin binding layer that delegates to the core implementation
#[wasm_bindgen]
pub struct SwirlDB {
    core: Rc<CoreSwirlDB>,
    id: usize,
}

#[wasm_bindgen]
impl SwirlDB {
    /// Create a new SwirlDB instance with default in-memory storage
    #[wasm_bindgen(constructor)]
    pub fn new() -> SwirlDB {
        console_error_panic_hook::set_once();
        let id = NEXT_ID.with(|next_id| {
            let id = *next_id.borrow();
            *next_id.borrow_mut() = id + 1;
            id
        });

        SwirlDB {
            core: Rc::new(CoreSwirlDB::new()),
            id,
        }
    }

    /// Create a new SwirlDB instance with LocalStorage persistence
    ///
    /// Example:
    /// ```javascript
    /// const db = await SwirlDB.withLocalStorage('my-app');
    /// ```
    #[wasm_bindgen(js_name = withLocalStorage)]
    pub fn with_local_storage(storage_key: String) -> Promise {
        future_to_promise(async move {
            console_error_panic_hook::set_once();
            let id = NEXT_ID.with(|next_id| {
                let id = *next_id.borrow();
                *next_id.borrow_mut() = id + 1;
                id
            });

            let storage = LocalDocumentStorage::new(&storage_key)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let core = CoreSwirlDB::with_storage(Arc::new(storage), "db").await;

            Ok(JsValue::from(SwirlDB {
                core: Rc::new(core),
                id,
            }))
        })
    }

    /// Create a new SwirlDB instance with IndexedDB persistence
    ///
    /// Example:
    /// ```javascript
    /// const db = await SwirlDB.withIndexedDB('my-app');
    /// ```
    #[wasm_bindgen(js_name = withIndexedDB)]
    pub fn with_indexed_db(db_name: String) -> Promise {
        future_to_promise(async move {
            console_error_panic_hook::set_once();
            let id = NEXT_ID.with(|next_id| {
                let id = *next_id.borrow();
                *next_id.borrow_mut() = id + 1;
                id
            });

            let storage = IndexedDBAdapter::new(&db_name).await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let core = CoreSwirlDB::with_storage(Arc::new(storage), "db").await;

            Ok(JsValue::from(SwirlDB {
                core: Rc::new(core),
                id,
            }))
        })
    }


    /// Set a value at the given dot-separated path
    #[wasm_bindgen(js_name = setPath)]
    pub fn set_path(&mut self, path: String, value: JsValue) -> Result<(), JsValue> {
        let scalar = js_to_scalar(value)?;
        self.core
            .set_path(&path, scalar)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        // Check observers after mutation
        self.check_observers();
        Ok(())
    }

    /// Get a value at the given dot-separated path
    #[wasm_bindgen(js_name = getPath)]
    pub fn get_path(&self, path: String) -> JsValue {
        match self.core.get_path(&path) {
            Some(value) => scalar_to_js(&value),
            None => JsValue::NULL,
        }
    }

    /// Set any JavaScript value (scalar, array, or object) at the given path
    ///
    /// This method accepts any JavaScript value and recursively converts it to native Automerge types:
    /// - Arrays become Automerge Lists (element-level CRDT)
    /// - Objects become Automerge Maps (key-level CRDT)
    /// - Scalars are stored as ScalarValue types
    ///
    /// Example:
    /// ```javascript
    /// db.setValue('messages', [
    ///   {id: '1', from: 'alice', text: 'Hello'},
    ///   {id: '2', from: 'bob', text: 'Hi!'}
    /// ]);
    /// ```
    #[wasm_bindgen(js_name = setValue)]
    pub fn set_value(&mut self, path: String, value: JsValue) -> Result<(), JsValue> {
        // Convert JS value to serde_json::Value
        let json_value: serde_json::Value = from_value(value)
            .map_err(|e| JsValue::from_str(&format!("Failed to convert value: {}", e)))?;

        self.core
            .set_value(&path, json_value)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        // Check observers after mutation
        self.check_observers();
        Ok(())
    }

    /// Get any JavaScript value (scalar, array, or object) at the given path
    ///
    /// Returns the value as a native JavaScript type:
    /// - Automerge Lists become JavaScript arrays
    /// - Automerge Maps become JavaScript objects
    /// - Scalars become JavaScript primitives
    ///
    /// Example:
    /// ```javascript
    /// const messages = db.getValue('messages');
    /// // Returns: [{id: '1', from: 'alice', text: 'Hello'}, ...]
    /// ```
    #[wasm_bindgen(js_name = getValue)]
    pub fn get_value(&self, path: String) -> JsValue {
        match self.core.get_value(&path) {
            Some(value) => {
                // Convert to JSON string, then parse in JavaScript for proper object creation
                // This ensures JavaScript receives proper objects instead of Proxy-like structures
                let json_str = value.to_string();
                match js_sys::JSON::parse(&json_str) {
                    Ok(js_val) => js_val,
                    Err(_) => JsValue::NULL
                }
            },
            None => JsValue::NULL,
        }
    }

    /// Get all root-level keys in the document
    ///
    /// Returns an array of strings representing all top-level keys
    ///
    /// Example:
    /// ```javascript
    /// const keys = db.getRootKeys();
    /// console.log('Root keys:', keys); // ['chat', 'user', 'settings', ...]
    /// ```
    #[wasm_bindgen(js_name = getRootKeys)]
    pub fn get_root_keys(&self) -> Vec<String> {
        self.core.get_root_keys()
    }

    /// Save the current state to a Uint8Array
    #[wasm_bindgen(js_name = saveState)]
    pub fn save_state(&self) -> Uint8Array {
        let bytes = self.core.save_state();
        Uint8Array::from(&bytes[..])
    }

    /// Load state from a Uint8Array (REPLACES current state)
    #[wasm_bindgen(js_name = loadState)]
    pub fn load_state(&mut self, input: Uint8Array) -> Result<(), JsValue> {
        let vec = input.to_vec();
        self.core
            .load_state(&vec)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        // Check observers after loading
        self.check_observers();
        Ok(())
    }

    /// Apply CRDT changes (MERGES with current state)
    ///
    /// This is the correct way to sync CRDT state - it merges changes
    /// rather than replacing the entire document like loadState() does.
    ///
    /// Example:
    /// ```javascript
    /// // Receive changes from server
    /// const changes = [change1Bytes, change2Bytes];
    /// db.applyChanges(changes);
    /// ```
    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(&mut self, changes: Vec<Uint8Array>) -> Result<(), JsValue> {
        let change_vecs: Vec<Vec<u8>> = changes.into_iter()
            .map(|arr| arr.to_vec())
            .collect();

        self.core
            .apply_changes(change_vecs)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        // Check observers after applying changes
        self.check_observers();
        Ok(())
    }

    /// Get all changes from the document as an array of Uint8Array
    ///
    /// This returns the complete change history that can be sent to other peers
    #[wasm_bindgen(js_name = getChanges)]
    pub fn get_changes(&self) -> Vec<Uint8Array> {
        self.core.get_changes()
            .into_iter()
            .map(|bytes| Uint8Array::from(&bytes[..]))
            .collect()
    }

    /// Get changes since the given heads (incremental sync)
    ///
    /// This returns only the changes that have been made since the given heads,
    /// enabling efficient incremental synchronization.
    ///
    /// Example:
    /// ```javascript
    /// // Get only new changes since last sync
    /// const newChanges = db.getChangesSince(lastSyncedHeads);
    /// ```
    #[wasm_bindgen(js_name = getChangesSince)]
    pub fn get_changes_since(&self, heads: Vec<Uint8Array>) -> Vec<Uint8Array> {
        let head_vecs: Vec<Vec<u8>> = heads.into_iter()
            .map(|arr| arr.to_vec())
            .collect();

        self.core.get_changes_since(&head_vecs)
            .into_iter()
            .map(|bytes| Uint8Array::from(&bytes[..]))
            .collect()
    }

    /// Get the current heads (tips of the change graph) as a flat Uint8Array
    ///
    /// Returns a Uint8Array containing all heads concatenated (each head is 32 bytes)
    /// These can be sent to the server for incremental sync
    #[wasm_bindgen(js_name = getHeads)]
    pub fn get_heads(&self) -> Uint8Array {
        let heads = self.core.get_heads();
        // Flatten all heads into a single byte array
        let flat_bytes: Vec<u8> = heads.into_iter().flatten().collect();
        Uint8Array::from(&flat_bytes[..])
    }

    /// Get the current heads as an array of Uint8Array
    ///
    /// Returns an array where each element is a single head (32 bytes)
    /// This format is compatible with getChangesSince()
    ///
    /// Example:
    /// ```javascript
    /// const heads = db.getHeadsArray();
    /// const newChanges = db.getChangesSince(heads);
    /// ```
    #[wasm_bindgen(js_name = getHeadsArray)]
    pub fn get_heads_array(&self) -> Vec<Uint8Array> {
        self.core.get_heads()
            .into_iter()
            .map(|bytes| Uint8Array::from(&bytes[..]))
            .collect()
    }

    /// Observe changes to a specific path
    ///
    /// The callback will be invoked with the new value whenever it changes
    #[wasm_bindgen(js_name = observe)]
    pub fn observe(&self, path: String, callback: Function) -> Result<(), JsValue> {
        let current_value = self.core.get_path(&path);

        OBSERVERS.with(|observers| {
            observers.borrow_mut().push((
                self.id,
                path,
                callback,
                current_value,
            ));
        });

        Ok(())
    }

    /// Enable auto-persistence (saves after every mutation)
    #[wasm_bindgen(js_name = enableAutoPersist)]
    pub fn enable_auto_persist(&mut self) {
        // We need to get a mutable reference to the core
        // This is a limitation of using Rc - we'll need to refactor if we want this
        // For now, document that auto-persist should be configured via TypeScript wrapper
    }

    /// Load policy configuration from JSON string
    ///
    /// Example:
    /// ```javascript
    /// const policyJson = JSON.stringify({
    ///   policies: {
    ///     rules: [
    ///       {
    ///         priority: 10,
    ///         actor: { type: "User" },
    ///         action: "Write",
    ///         path_pattern: "/user/{actor.id}/**",
    ///         effect: "Allow"
    ///       }
    ///     ]
    ///   }
    /// });
    /// db.loadPolicyConfig(policyJson);
    /// ```
    #[wasm_bindgen(js_name = loadPolicyConfig)]
    pub fn load_policy_config(&self, json_str: String) -> Result<(), JsValue> {
        let _engine = PolicyEngine::from_json(&json_str)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        // We need to modify the core instance, which requires interior mutability
        // Since core is Rc, we can't modify it directly
        // This is a limitation - we'll return an error for now
        Err(JsValue::from_str("Policy configuration must be set during construction. Use withPolicy() constructor instead."))
    }

    /// Create a new SwirlDB instance with policy configuration
    ///
    /// Example:
    /// ```javascript
    /// const policyJson = JSON.stringify({
    ///   policies: {
    ///     rules: [...]
    ///   }
    /// });
    /// const db = SwirlDB.withPolicy(policyJson);
    /// ```
    #[wasm_bindgen(js_name = withPolicy)]
    pub fn with_policy(json_str: String) -> Result<SwirlDB, JsValue> {
        console_error_panic_hook::set_once();
        let id = NEXT_ID.with(|next_id| {
            let id = *next_id.borrow();
            *next_id.borrow_mut() = id + 1;
            id
        });

        let engine = PolicyEngine::from_json(&json_str)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let core = CoreSwirlDB::new().with_policy(engine);

        Ok(SwirlDB {
            core: Rc::new(core),
            id,
        })
    }

    /// Authenticate with a JWT token
    ///
    /// This decodes the JWT token and extracts the actor information from the claims.
    /// The actor will then be used for all policy evaluations.
    ///
    /// **Important**: This only DECODES the token, it does NOT validate the signature!
    /// The JWT should be validated server-side before being passed to the client.
    ///
    /// Example:
    /// ```javascript
    /// // After receiving a JWT from your auth server
    /// const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...";
    /// db.authenticateJWT(token);
    ///
    /// // Now all operations use the authenticated actor
    /// db.setPath('/user/alice/profile.name', 'Alice'); // Uses actor from JWT
    /// ```
    #[wasm_bindgen(js_name = authenticateJWT)]
    pub fn authenticate_jwt(&self, token: String) -> Result<(), JsValue> {
        self.core.authenticate_jwt(&token)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the current actor as a JavaScript object
    ///
    /// Example:
    /// ```javascript
    /// const actor = db.getActor();
    /// console.log('Current actor:', actor.id, actor.type);
    /// ```
    #[wasm_bindgen(js_name = getActor)]
    pub fn get_actor(&self) -> JsValue {
        let actor = self.core.get_actor();
        serde_wasm_bindgen::to_value(&actor).unwrap_or(JsValue::NULL)
    }

    /// Manually persist current state to storage
    #[wasm_bindgen(js_name = persist)]
    pub fn persist(&self) -> Promise {
        let core = Rc::clone(&self.core);
        future_to_promise(async move {
            core.persist().await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Manually trigger observer checks
    #[wasm_bindgen(js_name = checkObservers)]
    pub fn check_observers(&self) {
        let db_id = self.id;

        OBSERVERS.with(|observers| {
            for (id, path, callback, last_value) in observers.borrow_mut().iter_mut() {
                // Only check observers for this DB instance
                if *id != db_id {
                    continue;
                }

                let current = self.core.get_path(path);

                // Compare values
                let changed = match (&*last_value, &current) {
                    (None, None) => false,
                    (Some(_), None) | (None, Some(_)) => true,
                    (Some(a), Some(b)) => !scalar_values_equal(a, b),
                };

                if changed {
                    let js_value = match &current {
                        Some(v) => scalar_to_js(v),
                        None => JsValue::NULL,
                    };

                    let _ = callback.call1(&JsValue::NULL, &js_value);
                    *last_value = current;
                }
            }
        });
    }

    // ===== Protocol Methods =====

    /// Connect to sync server with WebSocket (managed internally)
    ///
    /// WebSocket connection is managed entirely in WASM. TypeScript doesn't need to handle
    /// any protocol logic - just use the Proxy API for data access.
    ///
    /// Example:
    /// ```javascript
    /// db.connect('ws://localhost:3030/ws', 'alice', ['/**']);
    /// // That's it! Now mutations automatically sync:
    /// db.data.messages = [...messages, newMessage];
    /// ```
    #[wasm_bindgen(js_name = connect)]
    pub fn connect(&self, url: String, client_id: String, subscriptions: Vec<String>) -> Result<(), JsValue> {
        let ws = WebSocket::new(&url)
            .map_err(|e| JsValue::from_str(&format!("Failed to create WebSocket: {:?}", e)))?;

        ws.set_binary_type(BinaryType::Arraybuffer);

        let db_id = self.id;
        let core = Rc::clone(&self.core);

        // Create connection state
        let mut conn_state = ConnectionState::new(client_id.clone(), subscriptions.clone());

        // Get current heads for Connect message
        let heads = self.core.get_heads();
        let heads_bytes: Vec<u8> = heads.into_iter().flatten().collect();

        // Create Connect message
        let connect_msg = Message::Connect {
            client_id: client_id.clone(),
            subscriptions: subscriptions.clone(),
            heads: heads_bytes,
        };

        // Setup onopen handler
        let ws_clone = ws.clone();
        let connect_bytes = connect_msg.encode();
        let onopen = Closure::wrap(Box::new(move |_| {
            web_sys::console::log_1(&"✅ WebSocket connected".into());
            if let Err(e) = ws_clone.send_with_u8_array(&connect_bytes) {
                web_sys::console::error_1(&format!("Failed to send Connect: {:?}", e).into());
            }
        }) as Box<dyn FnMut(web_sys::Event)>);
        ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));

        // Setup onmessage handler
        let ws_clone = ws.clone();
        let onmessage = Closure::wrap(Box::new(move |event: MessageEvent| {
            if let Ok(array_buffer) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let bytes = js_sys::Uint8Array::new(&array_buffer).to_vec();

                // Decode and handle message
                match Message::decode(&bytes) {
                    Ok(msg) => {
                        CONNECTIONS.with(|conns| {
                            let mut conns_map = conns.borrow_mut();
                            if let Some(state) = conns_map.get_mut(&db_id) {
                                match msg {
                                    Message::Sync { heads, changes } => {
                                        // Apply changes from initial sync
                                        if !changes.is_empty() {
                                            let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
                                            web_sys::console::log_1(&format!("📥 SYNC: {} changes ({} bytes) from server",
                                                changes.len(), total_bytes).into());

                                            if let Err(e) = core.apply_changes(changes) {
                                                web_sys::console::error_1(&format!("Failed to apply changes: {}", e).into());
                                            } else {
                                                // Fire all observers after initial sync (no affected_paths in Sync)
                                                fire_all_observers(db_id, &core);
                                            }
                                        } else {
                                            web_sys::console::log_1(&"📥 SYNC: Already up to date".into());
                                        }
                                        // Update heads
                                        state.last_synced_heads.clear();
                                        let mut offset = 0;
                                        while offset + 32 <= heads.len() {
                                            state.last_synced_heads.push(heads[offset..offset+32].to_vec());
                                            offset += 32;
                                        }
                                    }
                                    Message::Broadcast { from_client_id, changes, affected_paths } => {
                                        if !changes.is_empty() {
                                            // Calculate total bytes
                                            let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
                                            web_sys::console::log_1(&format!("📥 RECV: {} changes ({} bytes) from {}",
                                                changes.len(), total_bytes, from_client_id).into());

                                            if let Err(e) = core.apply_changes(changes) {
                                                web_sys::console::error_1(&format!("Failed to apply changes: {}", e).into());
                                            } else {
                                                // Fire observers for affected paths only
                                                fire_observers_for_paths(db_id, &core, &affected_paths);
                                            }
                                        }
                                    }
                                    Message::PushAck { heads } => {
                                        // Update heads for incremental sync
                                        state.last_synced_heads.clear();
                                        let mut offset = 0;
                                        while offset + 32 <= heads.len() {
                                            state.last_synced_heads.push(heads[offset..offset+32].to_vec());
                                            offset += 32;
                                        }
                                    }
                                    Message::SubscribeAck { added: _, denied: _ } => {
                                        // Subscription confirmed
                                    }
                                    Message::Error { message } => {
                                        web_sys::console::error_1(&format!("Server error: {}", message).into());
                                    }
                                    _ => {}
                                }
                            }
                        });
                    }
                    Err(e) => {
                        web_sys::console::error_1(&format!("Failed to decode message: {}", e).into());
                    }
                }
            }
        }) as Box<dyn FnMut(MessageEvent)>);
        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

        // Setup onclose handler
        let onclose = Closure::wrap(Box::new(move |_: CloseEvent| {
            web_sys::console::log_1(&"WebSocket closed".into());
            CONNECTIONS.with(|conns| {
                conns.borrow_mut().remove(&db_id);
            });
        }) as Box<dyn FnMut(CloseEvent)>);
        ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        // Setup onerror handler
        let onerror = Closure::wrap(Box::new(move |e: ErrorEvent| {
            web_sys::console::error_1(&format!("WebSocket error: {:?}", e).into());
        }) as Box<dyn FnMut(ErrorEvent)>);
        ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        // Store connection state and closures
        conn_state.websocket = Some(ws);
        conn_state.onopen_closure = Some(onopen);
        conn_state.onmessage_closure = Some(onmessage);
        conn_state.onclose_closure = Some(onclose);
        conn_state.onerror_closure = Some(onerror);

        CONNECTIONS.with(|conns| {
            conns.borrow_mut().insert(self.id, conn_state);
        });

        Ok(())
    }

    /// Send local changes to server (called after mutations)
    #[wasm_bindgen(js_name = syncChanges)]
    pub fn sync_changes(&self) {
        CONNECTIONS.with(|conns| {
            let conns_map = conns.borrow();
            if let Some(state) = conns_map.get(&self.id) {
                // Get changes since last sync (incremental if we have heads)
                let changes = if state.last_synced_heads.is_empty() {
                    self.core.get_changes()
                } else {
                    self.core.get_changes_since(&state.last_synced_heads)
                };

                if changes.is_empty() {
                    return;
                }

                // Calculate total bytes
                let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
                let sync_mode = if state.last_synced_heads.is_empty() { "full" } else { "delta" };
                web_sys::console::log_1(&format!("📤 SEND: {} changes ({} bytes, {})",
                    changes.len(), total_bytes, sync_mode).into());

                // Get current heads
                let heads = self.core.get_heads();
                let heads_bytes: Vec<u8> = heads.into_iter().flatten().collect();

                // Create Push message
                let msg = Message::Push {
                    heads: heads_bytes,
                    changes,
                };

                // Send via WebSocket
                if let Some(ws) = &state.websocket {
                    let encoded = msg.encode();
                    if let Err(e) = ws.send_with_u8_array(&encoded) {
                        web_sys::console::error_1(&format!("Failed to send Push: {:?}", e).into());
                    }
                }
            }
        });
    }
}

/// Convert a JavaScript value to an Automerge ScalarValue
fn js_to_scalar(val: JsValue) -> Result<ScalarValue, JsValue> {
    if val.is_null() || val.is_undefined() {
        Ok(ScalarValue::Null)
    } else if val.is_string() {
        Ok(ScalarValue::Str(
            val.as_string()
                .ok_or_else(|| JsValue::from_str("Failed to convert to string"))?
                .into(),
        ))
    } else if let Some(b) = val.as_bool() {
        Ok(ScalarValue::Boolean(b))
    } else if let Some(n) = val.as_f64() {
        // Check if it's an integer
        if n.fract() == 0.0 && n.is_finite() {
            Ok(ScalarValue::Int(n as i64))
        } else {
            Ok(ScalarValue::F64(n))
        }
    } else {
        Err(JsValue::from_str("Unsupported value type"))
    }
}

/// Convert an Automerge ScalarValue to a JavaScript value
fn scalar_to_js(val: &ScalarValue) -> JsValue {
    match val {
        ScalarValue::Str(s) => JsValue::from(s.as_str()),
        ScalarValue::Int(i) => JsValue::from(*i as f64),
        ScalarValue::Uint(u) => JsValue::from(*u as f64),
        ScalarValue::F64(f) => JsValue::from(*f),
        ScalarValue::Boolean(b) => JsValue::from(*b),
        ScalarValue::Null => JsValue::NULL,
        _ => JsValue::NULL,
    }
}

/// Compare two scalar values for equality
fn scalar_values_equal(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Str(a), ScalarValue::Str(b)) => a == b,
        (ScalarValue::F64(a), ScalarValue::F64(b)) => a == b,
        (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => a == b,
        (ScalarValue::Null, ScalarValue::Null) => true,
        (ScalarValue::Int(a), ScalarValue::Int(b)) => a == b,
        (ScalarValue::Uint(a), ScalarValue::Uint(b)) => a == b,
        _ => false,
    }
}
