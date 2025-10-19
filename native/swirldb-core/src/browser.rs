use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;
use js_sys::{Function, Uint8Array, Promise};
use automerge::ScalarValue;
use crate::core::{SwirlDB as CoreSwirlDB, StorageHint};
use crate::storage::{LocalStorageAdapter, IndexedDBAdapter};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use serde_wasm_bindgen::{from_value, to_value};

thread_local! {
    static OBSERVERS: RefCell<Vec<(usize, String, Function, Option<ScalarValue>)>> = RefCell::new(Vec::new());
    static NEXT_ID: RefCell<usize> = RefCell::new(0);
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

            let storage = LocalStorageAdapter::new(&storage_key)
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

    /// Set storage hint for a path
    ///
    /// Example:
    /// ```javascript
    /// db.setStorageHint('session.temp', 'memory-only');
    /// db.setStorageHint('user.profile', 'persisted');
    /// db.setStorageHint('shared.doc', 'synced');
    /// ```
    #[wasm_bindgen(js_name = setStorageHint)]
    pub fn set_storage_hint(&self, path: String, hint: String) -> Result<(), JsValue> {
        let storage_hint = match hint.as_str() {
            "memory-only" => StorageHint::MemoryOnly,
            "persisted" => StorageHint::Persisted,
            "synced" => StorageHint::Synced,
            _ => return Err(JsValue::from_str("Invalid storage hint. Use 'memory-only', 'persisted', or 'synced'")),
        };

        self.core.set_storage_hint(&path, storage_hint);
        Ok(())
    }

    /// Enable auto-persistence (saves after every mutation)
    #[wasm_bindgen(js_name = enableAutoPersist)]
    pub fn enable_auto_persist(&mut self) {
        // We need to get a mutable reference to the core
        // This is a limitation of using Rc - we'll need to refactor if we want this
        // For now, document that auto-persist should be configured via TypeScript wrapper
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
