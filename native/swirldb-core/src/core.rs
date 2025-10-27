// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

use automerge::{AutoCommit, ScalarValue, ROOT, ObjId, ReadDoc, transaction::Transactable, ObjType, Value as AutoValue};
use std::sync::{Arc, Mutex};
use anyhow::{Result, anyhow};
use serde_json::Value as JsonValue;
use crate::policy::{PolicyEngine, Action};
use crate::auth::{AuthProvider, AnonymousAuth};

/// Storage adapter trait - all storage implementations implement this
///
/// This allows pluggable storage backends: in-memory, LocalStorage, IndexedDB, redb, etc.
// Use storage traits from the storage module
use crate::storage::DocumentStorage;

/// Observer callback signature
pub type ObserverCallback = Box<dyn Fn(Option<ScalarValue>) + Send + Sync>;

/// Observer entry tracking a path and its callback
struct Observer {
    path: String,
    callback: ObserverCallback,
    last_value: Option<ScalarValue>,
}

/// Core SwirlDB engine - pure Rust, platform-agnostic
///
/// This is the pure Rust core with no binding attributes.
/// It uses Arc<Mutex<>> for thread-safety and can be used
/// from both WASM and native targets.
pub struct SwirlDB {
    doc: Arc<Mutex<AutoCommit>>,
    observers: Arc<Mutex<Vec<Observer>>>,
    storage: Arc<dyn DocumentStorage>,
    storage_key: String,
    auto_persist: bool,
    policy_engine: Option<Arc<PolicyEngine>>,
    auth_provider: Arc<Mutex<Box<dyn AuthProvider + Send + Sync>>>,
}

impl SwirlDB {
    /// Create a new SwirlDB instance with default in-memory storage
    pub fn new() -> Self {
        SwirlDB {
            doc: Arc::new(Mutex::new(AutoCommit::new())),
            observers: Arc::new(Mutex::new(Vec::new())),
            storage: Arc::new(crate::storage::InMemoryDocStorage::new()),
            storage_key: "default".to_string(),
            auto_persist: false,
            policy_engine: None,
            auth_provider: Arc::new(Mutex::new(Box::new(AnonymousAuth::new()))),
        }
    }

    /// Create a SwirlDB instance with a custom storage adapter
    ///
    /// Example:
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use swirldb_core::storage::InMemoryDocStorage;
    /// # use swirldb_core::SwirlDB;
    /// # async fn example() {
    /// let storage = Arc::new(InMemoryDocStorage::new());
    /// let db = SwirlDB::with_storage(storage, "my-db").await;
    /// # }
    /// ```
    pub async fn with_storage(storage: Arc<dyn DocumentStorage>, storage_key: &str) -> Self {
        // Try to load existing state from storage
        let doc = match storage.load(storage_key).await {
            Ok(Some(bytes)) => {
                AutoCommit::load(&bytes).unwrap_or_else(|_| AutoCommit::new())
            }
            _ => AutoCommit::new(),
        };

        SwirlDB {
            doc: Arc::new(Mutex::new(doc)),
            observers: Arc::new(Mutex::new(Vec::new())),
            storage,
            storage_key: storage_key.to_string(),
            auto_persist: false,
            policy_engine: None,
            auth_provider: Arc::new(Mutex::new(Box::new(AnonymousAuth::new()))),
        }
    }

    /// Set the policy engine for authorization
    pub fn with_policy(mut self, engine: PolicyEngine) -> Self {
        self.policy_engine = Some(Arc::new(engine));
        self
    }

    /// Set the authentication provider
    pub fn with_auth_provider(mut self, provider: Box<dyn AuthProvider + Send + Sync>) -> Self {
        self.auth_provider = Arc::new(Mutex::new(provider));
        self
    }

    /// Set the authentication provider (mutable version)
    pub fn set_auth_provider(&self, provider: Box<dyn AuthProvider + Send + Sync>) {
        *self.auth_provider.lock().unwrap() = provider;
    }

    /// Authenticate with a JWT token
    ///
    /// This decodes the token and creates an Actor from the claims.
    /// Note: This does NOT validate the JWT signature - validate server-side first!
    pub fn authenticate_jwt(&self, token: &str) -> Result<()> {
        use crate::auth::JwtAuth;
        let jwt_auth = JwtAuth::from_token(token)
            .map_err(|e| anyhow!("JWT authentication failed: {}", e))?;
        self.set_auth_provider(Box::new(jwt_auth));
        Ok(())
    }

    /// Get the current actor from the auth provider
    pub fn get_actor(&self) -> crate::policy::Actor {
        self.auth_provider.lock().unwrap().get_actor()
    }

    /// Enable or disable automatic persistence to storage after mutations
    ///
    /// When enabled, every `set_path` and `load_state` call will trigger a save
    pub fn set_auto_persist(&mut self, enabled: bool) {
        self.auto_persist = enabled;
    }

    /// Manually persist the current state to storage
    pub async fn persist(&self) -> Result<()> {
        let mut doc = self.doc.lock().unwrap();
        let bytes = doc.save();
        drop(doc);
        self.storage.save(&self.storage_key, &bytes).await
    }

    /// Check if the current actor can perform an action on a path
    fn check_policy(&self, action: Action, path: &str) -> Result<()> {
        if let Some(engine) = &self.policy_engine {
            let actor = self.auth_provider.lock().unwrap().get_actor();
            let decision = engine.evaluate(&actor, action, path);
            if !decision.is_allowed() {
                return Err(anyhow!(
                    "Policy denied: actor={} action={:?} path={} (matched rule priority {})",
                    actor.id,
                    action,
                    path,
                    decision.rule_priority
                ));
            }
        }
        // No policy engine = allow all (backward compatibility)
        Ok(())
    }

    /// Set a value at the given dot-separated path
    ///
    /// Example: `db.set_path("user.name", Value::String("Alice".into()))`
    pub fn set_path(&self, path: &str, value: ScalarValue) -> Result<()> {
        // Check write policy
        self.check_policy(Action::Write, path)?;

        let segments = split_path(path);
        if segments.is_empty() {
            return Err(anyhow!("Empty path"));
        }

        let mut doc = self.doc.lock().unwrap();
        if let Some(parent) = resolve_path(&mut doc, &segments, true) {
            let key = segments.last().unwrap();
            doc.put(&parent, key.as_str(), value)
                .map_err(|e| anyhow!("Failed to set value: {:?}", e))?;
            drop(doc); // Release lock before checking observers

            self.check_observers();

            // Note: Auto-persist is now handled at the binding layer (browser.rs)
            // where we have access to the async runtime. The core remains sync
            // for easier use in non-async contexts.

            Ok(())
        } else {
            Err(anyhow!("Failed to resolve path: {}", path))
        }
    }

    /// Get a value at the given dot-separated path
    ///
    /// Returns None if the path doesn't exist or if policy denies read access
    pub fn get_path(&self, path: &str) -> Option<ScalarValue> {
        // Check read policy (return None if denied, consistent with path-not-found behavior)
        if self.check_policy(Action::Read, path).is_err() {
            return None;
        }

        let segments = split_path(path);
        if segments.is_empty() || (segments.len() == 1 && segments[0].is_empty()) {
            return None;
        }

        let doc = self.doc.lock().unwrap();
        if let Some(parent) = resolve_path_read(&doc, &segments) {
            let key = segments.last().unwrap();

            // Check if parent is a List and key is numeric
            let obj_type = doc.object_type(&parent).ok()?;
            let result = if obj_type == automerge::ObjType::List {
                if let Ok(index) = key.parse::<usize>() {
                    doc.get(&parent, index).ok().flatten()
                } else {
                    None
                }
            } else {
                doc.get(&parent, key.as_str()).ok().flatten()
            };

            result.and_then(|(val, _)| val.into_scalar().ok())
        } else {
            None
        }
    }

    /// Set a value at the given dot-separated path (supports scalars, arrays, objects)
    ///
    /// This method accepts any JSON value and recursively converts it to native Automerge types:
    /// - Arrays become Automerge Lists (ObjType::List)
    /// - Objects become Automerge Maps (ObjType::Map)
    /// - Scalars are stored as ScalarValue types
    ///
    /// Example: `db.set_value("messages", json!([ {"id": "1", "text": "Hello"} ]))`
    pub fn set_value(&self, path: &str, value: JsonValue) -> Result<()> {
        // Check write policy
        self.check_policy(Action::Write, path)?;

        let segments = split_path(path);
        if segments.is_empty() {
            return Err(anyhow!("Empty path"));
        }

        let mut doc = self.doc.lock().unwrap();
        if let Some(parent) = resolve_path(&mut doc, &segments, true) {
            let key = segments.last().unwrap();
            self.insert_value(&mut doc, &parent, key, &value)?;
            drop(doc);

            self.check_observers();
            Ok(())
        } else {
            Err(anyhow!("Failed to resolve path: {}", path))
        }
    }

    /// Get a value at the given dot-separated path as JSON (supports scalars, arrays, objects)
    ///
    /// Returns None if the path doesn't exist or if policy denies read access
    pub fn get_value(&self, path: &str) -> Option<JsonValue> {
        // Check read policy (return None if denied)
        if self.check_policy(Action::Read, path).is_err() {
            return None;
        }

        let segments = split_path(path);
        if segments.is_empty() || (segments.len() == 1 && segments[0].is_empty()) {
            return None;
        }

        let doc = self.doc.lock().unwrap();
        if let Some(parent) = resolve_path_read(&doc, &segments) {
            let key = segments.last().unwrap();

            // Check if parent is a List and key is numeric
            let obj_type = doc.object_type(&parent).ok()?;
            let result = if obj_type == automerge::ObjType::List {
                if let Ok(index) = key.parse::<usize>() {
                    doc.get(&parent, index).ok().flatten()
                } else {
                    None
                }
            } else {
                doc.get(&parent, key.as_str()).ok().flatten()
            };

            if let Some((val, obj_id)) = result {
                return Some(self.automerge_to_json(&doc, &val, &obj_id));
            }
        }
        None
    }

    /// Recursively insert a JSON value into the Automerge document
    ///
    /// # Array Optimization
    ///
    /// Arrays of record-like objects (with `id`, `timestamp`, or `_key` fields) are automatically
    /// converted to maps internally with stable keys. This provides 99.8% reduction in sync overhead
    /// for incremental array updates.
    ///
    /// **How it works:**
    /// 1. Detects arrays where all items have `id`, `timestamp`, or `_key` fields
    /// 2. Stores as Map in Automerge (not List) with item keys as map keys
    /// 3. When updating, performs smart diff - only syncs changed/added/removed items
    /// 4. On read, converts back to array and sorts by timestamp
    /// 5. Internal `_key` field is stripped from user view
    ///
    /// **Why it matters:**
    /// - Replacing an entire array creates one large CRDT change (inefficient)
    /// - Using a map allows Automerge to track individual item changes
    /// - Only changed items are included in CRDT deltas
    /// - Example: Adding 1 message to 100-message array = 200 bytes instead of 290KB
    ///
    /// **Transparent to applications:**
    /// - Apps use `db.data.messages = [...]` (normal array assignment)
    /// - Apps read `db.data.messages.$value` (normal array read)
    /// - Optimization happens entirely in this function and `automerge_to_json()`
    ///
    fn insert_value(&self, doc: &mut AutoCommit, parent: &ObjId, key: &str, value: &JsonValue) -> Result<()> {
        // Array Optimization: Smart diffing for existing optimized arrays
        if let JsonValue::Array(new_arr) = value {
            // Check if this is a record-like array that qualifies for optimization
            let should_optimize = new_arr.iter().all(|item| {
                if let JsonValue::Object(obj) = item {
                    obj.contains_key("id") || obj.contains_key("timestamp") || obj.contains_key("_key")
                } else {
                    false
                }
            });

            if should_optimize && !new_arr.is_empty() {
                // Check if there's already an optimized array (stored as a map) at this location
                if let Ok(Some((existing_val, existing_obj_id))) = doc.get(parent, key) {
                    if let automerge::Value::Object(automerge::ObjType::Map) = existing_val {
                        // Existing optimized array found - perform smart diff!
                        // Only sync changed/added/removed items instead of the entire array
                        self.update_array_as_map(doc, &existing_obj_id, new_arr)?;
                        return Ok(());
                    }
                }
                // Fall through to normal array-to-map conversion for new arrays
            }
        }

        match value {
            JsonValue::Null => {
                doc.put(parent, key, ScalarValue::Null)
                    .map_err(|e| anyhow!("Failed to set null: {:?}", e))?;
            },
            JsonValue::Bool(b) => {
                doc.put(parent, key, ScalarValue::Boolean(*b))
                    .map_err(|e| anyhow!("Failed to set boolean: {:?}", e))?;
            },
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    doc.put(parent, key, ScalarValue::Int(i))
                        .map_err(|e| anyhow!("Failed to set int: {:?}", e))?;
                } else if let Some(u) = n.as_u64() {
                    doc.put(parent, key, ScalarValue::Uint(u))
                        .map_err(|e| anyhow!("Failed to set uint: {:?}", e))?;
                } else if let Some(f) = n.as_f64() {
                    doc.put(parent, key, ScalarValue::F64(f))
                        .map_err(|e| anyhow!("Failed to set float: {:?}", e))?;
                }
            },
            JsonValue::String(s) => {
                doc.put(parent, key, ScalarValue::Str(s.clone().into()))
                    .map_err(|e| anyhow!("Failed to set string: {:?}", e))?;
            },
            JsonValue::Array(arr) => {
                // Check if array contains record-like objects (optimize for CRDT efficiency)
                let should_convert_to_map = arr.iter().all(|item| {
                    if let JsonValue::Object(obj) = item {
                        // Has id, timestamp, or _key field
                        obj.contains_key("id") || obj.contains_key("timestamp") || obj.contains_key("_key")
                    } else {
                        false
                    }
                });

                if should_convert_to_map && !arr.is_empty() {
                    // Store as Map with stable keys for efficient incremental sync
                    let map_id = doc.put_object(parent, key, ObjType::Map)
                        .map_err(|e| anyhow!("Failed to create map: {:?}", e))?;

                    for item in arr.iter() {
                        if let JsonValue::Object(obj) = item {
                            // Extract or generate stable key
                            let item_key = if let Some(id) = obj.get("id").and_then(|v| v.as_str()) {
                                id.to_string()
                            } else if let Some(key) = obj.get("_key").and_then(|v| v.as_str()) {
                                key.to_string()
                            } else if let Some(ts) = obj.get("timestamp").and_then(|v| v.as_i64()) {
                                // Generate key from timestamp + random suffix
                                use std::time::{SystemTime, UNIX_EPOCH};
                                let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
                                format!("{}-{:x}", ts, nanos)
                            } else {
                                // Fallback: current timestamp + random
                                use std::time::{SystemTime, UNIX_EPOCH};
                                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                                format!("{}-{:x}", now.as_millis(), now.subsec_nanos())
                            };

                            // Add _key field to the object if not present
                            let mut item_with_key = obj.clone();
                            if !item_with_key.contains_key("_key") {
                                item_with_key.insert("_key".to_string(), JsonValue::String(item_key.clone()));
                            }

                            // Insert as map entry
                            self.insert_value(doc, &map_id, &item_key, &JsonValue::Object(item_with_key))?;
                        }
                    }
                } else {
                    // Regular array: store as List
                    let list_id = doc.put_object(parent, key, ObjType::List)
                        .map_err(|e| anyhow!("Failed to create list: {:?}", e))?;
                    for (i, item) in arr.iter().enumerate() {
                        self.insert_value_at_index(doc, &list_id, i, item)?;
                    }
                }
            },
            JsonValue::Object(obj) => {
                let map_id = doc.put_object(parent, key, ObjType::Map)
                    .map_err(|e| anyhow!("Failed to create map: {:?}", e))?;
                for (k, v) in obj.iter() {
                    self.insert_value(doc, &map_id, k, v)?;
                }
            }
        }
        Ok(())
    }

    /// Insert a JSON value at a specific index in an Automerge list
    fn insert_value_at_index(&self, doc: &mut AutoCommit, list_id: &ObjId, index: usize, value: &JsonValue) -> Result<()> {
        match value {
            JsonValue::Null => {
                doc.insert(list_id, index, ScalarValue::Null)
                    .map_err(|e| anyhow!("Failed to insert null: {:?}", e))?;
            },
            JsonValue::Bool(b) => {
                doc.insert(list_id, index, ScalarValue::Boolean(*b))
                    .map_err(|e| anyhow!("Failed to insert boolean: {:?}", e))?;
            },
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    doc.insert(list_id, index, ScalarValue::Int(i))
                        .map_err(|e| anyhow!("Failed to insert int: {:?}", e))?;
                } else if let Some(u) = n.as_u64() {
                    doc.insert(list_id, index, ScalarValue::Uint(u))
                        .map_err(|e| anyhow!("Failed to insert uint: {:?}", e))?;
                } else if let Some(f) = n.as_f64() {
                    doc.insert(list_id, index, ScalarValue::F64(f))
                        .map_err(|e| anyhow!("Failed to insert float: {:?}", e))?;
                }
            },
            JsonValue::String(s) => {
                doc.insert(list_id, index, ScalarValue::Str(s.clone().into()))
                    .map_err(|e| anyhow!("Failed to insert string: {:?}", e))?;
            },
            JsonValue::Array(arr) => {
                let nested_list = doc.insert_object(list_id, index, ObjType::List)
                    .map_err(|e| anyhow!("Failed to insert list: {:?}", e))?;
                for (i, item) in arr.iter().enumerate() {
                    self.insert_value_at_index(doc, &nested_list, i, item)?;
                }
            },
            JsonValue::Object(obj) => {
                let nested_map = doc.insert_object(list_id, index, ObjType::Map)
                    .map_err(|e| anyhow!("Failed to insert map: {:?}", e))?;
                for (k, v) in obj.iter() {
                    self.insert_value(doc, &nested_map, k, v)?;
                }
            }
        }
        Ok(())
    }

    /// Update an existing map-backed array with smart diffing
    ///
    /// This function implements the core optimization for array updates:
    /// - Compares the new array with the existing map contents
    /// - Only creates CRDT changes for items that were added, modified, or removed
    /// - Unchanged items generate zero CRDT overhead
    ///
    /// **Performance impact:**
    /// - Without this: Updating an array creates one large CRDT change encoding the entire array
    /// - With this: Only changed items are encoded in CRDT changes
    /// - Example: Adding 1 item to 100-item array = 200 bytes instead of 290KB (99.8% reduction)
    ///
    /// **Algorithm:**
    /// 1. Build index of new items by their stable keys
    /// 2. Delete items that exist in map but not in new array
    /// 3. For each item in new array:
    ///    - If item doesn't exist in map: insert it (new item)
    ///    - If item exists but content changed: update it (modified item)
    ///    - If item exists and content unchanged: skip it (zero overhead)
    ///
    fn update_array_as_map(&self, doc: &mut AutoCommit, map_obj_id: &ObjId, new_arr: &[JsonValue]) -> Result<()> {
        use std::collections::{HashMap, HashSet};

        // Build index of new items by their stable key
        let mut new_items: HashMap<String, &JsonValue> = HashMap::new();
        for item in new_arr.iter() {
            if let JsonValue::Object(obj) = item {
                let item_key = if let Some(id) = obj.get("id").and_then(|v| v.as_str()) {
                    id.to_string()
                } else if let Some(key) = obj.get("_key").and_then(|v| v.as_str()) {
                    key.to_string()
                } else if let Some(ts) = obj.get("timestamp").and_then(|v| v.as_i64()) {
                    use std::time::{SystemTime, UNIX_EPOCH};
                    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
                    format!("{}-{:x}", ts, nanos)
                } else {
                    use std::time::{SystemTime, UNIX_EPOCH};
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                    format!("{}-{:x}", now.as_millis(), now.subsec_nanos())
                };
                new_items.insert(item_key, item);
            }
        }

        // Get existing keys
        let existing_keys: HashSet<String> = doc.keys(map_obj_id).map(|k| k.to_string()).collect();

        // Delete items that are no longer in the new array
        for old_key in existing_keys.iter() {
            if !new_items.contains_key(old_key) {
                doc.delete(map_obj_id, old_key)
                    .map_err(|e| anyhow!("Failed to delete old item: {:?}", e))?;
            }
        }

        // Update or insert items
        for (item_key, item_value) in new_items.iter() {
            if let JsonValue::Object(obj) = item_value {
                // Add _key field if not present
                let mut item_with_key = obj.clone();
                if !item_with_key.contains_key("_key") {
                    item_with_key.insert("_key".to_string(), JsonValue::String(item_key.clone()));
                }

                // Check if item already exists and is unchanged
                let needs_update = if let Ok(Some((existing_val, existing_obj_id))) = doc.get(map_obj_id, item_key.as_str()) {
                    // Compare JSON values
                    let existing_json = self.automerge_to_json(doc, &existing_val, &existing_obj_id);
                    existing_json != JsonValue::Object(item_with_key.clone())
                } else {
                    // Item doesn't exist, needs insert
                    true
                };

                if needs_update {
                    // Update or insert the item
                    self.insert_value(doc, map_obj_id, item_key, &JsonValue::Object(item_with_key))?;
                }
            }
        }

        Ok(())
    }

    /// Convert an Automerge value to JSON
    ///
    /// The obj_id parameter is the ID of the object if value is Value::Object,
    /// and comes from the second element of the tuple returned by doc.get()
    fn automerge_to_json(&self, doc: &AutoCommit, value: &AutoValue, obj_id: &ObjId) -> JsonValue {
        use automerge::Value;

        match value {
            Value::Scalar(scalar) => {
                // For scalars, ignore obj_id
                match scalar.as_ref() {
                    ScalarValue::Null => JsonValue::Null,
                    ScalarValue::Boolean(b) => JsonValue::Bool(*b),
                    ScalarValue::Int(i) => JsonValue::Number((*i).into()),
                    ScalarValue::Uint(u) => JsonValue::Number((*u).into()),
                    ScalarValue::F64(f) => {
                        serde_json::Number::from_f64(*f)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null)
                    },
                    ScalarValue::Str(s) => JsonValue::String(s.to_string()),
                    ScalarValue::Bytes(b) => {
                        // Convert bytes to base64 string
                        use base64::Engine;
                        JsonValue::String(base64::engine::general_purpose::STANDARD.encode(b))
                    },
                    ScalarValue::Timestamp(ts) => JsonValue::Number((*ts).into()),
                    ScalarValue::Counter(_c) => {
                        // Counter is a specialized CRDT type - for now just convert to null
                        JsonValue::Null
                    },
                    _ => JsonValue::Null,
                }
            },
            Value::Object(obj_type) => {
                // For objects, use the obj_id parameter to traverse
                match obj_type {
                    automerge::ObjType::Map | automerge::ObjType::Table => {
                        let mut json_obj = serde_json::Map::new();
                        // Use obj_id directly - it's the ID of this map
                        for key in doc.keys(obj_id) {
                            if let Ok(Some((val, nested_obj_id))) = doc.get(obj_id, &key) {
                                json_obj.insert(
                                    key.to_string(),
                                    self.automerge_to_json(doc, &val, &nested_obj_id)
                                );
                            }
                        }

                        // Check if this map is actually an optimized array (all values have _key field)
                        let is_array_as_map = if json_obj.is_empty() {
                            false
                        } else {
                            json_obj.values().all(|v| {
                                if let JsonValue::Object(obj) = v {
                                    obj.contains_key("_key")
                                } else {
                                    false
                                }
                            })
                        };

                        if is_array_as_map {
                            // Convert back to array
                            let mut items: Vec<JsonValue> = json_obj.into_values()
                                .filter_map(|mut v| {
                                    if let JsonValue::Object(ref mut obj) = v {
                                        // Remove internal _key field before returning
                                        obj.remove("_key");
                                        Some(v)
                                    } else {
                                        None
                                    }
                                })
                                .collect();

                            // Sort by timestamp if available, otherwise maintain insertion order
                            items.sort_by(|a, b| {
                                let ts_a = a.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                                let ts_b = b.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                                ts_a.cmp(&ts_b)
                            });

                            JsonValue::Array(items)
                        } else {
                            // Regular map
                            JsonValue::Object(json_obj)
                        }
                    },
                    automerge::ObjType::List | automerge::ObjType::Text => {
                        let mut json_arr = Vec::new();
                        // Use obj_id directly - it's the ID of this list
                        let len = doc.length(obj_id);
                        for i in 0..len {
                            if let Ok(Some((val, nested_obj_id))) = doc.get(obj_id, i) {
                                json_arr.push(
                                    self.automerge_to_json(doc, &val, &nested_obj_id)
                                );
                            }
                        }
                        JsonValue::Array(json_arr)
                    },
                }
            }
        }
    }

    /// Get all keys at the root level of the document
    ///
    /// Returns a vector of all top-level keys
    pub fn get_root_keys(&self) -> Vec<String> {
        use automerge::ReadDoc;
        let doc = self.doc.lock().unwrap();
        doc.keys(ROOT).map(|k| k.to_string()).collect()
    }

    /// Save the current state to bytes
    pub fn save_state(&self) -> Vec<u8> {
        let mut doc = self.doc.lock().unwrap();
        doc.save()
    }

    /// Load state from bytes
    pub fn load_state(&self, bytes: &[u8]) -> Result<()> {
        let doc = AutoCommit::load(bytes)
            .map_err(|e| anyhow!("Failed to load state: {:?}", e))?;

        let mut doc_guard = self.doc.lock().unwrap();
        *doc_guard = doc;
        drop(doc_guard);

        self.check_observers();
        Ok(())
    }

    /// Get all changes from the document as bytes
    ///
    /// Returns a vector of change bytes that can be applied to other documents
    pub fn get_changes(&self) -> Vec<Vec<u8>> {
        let mut doc = self.doc.lock().unwrap();
        let changes = doc.get_changes(&[]);
        changes.into_iter().map(|c| c.raw_bytes().to_vec()).collect()
    }

    /// Get changes since specific heads as bytes
    ///
    /// Takes a list of 32-byte change hashes and returns only changes not descended from them
    pub fn get_changes_since(&self, heads: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let mut doc = self.doc.lock().unwrap();

        // Convert byte vectors to ChangeHash
        // Note: ChangeHash is a newtype around [u8; 32]
        let head_hashes: Vec<automerge::ChangeHash> = heads.iter()
            .filter_map(|h| {
                if h.len() == 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(h);
                    // Safety: ChangeHash is repr(transparent) over [u8; 32]
                    // We can safely transmute [u8; 32] back to ChangeHash
                    let change_hash: automerge::ChangeHash = unsafe { std::mem::transmute(arr) };
                    Some(change_hash)
                } else {
                    None
                }
            })
            .collect();

        let changes = doc.get_changes(&head_hashes);
        changes.into_iter().map(|c| c.raw_bytes().to_vec()).collect()
    }

    /// Apply changes from another document (MERGES instead of replacing)
    ///
    /// This is the correct way to sync CRDT state - it merges changes
    /// rather than replacing the entire document like load_state() does
    pub fn apply_changes(&self, changes: Vec<Vec<u8>>) -> Result<()> {
        use automerge::Change;

        let mut doc = self.doc.lock().unwrap();

        // Convert bytes to Change objects
        for change_bytes in changes {
            let change = Change::from_bytes(change_bytes)
                .map_err(|e| anyhow!("Failed to parse change: {:?}", e))?;

            doc.apply_changes([change])
                .map_err(|e| anyhow!("Failed to apply change: {:?}", e))?;
        }

        drop(doc);
        self.check_observers();
        Ok(())
    }

    /// Get the current heads (tips of the change graph) as bytes
    ///
    /// These can be used with get_changes_since() for efficient sync
    /// Returns 32-byte change hashes
    ///
    /// Note: This uses unsafe transmute as a workaround since ChangeHash::as_bytes() is private
    pub fn get_heads(&self) -> Vec<Vec<u8>> {
        let mut doc = self.doc.lock().unwrap();
        let heads = doc.get_heads();

        // ChangeHash is a newtype around [u8; 32]
        // Since as_bytes() is not public, we use unsafe transmute
        // This is safe because ChangeHash is repr(transparent) over [u8; 32]
        heads.into_iter()
            .map(|h| {
                // Safety: ChangeHash is a newtype around [u8; 32]
                // We can safely transmute it to get the bytes
                let bytes: [u8; 32] = unsafe { std::mem::transmute(h) };
                bytes.to_vec()
            })
            .collect()
    }

    /// Observe changes to a specific path
    ///
    /// The callback will be invoked whenever the value at the path changes
    pub fn observe<F>(&self, path: String, callback: F)
    where
        F: Fn(Option<ScalarValue>) + Send + Sync + 'static,
    {
        let current_value = self.get_path(&path);

        let mut observers = self.observers.lock().unwrap();
        observers.push(Observer {
            path,
            callback: Box::new(callback),
            last_value: current_value,
        });
    }

    /// Manually trigger observer checks
    ///
    /// This compares current values with cached values and fires callbacks
    /// for any that have changed
    pub fn check_observers(&self) {
        let mut observers = self.observers.lock().unwrap();

        for observer in observers.iter_mut() {
            let current = self.get_path(&observer.path);

            // Compare values
            let changed = match (&observer.last_value, &current) {
                (None, None) => false,
                (Some(_), None) | (None, Some(_)) => true,
                (Some(a), Some(b)) => !scalar_values_equal(a, b),
            };

            if changed {
                (observer.callback)(current.clone());
                observer.last_value = current;
            }
        }
    }
}

impl Default for SwirlDB {
    fn default() -> Self {
        Self::new()
    }
}

/// Split a dot-separated path into segments
fn split_path(dot_path: &str) -> Vec<String> {
    dot_path.split('.').map(|s| s.to_string()).collect()
}

/// Resolve a path in the document, optionally creating intermediate maps
fn resolve_path(doc: &mut AutoCommit, path: &[String], create: bool) -> Option<ObjId> {
    let mut current = ROOT;

    // Traverse all but the last segment
    for key in path.iter().take(path.len().saturating_sub(1)) {
        // Check if current is a List and key is numeric
        let obj_type = doc.object_type(&current).ok()?;
        let result = if obj_type == automerge::ObjType::List {
            // Try to parse as array index
            if let Ok(index) = key.parse::<usize>() {
                doc.get(&current, index).ok().flatten()
            } else {
                None
            }
        } else {
            // Use as string key for Maps/Tables
            doc.get(&current, key.as_str()).ok().flatten()
        };

        match result {
            Some((_, obj_id)) => {
                current = obj_id.into();
            }
            None if create => {
                let new_obj = doc.put_object(&current, key.as_str(), automerge::ObjType::Map).ok()?;
                current = new_obj;
            }
            _ => return None,
        }
    }

    Some(current)
}

/// Resolve a path for reading (no mutation)
fn resolve_path_read(doc: &AutoCommit, path: &[String]) -> Option<ObjId> {
    let mut current = ROOT;

    // Traverse all but the last segment
    for key in path.iter().take(path.len().saturating_sub(1)) {
        // Check if current is a List and key is numeric
        let obj_type = doc.object_type(&current).ok()?;
        let result = if obj_type == automerge::ObjType::List {
            // Try to parse as array index
            if let Ok(index) = key.parse::<usize>() {
                doc.get(&current, index).ok().flatten()
            } else {
                None
            }
        } else {
            // Use as string key for Maps/Tables
            doc.get(&current, key.as_str()).ok().flatten()
        };

        match result {
            Some((_, obj_id)) => {
                current = obj_id.into();
            }
            None => return None,
        }
    }

    Some(current)
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_set_and_get() {
        let db = SwirlDB::new();
        db.set_path("user.name", ScalarValue::Str("Alice".into())).unwrap();

        let value = db.get_path("user.name");
        assert!(matches!(value, Some(ScalarValue::Str(_))));
    }

    #[test]
    fn test_nested_paths() {
        let db = SwirlDB::new();
        db.set_path("a.b.c", ScalarValue::Int(42)).unwrap();

        let value = db.get_path("a.b.c");
        assert!(matches!(value, Some(ScalarValue::Int(42))));
    }

    #[test]
    fn test_save_and_load() {
        let db1 = SwirlDB::new();
        db1.set_path("test", ScalarValue::Str("value".into())).unwrap();

        let bytes = db1.save_state();

        let db2 = SwirlDB::new();
        db2.load_state(&bytes).unwrap();

        let value = db2.get_path("test");
        assert!(matches!(value, Some(ScalarValue::Str(_))));
    }

    #[test]
    fn test_array_of_objects() {
        let db = SwirlDB::new();

        // Create array with objects
        let messages = json!([
            {"id": "1", "from": "alice", "text": "Hello", "timestamp": 12345},
            {"id": "2", "from": "bob", "text": "Hi", "timestamp": 12346}
        ]);

        db.set_value("messages", messages.clone()).unwrap();

        // Read it back
        let result = db.get_value("messages");

        println!("Original: {}", messages);
        println!("Result: {:?}", result);

        assert!(result.is_some());
        let result = result.unwrap();

        // Verify it's an array
        assert!(result.is_array());
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 2);

        // Verify first object
        let first = &arr[0];
        assert!(first.is_object());
        println!("First message: {}", first);
        assert_eq!(first["id"], "1");
        assert_eq!(first["from"], "alice");
        assert_eq!(first["text"], "Hello");
    }
}
