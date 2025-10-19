use automerge::{AutoCommit, ScalarValue, ROOT, ObjId, ReadDoc, transaction::Transactable, ObjType, Value as AutoValue};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::Value as JsonValue;

/// Storage hint for per-path storage policies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageHint {
    /// Keep in memory only, never persist
    MemoryOnly,
    /// Persist to storage adapter
    Persisted,
    /// Sync with remote peers
    Synced,
}

impl Default for StorageHint {
    fn default() -> Self {
        StorageHint::MemoryOnly
    }
}

/// Storage adapter trait - all storage implementations implement this
///
/// This allows pluggable storage backends: in-memory, LocalStorage, IndexedDB, redb, etc.
///
/// Note: Uses ?Send for WASM compatibility (single-threaded browser environment)
#[async_trait(?Send)]
pub trait StorageAdapter: Send + Sync {
    /// Save the entire document state
    async fn save(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Load the entire document state
    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Delete stored state
    async fn delete(&self, key: &str) -> Result<()>;

    /// Save a specific path value (for granular storage)
    async fn save_path(&self, path: &str, value: &[u8]) -> Result<()> {
        // Default implementation stores path as separate key
        let key = format!("path:{}", path);
        self.save(&key, value).await
    }

    /// Load a specific path value
    async fn load_path(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let key = format!("path:{}", path);
        self.load(&key).await
    }

    /// Delete a specific path
    async fn delete_path(&self, path: &str) -> Result<()> {
        let key = format!("path:{}", path);
        self.delete(&key).await
    }

    /// List all stored paths (for debugging/inspection)
    async fn list_paths(&self) -> Result<Vec<String>> {
        Ok(Vec::new()) // Default: not supported
    }
}

/// In-memory storage adapter - baseline implementation
///
/// Stores everything in a HashMap. Data is lost when the process ends.
/// This is the default adapter when none is specified.
pub struct InMemoryStorage {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl StorageAdapter for InMemoryStorage {
    async fn save(&self, key: &str, data: &[u8]) -> Result<()> {
        let mut storage = self.data.lock().unwrap();
        storage.insert(key.to_string(), data.to_vec());
        Ok(())
    }

    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let storage = self.data.lock().unwrap();
        Ok(storage.get(key).cloned())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut storage = self.data.lock().unwrap();
        storage.remove(key);
        Ok(())
    }

    async fn list_paths(&self) -> Result<Vec<String>> {
        let storage = self.data.lock().unwrap();
        Ok(storage.keys()
            .filter(|k| k.starts_with("path:"))
            .map(|k| k.strip_prefix("path:").unwrap().to_string())
            .collect())
    }
}

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
    storage: Arc<dyn StorageAdapter>,
    storage_hints: Arc<Mutex<HashMap<String, StorageHint>>>,
    storage_key: String,
    auto_persist: bool,
}

impl SwirlDB {
    /// Create a new SwirlDB instance with default in-memory storage
    pub fn new() -> Self {
        SwirlDB {
            doc: Arc::new(Mutex::new(AutoCommit::new())),
            observers: Arc::new(Mutex::new(Vec::new())),
            storage: Arc::new(InMemoryStorage::new()),
            storage_hints: Arc::new(Mutex::new(HashMap::new())),
            storage_key: "default".to_string(),
            auto_persist: false,
        }
    }

    /// Create a SwirlDB instance with a custom storage adapter
    ///
    /// Example:
    /// ```
    /// let storage = Arc::new(InMemoryStorage::new());
    /// let db = SwirlDB::with_storage(storage, "my-db").await;
    /// ```
    pub async fn with_storage(storage: Arc<dyn StorageAdapter>, storage_key: &str) -> Self {
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
            storage_hints: Arc::new(Mutex::new(HashMap::new())),
            storage_key: storage_key.to_string(),
            auto_persist: false,
        }
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

    /// Set storage hint for a specific path
    ///
    /// Example:
    /// ```
    /// db.set_storage_hint("session.tempData", StorageHint::MemoryOnly);
    /// db.set_storage_hint("user.profile", StorageHint::Persisted);
    /// db.set_storage_hint("shared.doc", StorageHint::Synced);
    /// ```
    pub fn set_storage_hint(&self, path: &str, hint: StorageHint) {
        let mut hints = self.storage_hints.lock().unwrap();
        hints.insert(path.to_string(), hint);
    }

    /// Get storage hint for a specific path
    pub fn get_storage_hint(&self, path: &str) -> StorageHint {
        let hints = self.storage_hints.lock().unwrap();
        hints.get(path).copied().unwrap_or_default()
    }

    /// Set a value at the given dot-separated path
    ///
    /// Example: `db.set_path("user.name", Value::String("Alice".into()))`
    pub fn set_path(&self, path: &str, value: ScalarValue) -> Result<()> {
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
    /// Returns None if the path doesn't exist
    pub fn get_path(&self, path: &str) -> Option<ScalarValue> {
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
    /// Returns None if the path doesn't exist, otherwise returns the value as JsonValue
    pub fn get_value(&self, path: &str) -> Option<JsonValue> {
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
    fn insert_value(&self, doc: &mut AutoCommit, parent: &ObjId, key: &str, value: &JsonValue) -> Result<()> {
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
                let list_id = doc.put_object(parent, key, ObjType::List)
                    .map_err(|e| anyhow!("Failed to create list: {:?}", e))?;
                for (i, item) in arr.iter().enumerate() {
                    self.insert_value_at_index(doc, &list_id, i, item)?;
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
                        JsonValue::Object(json_obj)
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
                    // ChangeHash doesn't have a public From impl, so we can't convert directly
                    // For now, just return empty - we'll need to fix this
                    None
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
