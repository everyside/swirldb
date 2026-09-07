// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

use crate::auth::{AnonymousAuth, AuthProvider};
use crate::paths::PathRegistry;
use crate::policy::{Action, PolicyEngine};
use anyhow::{anyhow, Result};
use automerge::patches::{Patch, PatchAction};
use automerge::{
    transaction::Transactable, AutoCommit, ChangeHash, LoadOptions, ObjId, ObjType, Prop, ReadDoc,
    ScalarValue, TextEncoding, Value as AutoValue, ROOT,
};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::sync::{Arc, Mutex, RwLock};

/// Storage adapter trait - all storage implementations implement this
///
/// This allows pluggable storage backends: in-memory, LocalStorage, IndexedDB, redb, etc.
// Use storage traits from the storage module
use crate::storage::DocumentStorage;

/// Notification passed to observers when a change occurs
#[derive(Clone, Debug)]
pub struct ChangeNotification {
    /// The new value at the observed path
    pub value: Option<ScalarValue>,
    /// List of field paths that changed (e.g., ["user.email", "user.profile.avatar"])
    pub changed_paths: Vec<String>,
}

/// Observer callback signature
pub type ObserverCallback = Box<dyn Fn(ChangeNotification) + Send + Sync>;

/// Observer entry tracking a path and its callback
struct Observer {
    path: String,
    callback: ObserverCallback,
    last_value: Option<ScalarValue>,
}

/// One edit to a text: `delete_count` units removed at `position`, then
/// `insert` put in their place. An insertion has `delete_count` zero, a
/// deletion an empty `insert`. Positions and counts are in the document's
/// [`TextEncoding`] — code points unless the instance was built with another —
/// so an editor that works in the same units can apply a splice as it is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TextSplice {
    pub position: usize,
    pub delete_count: usize,
    pub insert: String,
}

/// What happened to one text, handed to a text observer. The splices are in
/// the order they happened, each expressed against the text as the ones before
/// it left it. `local` is true when this instance made the edit itself, so an
/// editor that is the source of local edits can pass those by.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TextChange {
    pub path: String,
    pub splices: Vec<TextSplice>,
    pub local: bool,
}

/// Text observer callback signature
pub type TextObserverCallback = Box<dyn Fn(TextChange) + Send + Sync>;

struct TextObserver {
    path: String,
    callback: TextObserverCallback,
}

/// What applying changes did that a caller may want to pass on. Observers
/// registered on this instance are already told; this is for a layer that
/// keeps its own, such as the browser bindings.
#[derive(Clone, Debug, Default)]
pub struct Applied {
    /// Every text the changes edited, with the splices that edited it
    pub text_changes: Vec<TextChange>,
}

/// Core SwirlDB engine - pure Rust, platform-agnostic
///
/// This is the pure Rust core with no binding attributes.
/// It uses Arc<Mutex<>> for thread-safety and can be used
/// from both WASM and native targets.
pub struct SwirlDB {
    doc: Arc<Mutex<AutoCommit>>,
    observers: Arc<Mutex<Vec<Observer>>>,
    text_observers: Arc<Mutex<Vec<TextObserver>>>,
    /// The units text positions are counted in, fixed for the instance's life
    text_encoding: TextEncoding,
    storage: Arc<dyn DocumentStorage>,
    storage_key: String,
    auto_persist: bool,
    policy_engine: Option<Arc<PolicyEngine>>,
    auth_provider: Arc<Mutex<Box<dyn AuthProvider + Send + Sync>>>,
    path_registry: Arc<RwLock<PathRegistry>>,
}

impl SwirlDB {
    /// Create a new SwirlDB instance with default in-memory storage
    pub fn new() -> Self {
        Self::new_with_text_encoding(TextEncoding::UnicodeCodePoint)
    }

    /// Create an in-memory instance whose text positions are counted in the
    /// given units.
    ///
    /// The encoding is a property of this instance's view, not of the
    /// document: two peers may hold the same text under different encodings
    /// and every splice still merges, because what is stored is characters
    /// and what the encoding decides is only how they are counted. A browser
    /// wants UTF-16 code units, which is what a JavaScript string index and
    /// an editor offset already are; Rust code is happier in code points.
    pub fn new_with_text_encoding(text_encoding: TextEncoding) -> Self {
        Self::build(
            AutoCommit::new_with_encoding(text_encoding),
            Arc::new(crate::storage::InMemoryDocStorage::new()),
            "default",
            text_encoding,
        )
    }

    fn build(
        doc: AutoCommit,
        storage: Arc<dyn DocumentStorage>,
        storage_key: &str,
        text_encoding: TextEncoding,
    ) -> Self {
        let path_registry =
            PathRegistry::from_document(&doc).unwrap_or_else(|_| PathRegistry::new());
        Self {
            doc: Arc::new(Mutex::new(doc)),
            observers: Arc::new(Mutex::new(Vec::new())),
            text_observers: Arc::new(Mutex::new(Vec::new())),
            text_encoding,
            storage,
            storage_key: storage_key.to_string(),
            auto_persist: false,
            policy_engine: None,
            auth_provider: Arc::new(Mutex::new(Box::new(AnonymousAuth::new()))),
            path_registry: Arc::new(RwLock::new(path_registry)),
        }
    }

    /// Load a saved document under this instance's text encoding.
    fn load_document(&self, bytes: &[u8]) -> Result<AutoCommit> {
        AutoCommit::load_with_options(bytes, LoadOptions::new().text_encoding(self.text_encoding))
            .map_err(|e| anyhow!("Failed to load state: {:?}", e))
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
        Self::with_storage_and_text_encoding(storage, storage_key, TextEncoding::UnicodeCodePoint)
            .await
    }

    /// Create a SwirlDB instance with a custom storage adapter and text
    /// encoding; see [`Self::new_with_text_encoding`] for what the encoding
    /// decides.
    pub async fn with_storage_and_text_encoding(
        storage: Arc<dyn DocumentStorage>,
        storage_key: &str,
        text_encoding: TextEncoding,
    ) -> Self {
        // Try to load existing state from storage
        let doc = match storage.load(storage_key).await {
            Ok(Some(bytes)) => AutoCommit::load_with_options(
                &bytes,
                LoadOptions::new().text_encoding(text_encoding),
            )
            .unwrap_or_else(|_| AutoCommit::new_with_encoding(text_encoding)),
            _ => AutoCommit::new_with_encoding(text_encoding),
        };
        Self::build(doc, storage, storage_key, text_encoding)
    }

    /// The units this instance counts text positions in.
    pub fn text_encoding(&self) -> TextEncoding {
        self.text_encoding
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
        let jwt_auth =
            JwtAuth::from_token(token).map_err(|e| anyhow!("JWT authentication failed: {}", e))?;
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
        let bytes = {
            let mut doc = self.doc.lock().unwrap();
            doc.save()
        };
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
        if let Some(resolved) = resolve_path(&mut doc, &segments, true) {
            let key = segments.last().unwrap();
            doc.put(&resolved.parent, key.as_str(), value)
                .map_err(|e| anyhow!("Failed to set value: {:?}", e))?;
            drop(doc); // Release lock before updating registry

            // Register any newly created intermediate objects
            if !resolved.created.is_empty() {
                let mut registry = self.path_registry.write().unwrap();
                for (obj_id, obj_path) in &resolved.created {
                    registry.register(
                        obj_id.clone(),
                        crate::paths::PathBuf::from_dot_path(obj_path),
                    );
                }
            }

            // Notify observers with the path that was changed
            self.check_observers_with_paths(vec![path.to_string()]);

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

            result.and_then(|(val, obj_id)| match val {
                // A text reads as the string it currently spells, so the one
                // get path serves scalars and texts alike.
                AutoValue::Object(ObjType::Text) => doc
                    .text(&obj_id)
                    .ok()
                    .map(|text| ScalarValue::Str(text.into())),
                other => other.into_scalar().ok(),
            })
        } else {
            None
        }
    }

    /// Put a text at the given dot-separated path, replacing whatever was there.
    ///
    /// A text is the one value two people can edit at once: it is a sequence
    /// of characters under Automerge, and a [`Self::splice_text`] into it
    /// merges with everyone else's rather than replacing theirs, which is what
    /// a string set with [`Self::set_path`] would do. It still reads as a
    /// string through [`Self::get_path`] and [`Self::get_value`].
    ///
    /// Replacing a text that already exists throws away edits others may be
    /// making to it at that moment, so this is for creating one, and
    /// [`Self::splice_text`] is for changing it.
    pub fn set_text(&self, path: &str, text: &str) -> Result<()> {
        self.check_policy(Action::Write, path)?;

        let segments = split_path(path);
        if segments.is_empty() {
            return Err(anyhow!("Empty path"));
        }

        let mut doc = self.doc.lock().unwrap();
        let Some(resolved) = resolve_path(&mut doc, &segments, true) else {
            return Err(anyhow!("Failed to resolve path: {}", path));
        };
        let key = segments.last().unwrap();

        // If a text was here before, its length is what a text observer must
        // be told was removed. Read it before the replacement makes it gone.
        let replaced_length = match doc.get(&resolved.parent, key.as_str()) {
            Ok(Some((AutoValue::Object(ObjType::Text), previous))) => doc.length(&previous),
            _ => 0,
        };

        let text_id = doc
            .put_object(&resolved.parent, key.as_str(), ObjType::Text)
            .map_err(|e| anyhow!("Failed to create text: {:?}", e))?;
        if !text.is_empty() {
            doc.splice_text(&text_id, 0, 0, text)
                .map_err(|e| anyhow!("Failed to fill text: {:?}", e))?;
        }
        let inserted_length = doc.length(&text_id);
        drop(doc);

        {
            let mut registry = self.path_registry.write().unwrap();
            for (obj_id, obj_path) in &resolved.created {
                registry.register(
                    obj_id.clone(),
                    crate::paths::PathBuf::from_dot_path(obj_path),
                );
            }
            registry.register(text_id, crate::paths::PathBuf::from_dot_path(path));
        }

        self.check_observers_with_paths(vec![path.to_string()]);
        if replaced_length > 0 || inserted_length > 0 {
            self.notify_text_observers(&[TextChange {
                path: path.to_string(),
                splices: vec![TextSplice {
                    position: 0,
                    delete_count: replaced_length,
                    insert: text.to_string(),
                }],
                local: true,
            }]);
        }
        Ok(())
    }

    /// The length of the text at a path in this instance's
    /// [`Self::text_encoding`] units, or `None` when the path holds no text.
    /// This is the bound a [`Self::splice_text`] position must stay within.
    pub fn text_length(&self, path: &str) -> Option<usize> {
        if self.check_policy(Action::Read, path).is_err() {
            return None;
        }
        let doc = self.doc.lock().unwrap();
        text_object_at(&*doc, path).map(|id| doc.length(&id))
    }

    /// Edit the text at a path: remove `delete_count` units at `position`,
    /// then insert `insert` there. Units are this instance's
    /// [`Self::text_encoding`]. Fails when the path does not hold a text.
    pub fn splice_text(
        &self,
        path: &str,
        position: usize,
        delete_count: usize,
        insert: &str,
    ) -> Result<()> {
        self.check_policy(Action::Write, path)?;

        let mut doc = self.doc.lock().unwrap();
        let Some(text_id) = text_object_at(&*doc, path) else {
            return Err(anyhow!("{} is not a text; create one with set_text", path));
        };
        let length = doc.length(&text_id);
        if position > length || position + delete_count > length {
            return Err(anyhow!(
                "Splice at {} deleting {} is outside a text of length {}",
                position,
                delete_count,
                length
            ));
        }
        doc.splice_text(&text_id, position, delete_count as isize, insert)
            .map_err(|e| anyhow!("Failed to splice text: {:?}", e))?;
        drop(doc);

        self.check_observers_with_paths(vec![path.to_string()]);
        self.notify_text_observers(&[TextChange {
            path: path.to_string(),
            splices: vec![TextSplice {
                position,
                delete_count,
                insert: insert.to_string(),
            }],
            local: true,
        }]);
        Ok(())
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
        if let Some(resolved) = resolve_path(&mut doc, &segments, true) {
            let key = segments.last().unwrap();
            self.insert_value(&mut doc, &resolved.parent, key, &value)?;
            drop(doc);

            // Register any newly created intermediate objects
            if !resolved.created.is_empty() {
                let mut registry = self.path_registry.write().unwrap();
                for (obj_id, obj_path) in &resolved.created {
                    registry.register(
                        obj_id.clone(),
                        crate::paths::PathBuf::from_dot_path(obj_path),
                    );
                }
            }

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
    fn insert_value(
        &self,
        doc: &mut AutoCommit,
        parent: &ObjId,
        key: &str,
        value: &JsonValue,
    ) -> Result<()> {
        // Array Optimization: Smart diffing for existing optimized arrays
        if let JsonValue::Array(new_arr) = value {
            // Check if this is a record-like array that qualifies for optimization
            let should_optimize = new_arr.iter().all(|item| {
                if let JsonValue::Object(obj) = item {
                    obj.contains_key("id")
                        || obj.contains_key("timestamp")
                        || obj.contains_key("_key")
                } else {
                    false
                }
            });

            if should_optimize && !new_arr.is_empty() {
                // Check if there's already an optimized array (stored as a map) at this location
                if let Ok(Some((
                    automerge::Value::Object(automerge::ObjType::Map),
                    existing_obj_id,
                ))) = doc.get(parent, key)
                {
                    // Existing optimized array found - perform smart diff!
                    // Only sync changed/added/removed items instead of the entire array
                    self.update_array_as_map(doc, &existing_obj_id, new_arr)?;
                    return Ok(());
                }
                // Fall through to normal array-to-map conversion for new arrays
            }
        }

        match value {
            JsonValue::Null => {
                doc.put(parent, key, ScalarValue::Null)
                    .map_err(|e| anyhow!("Failed to set null: {:?}", e))?;
            }
            JsonValue::Bool(b) => {
                doc.put(parent, key, ScalarValue::Boolean(*b))
                    .map_err(|e| anyhow!("Failed to set boolean: {:?}", e))?;
            }
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
            }
            JsonValue::String(s) => {
                doc.put(parent, key, ScalarValue::Str(s.clone().into()))
                    .map_err(|e| anyhow!("Failed to set string: {:?}", e))?;
            }
            JsonValue::Array(arr) => {
                // Check if array contains record-like objects (optimize for CRDT efficiency)
                let should_convert_to_map = arr.iter().all(|item| {
                    if let JsonValue::Object(obj) = item {
                        // Has id, timestamp, or _key field
                        obj.contains_key("id")
                            || obj.contains_key("timestamp")
                            || obj.contains_key("_key")
                    } else {
                        false
                    }
                });

                if should_convert_to_map && !arr.is_empty() {
                    // Store as Map with stable keys for efficient incremental sync
                    let map_id = doc
                        .put_object(parent, key, ObjType::Map)
                        .map_err(|e| anyhow!("Failed to create map: {:?}", e))?;

                    for item in arr.iter() {
                        if let JsonValue::Object(obj) = item {
                            // Extract or generate stable key
                            let item_key = if let Some(id) = obj.get("id").and_then(|v| v.as_str())
                            {
                                id.to_string()
                            } else if let Some(key) = obj.get("_key").and_then(|v| v.as_str()) {
                                key.to_string()
                            } else if let Some(ts) = obj.get("timestamp").and_then(|v| v.as_i64()) {
                                // Generate key from timestamp + random suffix
                                use std::time::{SystemTime, UNIX_EPOCH};
                                let nanos = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .subsec_nanos();
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
                                item_with_key.insert(
                                    "_key".to_string(),
                                    JsonValue::String(item_key.clone()),
                                );
                            }

                            // Insert as map entry
                            self.insert_value(
                                doc,
                                &map_id,
                                &item_key,
                                &JsonValue::Object(item_with_key),
                            )?;
                        }
                    }
                } else {
                    // Regular array: store as List
                    let list_id = doc
                        .put_object(parent, key, ObjType::List)
                        .map_err(|e| anyhow!("Failed to create list: {:?}", e))?;
                    for (i, item) in arr.iter().enumerate() {
                        self.insert_value_at_index(doc, &list_id, i, item)?;
                    }
                }
            }
            JsonValue::Object(obj) => {
                let map_id = doc
                    .put_object(parent, key, ObjType::Map)
                    .map_err(|e| anyhow!("Failed to create map: {:?}", e))?;
                for (k, v) in obj.iter() {
                    self.insert_value(doc, &map_id, k, v)?;
                }
            }
        }
        Ok(())
    }

    /// Insert a JSON value at a specific index in an Automerge list
    fn insert_value_at_index(
        &self,
        doc: &mut AutoCommit,
        list_id: &ObjId,
        index: usize,
        value: &JsonValue,
    ) -> Result<()> {
        match value {
            JsonValue::Null => {
                doc.insert(list_id, index, ScalarValue::Null)
                    .map_err(|e| anyhow!("Failed to insert null: {:?}", e))?;
            }
            JsonValue::Bool(b) => {
                doc.insert(list_id, index, ScalarValue::Boolean(*b))
                    .map_err(|e| anyhow!("Failed to insert boolean: {:?}", e))?;
            }
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
            }
            JsonValue::String(s) => {
                doc.insert(list_id, index, ScalarValue::Str(s.clone().into()))
                    .map_err(|e| anyhow!("Failed to insert string: {:?}", e))?;
            }
            JsonValue::Array(arr) => {
                let nested_list = doc
                    .insert_object(list_id, index, ObjType::List)
                    .map_err(|e| anyhow!("Failed to insert list: {:?}", e))?;
                for (i, item) in arr.iter().enumerate() {
                    self.insert_value_at_index(doc, &nested_list, i, item)?;
                }
            }
            JsonValue::Object(obj) => {
                let nested_map = doc
                    .insert_object(list_id, index, ObjType::Map)
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
    fn update_array_as_map(
        &self,
        doc: &mut AutoCommit,
        map_obj_id: &ObjId,
        new_arr: &[JsonValue],
    ) -> Result<()> {
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
                    let nanos = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .subsec_nanos();
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
                let needs_update = if let Ok(Some((existing_val, existing_obj_id))) =
                    doc.get(map_obj_id, item_key.as_str())
                {
                    // Compare JSON values
                    let existing_json =
                        self.automerge_to_json(doc, &existing_val, &existing_obj_id);
                    existing_json != JsonValue::Object(item_with_key.clone())
                } else {
                    // Item doesn't exist, needs insert
                    true
                };

                if needs_update {
                    // Update or insert the item
                    self.insert_value(
                        doc,
                        map_obj_id,
                        item_key,
                        &JsonValue::Object(item_with_key),
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Convert an Automerge value to JSON
    ///
    /// The obj_id parameter is the ID of the object if value is Value::Object,
    /// and comes from the second element of the tuple returned by doc.get()
    #[allow(clippy::only_used_in_recursion)]
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
                    ScalarValue::F64(f) => serde_json::Number::from_f64(*f)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null),
                    ScalarValue::Str(s) => JsonValue::String(s.to_string()),
                    ScalarValue::Bytes(b) => {
                        // Convert bytes to base64 string
                        use base64::Engine;
                        JsonValue::String(base64::engine::general_purpose::STANDARD.encode(b))
                    }
                    ScalarValue::Timestamp(ts) => JsonValue::Number((*ts).into()),
                    ScalarValue::Counter(_c) => {
                        // Counter is a specialized CRDT type - for now just convert to null
                        JsonValue::Null
                    }
                    _ => JsonValue::Null,
                }
            }
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
                                    self.automerge_to_json(doc, &val, &nested_obj_id),
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
                            let mut items: Vec<JsonValue> = json_obj
                                .into_values()
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
                    }
                    automerge::ObjType::Text => {
                        JsonValue::String(doc.text(obj_id).unwrap_or_default())
                    }
                    automerge::ObjType::List => {
                        let mut json_arr = Vec::new();
                        // Use obj_id directly - it's the ID of this list
                        let len = doc.length(obj_id);
                        for i in 0..len {
                            if let Ok(Some((val, nested_obj_id))) = doc.get(obj_id, i) {
                                json_arr.push(self.automerge_to_json(doc, &val, &nested_obj_id));
                            }
                        }
                        JsonValue::Array(json_arr)
                    }
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

    /// Load state from bytes (REPLACES the current document)
    ///
    /// A text observer whose text differs between the old document and the
    /// new one is told the whole text was replaced: one splice deleting all
    /// of the old and inserting all of the new. Nothing finer is honest here,
    /// because the new document need not share any history with the old.
    pub fn load_state(&self, bytes: &[u8]) -> Result<Applied> {
        let doc = self.load_document(bytes)?;

        // Rebuild path registry from new document
        let registry = PathRegistry::from_document(&doc).unwrap_or_else(|_| PathRegistry::new());

        let observed_paths: Vec<String> = self
            .text_observers
            .lock()
            .unwrap()
            .iter()
            .map(|observer| observer.path.clone())
            .collect();

        let mut doc_guard = self.doc.lock().unwrap();
        let before: Vec<(String, Option<(usize, String)>)> = observed_paths
            .iter()
            .map(|path| (path.clone(), text_and_length_at(&*doc_guard, path)))
            .collect();
        *doc_guard = doc;
        let text_changes: Vec<TextChange> = before
            .into_iter()
            .filter_map(|(path, old)| {
                let new = text_and_length_at(&*doc_guard, &path);
                let old_text = old.as_ref().map(|(_, text)| text.as_str());
                let new_text = new.as_ref().map(|(_, text)| text.as_str());
                if old_text == new_text {
                    return None;
                }
                Some(TextChange {
                    path,
                    splices: vec![TextSplice {
                        position: 0,
                        delete_count: old.map(|(length, _)| length).unwrap_or(0),
                        insert: new.map(|(_, text)| text).unwrap_or_default(),
                    }],
                    local: false,
                })
            })
            .collect();
        drop(doc_guard);

        let mut registry_guard = self.path_registry.write().unwrap();
        *registry_guard = registry;
        drop(registry_guard);

        // Document replaced - all paths potentially changed
        self.check_observers_with_paths(vec![]);
        self.notify_text_observers(&text_changes);
        Ok(Applied { text_changes })
    }

    /// Get all changes from the document as bytes
    ///
    /// Returns a vector of change bytes that can be applied to other documents
    pub fn get_changes(&self) -> Vec<Vec<u8>> {
        let mut doc = self.doc.lock().unwrap();
        let changes = doc.get_changes(&[]);
        changes
            .into_iter()
            .map(|c| c.raw_bytes().to_vec())
            .collect()
    }

    /// Get changes since specific heads as bytes
    ///
    /// Takes a list of 32-byte change hashes and returns only changes not descended from them
    pub fn get_changes_since(&self, heads: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let mut doc = self.doc.lock().unwrap();

        // Convert byte vectors to ChangeHash
        // Note: ChangeHash is a newtype around [u8; 32]
        let head_hashes: Vec<automerge::ChangeHash> = heads
            .iter()
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
        changes
            .into_iter()
            .map(|c| c.raw_bytes().to_vec())
            .collect()
    }

    /// Apply changes from another document (MERGES instead of replacing)
    ///
    /// This is the correct way to sync CRDT state - it merges changes
    /// rather than replacing the entire document like load_state() does
    /// Extract affected paths from changes without applying them
    ///
    /// This is useful for determining which paths will be affected before broadcasting
    pub fn extract_affected_paths(&self, changes: &[Vec<u8>]) -> Result<Vec<String>> {
        use crate::paths::PathExtractor;
        use automerge::{Automerge, Change};

        // Fork the current document to apply changes for path extraction
        // This ensures we only extract paths for actual changes, not parent object creation
        let saved_bytes = {
            let mut doc = self.doc.lock().unwrap();
            doc.save()
        };
        let mut temp_doc = Automerge::load(&saved_bytes)
            .map_err(|e| anyhow!("Failed to clone document: {:?}", e))?;

        // Get registry - clone the PathRegistry itself
        // Note: The registry isn't actually used for patch extraction since patches
        // contain their full paths, but PathExtractor requires it
        let registry = {
            let guard = self.path_registry.read().unwrap();
            (*guard).clone()
        };
        let extractor = PathExtractor::new(registry);

        let mut all_paths = Vec::new();

        // Apply each change and extract paths from patches
        for change_bytes in changes {
            let change = Change::from_bytes(change_bytes.clone())
                .map_err(|e| anyhow!("Failed to parse change: {:?}", e))?;

            // Create patch log and apply change with patches
            let mut patch_log = automerge::patches::PatchLog::active(
                automerge::patches::TextRepresentation::String(
                    automerge::TextEncoding::UnicodeCodePoint,
                ),
            );

            temp_doc
                .apply_changes_log_patches([change], &mut patch_log)
                .map_err(|e| anyhow!("Failed to apply change: {:?}", e))?;

            // Extract paths from patches
            let patches = temp_doc.make_patches(&mut patch_log);
            let paths = extractor.extract_paths_from_patches(&temp_doc, &patches)?;
            all_paths.extend(paths);
        }

        // Deduplicate and sort
        all_paths.sort();
        all_paths.dedup();

        Ok(all_paths)
    }

    pub fn apply_changes(&self, changes: Vec<Vec<u8>>) -> Result<Applied> {
        use automerge::Change;

        let mut doc = self.doc.lock().unwrap();

        // Automerge can log patches while changes are applied and hand them
        // back afterwards. The cursor is set right before applying so the log
        // holds only what these changes did, and cleared right after so no
        // index is kept up between calls.
        let before = doc.get_heads();
        doc.update_diff_cursor();
        let applied = (|| {
            for change_bytes in changes {
                let change = Change::from_bytes(change_bytes)
                    .map_err(|e| anyhow!("Failed to parse change: {:?}", e))?;
                doc.apply_changes([change])
                    .map_err(|e| anyhow!("Failed to apply change: {:?}", e))?;
            }
            Ok::<(), anyhow::Error>(())
        })();
        let patches = doc.diff_incremental();
        doc.reset_diff_cursor();
        applied?;
        let text_changes = text_changes_from_patches(&doc, &before, &patches);

        // Rebuild path registry after applying changes
        let registry = PathRegistry::from_document(&*doc).unwrap_or_else(|_| PathRegistry::new());
        drop(doc);

        let mut registry_guard = self.path_registry.write().unwrap();
        *registry_guard = registry;
        drop(registry_guard);

        // Changes applied - paths may have changed
        self.check_observers_with_paths(vec![]);
        self.notify_text_observers(&text_changes);
        Ok(Applied { text_changes })
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
        heads
            .into_iter()
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
        F: Fn(ChangeNotification) + Send + Sync + 'static,
    {
        let current_value = self.get_path(&path);

        let mut observers = self.observers.lock().unwrap();
        observers.push(Observer {
            path,
            callback: Box::new(callback),
            last_value: current_value,
        });
    }

    /// Observe edits to the text at a path
    ///
    /// Where [`Self::observe`] hands an observer the new value, this hands it
    /// the edits: each [`TextChange`] carries the splices that happened, with
    /// positions, so an editor can apply them to what it is showing rather
    /// than replace it. Fires for this instance's own [`Self::splice_text`]
    /// and [`Self::set_text`] (with `local` true) and for changes that arrive
    /// through [`Self::apply_changes`] and [`Self::load_state`].
    pub fn observe_text<F>(&self, path: String, callback: F)
    where
        F: Fn(TextChange) + Send + Sync + 'static,
    {
        self.text_observers.lock().unwrap().push(TextObserver {
            path,
            callback: Box::new(callback),
        });
    }

    fn notify_text_observers(&self, changes: &[TextChange]) {
        if changes.is_empty() {
            return;
        }
        let observers = self.text_observers.lock().unwrap();
        for change in changes {
            for observer in observers.iter().filter(|o| o.path == change.path) {
                (observer.callback)(change.clone());
            }
        }
    }

    /// Manually trigger observer checks
    ///
    /// This compares current values with cached values and fires callbacks
    /// for any that have changed
    pub fn check_observers(&self) {
        self.check_observers_with_paths(vec![]);
    }

    /// Check observers and notify with specific changed paths
    fn check_observers_with_paths(&self, changed_paths: Vec<String>) {
        let mut observers = self.observers.lock().unwrap();

        for observer in observers.iter_mut() {
            let current = self.get_path(&observer.path);

            // Check if value changed
            let value_changed = match (&observer.last_value, &current) {
                (None, None) => false,
                (Some(_), None) | (None, Some(_)) => true,
                (Some(a), Some(b)) => !scalar_values_equal(a, b),
            };

            // Early exit if no value change and we have changed_paths to check
            if !value_changed && !changed_paths.is_empty() {
                // Check if any changed path affects this observer
                let affects_observer = changed_paths.iter().any(|changed_path| {
                    // Observer fires if:
                    // 1. Exact match: changed_path == observer.path
                    // 2. Child changed: changed_path is under observer.path
                    changed_path == &observer.path
                        || changed_path.starts_with(&format!("{}.", observer.path))
                });

                if !affects_observer {
                    continue; // Skip this observer
                }
            }

            // Observer should fire
            let notification = ChangeNotification {
                value: current.clone(),
                changed_paths: changed_paths.clone(),
            };
            (observer.callback)(notification);
            observer.last_value = current;
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

/// Result of resolving a path, including any intermediate objects created
struct ResolvedPath {
    parent: ObjId,
    /// Newly created intermediate objects: (ObjId, dot-path string)
    created: Vec<(ObjId, String)>,
}

/// Resolve a path in the document, optionally creating intermediate maps
fn resolve_path(doc: &mut AutoCommit, path: &[String], create: bool) -> Option<ResolvedPath> {
    let mut current = ROOT;
    let mut created = Vec::new();

    // Traverse all but the last segment
    for (i, key) in path.iter().take(path.len().saturating_sub(1)).enumerate() {
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
                current = obj_id;
            }
            None if create => {
                let new_obj = doc
                    .put_object(&current, key.as_str(), automerge::ObjType::Map)
                    .ok()?;
                // Track the path for this newly created object
                let obj_path = path[..=i].join(".");
                created.push((new_obj.clone(), obj_path));
                current = new_obj;
            }
            _ => return None,
        }
    }

    Some(ResolvedPath {
        parent: current,
        created,
    })
}

/// Resolve a path for reading (no mutation)
fn resolve_path_read(doc: &AutoCommit, path: &[String]) -> Option<ObjId> {
    resolve_path_read_in(doc, path)
}

/// Resolve a path for reading in any readable document
fn resolve_path_read_in<D: ReadDoc>(doc: &D, path: &[String]) -> Option<ObjId> {
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
                current = obj_id;
            }
            None => return None,
        }
    }

    Some(current)
}

/// The text object at a dot path, if the path holds one.
fn text_object_at<D: ReadDoc>(doc: &D, path: &str) -> Option<ObjId> {
    let segments = split_path(path);
    if segments.is_empty() || (segments.len() == 1 && segments[0].is_empty()) {
        return None;
    }
    let parent = resolve_path_read_in(doc, &segments)?;
    let key = segments.last().unwrap();
    let found = if doc.object_type(&parent).ok()? == ObjType::List {
        doc.get(&parent, key.parse::<usize>().ok()?).ok()?
    } else {
        doc.get(&parent, key.as_str()).ok()?
    };
    match found {
        Some((AutoValue::Object(ObjType::Text), id)) => Some(id),
        _ => None,
    }
}

/// The text at a path with its length in the document's encoding units.
fn text_and_length_at<D: ReadDoc>(doc: &D, path: &str) -> Option<(usize, String)> {
    let id = text_object_at(doc, path)?;
    Some((doc.length(&id), doc.text(&id).ok()?))
}

/// The dot path of the object a patch touches, from the patch's own path.
fn patch_object_path(patch: &Patch) -> String {
    let mut path = crate::paths::PathBuf::new();
    for (_, prop) in &patch.path {
        match prop {
            Prop::Map(key) => path.push_key(key),
            Prop::Seq(index) => path.push_index(*index),
        }
    }
    path.to_string()
}

/// Turn the patches Automerge logged for a batch of remote changes into text
/// changes, one per text touched, in the order the edits happened.
///
/// Two patch shapes are edits: `SpliceText` inserts, and `DeleteSeq` on an
/// object that is a text deletes. A third is a replacement: a `PutMap` whose
/// new value is a text, or a `DeleteMap` that removes one. Those carry no
/// length, so the length of what was there is read from the document as it
/// stood before the changes, at `before`, and reported as one deletion; the
/// new text's content then follows as the `SpliceText` that filled it.
fn text_changes_from_patches(
    doc: &AutoCommit,
    before: &[ChangeHash],
    patches: &[Patch],
) -> Vec<TextChange> {
    let mut changes: Vec<TextChange> = Vec::new();
    let mut record = |path: String, splice: TextSplice| match changes.last_mut() {
        Some(change) if change.path == path => change.splices.push(splice),
        _ => changes.push(TextChange {
            path,
            splices: vec![splice],
            local: false,
        }),
    };

    for patch in patches {
        match &patch.action {
            PatchAction::SpliceText { index, value, .. } => {
                record(
                    patch_object_path(patch),
                    TextSplice {
                        position: *index,
                        delete_count: 0,
                        insert: value.make_string(),
                    },
                );
            }
            PatchAction::DeleteSeq { index, length }
                if doc.object_type(&patch.obj).ok() == Some(ObjType::Text) =>
            {
                record(
                    patch_object_path(patch),
                    TextSplice {
                        position: *index,
                        delete_count: *length,
                        insert: String::new(),
                    },
                );
            }
            PatchAction::PutMap { key, .. } | PatchAction::DeleteMap { key } => {
                let removed_length = match doc.get_at(&patch.obj, key.as_str(), before) {
                    Ok(Some((AutoValue::Object(ObjType::Text), previous))) => {
                        doc.length_at(&previous, before)
                    }
                    _ => continue,
                };
                let mut path = crate::paths::PathBuf::new();
                for (_, prop) in &patch.path {
                    match prop {
                        Prop::Map(key) => path.push_key(key),
                        Prop::Seq(index) => path.push_index(*index),
                    }
                }
                path.push_key(key.clone());
                record(
                    path.to_string(),
                    TextSplice {
                        position: 0,
                        delete_count: removed_length,
                        insert: String::new(),
                    },
                );
            }
            _ => {}
        }
    }
    changes
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
        db.set_path("user.name", ScalarValue::Str("Alice".into()))
            .unwrap();

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
        db1.set_path("test", ScalarValue::Str("value".into()))
            .unwrap();

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

    #[test]
    fn test_observer_receives_changed_paths() {
        use std::sync::{Arc, Mutex};

        let db = SwirlDB::new();
        let received_paths: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let paths_clone = received_paths.clone();

        // Observe the user path
        db.observe("user".to_string(), move |notification| {
            let mut paths = paths_clone.lock().unwrap();
            paths.extend(notification.changed_paths);
        });

        // Make a change
        db.set_path("user.name", ScalarValue::Str("Alice".into()))
            .unwrap();

        // Verify observer received the changed path
        let paths = received_paths.lock().unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], "user.name");
    }

    #[test]
    fn test_observer_receives_value_and_paths() {
        use std::sync::{Arc, Mutex};

        let db = SwirlDB::new();
        db.set_path("user.name", ScalarValue::Str("Alice".into()))
            .unwrap();

        let received: Arc<Mutex<Option<ChangeNotification>>> = Arc::new(Mutex::new(None));
        let received_clone = received.clone();

        // Observe the user.name path
        db.observe("user.name".to_string(), move |notification| {
            let mut recv = received_clone.lock().unwrap();
            *recv = Some(notification);
        });

        // Update the value
        db.set_path("user.name", ScalarValue::Str("Bob".into()))
            .unwrap();

        // Verify observer received both value and paths
        let recv = received.lock().unwrap();
        assert!(recv.is_some());
        let notification = recv.as_ref().unwrap();

        // Check value
        assert!(matches!(&notification.value, Some(ScalarValue::Str(s)) if s == "Bob"));

        // Check changed paths
        assert_eq!(notification.changed_paths.len(), 1);
        assert_eq!(notification.changed_paths[0], "user.name");
    }

    #[test]
    fn test_observer_with_nested_path_changes() {
        use std::sync::{Arc, Mutex};

        let db = SwirlDB::new();
        db.set_path("user.profile.name", ScalarValue::Str("Alice".into()))
            .unwrap();

        let received_paths: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let paths_clone = received_paths.clone();

        // Observe the whole user object
        db.observe("user".to_string(), move |notification| {
            let mut paths = paths_clone.lock().unwrap();
            paths.extend(notification.changed_paths.clone());
        });

        // Change a nested value
        db.set_path("user.profile.avatar", ScalarValue::Str("avatar.png".into()))
            .unwrap();

        // Verify observer received the changed path
        let paths = received_paths.lock().unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], "user.profile.avatar");
    }

    #[test]
    fn test_extract_affected_paths() {
        let sender = SwirlDB::new();

        // Make some changes on the "sender" side
        sender
            .set_path("user.name", ScalarValue::Str("Alice".into()))
            .unwrap();
        sender
            .set_path("user.email", ScalarValue::Str("alice@example.com".into()))
            .unwrap();

        // Get changes (as a client would send them to the server)
        let changes = sender.get_changes();

        // Extract paths from a fresh "server" db that doesn't have these changes yet
        // (mirrors production: server receives changes it hasn't seen)
        let server = SwirlDB::new();
        let paths = server.extract_affected_paths(&changes).unwrap();

        // Should include both leaf paths and intermediate objects
        // When we set "user.name", we create both "user" (map) and "user.name" (value)
        assert_eq!(paths.len(), 3);
        assert!(paths.contains(&"user".to_string()));
        assert!(paths.contains(&"user.name".to_string()));
        assert!(paths.contains(&"user.email".to_string()));
    }

    #[test]
    fn test_extract_affected_paths_deduplication() {
        let sender = SwirlDB::new();

        // Make multiple changes to the same path
        sender.set_path("counter", ScalarValue::Int(1)).unwrap();
        sender.set_path("counter", ScalarValue::Int(2)).unwrap();
        sender.set_path("counter", ScalarValue::Int(3)).unwrap();

        // Get changes
        let changes = sender.get_changes();

        // Extract paths from a fresh db (mirrors server receiving client changes)
        let server = SwirlDB::new();
        let paths = server.extract_affected_paths(&changes).unwrap();

        // Should only have one path, even though we changed it 3 times
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], "counter");
    }

    #[test]
    fn test_extract_affected_paths_nested() {
        let sender = SwirlDB::new();

        // Make nested changes
        sender
            .set_path("user.profile.name", ScalarValue::Str("Alice".into()))
            .unwrap();
        sender
            .set_path("user.profile.avatar", ScalarValue::Str("pic.png".into()))
            .unwrap();
        sender
            .set_path("user.settings.theme", ScalarValue::Str("dark".into()))
            .unwrap();

        // Get changes
        let changes = sender.get_changes();

        // Extract paths from a fresh db
        let server = SwirlDB::new();
        let paths = server.extract_affected_paths(&changes).unwrap();

        // Should detect all nested paths including intermediate objects
        // Creating "user.profile.name" creates: user, user.profile, user.profile.name
        // Creating "user.profile.avatar" creates: user.profile.avatar (user.profile already exists)
        // Creating "user.settings.theme" creates: user.settings, user.settings.theme (user already exists)
        assert!(paths.contains(&"user".to_string()));
        assert!(paths.contains(&"user.profile".to_string()));
        assert!(paths.contains(&"user.profile.name".to_string()));
        assert!(paths.contains(&"user.profile.avatar".to_string()));
        assert!(paths.contains(&"user.settings".to_string()));
        assert!(paths.contains(&"user.settings.theme".to_string()));
    }

    #[test]
    fn test_registry_updated_on_set_path() {
        let db = SwirlDB::new();

        // Set a nested path - this creates intermediate "user" and "profile" objects
        db.set_path("user.profile.name", ScalarValue::Str("Alice".into()))
            .unwrap();

        // Registry should know about the intermediate objects
        let registry = db.path_registry.read().unwrap();

        // Should be able to look up the intermediate paths
        assert!(
            registry.get_exid("user").is_some(),
            "registry should contain 'user'"
        );
        assert!(
            registry.get_exid("user.profile").is_some(),
            "registry should contain 'user.profile'"
        );
    }

    #[test]
    fn test_registry_not_duplicated_on_existing_path() {
        let db = SwirlDB::new();

        // Create the path
        db.set_path("user.name", ScalarValue::Str("Alice".into()))
            .unwrap();

        let count_after_first = db.path_registry.read().unwrap().len();

        // Set another value under same parent - should NOT create new intermediate
        db.set_path("user.email", ScalarValue::Str("alice@example.com".into()))
            .unwrap();

        let count_after_second = db.path_registry.read().unwrap().len();

        // Registry size shouldn't grow since "user" already existed
        assert_eq!(count_after_first, count_after_second);
    }

    #[test]
    fn test_text_reads_as_a_string_and_converts_to_json() {
        let db = SwirlDB::new();
        db.set_text("pattern.source", "hue = t").unwrap();

        assert_eq!(
            db.get_path("pattern.source"),
            Some(ScalarValue::Str("hue = t".into()))
        );
        assert_eq!(
            db.get_value("pattern"),
            Some(json!({ "source": "hue = t" }))
        );
    }

    #[test]
    fn test_splice_text_edits_in_place() {
        let db = SwirlDB::new();
        db.set_text("source", "hello world").unwrap();

        db.splice_text("source", 5, 0, ",").unwrap();
        db.splice_text("source", 7, 5, "there").unwrap();
        assert_eq!(
            db.get_path("source"),
            Some(ScalarValue::Str("hello, there".into()))
        );

        // Outside the text is an error, not a panic.
        assert!(db.splice_text("source", 13, 0, "!").is_err());
        assert!(db.splice_text("source", 10, 5, "").is_err());
        // A scalar string is not a text.
        db.set_path("name", ScalarValue::Str("Alice".into()))
            .unwrap();
        assert!(db.splice_text("name", 0, 0, "x").is_err());
        assert!(db.splice_text("missing", 0, 0, "x").is_err());
    }

    #[test]
    fn test_text_observer_receives_splices_with_positions() {
        use std::sync::{Arc, Mutex};

        let writer = SwirlDB::new();
        writer.set_text("source", "abc").unwrap();

        let reader = SwirlDB::new();
        reader.apply_changes(writer.get_changes()).unwrap();

        let seen: Arc<Mutex<Vec<TextChange>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        reader.observe_text("source".to_string(), move |change| {
            sink.lock().unwrap().push(change);
        });

        // Remote edits: an insertion and then a deletion, applied as one batch.
        let heads = writer.get_heads();
        writer.splice_text("source", 3, 0, "def").unwrap();
        writer.splice_text("source", 0, 2, "").unwrap();
        let applied = reader
            .apply_changes(writer.get_changes_since(&heads))
            .unwrap();

        let expected = vec![
            TextSplice {
                position: 3,
                delete_count: 0,
                insert: "def".to_string(),
            },
            TextSplice {
                position: 0,
                delete_count: 2,
                insert: String::new(),
            },
        ];
        assert_eq!(applied.text_changes.len(), 1);
        assert_eq!(applied.text_changes[0].path, "source");
        assert_eq!(applied.text_changes[0].splices, expected);
        assert!(!applied.text_changes[0].local);

        let seen_so_far = seen.lock().unwrap().clone();
        assert_eq!(seen_so_far.len(), 1);
        assert_eq!(seen_so_far[0].splices, expected);
        assert!(!seen_so_far[0].local);
        assert_eq!(
            reader.get_path("source"),
            Some(ScalarValue::Str("cdef".into()))
        );

        // A local edit is reported too, marked local.
        reader.splice_text("source", 4, 0, "!").unwrap();
        let seen_so_far = seen.lock().unwrap().clone();
        assert_eq!(seen_so_far.len(), 2);
        assert!(seen_so_far[1].local);
        assert_eq!(
            seen_so_far[1].splices,
            vec![TextSplice {
                position: 4,
                delete_count: 0,
                insert: "!".to_string()
            }]
        );

        // Edits to another text do not reach this observer.
        writer.set_text("other", "x").unwrap();
        reader.apply_changes(writer.get_changes()).unwrap();
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    #[test]
    fn test_text_observer_sees_a_replacement_as_delete_then_insert() {
        use std::sync::{Arc, Mutex};

        let writer = SwirlDB::new();
        writer.set_text("source", "old text").unwrap();
        let reader = SwirlDB::new();
        reader.apply_changes(writer.get_changes()).unwrap();

        let seen: Arc<Mutex<Vec<TextChange>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        reader.observe_text("source".to_string(), move |change| {
            sink.lock().unwrap().push(change);
        });

        let heads = writer.get_heads();
        writer.set_text("source", "new").unwrap();
        reader
            .apply_changes(writer.get_changes_since(&heads))
            .unwrap();

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].splices,
            vec![
                TextSplice {
                    position: 0,
                    delete_count: 8,
                    insert: String::new()
                },
                TextSplice {
                    position: 0,
                    delete_count: 0,
                    insert: "new".to_string()
                },
            ]
        );
        assert_eq!(
            reader.get_path("source"),
            Some(ScalarValue::Str("new".into()))
        );
    }

    #[test]
    fn test_concurrent_splices_both_survive() {
        let alice = SwirlDB::new();
        alice.set_text("source", "the cat").unwrap();
        let bob = SwirlDB::new();
        bob.apply_changes(alice.get_changes()).unwrap();
        let common = alice.get_heads();

        // Each types at a different place without seeing the other.
        alice.splice_text("source", 4, 0, "black ").unwrap();
        bob.splice_text("source", 7, 0, " sat").unwrap();

        bob.apply_changes(alice.get_changes_since(&common)).unwrap();
        alice.apply_changes(bob.get_changes_since(&common)).unwrap();

        assert_eq!(
            alice.get_path("source"),
            Some(ScalarValue::Str("the black cat sat".into()))
        );
        assert_eq!(alice.get_path("source"), bob.get_path("source"));
    }

    #[test]
    fn test_text_positions_follow_the_instance_encoding() {
        // "é" is one code point and one UTF-16 unit; "😀" is one code point
        // and two UTF-16 units. The same document counts differently per view.
        let code_points = SwirlDB::new();
        code_points.set_text("source", "é😀x").unwrap();

        let utf16 = SwirlDB::new_with_text_encoding(TextEncoding::Utf16CodeUnit);
        utf16.apply_changes(code_points.get_changes()).unwrap();

        code_points.splice_text("source", 2, 1, "y").unwrap();
        assert_eq!(
            code_points.get_path("source"),
            Some(ScalarValue::Str("é😀y".into()))
        );

        let heads = utf16.get_heads();
        let applied = utf16
            .apply_changes(code_points.get_changes_since(&heads))
            .unwrap();
        // Under UTF-16 the same edit lands at unit 3, after the two of "😀".
        assert_eq!(applied.text_changes[0].splices[0].position, 3);
        assert_eq!(applied.text_changes[0].splices[1].position, 3);

        utf16.splice_text("source", 3, 1, "z").unwrap();
        assert_eq!(
            utf16.get_path("source"),
            Some(ScalarValue::Str("é😀z".into()))
        );
    }

    #[test]
    fn test_load_state_reports_a_text_as_replaced() {
        use std::sync::{Arc, Mutex};

        let db = SwirlDB::new();
        db.set_text("source", "before").unwrap();
        let seen: Arc<Mutex<Vec<TextChange>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        db.observe_text("source".to_string(), move |change| {
            sink.lock().unwrap().push(change);
        });

        let other = SwirlDB::new();
        other.set_text("source", "after").unwrap();
        let applied = db.load_state(&other.save_state()).unwrap();

        let expected = TextSplice {
            position: 0,
            delete_count: 6,
            insert: "after".to_string(),
        };
        assert_eq!(applied.text_changes[0].splices, vec![expected.clone()]);
        assert_eq!(seen.lock().unwrap()[0].splices, vec![expected]);
    }

    #[test]
    fn test_affected_paths_name_the_text_not_a_position() {
        let sender = SwirlDB::new();
        sender.set_text("pattern.source", "abc").unwrap();
        let receiver = SwirlDB::new();
        receiver.apply_changes(sender.get_changes()).unwrap();

        let heads = sender.get_heads();
        sender.splice_text("pattern.source", 1, 1, "").unwrap();
        sender.splice_text("pattern.source", 0, 0, "z").unwrap();
        let paths = receiver
            .extract_affected_paths(&sender.get_changes_since(&heads))
            .unwrap();
        assert_eq!(paths, vec!["pattern.source".to_string()]);
    }
}
