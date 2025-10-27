// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

/// Browser-specific storage adapters
///
/// This module provides LocalStorage and IndexedDB adapters for browser environments

use web_sys::{window, Storage};
use anyhow::{Result, anyhow};
use swirldb_core::storage::{DocumentStorage, DocumentStorageMarker};
use async_trait::async_trait;

/// LocalStorage adapter for browser environments
///
/// Uses the browser's localStorage API to persist data.
/// Data is stored as base64-encoded strings.
pub struct LocalDocumentStorage {
    prefix: String,
}

impl LocalDocumentStorage {
    /// Create a new LocalStorage adapter with optional key prefix
    pub fn new(prefix: &str) -> Result<Self> {
        // Verify localStorage is available
        window()
            .and_then(|w| w.local_storage().ok().flatten())
            .ok_or_else(|| anyhow!("localStorage not available"))?;

        Ok(Self {
            prefix: prefix.to_string(),
        })
    }

    fn get_storage(&self) -> Result<Storage> {
        window()
            .and_then(|w| w.local_storage().ok().flatten())
            .ok_or_else(|| anyhow!("localStorage not available"))
    }

    fn make_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}:{}", self.prefix, key)
        }
    }

    fn bytes_to_base64(&self, bytes: &[u8]) -> String {
        // Use built-in base64 encoding
        use base64::{Engine as _, engine::general_purpose};
        general_purpose::STANDARD.encode(bytes)
    }

    fn base64_to_bytes(&self, base64: &str) -> Result<Vec<u8>> {
        use base64::{Engine as _, engine::general_purpose};
        general_purpose::STANDARD.decode(base64)
            .map_err(|e| anyhow!("Failed to decode base64: {}", e))
    }
}

impl DocumentStorageMarker for LocalDocumentStorage {}

#[async_trait(?Send)]
impl DocumentStorage for LocalDocumentStorage {
    async fn save(&self, key: &str, data: &[u8]) -> Result<()> {
        let storage = self.get_storage()?;
        let full_key = self.make_key(key);
        let base64 = self.bytes_to_base64(data);

        storage.set_item(&full_key, &base64)
            .map_err(|_| anyhow!("Failed to save to localStorage"))?;

        Ok(())
    }

    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let storage = self.get_storage()?;
        let full_key = self.make_key(key);

        match storage.get_item(&full_key) {
            Ok(Some(base64)) => {
                let bytes = self.base64_to_bytes(&base64)?;
                Ok(Some(bytes))
            }
            Ok(None) => Ok(None),
            Err(_) => Err(anyhow!("Failed to load from localStorage")),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let storage = self.get_storage()?;
        let full_key = self.make_key(key);

        storage.remove_item(&full_key)
            .map_err(|_| anyhow!("Failed to delete from localStorage"))?;

        Ok(())
    }

    async fn list_keys(&self) -> Result<Vec<String>> {
        let storage = self.get_storage()?;
        let mut paths = Vec::new();

        if let Ok(len) = storage.length() {
            for i in 0..len {
                if let Ok(Some(key)) = storage.key(i) {
                    if key.starts_with(&format!("{}:path:", self.prefix)) {
                        if let Some(path) = key.strip_prefix(&format!("{}:path:", self.prefix)) {
                            paths.push(path.to_string());
                        }
                    }
                }
            }
        }

        Ok(paths)
    }
}

/// IndexedDB adapter for browser environments
///
/// Uses the browser's IndexedDB API to persist data.
/// IndexedDB supports much larger storage (~50MB-1GB+) compared to localStorage (~5-10MB).
pub struct IndexedDBAdapter {
    db_name: String,
    // Note: We don't store IdbDatabase directly because it's not Send/Sync
    // Instead, we access it fresh on each operation
}

impl IndexedDBAdapter {
    /// Create a new IndexedDB adapter with the given database name
    pub async fn new(db_name: &str) -> Result<Self> {
        // Verify IndexedDB is available
        let _idb = Self::get_indexed_db()
            .ok_or_else(|| anyhow!("IndexedDB not available"))?;

        // Initialize the database
        let adapter = Self {
            db_name: db_name.to_string(),
        };

        adapter.init_db().await?;
        Ok(adapter)
    }

    fn get_indexed_db() -> Option<web_sys::IdbFactory> {
        window()?.indexed_db().ok()?
    }

    async fn init_db(&self) -> Result<()> {
        use wasm_bindgen::JsCast;

        let idb = Self::get_indexed_db()
            .ok_or_else(|| anyhow!("IndexedDB not available"))?;

        // Open database (version 1)
        let open_request = idb.open_with_u32(&self.db_name, 1)
            .map_err(|_| anyhow!("Failed to open IndexedDB"))?;

        // Set up upgrade handler
        let onupgradeneeded = wasm_bindgen::closure::Closure::wrap(Box::new(move |event: web_sys::IdbVersionChangeEvent| {
            if let Some(target) = event.target() {
                if let Ok(request) = target.dyn_into::<web_sys::IdbOpenDbRequest>() {
                    if let Ok(db_result) = request.result() {
                        if let Ok(db) = db_result.dyn_into::<web_sys::IdbDatabase>() {
                            // Create object store if it doesn't exist
                            let names = db.object_store_names();
                            let mut found = false;
                            for i in 0..names.length() {
                                if let Some(name) = names.get(i) {
                                    if name == "swirldb" {
                                        found = true;
                                        break;
                                    }
                                }
                            }
                            if !found {
                                let _ = db.create_object_store("swirldb");
                            }
                        }
                    }
                }
            }
        }) as Box<dyn FnMut(_)>);

        open_request.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));
        onupgradeneeded.forget();

        // Wait for the database to open using a promise wrapper
        Self::request_to_js_value(&open_request).await?;

        Ok(())
    }

    async fn get_db(&self) -> Result<web_sys::IdbDatabase> {
        use wasm_bindgen::JsCast;

        let idb = Self::get_indexed_db()
            .ok_or_else(|| anyhow!("IndexedDB not available"))?;

        let open_request = idb.open(&self.db_name)
            .map_err(|_| anyhow!("Failed to open IndexedDB"))?;

        let result = Self::request_to_js_value(&open_request).await?;

        result.dyn_into::<web_sys::IdbDatabase>()
            .map_err(|_| anyhow!("Invalid database result"))
    }

    // Helper to convert IdbRequest to JsValue via Promise
    //
    // This properly bridges IndexedDB's callback-based API to Rust async/await.
    // We create a Promise that resolves when the request succeeds or rejects when it fails.
    async fn request_to_js_value(request: &web_sys::IdbRequest) -> Result<wasm_bindgen::JsValue> {
        use wasm_bindgen::JsCast;
        use wasm_bindgen_futures::JsFuture;
        use std::rc::Rc;
        use std::cell::RefCell;

        // Clone the request so we can move it into closures
        let request_clone = request.clone();

        let promise = js_sys::Promise::new(&mut |resolve, reject| {
            // Use Rc<RefCell> to share mutable state between closures
            let resolve = Rc::new(RefCell::new(Some(resolve)));
            let reject = Rc::new(RefCell::new(Some(reject)));

            // Success handler
            {
                let resolve = Rc::clone(&resolve);
                let request = request_clone.clone();
                let onsuccess = wasm_bindgen::closure::Closure::wrap(Box::new(move |_event: web_sys::Event| {
                    if let Ok(result) = request.result() {
                        if let Some(resolve_fn) = resolve.borrow_mut().take() {
                            let _ = resolve_fn.call1(&wasm_bindgen::JsValue::NULL, &result);
                        }
                    }
                }) as Box<dyn FnMut(_)>);

                request_clone.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
                onsuccess.forget();
            }

            // Error handler
            {
                let reject = Rc::clone(&reject);
                let onerror = wasm_bindgen::closure::Closure::wrap(Box::new(move |_event: web_sys::Event| {
                    // IndexedDB error occurred - provide a descriptive message
                    let error_msg = "IndexedDB operation failed";
                    if let Some(reject_fn) = reject.borrow_mut().take() {
                        let _ = reject_fn.call1(&wasm_bindgen::JsValue::NULL, &wasm_bindgen::JsValue::from_str(error_msg));
                    }
                }) as Box<dyn FnMut(_)>);

                request_clone.set_onerror(Some(onerror.as_ref().unchecked_ref()));
                onerror.forget();
            }
        });

        JsFuture::from(promise).await
            .map_err(|e| {
                let error_msg = e.as_string().unwrap_or_else(|| "IndexedDB request failed".to_string());
                anyhow!(error_msg)
            })
    }

    fn bytes_to_js_array(&self, bytes: &[u8]) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(bytes)
    }

    fn js_array_to_bytes(&self, array: js_sys::Uint8Array) -> Vec<u8> {
        array.to_vec()
    }
}

impl DocumentStorageMarker for IndexedDBAdapter {}

#[async_trait(?Send)]
impl DocumentStorage for IndexedDBAdapter {
    async fn save(&self, key: &str, data: &[u8]) -> Result<()> {
        let db = self.get_db().await?;

        let transaction = db.transaction_with_str_and_mode(
            "swirldb",
            web_sys::IdbTransactionMode::Readwrite
        ).map_err(|_| anyhow!("Failed to create transaction"))?;

        let store = transaction.object_store("swirldb")
            .map_err(|_| anyhow!("Failed to get object store"))?;

        let js_array = self.bytes_to_js_array(data);
        let request = store.put_with_key(&js_array, &wasm_bindgen::JsValue::from_str(key))
            .map_err(|_| anyhow!("Failed to put data"))?;

        Self::request_to_js_value(&request).await?;

        Ok(())
    }

    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>> {
        use wasm_bindgen::JsCast;

        let db = self.get_db().await?;

        let transaction = db.transaction_with_str("swirldb")
            .map_err(|_| anyhow!("Failed to create transaction"))?;

        let store = transaction.object_store("swirldb")
            .map_err(|_| anyhow!("Failed to get object store"))?;

        let request = store.get(&wasm_bindgen::JsValue::from_str(key))
            .map_err(|_| anyhow!("Failed to get data"))?;

        let result = Self::request_to_js_value(&request).await?;

        if result.is_undefined() || result.is_null() {
            return Ok(None);
        }

        let array = result.dyn_into::<js_sys::Uint8Array>()
            .map_err(|_| anyhow!("Invalid data format in IndexedDB"))?;

        Ok(Some(self.js_array_to_bytes(array)))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let db = self.get_db().await?;

        let transaction = db.transaction_with_str_and_mode(
            "swirldb",
            web_sys::IdbTransactionMode::Readwrite
        ).map_err(|_| anyhow!("Failed to create transaction"))?;

        let store = transaction.object_store("swirldb")
            .map_err(|_| anyhow!("Failed to get object store"))?;

        let request = store.delete(&wasm_bindgen::JsValue::from_str(key))
            .map_err(|_| anyhow!("Failed to delete data"))?;

        Self::request_to_js_value(&request).await?;

        Ok(())
    }

    async fn list_keys(&self) -> Result<Vec<String>> {
        // TODO: Implement listing all keys with path: prefix
        // This requires iterating through all keys in the object store
        Ok(Vec::new())
    }
}
