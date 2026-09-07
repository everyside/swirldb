// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Browser bindings for SwirlDB.
//!
//! Two things are exposed to JavaScript. `SwirlDB` is one document: a local
//! Automerge history with the path API, observers and ephemeral handlers.
//! `Connection` is one WebSocket to a server, carrying any number of
//! documents: `openDocument(id)` asks the server for a document and resolves
//! to a `SwirlDB` handle on it, several at once over the same socket, each
//! synced whole and independently. `SwirlDB.connect(...)` — the original
//! single-document API — is a `Connection` with the default document opened
//! on it, so nothing written against it changes.
//!
//! Frames arriving on a connection are routed by the document id they carry
//! to the handle bound to that document; observers and ephemeral handlers are
//! registered per handle, so presence and cursors on one document never reach
//! another.

use automerge::ScalarValue;
use js_sys::{Function, Promise, Uint8Array};
use serde_wasm_bindgen::from_value;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use swirldb_core::core::SwirlDB as CoreSwirlDB;
use swirldb_core::policy::PolicyEngine;
use swirldb_core::protocol::{Access, Message, DEFAULT_DOCUMENT};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;
use web_sys::{BinaryType, CloseEvent, ErrorEvent, MessageEvent, WebSocket};

mod storage;
use storage::{IndexedDBAdapter, LocalDocumentStorage};

/// Size of an Automerge change hash.
const HEAD_SIZE: usize = 32;

thread_local! {
    /// Observers: (handle id, path, callback, last scalar value)
    #[allow(clippy::type_complexity)]
    static OBSERVERS: RefCell<Vec<(usize, String, Function, Option<ScalarValue>)>> = const { RefCell::new(Vec::new()) };
    static NEXT_ID: RefCell<usize> = const { RefCell::new(0) };
    /// Connections by connection id
    #[allow(clippy::missing_const_for_thread_local)]
    static CONNECTIONS: RefCell<HashMap<usize, ConnectionState>> = RefCell::new(HashMap::new());
    static NEXT_CONNECTION_ID: RefCell<usize> = const { RefCell::new(0) };
    /// Ephemeral message handlers: (handle id, handler id, path pattern, callback)
    #[allow(clippy::type_complexity)]
    static EPHEMERAL_HANDLERS: RefCell<Vec<(usize, u32, String, Function)>> = const { RefCell::new(Vec::new()) };
    static NEXT_HANDLER_ID: RefCell<u32> = const { RefCell::new(0) };
}

fn next_handle_id() -> usize {
    NEXT_ID.with(|next_id| {
        let id = *next_id.borrow();
        *next_id.borrow_mut() = id + 1;
        id
    })
}

/// A document open on a connection: the handle that holds it locally and
/// what the server has told us about it.
struct DocumentBinding {
    /// The `SwirlDB` handle's id, for routing observers and ephemeral handlers
    handle_id: usize,
    /// The handle's document, shared so frames can be applied to it
    core: Rc<CoreSwirlDB>,
    /// Heads the server last acknowledged, for delta pushes
    last_synced_heads: Vec<Vec<u8>>,
    /// What the server granted, once its SubscribeAck has arrived
    access: Option<Access>,
    /// An `openDocument` still waiting for its first Sync: resolve, reject,
    /// and the handle to resolve with
    waiting: Option<(Function, Function, JsValue)>,
}

/// One WebSocket, many documents.
struct ConnectionState {
    client_id: String,
    websocket: Option<WebSocket>,
    /// The socket has opened; frames go straight out rather than queueing
    socket_open: bool,
    /// The first document goes in `Connect`; every later one in `Open`
    sent_connect: bool,
    /// Frames written before the socket opened
    pending: Vec<Vec<u8>>,
    documents: HashMap<String, DocumentBinding>,
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
    fn new(client_id: String) -> Self {
        Self {
            client_id,
            websocket: None,
            socket_open: false,
            sent_connect: false,
            pending: Vec::new(),
            documents: HashMap::new(),
            onopen_closure: None,
            onmessage_closure: None,
            onclose_closure: None,
            onerror_closure: None,
        }
    }

    /// Send now if the socket is open, otherwise hold the frame until it is.
    fn send(&mut self, frame: Vec<u8>) {
        match (&self.websocket, self.socket_open) {
            (Some(ws), true) => {
                if let Err(e) = ws.send_with_u8_array(&frame) {
                    web_sys::console::error_1(&format!("Failed to send: {:?}", e).into());
                }
            }
            _ => self.pending.push(frame),
        }
    }

    /// The frame that opens a document on this connection.
    fn open_frame(
        &mut self,
        document: &str,
        subscriptions: Vec<String>,
        heads: Vec<u8>,
    ) -> Vec<u8> {
        if !self.sent_connect {
            self.sent_connect = true;
            Message::Connect {
                client_id: self.client_id.clone(),
                subscriptions,
                heads,
                document: document.to_string(),
            }
            .encode()
        } else {
            Message::Open {
                document: document.to_string(),
                subscriptions,
                heads,
            }
            .encode()
        }
    }
}

fn parse_heads(flat: &[u8]) -> Vec<Vec<u8>> {
    flat.as_chunks::<HEAD_SIZE>()
        .0
        .iter()
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn flatten_heads(heads: Vec<Vec<u8>>) -> Vec<u8> {
    heads.into_iter().flatten().collect()
}

/// Convert a core JSON value to a JavaScript value by way of JSON text, so
/// JavaScript receives plain objects and arrays.
fn json_to_js(value: Option<serde_json::Value>) -> JsValue {
    match value {
        Some(value) => js_sys::JSON::parse(&value.to_string()).unwrap_or(JsValue::NULL),
        None => JsValue::NULL,
    }
}

/// Fire observers for paths affected by remote changes
/// Only fires observers whose paths match the affected paths
fn fire_observers_for_paths(handle_id: usize, core: &CoreSwirlDB, affected_paths: &[String]) {
    // Collect first, call after: a callback may re-enter and register an observer.
    let callbacks: Vec<(Function, JsValue)> = OBSERVERS.with(|observers| {
        observers
            .borrow()
            .iter()
            .filter(|(id, path, _, _)| {
                *id == handle_id
                    && affected_paths.iter().any(|affected| {
                        // Match if observer path is a prefix of affected path, or vice versa
                        // e.g., observer="messages" matches affected="messages.msg_123"
                        // Also handles glob patterns like "**"
                        affected.starts_with(path.as_str())
                            || path.starts_with(affected.as_str())
                            || affected == "**"
                            || path == "**"
                    })
            })
            .map(|(_, path, callback, _)| (callback.clone(), json_to_js(core.get_value(path))))
            .collect()
    });
    for (callback, value) in callbacks {
        // Always fire for broadcast changes: the server sent changes, so it changed.
        let _ = callback.call1(&JsValue::NULL, &value);
    }
}

/// Fire all observers for a handle (used for Sync messages without affected_paths)
fn fire_all_observers(handle_id: usize, core: &CoreSwirlDB) {
    let callbacks: Vec<(Function, JsValue)> = OBSERVERS.with(|observers| {
        let mut observers = observers.borrow_mut();
        observers
            .iter_mut()
            .filter(|(id, _, _, _)| *id == handle_id)
            .map(|(_, path, callback, last_value)| {
                // Update last_value for change detection (use scalar for comparison)
                *last_value = core.get_path(path);
                (callback.clone(), json_to_js(core.get_value(path)))
            })
            .collect()
    });
    for (callback, value) in callbacks {
        let _ = callback.call1(&JsValue::NULL, &value);
    }
}

/// Fire ephemeral handlers for incoming ephemeral updates
/// Uses PathPatternMatcher to match handler patterns against update paths
fn fire_ephemeral_handlers(handle_id: usize, updates: &[(String, Vec<u8>)]) {
    use swirldb_core::policy::{Actor, PathPatternMatcher};

    let actor = Actor::anonymous();

    let handlers: Vec<(String, Function)> = EPHEMERAL_HANDLERS.with(|handlers| {
        handlers
            .borrow()
            .iter()
            .filter(|(id, _, _, _)| *id == handle_id)
            .map(|(_, _, pattern, callback)| (pattern.clone(), callback.clone()))
            .collect()
    });

    for (pattern, callback) in handlers {
        for (path, data) in updates {
            if PathPatternMatcher::matches(&pattern, path, &actor) {
                let js_path = JsValue::from(path.as_str());
                let js_data = Uint8Array::from(&data[..]);
                let _ = callback.call2(&JsValue::NULL, &js_path, &js_data.into());
            }
        }
    }
}

/// Route one server frame to the document it names.
///
/// The connection map is borrowed only long enough to find the binding and
/// take what is needed from it; the document is changed and callbacks are
/// called with the borrow released, because a callback may well call back in.
fn dispatch(connection_id: usize, msg: Message) {
    match msg {
        Message::Sync {
            heads,
            changes,
            document,
        } => {
            let found = CONNECTIONS.with(|connections| {
                let mut connections = connections.borrow_mut();
                let binding = connections
                    .get_mut(&connection_id)?
                    .documents
                    .get_mut(&document)?;
                binding.last_synced_heads = parse_heads(&heads);
                Some((
                    binding.handle_id,
                    binding.core.clone(),
                    binding.waiting.take(),
                ))
            });
            let Some((handle_id, core, waiting)) = found else {
                web_sys::console::warn_1(
                    &format!("Sync for {}, which is not open here", document).into(),
                );
                return;
            };
            if changes.is_empty() {
                web_sys::console::log_1(
                    &format!("📥 SYNC [{}]: already up to date", document).into(),
                );
            } else {
                let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
                web_sys::console::log_1(
                    &format!(
                        "📥 SYNC [{}]: {} changes ({} bytes) from server",
                        document,
                        changes.len(),
                        total_bytes
                    )
                    .into(),
                );
                if let Err(e) = core.apply_changes(changes) {
                    web_sys::console::error_1(&format!("Failed to apply changes: {}", e).into());
                } else {
                    fire_all_observers(handle_id, &core);
                }
            }
            if let Some((resolve, _reject, handle)) = waiting {
                let _ = resolve.call1(&JsValue::NULL, &handle);
            }
        }

        Message::Broadcast {
            from_client_id,
            changes,
            affected_paths,
            document,
        } => {
            let found = CONNECTIONS.with(|connections| {
                let connections = connections.borrow();
                let binding = connections.get(&connection_id)?.documents.get(&document)?;
                Some((binding.handle_id, binding.core.clone()))
            });
            let Some((handle_id, core)) = found else {
                return;
            };
            if changes.is_empty() {
                return;
            }
            let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
            web_sys::console::log_1(
                &format!(
                    "📥 RECV [{}]: {} changes ({} bytes) from {}",
                    document,
                    changes.len(),
                    total_bytes,
                    from_client_id
                )
                .into(),
            );
            if let Err(e) = core.apply_changes(changes) {
                web_sys::console::error_1(&format!("Failed to apply changes: {}", e).into());
            } else {
                fire_observers_for_paths(handle_id, &core, &affected_paths);
            }
        }

        Message::PushAck { heads, document } => {
            CONNECTIONS.with(|connections| {
                if let Some(binding) = connections
                    .borrow_mut()
                    .get_mut(&connection_id)
                    .and_then(|connection| connection.documents.get_mut(&document))
                {
                    binding.last_synced_heads = parse_heads(&heads);
                }
            });
        }

        Message::SubscribeAck {
            document, access, ..
        } => {
            CONNECTIONS.with(|connections| {
                if let Some(binding) = connections
                    .borrow_mut()
                    .get_mut(&connection_id)
                    .and_then(|connection| connection.documents.get_mut(&document))
                {
                    binding.access = Some(access);
                }
            });
        }

        Message::OpenDenied { document, reason } => {
            web_sys::console::warn_1(&format!("🚫 {}: {}", document, reason).into());
            let waiting = CONNECTIONS.with(|connections| {
                connections
                    .borrow_mut()
                    .get_mut(&connection_id)
                    .and_then(|connection| connection.documents.remove(&document))
                    .and_then(|binding| binding.waiting)
            });
            if let Some((_resolve, reject, _handle)) = waiting {
                let _ = reject.call1(&JsValue::NULL, &JsValue::from_str(&reason));
            }
        }

        Message::Ephemeral {
            path,
            data,
            document,
        } => {
            if let Some(handle_id) = handle_for(connection_id, &document) {
                fire_ephemeral_handlers(handle_id, &[(path, data)]);
            }
        }

        Message::EphemeralBatch { updates, document } => {
            if let Some(handle_id) = handle_for(connection_id, &document) {
                fire_ephemeral_handlers(handle_id, &updates);
            }
        }

        Message::Error { message } => {
            web_sys::console::error_1(&format!("Server error: {}", message).into());
        }

        _ => {}
    }
}

fn handle_for(connection_id: usize, document: &str) -> Option<usize> {
    CONNECTIONS.with(|connections| {
        connections
            .borrow()
            .get(&connection_id)?
            .documents
            .get(document)
            .map(|binding| binding.handle_id)
    })
}

/// Open a WebSocket and register the connection. Nothing is sent until a
/// document is opened on it; a connection exists to carry documents.
fn create_connection(url: &str, client_id: String) -> Result<usize, JsValue> {
    let ws = WebSocket::new(url)
        .map_err(|e| JsValue::from_str(&format!("Failed to create WebSocket: {:?}", e)))?;
    ws.set_binary_type(BinaryType::Arraybuffer);

    let connection_id = NEXT_CONNECTION_ID.with(|next| {
        let id = *next.borrow();
        *next.borrow_mut() = id + 1;
        id
    });

    let mut state = ConnectionState::new(client_id);

    let onopen = Closure::wrap(Box::new(move |_| {
        web_sys::console::log_1(&"✅ WebSocket connected".into());
        // Take the queue out, then send with the borrow released.
        let flush = CONNECTIONS.with(|connections| {
            let mut connections = connections.borrow_mut();
            let connection = connections.get_mut(&connection_id)?;
            connection.socket_open = true;
            Some((
                connection.websocket.clone()?,
                std::mem::take(&mut connection.pending),
            ))
        });
        if let Some((ws, frames)) = flush {
            for frame in frames {
                if let Err(e) = ws.send_with_u8_array(&frame) {
                    web_sys::console::error_1(&format!("Failed to send: {:?}", e).into());
                }
            }
        }
    }) as Box<dyn FnMut(web_sys::Event)>);
    ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));

    let onmessage = Closure::wrap(Box::new(move |event: MessageEvent| {
        if let Ok(array_buffer) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
            let bytes = js_sys::Uint8Array::new(&array_buffer).to_vec();
            match Message::decode(&bytes) {
                Ok(msg) => dispatch(connection_id, msg),
                Err(e) => {
                    web_sys::console::error_1(&format!("Failed to decode message: {}", e).into())
                }
            }
        }
    }) as Box<dyn FnMut(MessageEvent)>);
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

    let onclose = Closure::wrap(Box::new(move |_: CloseEvent| {
        web_sys::console::log_1(&"WebSocket closed".into());
        // Anything still waiting to open will never be answered.
        let waiting: Vec<Function> = CONNECTIONS.with(|connections| {
            connections
                .borrow_mut()
                .remove(&connection_id)
                .map(|connection| {
                    connection
                        .documents
                        .into_values()
                        .filter_map(|binding| binding.waiting.map(|(_, reject, _)| reject))
                        .collect()
                })
                .unwrap_or_default()
        });
        for reject in waiting {
            let _ = reject.call1(&JsValue::NULL, &JsValue::from_str("connection closed"));
        }
    }) as Box<dyn FnMut(CloseEvent)>);
    ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));

    let onerror = Closure::wrap(Box::new(move |e: ErrorEvent| {
        web_sys::console::error_1(&format!("WebSocket error: {:?}", e).into());
    }) as Box<dyn FnMut(ErrorEvent)>);
    ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));

    state.websocket = Some(ws);
    state.onopen_closure = Some(onopen);
    state.onmessage_closure = Some(onmessage);
    state.onclose_closure = Some(onclose);
    state.onerror_closure = Some(onerror);

    CONNECTIONS.with(|connections| {
        connections.borrow_mut().insert(connection_id, state);
    });

    Ok(connection_id)
}

/// Bind a handle to a document on a connection and send the frame that opens it.
fn open_on(
    connection_id: usize,
    document: &str,
    handle_id: usize,
    core: Rc<CoreSwirlDB>,
    subscriptions: Vec<String>,
    waiting: Option<(Function, Function, JsValue)>,
) -> Result<(), JsValue> {
    let heads = flatten_heads(core.get_heads());
    CONNECTIONS.with(|connections| {
        let mut connections = connections.borrow_mut();
        let connection = connections
            .get_mut(&connection_id)
            .ok_or_else(|| JsValue::from_str("Connection is closed"))?;
        if connection.documents.contains_key(document) {
            return Err(JsValue::from_str(&format!(
                "{} is already open on this connection",
                document
            )));
        }
        connection.documents.insert(
            document.to_string(),
            DocumentBinding {
                handle_id,
                core,
                last_synced_heads: Vec::new(),
                access: None,
                waiting,
            },
        );
        let frame = connection.open_frame(document, subscriptions, heads);
        connection.send(frame);
        Ok(())
    })
}

/// One WebSocket to a server, carrying any number of documents.
///
/// ```javascript
/// const connection = new Connection('ws://localhost:3030/ws', 'alice');
/// const pattern = await connection.openDocument('pattern.7');
/// const palette = await connection.openDocument('palette.3');
/// pattern.setPath('source', '...'); pattern.syncChanges();
/// ```
#[wasm_bindgen]
pub struct Connection {
    id: usize,
    client_id: String,
}

#[wasm_bindgen]
impl Connection {
    /// Open a socket to `url` as `client_id`. Nothing is sent until the first
    /// `openDocument`.
    #[wasm_bindgen(constructor)]
    pub fn new(url: String, client_id: String) -> Result<Connection, JsValue> {
        console_error_panic_hook::set_once();
        let id = create_connection(&url, client_id.clone())?;
        Ok(Connection { id, client_id })
    }

    /// Ask the server for a document. Resolves to a `SwirlDB` handle on it
    /// once its history has arrived; rejects with the server's reason when the
    /// authority refuses. `subscriptions` defaults to the whole document.
    #[wasm_bindgen(js_name = openDocument)]
    pub fn open_document(&self, document: String, subscriptions: Option<Vec<String>>) -> Promise {
        let connection_id = self.id;
        let subscriptions = subscriptions.unwrap_or_else(|| vec!["**".to_string()]);
        Promise::new(&mut |resolve: Function, reject: Function| {
            if document.is_empty() {
                let _ = reject.call1(
                    &JsValue::NULL,
                    &JsValue::from_str("Document id must not be empty"),
                );
                return;
            }
            let core = Rc::new(CoreSwirlDB::new());
            let handle_id = next_handle_id();
            let handle = JsValue::from(SwirlDB {
                core: core.clone(),
                id: handle_id,
                document: document.clone(),
                connection: Rc::new(Cell::new(Some(connection_id))),
            });
            if let Err(e) = open_on(
                connection_id,
                &document,
                handle_id,
                core,
                subscriptions.clone(),
                Some((resolve.clone(), reject.clone(), handle)),
            ) {
                let _ = reject.call1(&JsValue::NULL, &e);
            }
        })
    }

    /// Stop receiving a document. The socket stays open for the others.
    #[wasm_bindgen(js_name = closeDocument)]
    pub fn close_document(&self, document: String) {
        close_document_on(self.id, &document);
    }

    /// The documents currently open on this connection.
    #[wasm_bindgen(js_name = openDocuments)]
    pub fn open_documents(&self) -> Vec<String> {
        CONNECTIONS.with(|connections| {
            connections
                .borrow()
                .get(&self.id)
                .map(|connection| {
                    let mut ids: Vec<String> = connection.documents.keys().cloned().collect();
                    ids.sort();
                    ids
                })
                .unwrap_or_default()
        })
    }

    #[wasm_bindgen(js_name = clientId)]
    pub fn client_id(&self) -> String {
        self.client_id.clone()
    }

    /// Close the socket and every document on it.
    pub fn close(&self) {
        let ws = CONNECTIONS.with(|connections| {
            connections
                .borrow()
                .get(&self.id)
                .and_then(|connection| connection.websocket.clone())
        });
        if let Some(ws) = ws {
            let _ = ws.close();
        }
    }
}

fn close_document_on(connection_id: usize, document: &str) {
    CONNECTIONS.with(|connections| {
        let mut connections = connections.borrow_mut();
        if let Some(connection) = connections.get_mut(&connection_id) {
            if connection.documents.remove(document).is_some() {
                let frame = Message::Close {
                    document: document.to_string(),
                }
                .encode();
                connection.send(frame);
            }
        }
    });
}

/// One document, held locally.
///
/// This is a thin binding layer that delegates to the core implementation.
/// Constructed on its own it is the default document; `Connection.openDocument`
/// constructs handles on named documents.
#[wasm_bindgen]
pub struct SwirlDB {
    core: Rc<CoreSwirlDB>,
    id: usize,
    /// The document this handle is on
    document: String,
    /// The connection carrying it, once connected
    connection: Rc<Cell<Option<usize>>>,
}

impl SwirlDB {
    fn from_core(core: CoreSwirlDB) -> Self {
        Self {
            core: Rc::new(core),
            id: next_handle_id(),
            document: DEFAULT_DOCUMENT.to_string(),
            connection: Rc::new(Cell::new(None)),
        }
    }
}

#[wasm_bindgen]
impl SwirlDB {
    /// Create a new SwirlDB instance with default in-memory storage
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        console_error_panic_hook::set_once();
        Self::from_core(CoreSwirlDB::new())
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
            let storage = LocalDocumentStorage::new(&storage_key)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            let core = CoreSwirlDB::with_storage(Arc::new(storage), "db").await;
            Ok(JsValue::from(SwirlDB::from_core(core)))
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
            let storage = IndexedDBAdapter::new(&db_name)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            let core = CoreSwirlDB::with_storage(Arc::new(storage), "db").await;
            Ok(JsValue::from(SwirlDB::from_core(core)))
        })
    }

    /// The document this handle is on. `"default"` unless opened by name.
    #[wasm_bindgen(getter)]
    pub fn document(&self) -> String {
        self.document.clone()
    }

    /// What the server granted on this document: `"read"`, `"write"`, or
    /// `null` before the server has answered or when not connected.
    #[wasm_bindgen(getter)]
    pub fn access(&self) -> JsValue {
        let access = self.connection.get().and_then(|connection_id| {
            CONNECTIONS.with(|connections| {
                connections
                    .borrow()
                    .get(&connection_id)?
                    .documents
                    .get(&self.document)?
                    .access
            })
        });
        match access {
            Some(Access::Read) => JsValue::from_str("read"),
            Some(Access::Write) => JsValue::from_str("write"),
            None => JsValue::NULL,
        }
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
        json_to_js(self.core.get_value(&path))
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
        let change_vecs: Vec<Vec<u8>> = changes.into_iter().map(|arr| arr.to_vec()).collect();

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
        self.core
            .get_changes()
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
        let head_vecs: Vec<Vec<u8>> = heads.into_iter().map(|arr| arr.to_vec()).collect();

        self.core
            .get_changes_since(&head_vecs)
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
        let flat_bytes = flatten_heads(self.core.get_heads());
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
        self.core
            .get_heads()
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
            observers
                .borrow_mut()
                .push((self.id, path, callback, current_value));
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
    ///         path_pattern: "user.{actor.id}.**",
    ///         effect: "Allow"
    ///       }
    ///     ]
    ///   }
    /// });
    /// db.loadPolicyConfig(policyJson);
    /// ```
    #[wasm_bindgen(js_name = loadPolicyConfig)]
    pub fn load_policy_config(&self, json_str: String) -> Result<(), JsValue> {
        let _engine =
            PolicyEngine::from_json(&json_str).map_err(|e| JsValue::from_str(&e.to_string()))?;

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
        let engine =
            PolicyEngine::from_json(&json_str).map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(SwirlDB::from_core(CoreSwirlDB::new().with_policy(engine)))
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
        self.core
            .authenticate_jwt(&token)
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
            core.persist()
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Manually trigger observer checks
    #[wasm_bindgen(js_name = checkObservers)]
    pub fn check_observers(&self) {
        let handle_id = self.id;

        // Decide what changed under the borrow; call back with it released.
        let callbacks: Vec<(Function, JsValue)> = OBSERVERS.with(|observers| {
            let mut observers = observers.borrow_mut();
            let mut fired = Vec::new();
            for (id, path, callback, last_value) in observers.iter_mut() {
                if *id != handle_id {
                    continue;
                }

                let current_scalar = self.core.get_path(path);

                // Compare values (scalars only, for change detection)
                let changed = match (&*last_value, &current_scalar) {
                    (None, None) => false,
                    (Some(_), None) | (None, Some(_)) => true,
                    (Some(a), Some(b)) => !scalar_values_equal(a, b),
                };

                if changed {
                    // Full value (supports arrays/objects) for the callback
                    fired.push((callback.clone(), json_to_js(self.core.get_value(path))));
                    *last_value = current_scalar;
                }
            }
            fired
        });
        for (callback, value) in callbacks {
            let _ = callback.call1(&JsValue::NULL, &value);
        }
    }

    // ===== Protocol Methods =====

    /// Connect to a sync server and open this handle's document on the
    /// connection (managed internally).
    ///
    /// The WebSocket is managed entirely in WASM. TypeScript does not handle
    /// any protocol logic — just use the Proxy API for data access. This is
    /// the single-document API: one `SwirlDB`, one connection, the default
    /// document. For several documents on one connection use `Connection`.
    ///
    /// Example:
    /// ```javascript
    /// db.connect('ws://localhost:3030/ws', 'alice', ['**']);
    /// // That's it! Now mutations automatically sync:
    /// db.data.messages = [...messages, newMessage];
    /// ```
    #[wasm_bindgen(js_name = connect)]
    pub fn connect(
        &self,
        url: String,
        client_id: String,
        subscriptions: Vec<String>,
    ) -> Result<(), JsValue> {
        if self.connection.get().is_some() {
            return Err(JsValue::from_str("Already connected"));
        }
        let connection_id = create_connection(&url, client_id)?;
        open_on(
            connection_id,
            &self.document,
            self.id,
            self.core.clone(),
            subscriptions,
            None,
        )?;
        self.connection.set(Some(connection_id));
        Ok(())
    }

    /// Stop receiving this document. A handle from `Connection.openDocument`
    /// leaves the socket open for the connection's other documents; a handle
    /// that called `connect` closes its socket.
    pub fn close(&self) {
        let Some(connection_id) = self.connection.take() else {
            return;
        };
        close_document_on(connection_id, &self.document);
        let close_socket = CONNECTIONS.with(|connections| {
            connections
                .borrow()
                .get(&connection_id)
                .filter(|connection| connection.documents.is_empty())
                .and_then(|connection| connection.websocket.clone())
        });
        if let Some(ws) = close_socket {
            let _ = ws.close();
        }
    }

    /// Send local changes to server (called after mutations)
    #[wasm_bindgen(js_name = syncChanges)]
    pub fn sync_changes(&self) {
        let Some(connection_id) = self.connection.get() else {
            return;
        };
        let last_synced_heads = CONNECTIONS.with(|connections| {
            connections
                .borrow()
                .get(&connection_id)?
                .documents
                .get(&self.document)
                .map(|binding| binding.last_synced_heads.clone())
        });
        let Some(last_synced_heads) = last_synced_heads else {
            return;
        };

        // Get changes since last sync (incremental if we have heads)
        let changes = if last_synced_heads.is_empty() {
            self.core.get_changes()
        } else {
            self.core.get_changes_since(&last_synced_heads)
        };

        if changes.is_empty() {
            return;
        }

        let total_bytes: usize = changes.iter().map(|c| c.len()).sum();
        let sync_mode = if last_synced_heads.is_empty() {
            "full"
        } else {
            "delta"
        };
        web_sys::console::log_1(
            &format!(
                "📤 SEND [{}]: {} changes ({} bytes, {})",
                self.document,
                changes.len(),
                total_bytes,
                sync_mode
            )
            .into(),
        );

        let frame = Message::Push {
            heads: flatten_heads(self.core.get_heads()),
            changes,
            document: self.document.clone(),
        }
        .encode();
        self.send_frame(connection_id, frame);
    }

    fn send_frame(&self, connection_id: usize, frame: Vec<u8>) {
        CONNECTIONS.with(|connections| {
            if let Some(connection) = connections.borrow_mut().get_mut(&connection_id) {
                connection.send(frame);
            }
        });
    }

    fn connected(&self) -> Result<usize, JsValue> {
        self.connection
            .get()
            .ok_or_else(|| JsValue::from_str("Not connected"))
    }

    // ===== Ephemeral Methods =====

    /// Send an ephemeral message on this document (bypasses CRDT and storage,
    /// pure pub/sub). Presence and cursors go this way.
    ///
    /// Example:
    /// ```javascript
    /// const colorData = new Uint8Array([255, 0, 128, 255]); // RGBA
    /// db.sendEphemeral('fixtures.1.color', colorData);
    /// ```
    #[wasm_bindgen(js_name = sendEphemeral)]
    pub fn send_ephemeral(&self, path: String, data: &[u8]) -> Result<(), JsValue> {
        let connection_id = self.connected()?;
        let frame = Message::Ephemeral {
            path,
            data: data.to_vec(),
            document: self.document.clone(),
        }
        .encode();
        self.send_frame(connection_id, frame);
        Ok(())
    }

    /// Send a batch of ephemeral messages (single frame, lower overhead for 60fps updates)
    ///
    /// Takes an array of [path, Uint8Array] pairs.
    ///
    /// Example:
    /// ```javascript
    /// db.sendEphemeralBatch([
    ///   ['fixtures.1.color', new Uint8Array([255, 0, 0])],
    ///   ['fixtures.2.color', new Uint8Array([0, 255, 0])],
    ///   ['beat.bpm', new Uint8Array([0, 0, 0, 120])],
    /// ]);
    /// ```
    #[wasm_bindgen(js_name = sendEphemeralBatch)]
    pub fn send_ephemeral_batch(&self, updates: JsValue) -> Result<(), JsValue> {
        let array: js_sys::Array = updates
            .dyn_into()
            .map_err(|_| JsValue::from_str("Expected array of [path, data] pairs"))?;

        let mut update_vec = Vec::new();
        for i in 0..array.length() {
            let pair: js_sys::Array = array
                .get(i)
                .dyn_into()
                .map_err(|_| JsValue::from_str("Expected [path, data] pair"))?;
            let path = pair
                .get(0)
                .as_string()
                .ok_or_else(|| JsValue::from_str("Path must be a string"))?;
            let data_val = pair.get(1);
            let data_arr: Uint8Array = data_val
                .dyn_into()
                .map_err(|_| JsValue::from_str("Data must be a Uint8Array"))?;
            update_vec.push((path, data_arr.to_vec()));
        }

        let connection_id = self.connected()?;
        let frame = Message::EphemeralBatch {
            updates: update_vec,
            document: self.document.clone(),
        }
        .encode();
        self.send_frame(connection_id, frame);
        Ok(())
    }

    /// Register a handler for incoming ephemeral messages matching a path pattern
    ///
    /// Returns a handler ID that can be used to unregister later.
    /// The callback receives (path: string, data: Uint8Array) for each matching update.
    ///
    /// Example:
    /// ```javascript
    /// const handlerId = db.onEphemeral('fixtures.**', (path, data) => {
    ///   console.log('Got ephemeral:', path, data);
    /// });
    /// ```
    #[wasm_bindgen(js_name = onEphemeral)]
    pub fn on_ephemeral(&self, path_pattern: String, callback: Function) -> u32 {
        let handler_id = NEXT_HANDLER_ID.with(|id| {
            let current = *id.borrow();
            *id.borrow_mut() = current + 1;
            current
        });

        EPHEMERAL_HANDLERS.with(|handlers| {
            handlers
                .borrow_mut()
                .push((self.id, handler_id, path_pattern, callback));
        });

        handler_id
    }

    /// Remove an ephemeral handler by its ID
    #[wasm_bindgen(js_name = offEphemeral)]
    pub fn off_ephemeral(&self, handler_id: u32) {
        EPHEMERAL_HANDLERS.with(|handlers| {
            handlers
                .borrow_mut()
                .retain(|(_, id, _, _)| *id != handler_id);
        });
    }

    // ===== Manual Protocol Methods (for testing) =====

    /// Encode a Connect message (for manual WebSocket control)
    ///
    /// This is primarily for testing - normally use connect() instead.
    /// `document` defaults to this handle's document.
    #[wasm_bindgen(js_name = encodeConnectMessage)]
    pub fn encode_connect_message(
        &self,
        client_id: String,
        subscriptions: Vec<String>,
        heads: Uint8Array,
        document: Option<String>,
    ) -> Uint8Array {
        let msg = Message::Connect {
            client_id,
            subscriptions,
            heads: heads.to_vec(),
            document: document.unwrap_or_else(|| self.document.clone()),
        };
        let encoded = msg.encode();
        Uint8Array::from(&encoded[..])
    }

    /// Encode a Push message (for manual WebSocket control)
    ///
    /// This is primarily for testing - normally use syncChanges() instead.
    /// `document` defaults to this handle's document.
    #[wasm_bindgen(js_name = encodePushMessage)]
    pub fn encode_push_message(
        &self,
        heads: Uint8Array,
        changes: Vec<Uint8Array>,
        document: Option<String>,
    ) -> Uint8Array {
        let changes_vec: Vec<Vec<u8>> = changes.into_iter().map(|arr| arr.to_vec()).collect();
        let msg = Message::Push {
            heads: heads.to_vec(),
            changes: changes_vec,
            document: document.unwrap_or_else(|| self.document.clone()),
        };
        let encoded = msg.encode();
        Uint8Array::from(&encoded[..])
    }

    /// Decode a protocol message (for manual WebSocket control)
    ///
    /// Returns a JavaScript object with the message type and fields
    /// This is primarily for testing - normally use connect() instead
    #[wasm_bindgen(js_name = decodeMessage)]
    pub fn decode_message(&self, data: Uint8Array) -> Result<JsValue, JsValue> {
        let bytes = data.to_vec();
        let msg = Message::decode(&bytes)
            .map_err(|e| JsValue::from_str(&format!("Failed to decode message: {}", e)))?;
        message_to_js(msg)
    }
}

fn strings_to_array(strings: Vec<String>) -> js_sys::Array {
    strings.into_iter().map(JsValue::from).collect()
}

fn changes_to_array(changes: Vec<Vec<u8>>) -> js_sys::Array {
    changes
        .into_iter()
        .map(|c| Uint8Array::from(&c[..]))
        .map(JsValue::from)
        .collect()
}

fn updates_to_array(updates: Vec<(String, Vec<u8>)>) -> js_sys::Array {
    let array = js_sys::Array::new();
    for (path, data) in updates {
        let pair = js_sys::Array::new();
        pair.push(&JsValue::from(path));
        pair.push(&Uint8Array::from(&data[..]).into());
        array.push(&pair);
    }
    array
}

fn access_to_js(access: Access) -> JsValue {
    match access {
        Access::Read => JsValue::from_str("read"),
        Access::Write => JsValue::from_str("write"),
    }
}

/// Convert a Message to a JavaScript-friendly object
fn message_to_js(msg: Message) -> Result<JsValue, JsValue> {
    let obj = js_sys::Object::new();
    let set = |key: &str, value: &JsValue| js_sys::Reflect::set(&obj, &key.into(), value);
    match msg {
        Message::Connect {
            client_id,
            subscriptions,
            heads,
            document,
        } => {
            set("type", &"Connect".into())?;
            set("clientId", &client_id.into())?;
            set("subscriptions", &strings_to_array(subscriptions))?;
            set("heads", &Uint8Array::from(&heads[..]))?;
            set("document", &document.into())?;
        }
        Message::Open {
            document,
            subscriptions,
            heads,
        } => {
            set("type", &"Open".into())?;
            set("document", &document.into())?;
            set("subscriptions", &strings_to_array(subscriptions))?;
            set("heads", &Uint8Array::from(&heads[..]))?;
        }
        Message::Close { document } => {
            set("type", &"Close".into())?;
            set("document", &document.into())?;
        }
        Message::OpenDenied { document, reason } => {
            set("type", &"OpenDenied".into())?;
            set("document", &document.into())?;
            set("reason", &reason.into())?;
        }
        Message::SubscribeAck {
            added,
            denied,
            document,
            access,
        } => {
            set("type", &"SubscribeAck".into())?;
            set("added", &strings_to_array(added))?;
            set("denied", &strings_to_array(denied))?;
            set("document", &document.into())?;
            set("access", &access_to_js(access))?;
        }
        Message::Sync {
            heads,
            changes,
            document,
        } => {
            set("type", &"Sync".into())?;
            set("heads", &Uint8Array::from(&heads[..]))?;
            set("changes", &changes_to_array(changes))?;
            set("document", &document.into())?;
        }
        Message::Push {
            heads,
            changes,
            document,
        } => {
            set("type", &"Push".into())?;
            set("heads", &Uint8Array::from(&heads[..]))?;
            set("changes", &changes_to_array(changes))?;
            set("document", &document.into())?;
        }
        Message::Broadcast {
            from_client_id,
            changes,
            affected_paths,
            document,
        } => {
            set("type", &"Broadcast".into())?;
            set("fromClientId", &from_client_id.into())?;
            set("changes", &changes_to_array(changes))?;
            set("affectedPaths", &strings_to_array(affected_paths))?;
            set("document", &document.into())?;
        }
        Message::PushAck { heads, document } => {
            set("type", &"PushAck".into())?;
            set("heads", &Uint8Array::from(&heads[..]))?;
            set("document", &document.into())?;
        }
        Message::Error { message } => {
            set("type", &"Error".into())?;
            set("message", &message.into())?;
        }
        Message::Ephemeral {
            path,
            data,
            document,
        } => {
            set("type", &"Ephemeral".into())?;
            set("path", &path.into())?;
            set("data", &Uint8Array::from(&data[..]))?;
            set("document", &document.into())?;
        }
        Message::EphemeralBatch { updates, document } => {
            set("type", &"EphemeralBatch".into())?;
            set("updates", &updates_to_array(updates))?;
            set("document", &document.into())?;
        }
        Message::EphemeralRelay {
            origin,
            seq,
            path_through,
            updates,
            document,
        } => {
            set("type", &"EphemeralRelay".into())?;
            set("origin", &origin.into())?;
            set("seq", &JsValue::from(seq as f64))?;
            set("pathThrough", &strings_to_array(path_through))?;
            set("updates", &updates_to_array(updates))?;
            set("document", &document.into())?;
        }
        Message::Subscribe {
            add,
            remove,
            document,
        } => {
            set("type", &"Subscribe".into())?;
            set("add", &strings_to_array(add))?;
            set("remove", &strings_to_array(remove))?;
            set("document", &document.into())?;
        }
        Message::Ping => {
            set("type", &"Ping".into())?;
        }
        Message::Pong => {
            set("type", &"Pong".into())?;
        }
    }
    Ok(obj.into())
}

impl Default for SwirlDB {
    fn default() -> Self {
        Self::new()
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

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    fn test_can_instantiate() {
        let db = SwirlDB::new();
        assert_eq!(db.document(), DEFAULT_DOCUMENT);
        assert!(db.access().is_null());
    }

    #[wasm_bindgen_test]
    fn test_connect_message_names_the_document() {
        let db = SwirlDB::new();
        let frame = db.encode_connect_message(
            "alice".into(),
            vec!["**".into()],
            Uint8Array::new_with_length(0),
            Some("pattern.7".into()),
        );
        let decoded = db.decode_message(frame).unwrap();
        let document = js_sys::Reflect::get(&decoded, &"document".into()).unwrap();
        assert_eq!(document.as_string().unwrap(), "pattern.7");

        // Without a document the handle's own is used: the default.
        let frame = db.encode_connect_message(
            "alice".into(),
            vec!["**".into()],
            Uint8Array::new_with_length(0),
            None,
        );
        let decoded = db.decode_message(frame).unwrap();
        let document = js_sys::Reflect::get(&decoded, &"document".into()).unwrap();
        assert_eq!(document.as_string().unwrap(), DEFAULT_DOCUMENT);
    }

    #[wasm_bindgen_test]
    fn test_open_and_denied_messages_decode() {
        let db = SwirlDB::new();
        let open = Message::Open {
            document: "palette.3".into(),
            subscriptions: vec!["**".into()],
            heads: vec![],
        }
        .encode();
        let decoded = db.decode_message(Uint8Array::from(&open[..])).unwrap();
        assert_eq!(
            js_sys::Reflect::get(&decoded, &"type".into())
                .unwrap()
                .as_string()
                .unwrap(),
            "Open"
        );

        let denied = Message::OpenDenied {
            document: "secret".into(),
            reason: "not a member".into(),
        }
        .encode();
        let decoded = db.decode_message(Uint8Array::from(&denied[..])).unwrap();
        assert_eq!(
            js_sys::Reflect::get(&decoded, &"reason".into())
                .unwrap()
                .as_string()
                .unwrap(),
            "not a member"
        );

        let ack = Message::SubscribeAck {
            added: vec![],
            denied: vec![],
            document: "secret".into(),
            access: Access::Read,
        }
        .encode();
        let decoded = db.decode_message(Uint8Array::from(&ack[..])).unwrap();
        assert_eq!(
            js_sys::Reflect::get(&decoded, &"access".into())
                .unwrap()
                .as_string()
                .unwrap(),
            "read"
        );
    }

    #[wasm_bindgen_test]
    fn test_scalar_values_equal() {
        assert!(scalar_values_equal(&ScalarValue::Null, &ScalarValue::Null));
        assert!(scalar_values_equal(
            &ScalarValue::Boolean(true),
            &ScalarValue::Boolean(true)
        ));
        assert!(!scalar_values_equal(
            &ScalarValue::Boolean(true),
            &ScalarValue::Boolean(false)
        ));
        assert!(scalar_values_equal(
            &ScalarValue::Int(42),
            &ScalarValue::Int(42)
        ));
        assert!(!scalar_values_equal(
            &ScalarValue::Int(42),
            &ScalarValue::Int(43)
        ));
    }
}
