// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Browser WASM client test infrastructure
//!
//! Uses headless browser (via Playwright) to test WASM client.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{self, mpsc};

#[derive(Debug, Serialize)]
#[serde(tag = "cmd")]
enum IpcCommand {
    #[serde(rename = "connect")]
    Connect {
        #[serde(rename = "wsUrl")]
        ws_url: String,
        subscriptions: Vec<String>,
    },
    #[serde(rename = "setPath")]
    SetPath {
        path: String,
        value: serde_json::Value,
    },
    #[serde(rename = "getPath")]
    GetPath { path: String },
    #[serde(rename = "waitForBroadcast")]
    WaitForBroadcast,
    #[serde(rename = "openDocuments")]
    OpenDocuments {
        #[serde(rename = "wsUrl")]
        ws_url: String,
        documents: Vec<String>,
    },
    #[serde(rename = "setDocumentPath")]
    SetDocumentPath {
        document: String,
        path: String,
        value: serde_json::Value,
    },
    #[serde(rename = "getDocumentPath")]
    GetDocumentPath { document: String, path: String },
    #[serde(rename = "getDocumentValue")]
    GetDocumentValue { document: String, path: String },
    #[serde(rename = "deleteDocumentPath")]
    DeleteDocumentPath { document: String, path: String },
    #[serde(rename = "observeDocumentPath")]
    ObserveDocumentPath { document: String, path: String },
    #[serde(rename = "takeDocumentObservations")]
    TakeDocumentObservations { document: String, path: String },
    #[serde(rename = "waitForDocumentObservation")]
    WaitForDocumentObservation { document: String, path: String },
    #[serde(rename = "setDocumentText")]
    SetDocumentText {
        document: String,
        path: String,
        text: String,
    },
    #[serde(rename = "spliceDocumentText")]
    SpliceDocumentText {
        document: String,
        path: String,
        position: usize,
        #[serde(rename = "deleteCount")]
        delete_count: usize,
        insert: String,
    },
    #[serde(rename = "takeDocumentTextChanges")]
    TakeDocumentTextChanges { document: String },
    #[serde(rename = "waitForDocumentTextChange")]
    WaitForDocumentTextChange { document: String },
    #[serde(rename = "documentAccess")]
    DocumentAccess { document: String },
    #[serde(rename = "sendDocumentPresence")]
    SendDocumentPresence {
        document: String,
        path: String,
        bytes: Vec<u8>,
    },
    #[serde(rename = "takeDocumentPresence")]
    TakeDocumentPresence { document: String },
    #[serde(rename = "waitForDocumentChange")]
    WaitForDocumentChange { document: String },
    #[serde(rename = "close")]
    Close,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum IpcResponse {
    #[serde(rename = "ready")]
    Ready,
    #[serde(rename = "connected")]
    Connected,
    #[serde(rename = "set_complete")]
    SetComplete,
    #[serde(rename = "value")]
    Value { value: serde_json::Value },
    #[serde(rename = "broadcast_received")]
    BroadcastReceived,
    #[serde(rename = "documents_opened")]
    DocumentsOpened { documents: Vec<String> },
    #[serde(rename = "error")]
    Error { error: String },
}

/// One presence update a browser handle saw on a document.
#[derive(Debug, Clone, Deserialize)]
pub struct SeenPresence {
    pub path: String,
    pub data: Vec<u8>,
}

/// One splice as the browser's `observeText` reported it, positions in
/// UTF-16 code units.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SeenTextSplice {
    pub position: usize,
    #[serde(rename = "deleteCount")]
    pub delete_count: usize,
    pub insert: String,
}

/// One text change the browser heard from the server.
#[derive(Debug, Clone, Deserialize)]
pub struct SeenTextChange {
    pub path: String,
    pub splices: Vec<SeenTextSplice>,
    pub local: bool,
}

/// One call of an `observe` callback in the browser: the value it was
/// handed and the change beside it.
#[derive(Debug, Clone, Deserialize)]
pub struct SeenObservation {
    pub value: serde_json::Value,
    pub path: String,
    #[serde(rename = "changedPaths")]
    pub changed_paths: Vec<String>,
    pub local: bool,
}

pub struct BrowserTestClient {
    process: Arc<Mutex<Option<Child>>>,
    stdin: Arc<sync::Mutex<tokio::process::ChildStdin>>,
    rx: Arc<sync::Mutex<mpsc::UnboundedReceiver<IpcResponse>>>,
}

impl BrowserTestClient {
    /// Start a browser and connect it on the default document, driving the
    /// protocol by hand as the original tests do.
    pub async fn start(ws_url: &str, subscriptions: Vec<String>) -> Result<Self> {
        let client = Self::launch().await?;

        client
            .send_command(IpcCommand::Connect {
                ws_url: ws_url.to_string(),
                subscriptions,
            })
            .await?;

        client
            .wait_for_response(|r| matches!(r, IpcResponse::Connected))
            .await?;

        Ok(client)
    }

    /// Start a browser and open several documents over one `Connection`.
    /// Fails with the server's reason when a document is refused.
    pub async fn start_with_documents(ws_url: &str, documents: Vec<String>) -> Result<Self> {
        let client = Self::launch().await?;

        client
            .send_command(IpcCommand::OpenDocuments {
                ws_url: ws_url.to_string(),
                documents: documents.clone(),
            })
            .await?;

        match client
            .wait_for_response(|r| {
                matches!(
                    r,
                    IpcResponse::DocumentsOpened { .. } | IpcResponse::Error { .. }
                )
            })
            .await?
        {
            IpcResponse::Error { error } => anyhow::bail!("{}", error),
            IpcResponse::DocumentsOpened { documents: opened } => {
                if opened != documents {
                    anyhow::bail!("Asked to open {:?}, browser opened {:?}", documents, opened);
                }
                Ok(client)
            }
            _ => unreachable!("the predicate admits only those two"),
        }
    }

    async fn launch() -> Result<Self> {
        let script_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("integration/browser-launcher.js");

        if !script_path.exists() {
            anyhow::bail!("Browser launcher script not found at {:?}", script_path);
        }

        // Spawn Node.js + Playwright process
        let mut child = Command::new("node")
            .arg(script_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let stdout = tokio::process::ChildStdout::from_std(child.stdout.take().unwrap())?;
        let stdin = child.stdin.take().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn task to read IPC messages
        tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                if line.starts_with("IPC:") {
                    let json = line.strip_prefix("IPC:").unwrap().trim();
                    if let Ok(response) = serde_json::from_str::<IpcResponse>(json) {
                        let _ = tx.send(response);
                    }
                }
            }
        });

        let stdin = tokio::process::ChildStdin::from_std(stdin)?;

        let client = BrowserTestClient {
            process: Arc::new(Mutex::new(Some(child))),
            stdin: Arc::new(sync::Mutex::new(stdin)),
            rx: Arc::new(sync::Mutex::new(rx)),
        };

        // Wait for ready signal
        client
            .wait_for_response(|r| matches!(r, IpcResponse::Ready))
            .await?;

        Ok(client)
    }

    async fn send_command(&self, cmd: IpcCommand) -> Result<()> {
        let json = serde_json::to_string(&cmd)?;
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(json.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    async fn wait_for_response<F>(&self, predicate: F) -> Result<IpcResponse>
    where
        F: Fn(&IpcResponse) -> bool,
    {
        let timeout = tokio::time::Duration::from_secs(10);
        let start = tokio::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                anyhow::bail!("Timeout waiting for IPC response");
            }

            let result = {
                let mut rx = self.rx.lock().await;
                rx.try_recv()
            };
            match result {
                Ok(response) => {
                    if predicate(&response) {
                        return Ok(response);
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    anyhow::bail!("IPC channel disconnected");
                }
            }
        }
    }

    pub async fn set_path(&self, path: &str, value: serde_json::Value) -> Result<()> {
        self.send_command(IpcCommand::SetPath {
            path: path.to_string(),
            value,
        })
        .await?;

        self.wait_for_response(|r| matches!(r, IpcResponse::SetComplete))
            .await?;
        Ok(())
    }

    pub async fn get_path(&self, path: &str) -> Result<Option<serde_json::Value>> {
        self.send_command(IpcCommand::GetPath {
            path: path.to_string(),
        })
        .await?;

        match self
            .wait_for_response(|r| matches!(r, IpcResponse::Value { .. }))
            .await?
        {
            IpcResponse::Value { value } => Ok(Some(value)),
            _ => Ok(None),
        }
    }

    pub async fn wait_for_sync(&self) -> Result<()> {
        self.send_command(IpcCommand::WaitForBroadcast).await?;
        self.wait_for_response(|r| matches!(r, IpcResponse::BroadcastReceived))
            .await?;
        Ok(())
    }

    // ---- several documents over one connection ----

    /// Set a path on one open document and push it.
    pub async fn set_document_path(
        &self,
        document: &str,
        path: &str,
        value: serde_json::Value,
    ) -> Result<()> {
        self.send_command(IpcCommand::SetDocumentPath {
            document: document.to_string(),
            path: path.to_string(),
            value,
        })
        .await?;
        self.wait_for_response(|r| matches!(r, IpcResponse::SetComplete))
            .await?;
        Ok(())
    }

    /// Put a text at a path on one open document, push it, and start
    /// recording the text changes the server sends for it.
    pub async fn set_document_text(&self, document: &str, path: &str, text: &str) -> Result<()> {
        self.send_command(IpcCommand::SetDocumentText {
            document: document.to_string(),
            path: path.to_string(),
            text: text.to_string(),
        })
        .await?;
        self.wait_for_response(|r| matches!(r, IpcResponse::SetComplete))
            .await?;
        Ok(())
    }

    /// Splice the text at a path on one open document and push the edit.
    /// `position` and `delete_count` are UTF-16 code units, as the browser counts.
    pub async fn splice_document_text(
        &self,
        document: &str,
        path: &str,
        position: usize,
        delete_count: usize,
        insert: &str,
    ) -> Result<()> {
        self.send_command(IpcCommand::SpliceDocumentText {
            document: document.to_string(),
            path: path.to_string(),
            position,
            delete_count,
            insert: insert.to_string(),
        })
        .await?;
        self.wait_for_response(|r| matches!(r, IpcResponse::SetComplete))
            .await?;
        Ok(())
    }

    /// The text changes heard from the server on a document since last taken.
    pub async fn take_document_text_changes(&self, document: &str) -> Result<Vec<SeenTextChange>> {
        self.send_command(IpcCommand::TakeDocumentTextChanges {
            document: document.to_string(),
        })
        .await?;
        match self
            .wait_for_response(|r| matches!(r, IpcResponse::Value { .. }))
            .await?
        {
            IpcResponse::Value { value } => Ok(serde_json::from_value(value)?),
            _ => Ok(Vec::new()),
        }
    }

    /// Wait until the browser has heard at least one text change on a document.
    pub async fn wait_for_document_text_change(&self, document: &str) -> Result<()> {
        self.send_command(IpcCommand::WaitForDocumentTextChange {
            document: document.to_string(),
        })
        .await?;
        match self
            .wait_for_response(|r| {
                matches!(
                    r,
                    IpcResponse::BroadcastReceived | IpcResponse::Error { .. }
                )
            })
            .await?
        {
            IpcResponse::Error { error } => anyhow::bail!("{}", error),
            _ => Ok(()),
        }
    }

    /// Read a path from one open document's local copy.
    pub async fn get_document_path(
        &self,
        document: &str,
        path: &str,
    ) -> Result<Option<serde_json::Value>> {
        self.send_command(IpcCommand::GetDocumentPath {
            document: document.to_string(),
            path: path.to_string(),
        })
        .await?;
        self.value_response().await
    }

    /// Read a path from one open document's local copy as JSON: a list as
    /// an array, a map as an object.
    pub async fn get_document_value(
        &self,
        document: &str,
        path: &str,
    ) -> Result<Option<serde_json::Value>> {
        self.send_command(IpcCommand::GetDocumentValue {
            document: document.to_string(),
            path: path.to_string(),
        })
        .await?;
        self.value_response().await
    }

    /// Delete a path on one open document and push the deletion.
    pub async fn delete_document_path(&self, document: &str, path: &str) -> Result<()> {
        self.send_command(IpcCommand::DeleteDocumentPath {
            document: document.to_string(),
            path: path.to_string(),
        })
        .await?;
        self.wait_for_response(|r| matches!(r, IpcResponse::SetComplete))
            .await?;
        Ok(())
    }

    /// Observe a path on one open document and start recording every call
    /// of the observer, this browser's own writes included.
    pub async fn observe_document_path(&self, document: &str, path: &str) -> Result<()> {
        self.send_command(IpcCommand::ObserveDocumentPath {
            document: document.to_string(),
            path: path.to_string(),
        })
        .await?;
        self.wait_for_response(|r| matches!(r, IpcResponse::SetComplete))
            .await?;
        Ok(())
    }

    /// The observer calls recorded for a path since last taken.
    pub async fn take_document_observations(
        &self,
        document: &str,
        path: &str,
    ) -> Result<Vec<SeenObservation>> {
        self.send_command(IpcCommand::TakeDocumentObservations {
            document: document.to_string(),
            path: path.to_string(),
        })
        .await?;
        match self
            .wait_for_response(|r| matches!(r, IpcResponse::Value { .. }))
            .await?
        {
            IpcResponse::Value { value } => Ok(serde_json::from_value(value)?),
            _ => Ok(Vec::new()),
        }
    }

    /// Wait until the observer on a path has been called at least once.
    pub async fn wait_for_document_observation(&self, document: &str, path: &str) -> Result<()> {
        self.send_command(IpcCommand::WaitForDocumentObservation {
            document: document.to_string(),
            path: path.to_string(),
        })
        .await?;
        match self
            .wait_for_response(|r| {
                matches!(
                    r,
                    IpcResponse::BroadcastReceived | IpcResponse::Error { .. }
                )
            })
            .await?
        {
            IpcResponse::Error { error } => anyhow::bail!("{}", error),
            _ => Ok(()),
        }
    }

    /// What the server granted on the document: "read", "write", or null.
    pub async fn document_access(&self, document: &str) -> Result<Option<serde_json::Value>> {
        self.send_command(IpcCommand::DocumentAccess {
            document: document.to_string(),
        })
        .await?;
        self.value_response().await
    }

    /// Send an ephemeral frame on one document.
    pub async fn send_document_presence(
        &self,
        document: &str,
        path: &str,
        bytes: Vec<u8>,
    ) -> Result<()> {
        self.send_command(IpcCommand::SendDocumentPresence {
            document: document.to_string(),
            path: path.to_string(),
            bytes,
        })
        .await?;
        self.wait_for_response(|r| matches!(r, IpcResponse::SetComplete))
            .await?;
        Ok(())
    }

    /// The presence frames a document's handle has seen since last asked.
    pub async fn take_document_presence(&self, document: &str) -> Result<Vec<SeenPresence>> {
        self.send_command(IpcCommand::TakeDocumentPresence {
            document: document.to_string(),
        })
        .await?;
        let value = self
            .value_response()
            .await?
            .unwrap_or(serde_json::Value::Null);
        Ok(serde_json::from_value(value).unwrap_or_default())
    }

    /// Wait for a broadcast to land on one document. Errors on timeout.
    pub async fn wait_for_document_change(&self, document: &str) -> Result<()> {
        self.send_command(IpcCommand::WaitForDocumentChange {
            document: document.to_string(),
        })
        .await?;
        match self
            .wait_for_response(|r| {
                matches!(
                    r,
                    IpcResponse::BroadcastReceived | IpcResponse::Error { .. }
                )
            })
            .await?
        {
            IpcResponse::Error { error } => anyhow::bail!("{}", error),
            _ => Ok(()),
        }
    }

    async fn value_response(&self) -> Result<Option<serde_json::Value>> {
        match self
            .wait_for_response(|r| matches!(r, IpcResponse::Value { .. }))
            .await?
        {
            IpcResponse::Value { value } => Ok(Some(value)),
            _ => Ok(None),
        }
    }

    pub async fn close(self) -> Result<()> {
        self.send_command(IpcCommand::Close).await?;

        if let Some(mut child) = self.process.lock().unwrap().take() {
            let _ = child.kill();
            let _ = child.wait();
        }

        Ok(())
    }
}

impl Drop for BrowserTestClient {
    fn drop(&mut self) {
        if let Some(mut child) = self.process.lock().unwrap().take() {
            let _ = child.kill();
        }
    }
}
