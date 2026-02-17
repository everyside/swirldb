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
use tokio::sync::mpsc;

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
    SetPath { path: String, value: serde_json::Value },
    #[serde(rename = "getPath")]
    GetPath { path: String },
    #[serde(rename = "waitForBroadcast")]
    WaitForBroadcast,
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
    #[serde(rename = "error")]
    Error { _error: String },
}

pub struct BrowserTestClient {
    process: Arc<Mutex<Option<Child>>>,
    stdin: Arc<Mutex<tokio::process::ChildStdin>>,
    rx: Arc<Mutex<mpsc::UnboundedReceiver<IpcResponse>>>,
}

impl BrowserTestClient {
    pub async fn start(ws_url: &str, subscriptions: Vec<String>) -> Result<Self> {
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
            stdin: Arc::new(Mutex::new(stdin)),
            rx: Arc::new(Mutex::new(rx)),
        };

        // Wait for ready signal
        client.wait_for_response(|r| matches!(r, IpcResponse::Ready)).await?;

        // Send connect command
        client.send_command(IpcCommand::Connect {
            ws_url: ws_url.to_string(),
            subscriptions,
        }).await?;

        // Wait for connected
        client.wait_for_response(|r| matches!(r, IpcResponse::Connected)).await?;

        Ok(client)
    }

    async fn send_command(&self, cmd: IpcCommand) -> Result<()> {
        let json = serde_json::to_string(&cmd)?;
        let mut stdin = self.stdin.lock().unwrap();
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

            let mut rx = self.rx.lock().unwrap();
            match rx.try_recv() {
                Ok(response) => {
                    if predicate(&response) {
                        return Ok(response);
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    drop(rx);
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
        }).await?;

        self.wait_for_response(|r| matches!(r, IpcResponse::SetComplete)).await?;
        Ok(())
    }

    pub async fn get_path(&self, path: &str) -> Result<Option<serde_json::Value>> {
        self.send_command(IpcCommand::GetPath {
            path: path.to_string(),
        }).await?;

        match self.wait_for_response(|r| matches!(r, IpcResponse::Value { .. })).await? {
            IpcResponse::Value { value } => Ok(Some(value)),
            _ => Ok(None),
        }
    }

    pub async fn wait_for_sync(&self) -> Result<()> {
        self.send_command(IpcCommand::WaitForBroadcast).await?;
        self.wait_for_response(|r| matches!(r, IpcResponse::BroadcastReceived)).await?;
        Ok(())
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
