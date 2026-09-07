// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Test server infrastructure for integration tests
//!
//! Provides an in-process SwirlDB server on a random port for testing.

use anyhow::Result;
use axum::{
    extract::{ws::WebSocketUpgrade, Query, State as AxumState},
    http::HeaderMap,
    response::Response,
    routing::get,
    Router,
};
use std::collections::HashMap;
use std::sync::Arc;
use swirldb_core::storage::InMemoryDocStorage;
use swirldb_server::authority::{Authority, OpenToAll};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

// Re-export server state types (we'll need to access server internals)
pub use swirldb_server::state::{BroadcastMessage, EphemeralMessage, ServerState};

pub struct TestServer {
    pub port: u16,
    pub state: ServerState,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    server_handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestServer {
    /// Start a new test server on a random available port
    pub async fn start() -> Result<Self> {
        Self::start_with_policy(None).await
    }

    /// Start a test server with a custom policy
    pub async fn start_with_policy(
        policy: Option<swirldb_core::policy::PolicyEngine>,
    ) -> Result<Self> {
        Self::start_with(policy, Arc::new(OpenToAll)).await
    }

    /// Start a test server whose documents open only as `authority` allows
    pub async fn start_with_authority(authority: Arc<dyn Authority>) -> Result<Self> {
        Self::start_with(None, authority).await
    }

    async fn start_with(
        policy: Option<swirldb_core::policy::PolicyEngine>,
        authority: Arc<dyn Authority>,
    ) -> Result<Self> {
        // Bind to port 0 to get a random available port
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let port = addr.port();

        let storage = Arc::new(InMemoryDocStorage::new());
        let state = ServerState::with_authority(policy, storage, authority).await;

        let app = Router::new()
            .route("/ws", get(websocket_handler))
            .layer(CorsLayer::permissive())
            .with_state(state.clone());

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let server_handle = tokio::spawn(async move {
            let serve = axum::serve(listener, app);
            let graceful = serve.with_graceful_shutdown(async {
                shutdown_rx.await.ok();
            });

            if let Err(e) = graceful.await {
                error!("Server error: {}", e);
            }
        });

        info!("Test server started on port {}", port);

        Ok(TestServer {
            port,
            state,
            shutdown_tx: Some(shutdown_tx),
            server_handle: Some(server_handle),
        })
    }

    /// Get the WebSocket URL for this server
    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}/ws", self.port)
    }

    /// Get the number of active connections
    pub fn connection_count(&self) -> usize {
        self.state.get_connection_count()
    }

    /// Shutdown the server
    pub async fn shutdown(mut self) -> Result<()> {
        // Take ownership and send shutdown signal
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        // Server handle will be dropped, aborting the task
        Ok(())
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Send shutdown signal if still available
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        // Abort the server task if it's still running
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }
    }
}

/// WebSocket upgrade handler — delegates to the shared handler in swirldb_server
async fn websocket_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    AxumState(state): AxumState<ServerState>,
) -> Response {
    let token = swirldb_server::handler::bearer_token(&headers, &query);
    ws.on_upgrade(move |socket| swirldb_server::handler::handle_websocket(socket, state, token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_starts_and_stops() {
        let server = TestServer::start().await.unwrap();
        assert!(server.port > 0);
        assert!(server.ws_url().starts_with("ws://127.0.0.1:"));
        server.shutdown().await.unwrap();
    }
}
