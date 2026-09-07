// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Browser WASM client sync tests
//!
//! Tests Browser WASM ↔ Server sync using real headless browser.

use super::{
    browser_client::BrowserTestClient, init_test_logging, rust_client::RustClient,
    test_server::TestServer,
};
use automerge::ScalarValue;
use std::time::Duration;

#[tokio::test]
async fn test_browser_to_server_sync() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    // Start browser client
    let browser = BrowserTestClient::start(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Browser sets data
    browser
        .set_path("message", serde_json::json!("Hello from Browser"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Rust client verifies server has it
    let rust_client = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    assert_eq!(
        rust_client.get_path("message").await,
        Some(ScalarValue::Str("Hello from Browser".into()))
    );

    browser.close().await.unwrap();
    rust_client.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_server_to_browser_sync() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    let browser = BrowserTestClient::start(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut rust_client = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Rust sets data
    rust_client
        .set_path("data.from_rust", ScalarValue::Str("Hello from Rust".into()))
        .await
        .unwrap();

    // Browser should receive it
    browser.wait_for_sync().await.unwrap();

    let value = browser.get_path("data.from_rust").await.unwrap();
    assert_eq!(value, Some(serde_json::json!("Hello from Rust")));

    browser.close().await.unwrap();
    rust_client.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_browser_wasm_serialization() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    let browser = BrowserTestClient::start(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut rust_client = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Test various types from browser
    browser
        .set_path("types.string", serde_json::json!("test"))
        .await
        .unwrap();
    browser
        .set_path("types.number", serde_json::json!(42))
        .await
        .unwrap();
    browser
        .set_path("types.bool", serde_json::json!(true))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    for _ in 0..3 {
        let _ = rust_client
            .wait_for_broadcast_timeout(Duration::from_millis(200))
            .await;
    }

    // Verify Rust received them
    assert_eq!(
        rust_client.get_path("types.string").await,
        Some(ScalarValue::Str("test".into()))
    );
    assert_eq!(
        rust_client.get_path("types.number").await,
        Some(ScalarValue::Int(42))
    );
    assert_eq!(
        rust_client.get_path("types.bool").await,
        Some(ScalarValue::Boolean(true))
    );

    browser.close().await.unwrap();
    rust_client.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_two_browsers_sync() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    let browser1 = BrowserTestClient::start(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let browser2 = BrowserTestClient::start(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Browser 1 sets data
    browser1
        .set_path("shared.data", serde_json::json!("from browser1"))
        .await
        .unwrap();

    // Browser 2 should receive it
    browser2.wait_for_sync().await.unwrap();

    let value = browser2.get_path("shared.data").await.unwrap();
    assert_eq!(value, Some(serde_json::json!("from browser1")));

    browser1.close().await.unwrap();
    browser2.close().await.unwrap();
    server.shutdown().await.unwrap();
}

/// One browser, one connection, two documents. Each handle sees its own
/// document's changes and presence and nothing of the other's.
#[tokio::test]
async fn test_browser_holds_two_documents_over_one_connection() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    let browser = BrowserTestClient::start_with_documents(
        &server.ws_url(),
        vec!["alpha".to_string(), "beta".to_string()],
    )
    .await
    .unwrap();

    let mut alpha = RustClient::open(&server.ws_url(), "alpha", vec!["**".to_string()])
        .await
        .unwrap();
    let mut beta = RustClient::open(&server.ws_url(), "beta", vec!["**".to_string()])
        .await
        .unwrap();

    // The browser is one socket; the two Rust clients are two.
    assert_eq!(server.connection_count(), 3);
    assert_eq!(
        browser.document_access("alpha").await.unwrap(),
        Some(serde_json::json!("write"))
    );

    // Server → browser, on alpha only.
    alpha
        .set_path("title", ScalarValue::Str("From alpha".into()))
        .await
        .unwrap();
    browser.wait_for_document_change("alpha").await.unwrap();
    assert_eq!(
        browser.get_document_path("alpha", "title").await.unwrap(),
        Some(serde_json::json!("From alpha"))
    );
    assert_eq!(
        browser.get_document_path("beta", "title").await.unwrap(),
        Some(serde_json::Value::Null)
    );
    assert!(browser.wait_for_document_change("beta").await.is_err());

    // Browser → server, on beta only.
    browser
        .set_document_path("beta", "title", serde_json::json!("From the browser"))
        .await
        .unwrap();
    beta.wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        beta.get_path("title").await,
        Some(ScalarValue::Str("From the browser".into()))
    );
    assert!(alpha
        .wait_for_broadcast_timeout(Duration::from_millis(300))
        .await
        .is_err());
    assert_eq!(
        alpha.get_path("title").await,
        Some(ScalarValue::Str("From alpha".into()))
    );

    // Presence rides the ephemeral channel and stays within its document.
    alpha
        .send_ephemeral("presence.alpha-client", &[1, 2, 3])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let seen_on_alpha = browser.take_document_presence("alpha").await.unwrap();
    assert_eq!(seen_on_alpha.len(), 1);
    assert_eq!(seen_on_alpha[0].path, "presence.alpha-client");
    assert_eq!(seen_on_alpha[0].data, vec![1, 2, 3]);
    assert!(browser
        .take_document_presence("beta")
        .await
        .unwrap()
        .is_empty());

    browser
        .send_document_presence("beta", "presence.browser", vec![9])
        .await
        .unwrap();
    let updates = beta
        .wait_for_ephemeral_timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(updates[0].0, "presence.browser");
    assert!(alpha
        .wait_for_ephemeral_timeout(Duration::from_millis(300))
        .await
        .is_err());

    browser.close().await.unwrap();
    alpha.close().await.unwrap();
    beta.close().await.unwrap();
    server.shutdown().await.unwrap();
}

/// The authority's refusal reaches the browser as a rejected `openDocument`.
#[tokio::test]
async fn test_browser_open_is_refused_by_the_authority() {
    init_test_logging();

    let engine = swirldb_core::policy::PolicyEngine::from_json(
        r#"{"policies":{"rules":[
            {"priority":10,"actor":{"type":"Any"},"action":"Write","path_pattern":"shared.*","effect":"Allow"}
        ]}}"#,
    )
    .unwrap();
    let server = TestServer::start_with_authority(std::sync::Arc::new(
        swirldb_server::authority::PolicyAuthority::new(engine),
    ))
    .await
    .unwrap();

    let refused = BrowserTestClient::start_with_documents(
        &server.ws_url(),
        vec!["shared.1".to_string(), "private.1".to_string()],
    )
    .await;
    let message = refused.err().expect("private.1 is refused").to_string();
    assert!(message.contains("may not open private.1"), "{}", message);

    server.shutdown().await.unwrap();
}
