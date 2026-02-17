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
        rust_client.get_path("message"),
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
        rust_client.get_path("types.string"),
        Some(ScalarValue::Str("test".into()))
    );
    assert_eq!(
        rust_client.get_path("types.number"),
        Some(ScalarValue::Int(42))
    );
    assert_eq!(
        rust_client.get_path("types.bool"),
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
