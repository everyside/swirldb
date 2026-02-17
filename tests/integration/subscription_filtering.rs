// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Subscription filtering tests
//!
//! Tests that clients only receive updates matching their subscription patterns.

use super::{init_test_logging, rust_client::RustClient, test_server::TestServer};
use automerge::ScalarValue;
use std::time::Duration;

#[tokio::test]
async fn test_subscription_wildcard_filter() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    // Client 1 subscribes to user.**
    let mut client1 = RustClient::connect(&server.ws_url(), vec!["user.**".to_string()])
        .await
        .unwrap();

    // Client 2 subscribes to settings.**
    let mut client2 = RustClient::connect(&server.ws_url(), vec!["settings.**".to_string()])
        .await
        .unwrap();

    // Client 1 writes to user.name (should NOT reach client2)
    client1
        .set_path("user.name", ScalarValue::Str("Alice".into()))
        .await
        .unwrap();

    // Client 2 should NOT receive this (wrong subscription)
    let result = client2
        .wait_for_broadcast_timeout(Duration::from_millis(500))
        .await;
    assert!(result.is_err(), "Client2 should not receive user updates");

    // Client 2 writes to settings.theme (should NOT reach client1)
    client2
        .set_path("settings.theme", ScalarValue::Str("dark".into()))
        .await
        .unwrap();

    // Client 1 should NOT receive this
    let result = client1
        .wait_for_broadcast_timeout(Duration::from_millis(500))
        .await;
    assert!(
        result.is_err(),
        "Client1 should not receive settings updates"
    );

    // Cleanup
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_subscription_exact_path_match() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    // Client subscribes to exact path config.version
    let mut client1 = RustClient::connect(&server.ws_url(), vec!["config.version".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Client 2 writes to config.version (client1 should receive)
    client2
        .set_path("config.version", ScalarValue::Str("1.0.0".into()))
        .await
        .unwrap();

    client1
        .wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(
        client1.get_path("config.version"),
        Some(ScalarValue::Str("1.0.0".into()))
    );

    // Client 2 writes to config.other (client1 should NOT receive)
    client2
        .set_path("config.other", ScalarValue::Str("value".into()))
        .await
        .unwrap();

    let result = client1
        .wait_for_broadcast_timeout(Duration::from_millis(500))
        .await;
    assert!(
        result.is_err(),
        "Client1 should not receive updates to non-subscribed paths"
    );

    // Cleanup
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_multiple_subscription_patterns() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    // Client subscribes to multiple patterns
    let mut client1 = RustClient::connect(
        &server.ws_url(),
        vec!["user.**".to_string(), "settings.**".to_string()],
    )
    .await
    .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Client 2 writes to user.name (should reach client1)
    client2
        .set_path("user.name", ScalarValue::Str("Bob".into()))
        .await
        .unwrap();

    client1
        .wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(
        client1.get_path("user.name"),
        Some(ScalarValue::Str("Bob".into()))
    );

    // Client 2 writes to settings.lang (should also reach client1)
    client2
        .set_path("settings.lang", ScalarValue::Str("en".into()))
        .await
        .unwrap();

    client1
        .wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(
        client1.get_path("settings.lang"),
        Some(ScalarValue::Str("en".into()))
    );

    // Client 2 writes to data.value (should NOT reach client1)
    client2
        .set_path("data.value", ScalarValue::Int(42))
        .await
        .unwrap();

    let result = client1
        .wait_for_broadcast_timeout(Duration::from_millis(500))
        .await;
    assert!(result.is_err(), "Client1 should not receive data updates");

    // Cleanup
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_subscription_nested_paths() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    // Client subscribes to org.team.**
    let mut client1 = RustClient::connect(&server.ws_url(), vec!["org.team.**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Write to nested path (should reach client1)
    client2
        .set_path("org.team.members.alice", ScalarValue::Str("admin".into()))
        .await
        .unwrap();

    client1
        .wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(
        client1.get_path("org.team.members.alice"),
        Some(ScalarValue::Str("admin".into()))
    );

    // Write to non-matching nested path (should NOT reach client1)
    client2
        .set_path("org.settings.name", ScalarValue::Str("ACME".into()))
        .await
        .unwrap();

    let result = client1
        .wait_for_broadcast_timeout(Duration::from_millis(500))
        .await;
    assert!(
        result.is_err(),
        "Client1 should not receive org.settings updates"
    );

    // Cleanup
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}
