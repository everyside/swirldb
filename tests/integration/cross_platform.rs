// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Cross-platform compatibility tests
//!
//! Tests serialization compatibility and protocol compatibility across platforms.

use super::{init_test_logging, rust_client::RustClient, test_server::TestServer};
use automerge::ScalarValue;
use std::time::Duration;

#[tokio::test]
async fn test_rust_to_rust_serialization() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Test various data types
    client1
        .set_path("types.string", ScalarValue::Str("test".into()))
        .await
        .unwrap();
    client1
        .set_path("types.int", ScalarValue::Int(42))
        .await
        .unwrap();
    client1
        .set_path("types.uint", ScalarValue::Uint(100))
        .await
        .unwrap();
    client1
        .set_path("types.float", ScalarValue::F64(3.15))
        .await
        .unwrap();
    client1
        .set_path("types.bool", ScalarValue::Boolean(true))
        .await
        .unwrap();

    // Wait for broadcasts
    tokio::time::sleep(Duration::from_millis(500)).await;

    for _ in 0..5 {
        let _ = client2
            .wait_for_broadcast_timeout(Duration::from_millis(200))
            .await;
    }

    // Verify all types transmitted correctly
    assert_eq!(
        client2.get_path("types.string").await,
        Some(ScalarValue::Str("test".into()))
    );
    assert_eq!(
        client2.get_path("types.int").await,
        Some(ScalarValue::Int(42))
    );
    assert_eq!(
        client2.get_path("types.uint").await,
        Some(ScalarValue::Uint(100))
    );
    assert_eq!(
        client2.get_path("types.float").await,
        Some(ScalarValue::F64(3.15))
    );
    assert_eq!(
        client2.get_path("types.bool").await,
        Some(ScalarValue::Boolean(true))
    );

    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_nested_objects_serialization() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Create nested structure
    client1
        .set_path("user.profile.name", ScalarValue::Str("Alice".into()))
        .await
        .unwrap();
    client1
        .set_path("user.profile.age", ScalarValue::Int(30))
        .await
        .unwrap();
    client1
        .set_path("user.settings.theme", ScalarValue::Str("dark".into()))
        .await
        .unwrap();

    // Wait for all broadcasts
    tokio::time::sleep(Duration::from_millis(500)).await;

    for _ in 0..3 {
        let _ = client2
            .wait_for_broadcast_timeout(Duration::from_millis(200))
            .await;
    }

    // Verify nested structure
    assert_eq!(
        client2.get_path("user.profile.name").await,
        Some(ScalarValue::Str("Alice".into()))
    );
    assert_eq!(
        client2.get_path("user.profile.age").await,
        Some(ScalarValue::Int(30))
    );
    assert_eq!(
        client2.get_path("user.settings.theme").await,
        Some(ScalarValue::Str("dark".into()))
    );

    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_large_data_transmission() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Send large string
    let large_string = "x".repeat(10000);
    client1
        .set_path("data.large", ScalarValue::Str(large_string.clone().into()))
        .await
        .unwrap();

    client2
        .wait_for_broadcast_timeout(Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(
        client2.get_path("data.large").await,
        Some(ScalarValue::Str(large_string.into()))
    );

    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}

// Placeholder tests for when browser and Node.js clients are ready

#[ignore = "Browser client not yet implemented"]
#[tokio::test]
async fn test_browser_to_rust_serialization() {
    // TODO: Browser creates data → Rust receives → verify identical
    panic!("Not yet implemented");
}
