// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Network resilience tests
//!
//! Tests disconnect/reconnect scenarios and data consistency.

use super::{init_test_logging, rust_client::RustClient, test_server::TestServer};
use automerge::ScalarValue;
use std::time::Duration;

#[tokio::test]
async fn test_client_reconnect_after_disconnect() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    // Client 1 connects and sets data
    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    client1
        .set_path("data.before", ScalarValue::Str("original".into()))
        .await
        .unwrap();

    // Disconnect client 1
    client1.close().await.unwrap();

    // Wait for server to process the disconnect (background task needs time
    // to detect the WebSocket close and unregister)
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(server.connection_count(), 0);

    // Reconnect client 1
    let client1_reconnected = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Should get state from server
    assert_eq!(
        client1_reconnected.get_path("data.before").await,
        Some(ScalarValue::Str("original".into()))
    );

    client1_reconnected.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_offline_writes_sync_on_reconnect() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    // Client 1 and 2 connect
    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Client 2 writes something
    client2
        .set_path("data.shared", ScalarValue::Str("from client2".into()))
        .await
        .unwrap();

    // Client 1 receives it
    client1
        .wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(
        client1.get_path("data.shared").await,
        Some(ScalarValue::Str("from client2".into()))
    );

    // Disconnect client 1 temporarily
    client1.close().await.unwrap();

    // While client 1 is offline, client 2 makes more changes
    client2
        .set_path("data.offline", ScalarValue::Str("missed by client1".into()))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client 1 reconnects
    let client1_new = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Should receive all changes in initial sync
    assert_eq!(
        client1_new.get_path("data.shared").await,
        Some(ScalarValue::Str("from client2".into()))
    );
    assert_eq!(
        client1_new.get_path("data.offline").await,
        Some(ScalarValue::Str("missed by client1".into()))
    );

    client1_new.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_server_maintains_state_across_client_disconnects() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    // Client 1 sets data and disconnects
    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    client1
        .set_path("persistent.data", ScalarValue::Int(12345))
        .await
        .unwrap();

    client1.close().await.unwrap();

    // Wait
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client 2 connects fresh
    let client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Should get the data from server
    assert_eq!(
        client2.get_path("persistent.data").await,
        Some(ScalarValue::Int(12345))
    );

    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_multiple_disconnect_reconnect_cycles() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    for i in 0..3 {
        let mut client = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
            .await
            .unwrap();

        client
            .set_path(
                &format!("cycle.{}", i),
                ScalarValue::Str(format!("data_{}", i).into()),
            )
            .await
            .unwrap();

        client.close().await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Final client should see all data
    let final_client = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    assert_eq!(
        final_client.get_path("cycle.0").await,
        Some(ScalarValue::Str("data_0".into()))
    );
    assert_eq!(
        final_client.get_path("cycle.1").await,
        Some(ScalarValue::Str("data_1".into()))
    );
    assert_eq!(
        final_client.get_path("cycle.2").await,
        Some(ScalarValue::Str("data_2".into()))
    );

    final_client.close().await.unwrap();
    server.shutdown().await.unwrap();
}
