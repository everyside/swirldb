// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Ephemeral messaging tests
//!
//! Tests the high-frequency pub/sub path that bypasses Automerge and storage.

use super::{init_test_logging, rust_client::RustClient, test_server::TestServer};
use std::time::Duration;

#[tokio::test]
async fn test_ephemeral_single_message() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Client 1 sends ephemeral
    client1
        .send_ephemeral("fixtures.1.color", &[255, 0, 128, 255])
        .await
        .unwrap();

    // Client 2 should receive it
    let updates = client2
        .wait_for_ephemeral_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].0, "fixtures.1.color");
    assert_eq!(updates[0].1, vec![255, 0, 128, 255]);

    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_ephemeral_batch() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Client 1 sends ephemeral batch with 3 updates
    client1
        .send_ephemeral_batch(&[
            ("fixtures.1.color", &[255, 0, 0]),
            ("fixtures.2.color", &[0, 255, 0]),
            ("beat.bpm", &[0, 0, 0, 120]),
        ])
        .await
        .unwrap();

    // Client 2 should receive all 3
    let updates = client2
        .wait_for_ephemeral_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(updates.len(), 3);
    assert_eq!(updates[0].0, "fixtures.1.color");
    assert_eq!(updates[1].0, "fixtures.2.color");
    assert_eq!(updates[2].0, "beat.bpm");

    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_ephemeral_subscription_filter() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    // Client A: sender with ** subscription
    let mut client_a = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Client B: subscribes to fixtures.**
    let mut client_b = RustClient::connect(&server.ws_url(), vec!["fixtures.**".to_string()])
        .await
        .unwrap();

    // Client C: subscribes to beat.**
    let mut client_c = RustClient::connect(&server.ws_url(), vec!["beat.**".to_string()])
        .await
        .unwrap();

    // Client A sends batch with both fixture and beat data
    client_a
        .send_ephemeral_batch(&[
            ("fixtures.1.color", &[255, 0, 0]),
            ("beat.bpm", &[0, 0, 0, 120]),
        ])
        .await
        .unwrap();

    // Client B should receive (subscribed to fixtures.**)
    let updates_b = client_b
        .wait_for_ephemeral_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    // Client B gets the full batch since its subscription matches at least one path
    // The server sends the full EphemeralBatch to any subscriber that matches any path
    assert!(!updates_b.is_empty());

    // Client C should receive (subscribed to beat.**)
    let updates_c = client_c
        .wait_for_ephemeral_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    assert!(!updates_c.is_empty());

    client_a.close().await.unwrap();
    client_b.close().await.unwrap();
    client_c.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_ephemeral_no_persistence() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Send ephemeral data
    client1
        .send_ephemeral("fixtures.1.color", &[255, 0, 0])
        .await
        .unwrap();

    // Client 2 receives it
    let _ = client2
        .wait_for_ephemeral_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    // Disconnect both clients
    client1.close().await.unwrap();
    client2.close().await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect a new client - should NOT have the ephemeral data in CRDT
    let client3 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // The ephemeral path should NOT exist in the CRDT database
    assert_eq!(client3.get_path("fixtures.1.color").await, None);

    client3.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_ephemeral_not_echoed_to_sender() {
    init_test_logging();

    let server = TestServer::start().await.unwrap();

    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Also connect a second client so there's a valid recipient
    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    // Client 1 sends ephemeral
    client1
        .send_ephemeral("fixtures.1.color", &[255, 0, 0])
        .await
        .unwrap();

    // Client 1 should NOT receive its own message back
    let result = client1
        .wait_for_ephemeral_timeout(Duration::from_millis(500))
        .await;
    assert!(
        result.is_err(),
        "Sender should not receive their own ephemeral message"
    );

    // But client 2 should have received it
    // (already consumed by the time we check, but let's verify client2 gets it)
    // Re-send for clean test
    client1
        .send_ephemeral("fixtures.2.color", &[0, 255, 0])
        .await
        .unwrap();

    let updates = client2
        .wait_for_ephemeral_timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert!(!updates.is_empty());

    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
}
