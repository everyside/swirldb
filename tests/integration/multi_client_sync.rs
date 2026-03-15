// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Multi-client sync tests with Rust clients
//!
//! Tests CRDT sync behavior with 2-3 Rust clients.

use super::{init_test_logging, rust_client::RustClient, test_server::TestServer};
use automerge::ScalarValue;
use std::time::Duration;
use tracing::info;

/// # Scenario: Two clients sync messages bidirectionally
///
/// **Given:**
/// - Two clients connected to the same server
/// - Both subscribed to all paths ("**")
///
/// **When:**
/// - Client 1 writes a message
///
/// **Then:**
/// - Client 2 receives the message via broadcast
/// - Both clients have identical state
#[tokio::test]
async fn test_two_client_sync() {
    init_test_logging();

    info!("🧪 Starting test: Two client sync");
    info!("   Scenario: Client 1 writes → Client 2 receives broadcast");

    // Start server
    let server = TestServer::start().await.unwrap();
    info!("✓ Server started");

    // Connect two clients
    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    assert_eq!(server.connection_count(), 2);
    info!("✓ Two clients connected");

    // Client 1 sets a value
    info!("→ Client 1 writes: message = 'Hello from client1'");
    client1
        .set_path("message", ScalarValue::Str("Hello from client1".into()))
        .await
        .unwrap();

    // Client 2 should receive the broadcast
    info!("← Client 2 waiting for broadcast...");
    client2
        .wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    // Verify client 2 has the value
    let value = client2.get_path("message").await;
    assert_eq!(value, Some(ScalarValue::Str("Hello from client1".into())));
    info!("✓ Client 2 received message: {:?}", value);

    // Cleanup
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
    info!("✅ Test passed: Two client sync");
}

/// # Scenario: Three clients sync in real-time
///
/// **Given:**
/// - Three clients connected to the same server
/// - All subscribed to all paths ("**")
///
/// **When:**
/// - Client 1 writes a value
///
/// **Then:**
/// - Both Client 2 and Client 3 receive the broadcast
/// - All three clients have identical state
#[tokio::test]
async fn test_three_client_sync() {
    init_test_logging();

    info!("🧪 Starting test: Three client sync");
    info!("   Scenario: One client writes → Two others receive");

    let server = TestServer::start().await.unwrap();
    info!("✓ Server started");

    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client3 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    assert_eq!(server.connection_count(), 3);
    info!("✓ Three clients connected");

    // Client 1 sets a value
    info!("→ Client 1 writes: data.count = 42");
    client1
        .set_path("data.count", ScalarValue::Int(42))
        .await
        .unwrap();

    // Both client 2 and 3 should receive it
    info!("← Clients 2 and 3 waiting for broadcasts...");
    client2
        .wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    client3
        .wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    // Verify both have the value
    assert_eq!(
        client2.get_path("data.count").await,
        Some(ScalarValue::Int(42))
    );
    assert_eq!(
        client3.get_path("data.count").await,
        Some(ScalarValue::Int(42))
    );
    info!("✓ Both clients received: data.count = 42");

    // Cleanup
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    client3.close().await.unwrap();
    server.shutdown().await.unwrap();
    info!("✅ Test passed: Three client sync");
}

/// # Scenario: Concurrent writes merge via CRDT
///
/// **Given:**
/// - Two clients connected to the server
/// - Both have synchronized initial state
///
/// **When:**
/// - Client 1 writes: user.name = "Alice"
/// - Client 2 writes: user.age = 30
/// - Writes are sequenced with broadcast synchronization
///
/// **Then:**
/// - Server merges both changes
/// - Both clients receive broadcasts and merge locally
/// - Final state on both: {user: {name: "Alice", age: 30}}
/// - No conflicts, no data loss
#[tokio::test]
async fn test_concurrent_writes_crdt_merge() {
    init_test_logging();

    info!("🧪 Starting test: Concurrent writes with CRDT merge");
    info!("   Scenario: Two clients write different fields → Both converge to same state");

    let server = TestServer::start().await.unwrap();
    info!("✓ Server started");

    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();
    info!("✓ Two clients connected");

    // Client1 writes first
    info!("→ Client 1 writes: user.name = 'Alice'");
    client1
        .set_path("user.name", ScalarValue::Str("Alice".into()))
        .await
        .unwrap();

    // Wait for client2 to receive the broadcast
    info!("← Client 2 waiting for broadcast...");
    client2.wait_for_broadcast().await.unwrap();
    info!("✓ Client 2 received broadcast from Client 1");

    // Now client2 writes (it has both its own data and client1's)
    info!("→ Client 2 writes: user.age = 30");
    client2
        .set_path("user.age", ScalarValue::Int(30))
        .await
        .unwrap();

    // Wait for client1 to receive the broadcast
    info!("← Client 1 waiting for broadcast...");
    client1.wait_for_broadcast().await.unwrap();
    info!("✓ Client 1 received broadcast from Client 2");

    // Both clients should have both values (CRDT merge)
    assert_eq!(
        client1.get_path("user.name").await,
        Some(ScalarValue::Str("Alice".into()))
    );
    assert_eq!(
        client1.get_path("user.age").await,
        Some(ScalarValue::Int(30))
    );
    info!("✓ Client 1 has both fields: {{name: Alice, age: 30}}");

    assert_eq!(
        client2.get_path("user.name").await,
        Some(ScalarValue::Str("Alice".into()))
    );
    assert_eq!(
        client2.get_path("user.age").await,
        Some(ScalarValue::Int(30))
    );
    info!("✓ Client 2 has both fields: {{name: Alice, age: 30}}");

    // Cleanup
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
    info!("✅ Test passed: CRDT merge successful, no data loss");
}

/// # Scenario: Late joiner receives full state on connect
///
/// **Given:**
/// - Client 1 is connected and has written data
/// - Server has persisted the state
///
/// **When:**
/// - Client 2 connects later
///
/// **Then:**
/// - Client 2 receives full state in initial Sync message
/// - Client 2 has all existing data without needing to replay history
#[tokio::test]
async fn test_late_joiner_gets_full_state() {
    init_test_logging();

    info!("🧪 Starting test: Late joiner gets full state");
    info!("   Scenario: Client 1 writes → Client 2 joins later → Gets full state");

    let server = TestServer::start().await.unwrap();
    info!("✓ Server started");

    // Client 1 connects and sets some data
    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();
    info!("✓ Client 1 connected");

    info!("→ Client 1 writes: existing.data = 'Already here'");
    client1
        .set_path("existing.data", ScalarValue::Str("Already here".into()))
        .await
        .unwrap();

    // Wait for server to process
    tokio::time::sleep(Duration::from_millis(100)).await;
    info!("✓ Server processed write");

    // Client 2 connects later (should get full state in initial sync)
    info!("→ Client 2 connecting (late joiner)...");
    let client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();
    info!("✓ Client 2 connected");

    // Client 2 should have the data from initial sync
    let value = client2.get_path("existing.data").await;
    assert_eq!(value, Some(ScalarValue::Str("Already here".into())));
    info!("✓ Client 2 has existing data: {:?}", value);

    // Cleanup
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
    info!("✅ Test passed: Late joiner received full state");
}

/// # Scenario: Changes flow bidirectionally
///
/// **Given:**
/// - Two clients connected to the server
///
/// **When:**
/// - Client 1 writes msg1
/// - Client 2 writes msg2
///
/// **Then:**
/// - Client 1 receives msg2 from Client 2
/// - Client 2 receives msg1 from Client 1
/// - Both clients end up with both messages
#[tokio::test]
async fn test_bidirectional_sync() {
    init_test_logging();

    info!("🧪 Starting test: Bidirectional sync");
    info!("   Scenario: Both clients write → Both receive each other's changes");

    let server = TestServer::start().await.unwrap();
    info!("✓ Server started");

    let mut client1 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();

    let mut client2 = RustClient::connect(&server.ws_url(), vec!["**".to_string()])
        .await
        .unwrap();
    info!("✓ Two clients connected");

    // Client 1 -> Client 2
    info!("→ Client 1 writes: msg1 = 'from client1'");
    client1
        .set_path("msg1", ScalarValue::Str("from client1".into()))
        .await
        .unwrap();

    info!("← Client 2 waiting for broadcast...");
    client2
        .wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(
        client2.get_path("msg1").await,
        Some(ScalarValue::Str("from client1".into()))
    );
    info!("✓ Client 2 received msg1");

    // Client 2 -> Client 1
    info!("→ Client 2 writes: msg2 = 'from client2'");
    client2
        .set_path("msg2", ScalarValue::Str("from client2".into()))
        .await
        .unwrap();

    info!("← Client 1 waiting for broadcast...");
    client1
        .wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(
        client1.get_path("msg2").await,
        Some(ScalarValue::Str("from client2".into()))
    );
    info!("✓ Client 1 received msg2");

    // Both clients should have both messages
    assert_eq!(
        client1.get_path("msg1").await,
        Some(ScalarValue::Str("from client1".into()))
    );
    assert_eq!(
        client1.get_path("msg2").await,
        Some(ScalarValue::Str("from client2".into()))
    );

    assert_eq!(
        client2.get_path("msg1").await,
        Some(ScalarValue::Str("from client1".into()))
    );
    assert_eq!(
        client2.get_path("msg2").await,
        Some(ScalarValue::Str("from client2".into()))
    );
    info!("✓ Both clients have both messages");

    // Cleanup
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    server.shutdown().await.unwrap();
    info!("✅ Test passed: Bidirectional sync working");
}
