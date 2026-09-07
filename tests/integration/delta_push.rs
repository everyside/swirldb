// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Delta push tests
//!
//! A push carries what the client wrote since its last push and nothing
//! more: not the history it was given at open, not a change it heard from
//! the server in between. The server's activity log records how many
//! changes each push carried, which is what these tests read; it keeps the
//! last hundred events, which bounds how long a history they build.

use super::{init_test_logging, test_server::TestServer};
use automerge::ScalarValue;
use serde_json::json;
use std::time::Duration;
use swirldb_client::{Change, SyncClient};
use swirldb_server::state::ActivityEvent;
use tokio::sync::broadcast;

fn everything() -> Vec<String> {
    vec!["**".to_string()]
}

/// The next change a client hears, within a bound — its own or another's.
async fn next_change(receiver: &mut broadcast::Receiver<Change>) -> Change {
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("a change arrives in time")
        .expect("the change channel is open")
}

/// How many changes each push from `client_id` on `document` carried, in
/// the order the server applied them.
async fn pushes_from(server: &TestServer, client_id: &str, document: &str) -> Vec<usize> {
    let mut counts: Vec<usize> = server
        .state
        .get_activity()
        .await
        .into_iter()
        .filter_map(|event| match event {
            ActivityEvent::ChangesApplied {
                from_client_id,
                document: applied_to,
                change_count,
                ..
            } if from_client_id == client_id && applied_to == document => Some(change_count),
            _ => None,
        })
        .collect();
    counts.reverse(); // the log is most recent first
    counts
}

/// Wait until the server has applied `count` pushes from `client_id`.
async fn wait_for_pushes(server: &TestServer, client_id: &str, document: &str, count: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while pushes_from(server, client_id, document).await.len() < count {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the server never saw {} pushes from {}",
            count,
            client_id
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn a_one_scalar_write_on_a_long_history_pushes_one_change() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    // Alice writes a long history: fifty scalars, a text, a list.
    let alice = SyncClient::open_with_id(&server.ws_url(), "alice", "long.1", everything())
        .await
        .unwrap();
    for i in 0..50 {
        alice
            .set_path(&format!("field.{}", i), ScalarValue::Int(i))
            .await
            .unwrap();
    }
    alice.set_text("source", "hue = t").await.unwrap();
    alice
        .insert_list_item("stops", 0, json!({ "color": "#ff0000" }))
        .await
        .unwrap();
    wait_for_pushes(&server, "alice", "long.1", 52).await;

    // Every one of Alice's pushes carried the one change it made, the
    // fiftieth as much as the first.
    let counts = pushes_from(&server, "alice", "long.1").await;
    assert_eq!(counts.len(), 52);
    assert!(counts.iter().all(|&count| count == 1), "{:?}", counts);

    // Bob opens the document and is handed all of it. His one write pushes
    // one change, not the fifty-two he was given.
    let bob = SyncClient::open_with_id(&server.ws_url(), "bob", "long.1", everything())
        .await
        .unwrap();
    assert_eq!(bob.get_path("field.49").await, Some(ScalarValue::Int(49)));
    bob.set_path("field.50", ScalarValue::Int(50))
        .await
        .unwrap();
    bob.splice_text("source", 0, 0, "// ").await.unwrap();
    bob.delete_path("field.0").await.unwrap();
    wait_for_pushes(&server, "bob", "long.1", 3).await;
    assert_eq!(pushes_from(&server, "bob", "long.1").await, vec![1, 1, 1]);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn writes_made_through_the_core_ride_the_next_push() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let alice = SyncClient::open_with_id(&server.ws_url(), "alice", "batch.1", everything())
        .await
        .unwrap();
    let bob = SyncClient::open(&server.ws_url(), "batch.1", everything())
        .await
        .unwrap();
    let mut bob_changes = bob.on_change();

    // Three scalars through the core, which sends nothing, and a fourth
    // through the client, whose push carries all four — as one Automerge
    // change holding four operations, since nothing committed in between.
    {
        let db = alice.db().await;
        for (key, value) in [("a", 1), ("b", 2), ("c", 3)] {
            db.set_path(key, ScalarValue::Int(value)).unwrap();
        }
    }
    alice.set_path("d", ScalarValue::Int(4)).await.unwrap();
    let change = next_change(&mut bob_changes).await;
    assert_eq!(change.changed_paths, vec!["a", "b", "c", "d"]);
    assert!(!change.local);
    // The server broadcasts before it logs, so the log is waited for.
    wait_for_pushes(&server, "alice", "batch.1", 1).await;
    assert_eq!(pushes_from(&server, "alice", "batch.1").await, vec![1]);
    assert_eq!(bob.get_path("a").await, Some(ScalarValue::Int(1)));
    assert_eq!(bob.get_path("d").await, Some(ScalarValue::Int(4)));

    // Or a push on its own, with nothing more written.
    alice.db().await.set_path("e", ScalarValue::Int(5)).unwrap();
    alice.push().await.unwrap();
    assert_eq!(next_change(&mut bob_changes).await.changed_paths, vec!["e"]);
    wait_for_pushes(&server, "alice", "batch.1", 2).await;
    assert_eq!(pushes_from(&server, "alice", "batch.1").await, vec![1, 1]);

    // A push with nothing owed sends nothing.
    alice.push().await.unwrap();
    alice.delete_path("nowhere").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), bob_changes.recv())
            .await
            .is_err()
    );
    assert_eq!(pushes_from(&server, "alice", "batch.1").await, vec![1, 1]);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_change_heard_from_the_server_is_not_pushed_back() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let alice = SyncClient::open_with_id(&server.ws_url(), "alice", "echo.1", everything())
        .await
        .unwrap();
    let bob = SyncClient::open_with_id(&server.ws_url(), "bob", "echo.1", everything())
        .await
        .unwrap();
    let mut alice_changes = alice.on_change();
    let mut bob_changes = bob.on_change();

    // Alice writes and hears herself, marked local; Bob hears it as a
    // broadcast. Bob's next write is one change, not two: what he heard is
    // the server's already.
    alice
        .set_path("title", ScalarValue::Str("Alice".into()))
        .await
        .unwrap();
    let own = next_change(&mut alice_changes).await;
    assert!(own.local);
    assert_eq!(own.changed_paths, vec!["title"]);
    let heard = next_change(&mut bob_changes).await;
    assert!(!heard.local);
    assert_eq!(heard.changed_paths, vec!["title"]);
    bob.set_path("subtitle", ScalarValue::Str("Bob".into()))
        .await
        .unwrap();
    let heard = next_change(&mut alice_changes).await;
    assert!(!heard.local);
    assert_eq!(heard.changed_paths, vec!["subtitle"]);
    wait_for_pushes(&server, "bob", "echo.1", 1).await;
    assert_eq!(pushes_from(&server, "bob", "echo.1").await, vec![1]);

    // And nothing came back around: Alice heard her own write once, as
    // hers, and Bob's once, as his — never her own from the server.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), alice_changes.recv())
            .await
            .is_err()
    );
    assert_eq!(
        alice.get_path("subtitle").await,
        Some(ScalarValue::Str("Bob".into()))
    );

    server.shutdown().await.unwrap();
}
