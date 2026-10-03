// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Delta push tests
//!
//! A push carries what the client wrote since its last push and nothing
//! more: not the history it was given at open, not a change it heard from
//! the server in between. The server's activity log records how many
//! changes each push carried, which is what these tests read; it keeps the
//! last hundred events, which bounds how long a history they build.

use super::{browser_client::BrowserTestClient, init_test_logging, test_server::TestServer};
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

/// How many changes each push from the browser on `document` carried. The
/// test page names its connection `browser-<time>`, and each test here has
/// one browser.
async fn browser_pushes(server: &TestServer, document: &str) -> Vec<usize> {
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
            } if from_client_id.starts_with("browser-") && applied_to == document => {
                Some(change_count)
            }
            _ => None,
        })
        .collect();
    counts.reverse();
    counts
}

/// Wait until the server has applied `count` pushes from the browser.
async fn wait_for_browser_pushes(server: &TestServer, document: &str, count: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while browser_pushes(server, document).await.len() < count {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the server saw {:?} from the browser, not {} pushes",
            browser_pushes(server, document).await,
            count
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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

/// An editor pushes at every keystroke and does not wait for the server to
/// acknowledge one before the next; on a busy server none arrives for
/// seconds. Each push is the keystroke it follows and nothing before it:
/// the browser module used to send everything since the server's last
/// acknowledgement, so the thirty-second keystroke without one carried
/// thirty-two changes (Studio's S-21).
#[tokio::test]
async fn a_browser_typing_faster_than_the_server_acknowledges_pushes_each_keystroke_once() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let browser =
        BrowserTestClient::start_with_documents(&server.ws_url(), vec!["typing.1".into()])
            .await
            .unwrap();
    browser
        .set_document_text("typing.1", "source", "")
        .await
        .unwrap();
    wait_for_browser_pushes(&server, "typing.1", 1).await;

    let typed = "hue = time * 0.25 + index / 32.0";
    browser
        .type_document_text("typing.1", "source", 0, typed)
        .await
        .unwrap();
    let keystrokes = typed.chars().count();
    wait_for_browser_pushes(&server, "typing.1", 1 + keystrokes).await;
    let counts = browser_pushes(&server, "typing.1").await;
    assert_eq!(counts.len(), 1 + keystrokes, "{:?}", counts);
    assert!(counts.iter().all(|&count| count == 1), "{:?}", counts);

    // What the server holds is what was typed, in order.
    let listener = SyncClient::open(&server.ws_url(), "typing.1", everything())
        .await
        .unwrap();
    assert_eq!(
        listener.get_path("source").await,
        Some(ScalarValue::Str(typed.into()))
    );

    browser.close().await.unwrap();
    server.shutdown().await.unwrap();
}

/// Two people typing into one text: what the browser hears from the other
/// between its own keystrokes is the server's already and does not go back
/// with its next push, and what the server acknowledges (heads that may
/// name changes the browser has not heard yet) never makes it send more.
#[tokio::test]
async fn a_browser_typing_beside_another_pushes_only_its_own_keystrokes() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let browser =
        BrowserTestClient::start_with_documents(&server.ws_url(), vec!["typing.2".into()])
            .await
            .unwrap();
    browser
        .set_document_text("typing.2", "source", "")
        .await
        .unwrap();
    wait_for_browser_pushes(&server, "typing.2", 1).await;
    let worker = SyncClient::open_with_id(&server.ws_url(), "worker", "typing.2", everything())
        .await
        .unwrap();

    for round in 0..20 {
        browser
            .splice_document_text("typing.2", "source", 0, 0, "a")
            .await
            .unwrap();
        worker.splice_text("source", 0, 0, "b").await.unwrap();
        // Heard before the browser's next keystroke, every other round;
        // in the rest the browser types on before it has heard.
        if round % 2 == 0 {
            browser
                .wait_for_document_text_change("typing.2")
                .await
                .unwrap();
            browser
                .take_document_text_changes("typing.2")
                .await
                .unwrap();
        }
    }
    wait_for_browser_pushes(&server, "typing.2", 21).await;
    wait_for_pushes(&server, "worker", "typing.2", 20).await;
    let counts = browser_pushes(&server, "typing.2").await;
    assert_eq!(counts.len(), 21, "{:?}", counts);
    assert!(counts.iter().all(|&count| count == 1), "{:?}", counts);
    assert_eq!(
        pushes_from(&server, "worker", "typing.2").await,
        vec![1; 20]
    );

    // Both hold forty characters, the same forty.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let in_browser = browser
            .get_document_path("typing.2", "source")
            .await
            .unwrap();
        let in_worker = worker.get_path("source").await;
        if let (Some(serde_json::Value::String(seen)), Some(ScalarValue::Str(held))) =
            (&in_browser, &in_worker)
        {
            if seen.len() == 40 && seen.as_str() == held.as_str() {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the browser holds {:?} and the worker {:?}",
            in_browser,
            in_worker
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    browser.close().await.unwrap();
    server.shutdown().await.unwrap();
}
