// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Text tests
//!
//! A text is the one value two people can be inside at once. These tests
//! put two Rust clients on one document and have them type into the same
//! string without waiting for each other; both keep their characters, and a
//! third client that only listens sees the edits as splices with positions.

use super::{init_test_logging, rust_client::RustClient, test_server::TestServer};
use automerge::ScalarValue;
use std::time::Duration;
use swirldb_client::SyncClient;
use swirldb_core::core::{TextChange, TextSplice};
use tokio::sync::broadcast;

fn everything() -> Vec<String> {
    vec!["**".to_string()]
}

/// The next remote text change on a receiver, within a bound.
async fn next_remote(receiver: &mut broadcast::Receiver<TextChange>) -> TextChange {
    loop {
        let change = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("a text change arrives in time")
            .expect("the text change channel is open");
        if !change.local {
            return change;
        }
    }
}

#[tokio::test]
async fn two_clients_typing_into_one_string_both_keep_their_characters() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let alice = SyncClient::open(&server.ws_url(), "pattern.1", everything())
        .await
        .unwrap();
    let bob = SyncClient::open(&server.ws_url(), "pattern.1", everything())
        .await
        .unwrap();
    let mut bob_changes = bob.on_text_change();
    let mut alice_changes = alice.on_text_change();

    alice.set_text("source", "the cat").await.unwrap();
    next_remote(&mut bob_changes).await;
    assert_eq!(
        bob.get_path("source").await,
        Some(ScalarValue::Str("the cat".into()))
    );

    // Both type at once, neither waiting to hear from the other first.
    alice.splice_text("source", 4, 0, "black ").await.unwrap();
    bob.splice_text("source", 7, 0, " sat").await.unwrap();

    next_remote(&mut bob_changes).await;
    next_remote(&mut alice_changes).await;

    let merged = Some(ScalarValue::Str("the black cat sat".into()));
    assert_eq!(alice.get_path("source").await, merged);
    assert_eq!(bob.get_path("source").await, merged);

    // The server's copy is the same string, and a late opener gets it whole.
    let late = RustClient::open(&server.ws_url(), "pattern.1", everything())
        .await
        .unwrap();
    assert_eq!(late.get_path("source").await, merged);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_listener_receives_edits_as_splices_with_positions() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let writer = SyncClient::open(&server.ws_url(), "pattern.2", everything())
        .await
        .unwrap();
    writer.set_text("source", "abc").await.unwrap();

    let listener = SyncClient::open(&server.ws_url(), "pattern.2", everything())
        .await
        .unwrap();
    let mut heard = listener.on_text_change();

    writer.splice_text("source", 3, 0, "def").await.unwrap();
    let change = next_remote(&mut heard).await;
    assert_eq!(change.path, "source");
    assert_eq!(
        change.splices,
        vec![TextSplice {
            position: 3,
            delete_count: 0,
            insert: "def".to_string()
        }]
    );

    writer.splice_text("source", 0, 2, "").await.unwrap();
    let change = next_remote(&mut heard).await;
    assert_eq!(
        change.splices,
        vec![TextSplice {
            position: 0,
            delete_count: 2,
            insert: String::new()
        }]
    );
    assert_eq!(
        listener.get_path("source").await,
        Some(ScalarValue::Str("cdef".into()))
    );

    // The writer hears its own edits too, marked local.
    let mut own = writer.on_text_change();
    writer.splice_text("source", 4, 0, "!").await.unwrap();
    let change = own.recv().await.unwrap();
    assert!(change.local);
    assert_eq!(change.splices[0].position, 4);

    server.shutdown().await.unwrap();
}

/// A browser and a Rust client in one text. The browser counts positions in
/// UTF-16 code units and the Rust client in code points; "😀" is two of the
/// one and one of the other, and each hears the other's edit in its own units.
///
/// The two edits are sequenced — each waits to hear the other's before making
/// its own — because what this test pins down is the position each side is
/// told, and that depends on what its text was when the edit arrived. That
/// concurrent edits merge is what the two Rust clients above prove.
#[tokio::test]
async fn a_browser_and_a_rust_client_edit_one_string_in_their_own_units() {
    use super::browser_client::{BrowserTestClient, SeenTextSplice};

    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let browser =
        BrowserTestClient::start_with_documents(&server.ws_url(), vec!["pattern.3".into()])
            .await
            .unwrap();
    browser
        .set_document_text("pattern.3", "source", "😀 cat")
        .await
        .unwrap();

    let rust = SyncClient::open(&server.ws_url(), "pattern.3", everything())
        .await
        .unwrap();
    let mut rust_changes = rust.on_text_change();
    assert_eq!(
        rust.get_path("source").await,
        Some(ScalarValue::Str("😀 cat".into()))
    );

    // The Rust client appends after "cat": code point 5, which the browser
    // hears as UTF-16 unit 6, "😀" being two units there.
    rust.splice_text("source", 5, 0, " sat").await.unwrap();
    browser
        .wait_for_document_text_change("pattern.3")
        .await
        .unwrap();
    let heard = browser
        .take_document_text_changes("pattern.3")
        .await
        .unwrap();
    assert_eq!(heard.len(), 1);
    assert!(!heard[0].local);
    assert_eq!(
        heard[0].splices,
        vec![SeenTextSplice {
            position: 6,
            delete_count: 0,
            insert: " sat".to_string()
        }]
    );

    // The browser inserts after "😀 ": UTF-16 unit 3, which the Rust client
    // hears as code point 2.
    browser
        .splice_document_text("pattern.3", "source", 3, 0, "black ")
        .await
        .unwrap();
    let change = next_remote(&mut rust_changes).await;
    assert_eq!(
        change.splices,
        vec![TextSplice {
            position: 2,
            delete_count: 0,
            insert: "black ".to_string()
        }]
    );

    let merged = "😀 black cat sat";
    assert_eq!(
        browser
            .get_document_path("pattern.3", "source")
            .await
            .unwrap(),
        Some(serde_json::json!(merged))
    );
    assert_eq!(
        rust.get_path("source").await,
        Some(ScalarValue::Str(merged.into()))
    );

    browser.close().await.unwrap();
    server.shutdown().await.unwrap();
}
