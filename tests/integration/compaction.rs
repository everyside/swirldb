// Copyright 2026 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Compaction over the wire.
//!
//! A document past its threshold is folded into one change of its state when
//! nobody has it open: at the load that brings it into memory and at the
//! unload that lets it go. These tests hold the server to the three things
//! that make that safe for the people inside a document: nothing is compacted
//! under a connection that has it open, a client that opens afterwards syncs
//! with the snapshot as with any document, and a client whose copy predates
//! the compaction is told so rather than handed a merge that cannot work.

use super::{init_test_logging, test_server::TestServer};
use automerge::ScalarValue;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use swirldb_client::SyncClient;
use swirldb_core::compaction::CompactionThreshold;
use swirldb_core::protocol::Message;
use swirldb_core::storage::{DocumentStorage, InMemoryDocStorage};
use swirldb_core::SwirlDB;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

fn everything() -> Vec<String> {
    vec!["**".to_string()]
}

/// Low enough to cross in a few writes; the bytes bound stays out of the way.
const THRESHOLD: CompactionThreshold = CompactionThreshold {
    changes: 20,
    bytes: usize::MAX,
};

/// Wait until `check` holds, within a bound.
async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "never: {}", what);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A document as a store keeps it: `edits` changes to a source text and a
/// scalar beside it, each its own change.
fn a_long_history(edits: usize) -> SwirlDB {
    let db = SwirlDB::new();
    db.set_text("source", "").unwrap();
    db.get_heads();
    for index in 0..edits {
        db.splice_text("source", index, 0, "x").unwrap();
        db.set_path("revised", ScalarValue::Int(index as i64))
            .unwrap();
        db.get_heads();
    }
    db
}

/// The history a stored document carries, read from the storage itself.
async fn stored_history(
    storage: &InMemoryDocStorage,
    id: &str,
) -> Option<swirldb_core::compaction::History> {
    let bytes = storage.load(id).await.unwrap()?;
    let db = SwirlDB::new();
    db.load_state(&bytes).unwrap();
    Some(db.history())
}

#[tokio::test]
async fn nothing_is_compacted_under_an_open_document_and_its_last_close_compacts_it() {
    init_test_logging();
    let storage = Arc::new(InMemoryDocStorage::new());
    let server = TestServer::start_with_storage_and_compaction(storage.clone(), THRESHOLD)
        .await
        .unwrap();
    let url = server.ws_url();

    // Alice types well past the threshold with the document open.
    let alice = SyncClient::open_with_id(&url, "alice", "pattern.1", everything())
        .await
        .unwrap();
    alice.set_text("source", "").await.unwrap();
    for index in 0..30 {
        alice.splice_text("source", index, 0, "a").await.unwrap();
    }
    eventually("the server holds Alice's thirty-first change", || async {
        server
            .state
            .document("pattern.1")
            .await
            .read()
            .await
            .history()
            .changes
            >= 31
    })
    .await;

    // Bob arrives mid-session: he is sent the whole history, not a snapshot,
    // and the two of them go on syncing.
    let bob = SyncClient::open_with_id(&url, "bob", "pattern.1", everything())
        .await
        .unwrap();
    assert!(bob.db().await.history().changes >= 31);
    assert!(!bob.db().await.is_compacted());
    bob.splice_text("source", 0, 0, "b").await.unwrap();
    eventually("Alice hears Bob", || async {
        alice.get_path("source").await
            == Some(ScalarValue::Str(format!("b{}", "a".repeat(30)).into()))
    })
    .await;
    alice.splice_text("source", 31, 0, "!").await.unwrap();
    eventually("Bob hears Alice", || async {
        bob.get_path("source").await
            == Some(ScalarValue::Str(format!("b{}!", "a".repeat(30)).into()))
    })
    .await;
    assert!(stored_history(&storage, "pattern.1").await.unwrap().changes > 1);

    // Alice leaves; Bob still has it open, so it is still not compacted.
    drop(alice);
    eventually("the server lets Alice go", || async {
        server.connection_count() == 1
    })
    .await;
    assert!(stored_history(&storage, "pattern.1").await.unwrap().changes > 1);

    // Bob leaves too: the unload folds the history and stores the snapshot.
    drop(bob);
    eventually("the stored document is one change", || async {
        stored_history(&storage, "pattern.1").await.unwrap().changes == 1
    })
    .await;

    // Carol opens it afterwards, is sent the snapshot, writes, and Dave, who
    // opens after her, reads everything.
    let carol = SyncClient::open_with_id(&url, "carol", "pattern.1", everything())
        .await
        .unwrap();
    assert_eq!(carol.db().await.history().changes, 1);
    assert_eq!(
        carol.get_path("source").await,
        Some(ScalarValue::Str(format!("b{}!", "a".repeat(30)).into()))
    );
    carol.splice_text("source", 0, 1, "c").await.unwrap();
    carol
        .set_path("by", ScalarValue::Str("carol".into()))
        .await
        .unwrap();
    eventually("the server applies Carol's writes", || async {
        server
            .state
            .document("pattern.1")
            .await
            .read()
            .await
            .get_path("by")
            == Some(ScalarValue::Str("carol".into()))
    })
    .await;
    let dave = SyncClient::open_with_id(&url, "dave", "pattern.1", everything())
        .await
        .unwrap();
    assert_eq!(
        dave.get_path("source").await,
        Some(ScalarValue::Str(format!("c{}!", "a".repeat(30)).into()))
    );
    assert_eq!(
        dave.get_path("by").await,
        Some(ScalarValue::Str("carol".into()))
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_document_stored_past_the_threshold_is_compacted_before_anybody_is_sent_it() {
    init_test_logging();
    let storage = Arc::new(InMemoryDocStorage::new());
    let long = a_long_history(40);
    storage.save("pattern.2", &long.save_state()).await.unwrap();
    let small = a_long_history(5);
    storage
        .save("pattern.3", &small.save_state())
        .await
        .unwrap();

    let server = TestServer::start_with_storage_and_compaction(storage.clone(), THRESHOLD)
        .await
        .unwrap();

    let alice = SyncClient::open_with_id(&server.ws_url(), "alice", "pattern.2", everything())
        .await
        .unwrap();
    assert_eq!(alice.db().await.history().changes, 1);
    assert!(alice.db().await.is_compacted());
    assert_eq!(
        alice.db().await.get_value("source"),
        long.get_value("source")
    );
    assert_eq!(
        stored_history(&storage, "pattern.2").await.unwrap().changes,
        1
    );

    // Under the threshold, a document is sent as it was stored.
    let bob = SyncClient::open_with_id(&server.ws_url(), "bob", "pattern.3", everything())
        .await
        .unwrap();
    assert_eq!(bob.db().await.history(), small.history());

    server.shutdown().await.unwrap();
}

/// Send one frame and read frames until one satisfies `wanted`.
async fn exchange<S>(
    socket: &mut S,
    frame: Option<Message>,
    wanted: impl Fn(&Message) -> bool,
) -> Message
where
    S: futures::Sink<WsMessage>
        + futures::Stream<Item = Result<WsMessage, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
    <S as futures::Sink<WsMessage>>::Error: std::fmt::Debug,
{
    if let Some(frame) = frame {
        socket
            .send(WsMessage::Binary(frame.encode()))
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let WsMessage::Binary(data) = socket.next().await.unwrap().unwrap() else {
                continue;
            };
            let message = Message::decode(&data).unwrap();
            if wanted(&message) {
                return message;
            }
        }
    })
    .await
    .expect("the server answers in time")
}

/// The exact case compaction has to answer: a client kept its copy of the
/// document from before the compaction and comes back with that copy's heads.
#[tokio::test]
async fn a_client_whose_copy_predates_the_compaction_is_told_to_open_afresh() {
    init_test_logging();
    let storage = Arc::new(InMemoryDocStorage::new());
    let long = a_long_history(40);
    storage.save("pattern.4", &long.save_state()).await.unwrap();
    let server = TestServer::start_with_storage_and_compaction(storage.clone(), THRESHOLD)
        .await
        .unwrap();

    // The kept copy: the history as it stood, and an edit made on it.
    let kept = SwirlDB::new();
    kept.apply_changes(long.get_changes()).unwrap();
    let kept_heads = kept.get_heads();

    // Somebody opens it first, which compacts it.
    let alice = SyncClient::open_with_id(&server.ws_url(), "alice", "pattern.4", everything())
        .await
        .unwrap();
    assert!(alice.db().await.is_compacted());

    let (mut socket, _) = connect_async(server.ws_url()).await.unwrap();
    let answer = exchange(
        &mut socket,
        Some(Message::Connect {
            client_id: "kept".into(),
            subscriptions: everything(),
            heads: kept_heads.iter().flatten().copied().collect(),
            document: "pattern.4".into(),
        }),
        |message| {
            matches!(
                message,
                Message::OpenDenied { .. } | Message::SubscribeAck { .. } | Message::Sync { .. }
            )
        },
    )
    .await;
    match answer {
        Message::OpenDenied { document, reason } => {
            assert_eq!(document, "pattern.4");
            assert_eq!(reason, swirldb_server::handler::COMPACTED);
        }
        other => panic!("expected OpenDenied, got {:?}", other),
    }
    assert!(server
        .state
        .get_connections()
        .await
        .iter()
        .filter(|connection| connection.client_id == "kept")
        .all(|connection| connection.documents.is_empty()));

    // Opening afresh on the same connection works: the snapshot arrives.
    let synced = exchange(
        &mut socket,
        Some(Message::Open {
            document: "pattern.4".into(),
            subscriptions: everything(),
            heads: Vec::new(),
        }),
        |message| matches!(message, Message::Sync { .. }),
    )
    .await;
    let Message::Sync { changes, .. } = synced else {
        unreachable!()
    };
    assert_eq!(
        changes,
        server
            .state
            .document("pattern.4")
            .await
            .read()
            .await
            .get_changes()
    );

    // A push made on the kept copy depends on history the server no longer
    // has. It is refused, not acknowledged and silently queued.
    kept.splice_text("source", 0, 0, "lost? ").unwrap();
    let write = kept.local_changes_since(&kept_heads);
    let refused = exchange(
        &mut socket,
        Some(Message::Push {
            heads: kept.get_heads().into_iter().flatten().collect(),
            changes: write,
            document: "pattern.4".into(),
        }),
        |message| matches!(message, Message::Error { .. } | Message::PushAck { .. }),
    )
    .await;
    match refused {
        Message::Error { message } => {
            assert!(message.contains("depend on history"), "{}", message)
        }
        other => panic!("expected an Error, got {:?}", other),
    }
    assert_eq!(
        server
            .state
            .document("pattern.4")
            .await
            .read()
            .await
            .get_value("source"),
        long.get_value("source")
    );

    // And a write made on the snapshot, as a fresh client makes one, lands.
    let fresh = SwirlDB::new();
    fresh
        .apply_changes(
            server
                .state
                .document("pattern.4")
                .await
                .read()
                .await
                .get_changes(),
        )
        .unwrap();
    let fresh_heads = fresh.get_heads();
    fresh.set_value("note", json!("kept")).unwrap();
    let accepted = exchange(
        &mut socket,
        Some(Message::Push {
            heads: fresh.get_heads().into_iter().flatten().collect(),
            changes: fresh.local_changes_since(&fresh_heads),
            document: "pattern.4".into(),
        }),
        |message| matches!(message, Message::Error { .. } | Message::PushAck { .. }),
    )
    .await;
    assert!(
        matches!(accepted, Message::PushAck { .. }),
        "{:?}",
        accepted
    );
    assert_eq!(
        server
            .state
            .document("pattern.4")
            .await
            .read()
            .await
            .get_value("note"),
        Some(json!("kept"))
    );

    drop(alice);
    server.shutdown().await.unwrap();
}
