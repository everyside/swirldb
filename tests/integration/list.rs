// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! List tests
//!
//! A list is the one collection two people can add to at once. These tests
//! put two Rust clients on one document and have them insert into the same
//! list without waiting for each other; both keep their items, in an order
//! every copy agrees on, and a delete travels as a delete rather than as a
//! tombstone the readers have to skip.

use super::{init_test_logging, rust_client::RustClient, test_server::TestServer};
use serde_json::json;
use std::time::Duration;
use swirldb_client::{Change, SyncClient};
use tokio::sync::broadcast;

fn everything() -> Vec<String> {
    vec!["**".to_string()]
}

/// The paths of the next broadcast a client hears, within a bound. Its own
/// writes are on the same channel, marked local, and are passed by.
async fn next_change(receiver: &mut broadcast::Receiver<Change>) -> Vec<String> {
    loop {
        let change = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("a change arrives in time")
            .expect("the change channel is open");
        if !change.local {
            return change.changed_paths;
        }
    }
}

#[tokio::test]
async fn two_clients_inserting_into_one_list_both_keep_their_items() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let alice = SyncClient::open(&server.ws_url(), "palette.1", everything())
        .await
        .unwrap();
    let bob = SyncClient::open(&server.ws_url(), "palette.1", everything())
        .await
        .unwrap();
    let mut bob_changes = bob.on_change();
    let mut alice_changes = alice.on_change();

    alice
        .insert_list_item("stops", 0, json!({ "color": "#ff0000" }))
        .await
        .unwrap();
    next_change(&mut bob_changes).await;
    alice
        .insert_list_item("stops", 1, json!({ "color": "#0000ff" }))
        .await
        .unwrap();
    next_change(&mut bob_changes).await;
    assert_eq!(
        bob.db().await.get_value("stops"),
        Some(json!([{ "color": "#ff0000" }, { "color": "#0000ff" }]))
    );

    // Both insert between the two stops, neither waiting for the other.
    alice
        .insert_list_item("stops", 1, json!({ "color": "#00ff00" }))
        .await
        .unwrap();
    bob.insert_list_item("stops", 1, json!({ "color": "#ffff00" }))
        .await
        .unwrap();
    next_change(&mut bob_changes).await;
    next_change(&mut alice_changes).await;

    let merged = alice.db().await.get_value("stops").unwrap();
    assert_eq!(merged, bob.db().await.get_value("stops").unwrap());
    let colors: Vec<String> = merged
        .as_array()
        .unwrap()
        .iter()
        .map(|stop| stop["color"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(colors.len(), 4);
    assert_eq!(colors[0], "#ff0000");
    assert_eq!(colors[3], "#0000ff");
    assert!(colors.contains(&"#00ff00".to_string()));
    assert!(colors.contains(&"#ffff00".to_string()));

    // A late opener gets the same list, whole and in the same order.
    let late = RustClient::open(&server.ws_url(), "palette.1", everything())
        .await
        .unwrap();
    assert_eq!(late.get_value("stops").await, Some(merged));

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_splice_and_a_delete_reach_the_other_side_as_edits() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let writer = SyncClient::open(&server.ws_url(), "palette.2", everything())
        .await
        .unwrap();
    writer
        .splice_list(
            "stops",
            0,
            0,
            vec![json!("#ff0000"), json!("#00ff00"), json!("#0000ff")],
        )
        .await
        .unwrap();

    let reader = SyncClient::open(&server.ws_url(), "palette.2", everything())
        .await
        .unwrap();
    let mut heard = reader.on_change();
    assert_eq!(
        reader.db().await.get_value("stops"),
        Some(json!(["#ff0000", "#00ff00", "#0000ff"]))
    );

    // A reorder: the last stop moves to the front, as a removal and an
    // insertion. The reader is told the positions, by index.
    writer.splice_list("stops", 2, 1, vec![]).await.unwrap();
    assert_eq!(next_change(&mut heard).await, vec!["stops.2".to_string()]);
    writer
        .splice_list("stops", 0, 0, vec![json!("#0000ff")])
        .await
        .unwrap();
    assert_eq!(next_change(&mut heard).await, vec!["stops.0".to_string()]);
    assert_eq!(
        reader.db().await.get_value("stops"),
        Some(json!(["#0000ff", "#ff0000", "#00ff00"]))
    );

    // A delete is a delete on the other side too, and of a map key as well.
    writer
        .set_path("name", automerge::ScalarValue::Str("warm".into()))
        .await
        .unwrap();
    next_change(&mut heard).await;
    writer.delete_path("stops.1").await.unwrap();
    next_change(&mut heard).await;
    writer.delete_path("name").await.unwrap();
    assert_eq!(next_change(&mut heard).await, vec!["name".to_string()]);
    assert_eq!(
        reader.db().await.get_value("stops"),
        Some(json!(["#0000ff", "#00ff00"]))
    );
    assert_eq!(reader.get_path("name").await, None);

    // Deleting what is not there sends nothing.
    writer.delete_path("name").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), heard.recv())
            .await
            .is_err()
    );

    server.shutdown().await.unwrap();
}
