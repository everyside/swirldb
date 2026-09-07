// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Observer tests
//!
//! An observer on a path hears every write to it, under it or above it,
//! whoever made it: this handle's own writes with `local` true, the
//! server's with `local` false. A map or a list has no scalar to compare,
//! so it is the changed paths that carry the news, and these tests put a
//! real browser on a document to prove the glue fires for a local write
//! under a map — which is what Studio's browser tests had to fake.

use super::{
    browser_client::BrowserTestClient, init_test_logging, rust_client::RustClient,
    test_server::TestServer,
};
use automerge::ScalarValue;
use serde_json::json;
use std::time::Duration;
use swirldb_client::SyncClient;

fn everything() -> Vec<String> {
    vec!["**".to_string()]
}

#[tokio::test]
async fn a_map_observer_in_the_browser_hears_local_and_remote_writes_under_it() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let browser =
        BrowserTestClient::start_with_documents(&server.ws_url(), vec!["palette.1".into()])
            .await
            .unwrap();
    browser
        .observe_document_path("palette.1", "stops")
        .await
        .unwrap();

    // The browser's own writes under the map: each heard once, marked
    // local, with the map's whole value.
    browser
        .set_document_path("palette.1", "stops.a.color", json!("#ff0000"))
        .await
        .unwrap();
    browser
        .set_document_path("palette.1", "stops.b.color", json!("#0000ff"))
        .await
        .unwrap();
    let heard = browser
        .take_document_observations("palette.1", "stops")
        .await
        .unwrap();
    assert_eq!(heard.len(), 2, "{:?}", heard);
    assert!(heard.iter().all(|seen| seen.local && seen.path == "stops"));
    assert_eq!(heard[0].changed_paths, vec!["stops.a.color".to_string()]);
    assert_eq!(heard[0].value, json!({ "a": { "color": "#ff0000" } }));
    assert_eq!(
        heard[1].value,
        json!({ "a": { "color": "#ff0000" }, "b": { "color": "#0000ff" } })
    );

    // A write elsewhere is not heard.
    browser
        .set_document_path("palette.1", "name", json!("warm"))
        .await
        .unwrap();
    assert!(browser
        .take_document_observations("palette.1", "stops")
        .await
        .unwrap()
        .is_empty());

    // A Rust client writes under the map: heard once, not local, naming
    // the path, with the value as it now stands.
    let rust = SyncClient::open(&server.ws_url(), "palette.1", everything())
        .await
        .unwrap();
    rust.set_path("stops.a.color", ScalarValue::Str("#ff8800".into()))
        .await
        .unwrap();
    browser
        .wait_for_document_observation("palette.1", "stops")
        .await
        .unwrap();
    let heard = browser
        .take_document_observations("palette.1", "stops")
        .await
        .unwrap();
    assert_eq!(heard.len(), 1, "{:?}", heard);
    assert!(!heard[0].local);
    assert_eq!(heard[0].changed_paths, vec!["stops.a.color".to_string()]);
    assert_eq!(heard[0].value["a"]["color"], json!("#ff8800"));

    // A remote delete under the map is heard, and the map has lost the key.
    rust.delete_path("stops.b").await.unwrap();
    browser
        .wait_for_document_observation("palette.1", "stops")
        .await
        .unwrap();
    let heard = browser
        .take_document_observations("palette.1", "stops")
        .await
        .unwrap();
    assert_eq!(heard.len(), 1);
    assert_eq!(heard[0].changed_paths, vec!["stops.b".to_string()]);
    assert_eq!(heard[0].value, json!({ "a": { "color": "#ff8800" } }));

    browser.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_list_observer_in_the_browser_is_handed_the_array() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let browser =
        BrowserTestClient::start_with_documents(&server.ws_url(), vec!["palette.2".into()])
            .await
            .unwrap();
    browser
        .observe_document_path("palette.2", "stops")
        .await
        .unwrap();

    // A Rust client builds the list; the browser's observer is handed the
    // array as it grows, never a scalar.
    let rust = SyncClient::open(&server.ws_url(), "palette.2", everything())
        .await
        .unwrap();
    rust.insert_list_item("stops", 0, json!("#ff0000"))
        .await
        .unwrap();
    browser
        .wait_for_document_observation("palette.2", "stops")
        .await
        .unwrap();
    let heard = browser
        .take_document_observations("palette.2", "stops")
        .await
        .unwrap();
    assert_eq!(heard[0].value, json!(["#ff0000"]));
    // The change made the list and put the item in it: both are named.
    assert_eq!(
        heard[0].changed_paths,
        vec!["stops".to_string(), "stops.0".to_string()]
    );
    assert!(!heard[0].local);

    // The browser deletes an item it can see and hears its own delete,
    // marked local; the Rust client hears it too.
    let mut rust_changes = rust.on_change();
    browser
        .delete_document_path("palette.2", "stops.0")
        .await
        .unwrap();
    let heard = browser
        .take_document_observations("palette.2", "stops")
        .await
        .unwrap();
    assert_eq!(heard.len(), 1);
    assert!(heard[0].local);
    assert_eq!(heard[0].value, json!([]));
    let paths = tokio::time::timeout(Duration::from_secs(2), rust_changes.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(paths, vec!["stops.0".to_string()]);
    assert_eq!(rust.db().await.get_value("stops"), Some(json!([])));

    // Deleting what is not there fires nothing and sends nothing.
    browser
        .delete_document_path("palette.2", "stops.0")
        .await
        .unwrap();
    assert!(browser
        .take_document_observations("palette.2", "stops")
        .await
        .unwrap()
        .is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rust_changes.recv())
            .await
            .is_err()
    );

    browser.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_rust_observer_hears_a_remote_write_under_a_map() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let listener = SyncClient::open(&server.ws_url(), "palette.3", everything())
        .await
        .unwrap();
    let heard = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = heard.clone();
    listener
        .db()
        .await
        .observe("stops".to_string(), move |notification| {
            sink.lock().unwrap().push(notification);
        });
    let mut listener_changes = listener.on_change();

    let mut writer = RustClient::open(&server.ws_url(), "palette.3", everything())
        .await
        .unwrap();
    writer
        .set_path("stops.a.color", ScalarValue::Str("#ff0000".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), listener_changes.recv())
        .await
        .unwrap()
        .unwrap();

    let heard = heard.lock().unwrap().clone();
    assert_eq!(heard.len(), 1);
    assert!(!heard[0].local);
    assert!(heard[0].value.is_none(), "a map has no scalar");
    assert!(heard[0]
        .changed_paths
        .contains(&"stops.a.color".to_string()));

    server.shutdown().await.unwrap();
}
