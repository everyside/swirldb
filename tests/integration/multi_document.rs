// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Multi-document tests
//!
//! A server holds many documents, each synced whole to whoever has it open
//! and to nobody else; opening asks the authority. These tests use the Rust
//! client, which holds one document per connection; the browser test in
//! `browser_sync` covers several documents on one connection.

use super::{init_test_logging, rust_client::RustClient, test_server::TestServer};
use automerge::ScalarValue;
use axum::{extract::State, routing::post, Json, Router};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use swirldb_core::policy::PolicyEngine;
use swirldb_core::protocol::Access;
use swirldb_server::authority::{HttpAuthority, PolicyAuthority};

fn everything() -> Vec<String> {
    vec!["**".to_string()]
}

#[tokio::test]
async fn two_documents_sync_independently() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let mut alpha_writer = RustClient::open(&server.ws_url(), "alpha", everything())
        .await
        .unwrap();
    let mut alpha_reader = RustClient::open(&server.ws_url(), "alpha", everything())
        .await
        .unwrap();
    let mut beta = RustClient::open(&server.ws_url(), "beta", everything())
        .await
        .unwrap();

    alpha_writer
        .set_path("title", ScalarValue::Str("Alpha".into()))
        .await
        .unwrap();
    alpha_reader
        .wait_for_broadcast_timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        alpha_reader.get_path("title").await,
        Some(ScalarValue::Str("Alpha".into()))
    );

    // Beta hears nothing about alpha.
    assert!(beta
        .wait_for_broadcast_timeout(Duration::from_millis(300))
        .await
        .is_err());
    assert_eq!(beta.get_path("title").await, None);

    // And alpha hears nothing about beta.
    beta.set_path("title", ScalarValue::Str("Beta".into()))
        .await
        .unwrap();
    assert!(alpha_reader
        .wait_for_broadcast_timeout(Duration::from_millis(300))
        .await
        .is_err());
    assert_eq!(
        alpha_reader.get_path("title").await,
        Some(ScalarValue::Str("Alpha".into()))
    );

    // A late opener of beta receives beta's history and only beta's.
    let late = RustClient::open(&server.ws_url(), "beta", everything())
        .await
        .unwrap();
    assert_eq!(
        late.get_path("title").await,
        Some(ScalarValue::Str("Beta".into()))
    );

    // The default document is untouched by either.
    let default = RustClient::connect(&server.ws_url(), everything())
        .await
        .unwrap();
    assert_eq!(default.get_path("title").await, None);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn ephemeral_traffic_stays_within_its_document() {
    init_test_logging();
    let server = TestServer::start().await.unwrap();

    let mut alpha_sender = RustClient::open(&server.ws_url(), "alpha", everything())
        .await
        .unwrap();
    let mut alpha_receiver = RustClient::open(&server.ws_url(), "alpha", everything())
        .await
        .unwrap();
    let mut beta = RustClient::open(&server.ws_url(), "beta", everything())
        .await
        .unwrap();

    alpha_sender
        .send_ephemeral("presence.alice", &[1, 2, 3])
        .await
        .unwrap();

    let updates = alpha_receiver
        .wait_for_ephemeral_timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(updates[0].0, "presence.alice");
    assert!(beta
        .wait_for_ephemeral_timeout(Duration::from_millis(300))
        .await
        .is_err());

    server.shutdown().await.unwrap();
}

/// The policy engine as the authority: rules over document ids.
fn policy_authority() -> Arc<PolicyAuthority> {
    let engine = PolicyEngine::from_json(
        r#"{"policies":{"rules":[
            {"priority":10,"actor":{"type":"Any"},"action":"Write","path_pattern":"shared.*","effect":"Allow"},
            {"priority":20,"actor":{"type":"Any"},"action":"Read","path_pattern":"published.*","effect":"Allow"}
        ]}}"#,
    )
    .unwrap();
    Arc::new(PolicyAuthority::new(engine))
}

#[tokio::test]
async fn the_policy_authority_refuses_and_grades() {
    init_test_logging();
    let server = TestServer::start_with_authority(policy_authority())
        .await
        .unwrap();

    let refused = RustClient::open(&server.ws_url(), "private.1", everything()).await;
    let message = refused.err().expect("private.1 is refused").to_string();
    assert!(message.contains("Refused to open private.1"), "{}", message);

    // The default document is refused too: access is the authority's alone.
    assert!(RustClient::connect(&server.ws_url(), everything())
        .await
        .is_err());

    let writer = RustClient::open(&server.ws_url(), "shared.1", everything())
        .await
        .unwrap();
    assert_eq!(writer.access(), Access::Write);

    let reader = RustClient::open(&server.ws_url(), "published.1", everything())
        .await
        .unwrap();
    assert_eq!(reader.access(), Access::Read);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_reader_may_receive_but_not_write() {
    init_test_logging();
    let server = TestServer::start_with_authority(policy_authority())
        .await
        .unwrap();

    // Nobody can write published.*, so seed it through the server directly,
    // the way an application that owns the document would.
    {
        let document = server.state.document("published.1").await;
        let db = document.read().await;
        db.set_path("title", ScalarValue::Str("Published".into()))
            .unwrap();
    }

    let mut reader = RustClient::open(&server.ws_url(), "published.1", everything())
        .await
        .unwrap();
    assert_eq!(
        reader.get_path("title").await,
        Some(ScalarValue::Str("Published".into()))
    );

    // The reader's local copy changes; the server refuses the push.
    reader
        .set_path("title", ScalarValue::Str("Defaced".into()))
        .await
        .unwrap();
    let error = reader
        .wait_for_error_timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert!(error.contains("may not write published.1"), "{}", error);

    let document = server.state.document("published.1").await;
    assert_eq!(
        document.read().await.get_path("title"),
        Some(ScalarValue::Str("Published".into()))
    );

    server.shutdown().await.unwrap();
}

/// A stand-in for the application that owns membership: answers
/// `POST /may-open` from a table, and counts how often it was asked.
struct FakeApplication {
    asked: AtomicUsize,
}

#[derive(serde::Deserialize)]
struct MayOpen {
    subject: serde_json::Value,
    document: String,
}

async fn may_open(
    State(application): State<Arc<FakeApplication>>,
    Json(request): Json<MayOpen>,
) -> Json<serde_json::Value> {
    application.asked.fetch_add(1, Ordering::SeqCst);
    let subject = request.subject["id"].as_str().unwrap_or_default();
    let access = match (subject, request.document.as_str()) {
        ("alice", "draft.1") => "write",
        (_, "draft.1") => "read",
        _ => "none",
    };
    Json(serde_json::json!({ "access": access }))
}

async fn serve_fake_application() -> (String, Arc<FakeApplication>) {
    let application = Arc::new(FakeApplication {
        asked: AtomicUsize::new(0),
    });
    let router = Router::new()
        .route("/authority/may-open", post(may_open))
        .with_state(application.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{}/authority", address), application)
}

#[tokio::test]
async fn the_http_authority_asks_the_application_and_caches_briefly() {
    init_test_logging();
    let (endpoint, application) = serve_fake_application().await;
    let authority = Arc::new(HttpAuthority::with_time_to_live(
        endpoint,
        Duration::from_secs(30),
    ));
    let server = TestServer::start_with_authority(authority).await.unwrap();

    // The handler's subject is the client id until JWTs are wired in.
    let alice = swirldb_client::SyncClient::open_with_id(
        &server.ws_url(),
        "alice",
        "draft.1",
        everything(),
    )
    .await
    .unwrap();
    assert_eq!(alice.access(), Access::Write);

    let bob =
        swirldb_client::SyncClient::open_with_id(&server.ws_url(), "bob", "draft.1", everything())
            .await
            .unwrap();
    assert_eq!(bob.access(), Access::Read);

    let stranger = swirldb_client::SyncClient::open_with_id(
        &server.ws_url(),
        "carol",
        "draft.2",
        everything(),
    )
    .await;
    assert!(stranger.is_err());
    assert_eq!(application.asked.load(Ordering::SeqCst), 3);

    // Alice again, within the cache window: the application is not asked.
    drop(alice);
    let alice_again = swirldb_client::SyncClient::open_with_id(
        &server.ws_url(),
        "alice",
        "draft.1",
        everything(),
    )
    .await
    .unwrap();
    assert_eq!(alice_again.access(), Access::Write);
    assert_eq!(application.asked.load(Ordering::SeqCst), 3);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_unreachable_http_authority_refuses_everything() {
    init_test_logging();
    let authority = Arc::new(HttpAuthority::new("http://127.0.0.1:9/authority"));
    let server = TestServer::start_with_authority(authority).await.unwrap();
    assert!(RustClient::open(&server.ws_url(), "draft.1", everything())
        .await
        .is_err());
    server.shutdown().await.unwrap();
}
