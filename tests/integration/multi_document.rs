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
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::{extract::State, routing::post, Json, Router};
use base64::Engine;
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

/// A stand-in for the application that owns sessions and membership:
/// answers `POST /whoami` from a token table and `POST /may-open` from an
/// access table, and records what it was asked.
struct FakeApplication {
    asked: AtomicUsize,
    whoami_asked: AtomicUsize,
    /// The subject ids `may-open` was asked about, in order
    subjects_asked: std::sync::Mutex<Vec<String>>,
    /// The shared secret a request must prove before it is answered, as
    /// `Bearer <secret>` or as `Basic` for `swirldb:<secret>`; `None` answers
    /// anyone
    required_secret: Option<String>,
    /// Every `Authorization` header value received, in order, `""` for none
    authorization_seen: std::sync::Mutex<Vec<String>>,
}

impl FakeApplication {
    /// Records the credential and refuses the request if it is not the one
    /// required. Refused requests are not counted as asked: an application
    /// that has not admitted a caller has not consulted anything for it.
    fn admit(&self, headers: &HeaderMap) -> Result<(), StatusCode> {
        let authorization = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        self.authorization_seen
            .lock()
            .unwrap()
            .push(authorization.clone());
        let Some(secret) = &self.required_secret else {
            return Ok(());
        };
        let basic = base64::engine::general_purpose::STANDARD.encode(format!("swirldb:{secret}"));
        if authorization == format!("Bearer {secret}") || authorization == format!("Basic {basic}")
        {
            Ok(())
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }

    fn authorization_seen(&self) -> Vec<String> {
        self.authorization_seen.lock().unwrap().clone()
    }
}

#[derive(serde::Deserialize)]
struct WhoAmI {
    token: String,
}

async fn whoami(
    State(application): State<Arc<FakeApplication>>,
    headers: HeaderMap,
    Json(request): Json<WhoAmI>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    application.admit(&headers)?;
    application.whoami_asked.fetch_add(1, Ordering::SeqCst);
    let id = match request.token.as_str() {
        "alice-token" => "alice",
        "bob-token" => "bob",
        _ => return Err(StatusCode::UNAUTHORIZED),
    };
    Ok(Json(serde_json::json!({
        "subject": { "actor_type": "User", "id": id }
    })))
}

#[derive(serde::Deserialize)]
struct MayOpen {
    subject: serde_json::Value,
    document: String,
}

async fn may_open(
    State(application): State<Arc<FakeApplication>>,
    headers: HeaderMap,
    Json(request): Json<MayOpen>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    application.admit(&headers)?;
    application.asked.fetch_add(1, Ordering::SeqCst);
    let subject = request.subject["id"].as_str().unwrap_or_default();
    application
        .subjects_asked
        .lock()
        .unwrap()
        .push(subject.to_string());
    let access = match (subject, request.document.as_str()) {
        ("alice", "draft.1") => "write",
        (_, "draft.1") => "read",
        _ => "none",
    };
    Ok(Json(serde_json::json!({ "access": access })))
}

async fn serve_fake_application() -> (String, Arc<FakeApplication>) {
    serve_fake_application_requiring(None).await
}

/// The fake application, answering only requests that prove `secret`.
async fn serve_fake_application_requiring(secret: Option<&str>) -> (String, Arc<FakeApplication>) {
    let application = Arc::new(FakeApplication {
        asked: AtomicUsize::new(0),
        whoami_asked: AtomicUsize::new(0),
        subjects_asked: std::sync::Mutex::new(Vec::new()),
        required_secret: secret.map(str::to_string),
        authorization_seen: std::sync::Mutex::new(Vec::new()),
    });
    let router = Router::new()
        .route("/authority/whoami", post(whoami))
        .route("/authority/may-open", post(may_open))
        .with_state(application.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{}/authority", address), application)
}

async fn server_with_fake_application() -> (TestServer, Arc<FakeApplication>) {
    let (endpoint, application) = serve_fake_application().await;
    let authority = Arc::new(HttpAuthority::with_time_to_live(
        endpoint,
        Duration::from_secs(30),
    ));
    let server = TestServer::start_with_authority(authority).await.unwrap();
    (server, application)
}

#[tokio::test]
async fn the_http_authority_asks_the_application_and_caches_briefly() {
    init_test_logging();
    let (server, application) = server_with_fake_application().await;

    let alice = swirldb_client::SyncClient::open_authenticated(
        &server.ws_url(),
        "alice-token",
        "draft.1",
        everything(),
    )
    .await
    .unwrap();
    assert_eq!(alice.access(), Access::Write);

    let bob = swirldb_client::SyncClient::open_authenticated(
        &server.ws_url(),
        "bob-token",
        "draft.1",
        everything(),
    )
    .await
    .unwrap();
    assert_eq!(bob.access(), Access::Read);

    let stranger = swirldb_client::SyncClient::open_authenticated(
        &server.ws_url(),
        "bob-token",
        "draft.2",
        everything(),
    )
    .await;
    assert!(stranger.is_err());
    assert_eq!(application.asked.load(Ordering::SeqCst), 3);

    // Alice again, within the cache window: the application is not asked,
    // about her token or about her access.
    drop(alice);
    let alice_again = swirldb_client::SyncClient::open_authenticated(
        &server.ws_url(),
        "alice-token",
        "draft.1",
        everything(),
    )
    .await
    .unwrap();
    assert_eq!(alice_again.access(), Access::Write);
    assert_eq!(application.asked.load(Ordering::SeqCst), 3);
    assert_eq!(application.whoami_asked.load(Ordering::SeqCst), 2);

    server.shutdown().await.unwrap();
}

/// Alice, opening `draft.1` through an authority built by `configure` against
/// an application that requires `secret`. Returns what the application saw.
async fn open_through(
    secret: &str,
    configure: impl FnOnce(String) -> HttpAuthority,
) -> (
    Result<swirldb_client::SyncClient, anyhow::Error>,
    Arc<FakeApplication>,
) {
    let (endpoint, application) = serve_fake_application_requiring(Some(secret)).await;
    let server = TestServer::start_with_authority(Arc::new(configure(endpoint)))
        .await
        .unwrap();
    let alice = swirldb_client::SyncClient::open_authenticated(
        &server.ws_url(),
        "alice-token",
        "draft.1",
        everything(),
    )
    .await;
    server.shutdown().await.unwrap();
    (alice, application)
}

/// `http://swirldb:<secret>@host/authority`: the older way to carry the secret.
fn with_user_info(endpoint: &str, secret: &str) -> String {
    endpoint.replacen("http://", &format!("http://swirldb:{secret}@"), 1)
}

#[tokio::test]
async fn the_authority_secret_rides_as_a_bearer_on_both_questions() {
    init_test_logging();
    let (alice, application) = open_through("s3cret", |endpoint| {
        HttpAuthority::new(endpoint).with_secret("s3cret")
    })
    .await;
    assert_eq!(alice.unwrap().access(), Access::Write);

    let seen = application.authorization_seen();
    assert_eq!(seen.len(), 2, "whoami and may-open, one request each");
    assert!(
        seen.iter().all(|value| value == "Bearer s3cret"),
        "{seen:?}"
    );
    assert_eq!(application.whoami_asked.load(Ordering::SeqCst), 1);
    assert_eq!(application.asked.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn without_the_secret_the_application_refuses_and_so_does_the_authority() {
    init_test_logging();
    let (alice, application) = open_through("s3cret", HttpAuthority::new).await;
    assert!(alice.is_err());

    // The application saw a bare request and answered 401 before consulting
    // anything; the authority fails closed on it.
    assert_eq!(application.authorization_seen(), vec!["".to_string()]);
    assert_eq!(application.whoami_asked.load(Ordering::SeqCst), 0);
    assert_eq!(application.asked.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn user_info_in_the_url_still_rides_as_basic() {
    init_test_logging();
    let (alice, application) = open_through("s3cret", |endpoint| {
        HttpAuthority::new(with_user_info(&endpoint, "s3cret"))
    })
    .await;
    assert_eq!(alice.unwrap().access(), Access::Write);

    let seen = application.authorization_seen();
    assert_eq!(seen.len(), 2);
    assert!(
        seen.iter().all(|value| value.starts_with("Basic ")),
        "{seen:?}"
    );
}

#[tokio::test]
async fn the_bearer_wins_over_user_info_when_both_are_present() {
    init_test_logging();
    // The user-info carries a secret the application no longer accepts; the
    // bearer carries the one it does. Only the bearer must arrive: were both
    // sent, the application would read the Basic first and refuse.
    let (alice, application) = open_through("s3cret", |endpoint| {
        HttpAuthority::new(with_user_info(&endpoint, "stale")).with_secret("s3cret")
    })
    .await;
    assert_eq!(alice.unwrap().access(), Access::Write);

    let seen = application.authorization_seen();
    assert_eq!(seen.len(), 2);
    assert!(
        seen.iter().all(|value| value == "Bearer s3cret"),
        "{seen:?}"
    );
}

#[tokio::test]
async fn a_valid_token_yields_the_authority_subject_in_may_open() {
    init_test_logging();
    let (server, application) = server_with_fake_application().await;

    // The client calls itself "alice" but carries bob's token. The subject
    // the application is asked about is bob, and bob may only read.
    let pretender = swirldb_client::SyncClient::open_authenticated_with_id(
        &server.ws_url(),
        "alice",
        "bob-token",
        "draft.1",
        everything(),
    )
    .await
    .unwrap();
    assert_eq!(pretender.access(), Access::Read);
    assert_eq!(
        *application.subjects_asked.lock().unwrap(),
        vec!["bob".to_string()]
    );

    // And the claim buys nothing at write time either.
    let mut errors = pretender.on_error();
    pretender
        .set_path("title", ScalarValue::Str("Defaced".into()))
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), errors.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(error.contains("may not write draft.1"), "{}", error);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_connection_the_authority_does_not_know_is_refused() {
    init_test_logging();
    let (server, application) = server_with_fake_application().await;

    // No token at all: refused without troubling the application.
    let unnamed = RustClient::open(&server.ws_url(), "draft.1", everything()).await;
    let message = unnamed.err().expect("no token is refused").to_string();
    assert!(message.contains("not authenticated"), "{}", message);
    assert_eq!(application.whoami_asked.load(Ordering::SeqCst), 0);

    // A token the application does not recognize: refused, and the refusal
    // is remembered so a second try does not ask again.
    for _ in 0..2 {
        let forged = swirldb_client::SyncClient::open_authenticated_with_id(
            &server.ws_url(),
            "alice",
            "forged-token",
            "draft.1",
            everything(),
        )
        .await;
        let message = forged.err().expect("a forged token is refused").to_string();
        assert!(message.contains("not authenticated"), "{}", message);
    }
    assert_eq!(application.whoami_asked.load(Ordering::SeqCst), 1);
    assert_eq!(application.asked.load(Ordering::SeqCst), 0);

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
    assert!(swirldb_client::SyncClient::open_authenticated(
        &server.ws_url(),
        "alice-token",
        "draft.1",
        everything()
    )
    .await
    .is_err());
    server.shutdown().await.unwrap();
}
