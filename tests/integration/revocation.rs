// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Revocation tests
//!
//! The authority is asked once, at open, and a connection holds its answer
//! for as long as the document is open. `POST /admin/revoke` is how the
//! application that owns membership closes what a subject holds now:
//! the subject's clients are told `OpenDenied` with the reason `revoked`,
//! the server drops the documents from them, and the authority forgets
//! what it cached, so a reopen is asked afresh and refused.

use super::{
    browser_client::BrowserTestClient, init_test_logging, rust_client::RustClient,
    test_server::TestServer,
};
use automerge::ScalarValue;
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::{extract::State, routing::post, Json, Router};
use serde_json::json;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use swirldb_client::{Denied, SyncClient};
use swirldb_server::authority::HttpAuthority;

const SECRET: &str = "s3cret";

fn everything() -> Vec<String> {
    vec!["**".to_string()]
}

/// The application that owns membership: whose token is whose, and who may
/// write which document — a set the tests take grants out of.
struct Membership {
    grants: Mutex<HashSet<(String, String)>>,
    may_open_asked: AtomicUsize,
    whoami_asked: AtomicUsize,
}

impl Membership {
    fn revoke(&self, subject: &str, document: &str) {
        self.grants
            .lock()
            .unwrap()
            .remove(&(subject.to_string(), document.to_string()));
    }
}

fn admit(headers: &HeaderMap) -> Result<(), StatusCode> {
    let presented = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if presented == format!("Bearer {SECRET}") {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

#[derive(serde::Deserialize)]
struct WhoAmI {
    token: String,
}

async fn whoami(
    State(membership): State<Arc<Membership>>,
    headers: HeaderMap,
    Json(request): Json<WhoAmI>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    admit(&headers)?;
    membership.whoami_asked.fetch_add(1, Ordering::SeqCst);
    let id = match request.token.as_str() {
        "alice-token" => "alice",
        "bob-token" => "bob",
        _ => return Err(StatusCode::UNAUTHORIZED),
    };
    Ok(Json(
        json!({ "subject": { "actor_type": "User", "id": id } }),
    ))
}

#[derive(serde::Deserialize)]
struct MayOpen {
    subject: serde_json::Value,
    document: String,
}

async fn may_open(
    State(membership): State<Arc<Membership>>,
    headers: HeaderMap,
    Json(request): Json<MayOpen>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    admit(&headers)?;
    membership.may_open_asked.fetch_add(1, Ordering::SeqCst);
    let subject = request.subject["id"].as_str().unwrap_or_default();
    let granted = membership
        .grants
        .lock()
        .unwrap()
        .contains(&(subject.to_string(), request.document.clone()));
    Ok(Json(
        json!({ "access": if granted { "write" } else { "none" } }),
    ))
}

/// The membership application serving, and a sync server whose authority
/// asks it and whose administrative endpoints take its secret.
async fn server_and_membership() -> (TestServer, Arc<Membership>) {
    let membership = Arc::new(Membership {
        grants: Mutex::new(HashSet::from([
            ("alice".to_string(), "draft.1".to_string()),
            ("alice".to_string(), "draft.2".to_string()),
            ("bob".to_string(), "draft.1".to_string()),
        ])),
        may_open_asked: AtomicUsize::new(0),
        whoami_asked: AtomicUsize::new(0),
    });
    let router = Router::new()
        .route("/authority/whoami", post(whoami))
        .route("/authority/may-open", post(may_open))
        .with_state(membership.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let authority = HttpAuthority::with_time_to_live(
        format!("http://{}/authority", address),
        Duration::from_secs(60),
    )
    .with_secret(SECRET);
    let server = TestServer::start_with_authority_and_secret(Arc::new(authority), SECRET)
        .await
        .unwrap();
    (server, membership)
}

/// `POST /admin/revoke`, as the application would send it.
async fn revoke(
    server: &TestServer,
    bearer: Option<&str>,
    subject: &str,
    document: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let request = reqwest::Client::new()
        .post(format!("{}/admin/revoke", server.http_url()))
        .json(&json!({ "subject": subject, "document": document }));
    let request = match bearer {
        Some(bearer) => request.bearer_auth(bearer),
        None => request,
    };
    let response = request.send().await.unwrap();
    let status = StatusCode::from_u16(response.status().as_u16()).unwrap();
    let body = response.text().await.unwrap();
    (
        status,
        serde_json::from_str(&body).unwrap_or(serde_json::Value::String(body)),
    )
}

async fn next_denial(receiver: &mut tokio::sync::broadcast::Receiver<Denied>) -> Denied {
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("a denial arrives in time")
        .expect("the denial channel is open")
}

/// The Rust client ends its connection on a denial; a write soon fails.
async fn wait_until_writes_fail(client: &SyncClient) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while client
        .set_path("after", ScalarValue::Str("revoked".into()))
        .await
        .is_ok()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the connection never closed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn revoking_a_document_closes_it_for_the_subject_and_a_reopen_asks_again() {
    init_test_logging();
    let (server, membership) = server_and_membership().await;

    let alice = SyncClient::open_authenticated_with_id(
        &server.ws_url(),
        "alice",
        "alice-token",
        "draft.1",
        everything(),
    )
    .await
    .unwrap();
    let bob = SyncClient::open_authenticated_with_id(
        &server.ws_url(),
        "bob",
        "bob-token",
        "draft.1",
        everything(),
    )
    .await
    .unwrap();
    let mut alice_denied = alice.on_denied();
    let mut bob_denied = bob.on_denied();
    let mut bob_changes = bob.on_change();
    alice
        .set_path("title", ScalarValue::Str("Alice was here".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), bob_changes.recv())
        .await
        .unwrap()
        .unwrap();
    let asked_before = membership.may_open_asked.load(Ordering::SeqCst);

    // The application takes Alice off the document and tells the server.
    membership.revoke("alice", "draft.1");
    let (status, body) = revoke(&server, Some(SECRET), "alice", Some("draft.1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({ "closed": [{ "client_id": "alice", "document": "draft.1" }] })
    );

    // Alice hears why, and her connection is over.
    let denial = next_denial(&mut alice_denied).await;
    assert_eq!(
        denial,
        Denied {
            document: "draft.1".into(),
            reason: "revoked".into()
        }
    );
    wait_until_writes_fail(&alice).await;

    // Bob heard nothing and still writes; a late opener sees his write and
    // not the one Alice tried after.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), bob_denied.recv())
            .await
            .is_err()
    );
    bob.set_path("title", ScalarValue::Str("Bob's now".into()))
        .await
        .unwrap();
    let late = RustClient::open_authenticated(&server.ws_url(), "bob-token", "draft.1")
        .await
        .unwrap();
    assert_eq!(
        late.get_path("title").await,
        Some(ScalarValue::Str("Bob's now".into()))
    );
    assert_eq!(late.get_path("after").await, None);

    // Alice reopens: the cache was cleared, the application is asked, and
    // it says no.
    let again = SyncClient::open_authenticated_with_id(
        &server.ws_url(),
        "alice",
        "alice-token",
        "draft.1",
        everything(),
    )
    .await;
    let message = again.err().expect("alice is refused").to_string();
    assert!(message.contains("may not open draft.1"), "{}", message);
    assert!(membership.may_open_asked.load(Ordering::SeqCst) > asked_before);

    // Her other document was not named and is untouched.
    let other = SyncClient::open_authenticated_with_id(
        &server.ws_url(),
        "alice",
        "alice-token",
        "draft.2",
        everything(),
    )
    .await;
    assert!(other.is_ok());

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn revoking_everything_closes_every_document_the_subject_holds() {
    init_test_logging();
    let (server, membership) = server_and_membership().await;

    let alice_one = SyncClient::open_authenticated_with_id(
        &server.ws_url(),
        "alice-1",
        "alice-token",
        "draft.1",
        everything(),
    )
    .await
    .unwrap();
    let alice_two = SyncClient::open_authenticated_with_id(
        &server.ws_url(),
        "alice-2",
        "alice-token",
        "draft.2",
        everything(),
    )
    .await
    .unwrap();
    let bob = SyncClient::open_authenticated_with_id(
        &server.ws_url(),
        "bob",
        "bob-token",
        "draft.1",
        everything(),
    )
    .await
    .unwrap();
    let mut one_denied = alice_one.on_denied();
    let mut two_denied = alice_two.on_denied();
    let mut bob_denied = bob.on_denied();
    let whoami_before = membership.whoami_asked.load(Ordering::SeqCst);

    membership.revoke("alice", "draft.1");
    membership.revoke("alice", "draft.2");
    let (status, body) = revoke(&server, Some(SECRET), "alice", None).await;
    assert_eq!(status, StatusCode::OK);
    let mut closed: Vec<(String, String)> = body["closed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            (
                entry["client_id"].as_str().unwrap().to_string(),
                entry["document"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    closed.sort();
    assert_eq!(
        closed,
        vec![
            ("alice-1".to_string(), "draft.1".to_string()),
            ("alice-2".to_string(), "draft.2".to_string()),
        ]
    );

    assert_eq!(next_denial(&mut one_denied).await.document, "draft.1");
    assert_eq!(next_denial(&mut two_denied).await.document, "draft.2");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), bob_denied.recv())
            .await
            .is_err()
    );
    bob.set_path("title", ScalarValue::Str("still here".into()))
        .await
        .unwrap();

    // With no document named the authority forgot her token too: the
    // reopen asks whoami again, and may-open, and is refused.
    let again = SyncClient::open_authenticated_with_id(
        &server.ws_url(),
        "alice-1",
        "alice-token",
        "draft.2",
        everything(),
    )
    .await;
    assert!(again.is_err());
    assert!(membership.whoami_asked.load(Ordering::SeqCst) > whoami_before);

    // A subject nobody is closes nothing and is not an error.
    let (status, body) = revoke(&server, Some(SECRET), "carol", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "closed": [] }));

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn the_revoke_endpoint_takes_orders_from_the_secret_alone() {
    init_test_logging();
    let (server, _membership) = server_and_membership().await;
    let alice = SyncClient::open_authenticated_with_id(
        &server.ws_url(),
        "alice",
        "alice-token",
        "draft.1",
        everything(),
    )
    .await
    .unwrap();
    let mut alice_denied = alice.on_denied();

    let (status, _) = revoke(&server, None, "alice", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = revoke(&server, Some("wrong"), "alice", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = revoke(&server, Some(SECRET), "", None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Nothing happened to Alice.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), alice_denied.recv())
            .await
            .is_err()
    );
    alice
        .set_path("title", ScalarValue::Str("still mine".into()))
        .await
        .unwrap();
    server.shutdown().await.unwrap();

    // A server with no secret has no administrative endpoint to speak of.
    let closed = TestServer::start().await.unwrap();
    let (status, body) = revoke(&closed, Some(SECRET), "alice", None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{}", body);
    closed.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_browser_hears_its_document_closed_by_a_revocation() {
    init_test_logging();
    let (server, membership) = server_and_membership().await;

    let browser = BrowserTestClient::start_with_documents_as(
        &server.ws_url(),
        vec!["draft.1".into(), "draft.2".into()],
        Some("alice-token"),
    )
    .await
    .unwrap();
    assert_eq!(
        browser.document_access("draft.1").await.unwrap(),
        Some(json!("write"))
    );

    membership.revoke("alice", "draft.1");
    let (status, body) = revoke(&server, Some(SECRET), "alice", Some("draft.1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["closed"][0]["document"], json!("draft.1"));

    // The handle is told why; the document is gone from the connection and
    // the other one is still there, on the same socket.
    browser.wait_for_document_denial("draft.1").await.unwrap();
    assert_eq!(
        browser.take_document_denials("draft.1").await.unwrap(),
        vec!["revoked".to_string()]
    );
    assert_eq!(
        browser.document_access("draft.1").await.unwrap(),
        Some(json!(null))
    );
    assert_eq!(
        browser.open_documents().await.unwrap(),
        vec!["draft.2".to_string()]
    );
    assert_eq!(
        browser.document_access("draft.2").await.unwrap(),
        Some(json!("write"))
    );

    browser.close().await.unwrap();
    server.shutdown().await.unwrap();
}
