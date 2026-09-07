// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Administrative endpoints: orders from the application that owns
//! membership, given to the server that enforces it.
//!
//! The authority answers the server's questions once, at open, and the
//! server holds a connection to that answer for as long as the document is
//! open — so removing somebody from a team never closed a document they
//! already had open, and the ten seconds the answer is cached only decided
//! how soon a *reopen* would be refused. `POST /admin/revoke` is the other
//! direction of that seam: the application says a subject's access ended,
//! and the server closes what the subject holds now.
//!
//! Every request proves itself with `AUTHORITY_SECRET` as a bearer — the
//! secret the server presents to the authority, so the application that
//! answers the server is the one that may order it. With no secret
//! configured the endpoint is closed rather than open: an order anyone can
//! give is not an order.

use crate::state::{Revoked, ServerState};
use axum::{
    extract::State,
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};

/// The administrative routes, to be merged into the server's router.
pub fn router() -> Router<ServerState> {
    Router::new().route("/admin/revoke", post(revoke))
}

/// What `POST /admin/revoke` takes: the subject — the `id` of the actor
/// the authority's `whoami` named, never a client id — and the document to
/// close on it, or `null` for every document the subject has open.
#[derive(Debug, serde::Deserialize)]
pub struct RevokeRequest {
    pub subject: String,
    #[serde(default)]
    pub document: Option<String>,
}

/// What it answers: every document closed, by client id. Empty when the
/// subject held nothing, which is not an error — the cache is cleared
/// either way.
#[derive(Debug, serde::Serialize)]
pub struct RevokeResponse {
    pub closed: Vec<Revoked>,
}

/// The bearer token on a request, if any.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

/// Equal without saying, by how long it takes, where they differ.
fn same_secret(presented: &str, required: &str) -> bool {
    let presented = presented.as_bytes();
    let required = required.as_bytes();
    let mut difference = presented.len() ^ required.len();
    for (a, b) in presented.iter().zip(required.iter().cycle()) {
        difference |= usize::from(a ^ b);
    }
    difference == 0
}

async fn revoke(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(request): Json<RevokeRequest>,
) -> Response {
    let Some(required) = state.admin_secret() else {
        tracing::warn!(
            "A revocation was asked for, but with no AUTHORITY_SECRET the endpoint is closed"
        );
        return (
            StatusCode::FORBIDDEN,
            "revocation needs AUTHORITY_SECRET; the endpoint is closed without one",
        )
            .into_response();
    };
    if !bearer(&headers).is_some_and(|presented| same_secret(presented, required)) {
        return (
            StatusCode::UNAUTHORIZED,
            "the bearer is not AUTHORITY_SECRET",
        )
            .into_response();
    }
    if request.subject.is_empty() {
        return (StatusCode::BAD_REQUEST, "subject must not be empty").into_response();
    }
    let closed = state
        .revoke(&request.subject, request.document.as_deref())
        .await;
    Json(RevokeResponse { closed }).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_compare_whole() {
        assert!(same_secret("s3cret", "s3cret"));
        assert!(!same_secret("s3cret", "s3cre"));
        assert!(!same_secret("s3cre", "s3cret"));
        assert!(!same_secret("", "s3cret"));
        assert!(!same_secret("s3cret", ""));
        assert!(!same_secret("s3crEt", "s3cret"));
    }

    #[test]
    fn the_bearer_is_read_from_the_header() {
        let mut headers = HeaderMap::new();
        assert_eq!(bearer(&headers), None);
        headers.insert(AUTHORIZATION, "Bearer  s3cret ".parse().unwrap());
        assert_eq!(bearer(&headers), Some("s3cret"));
        headers.insert(AUTHORIZATION, "Basic abc".parse().unwrap());
        assert_eq!(bearer(&headers), None);
        headers.insert(AUTHORIZATION, "Bearer ".parse().unwrap());
        assert_eq!(bearer(&headers), None);
    }
}
