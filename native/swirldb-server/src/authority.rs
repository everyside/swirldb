// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Who a connection is, and which documents it may open.
//!
//! SwirlDB enforces access to documents; it does not decide it. The decision
//! belongs to whatever owns membership — an application's own database, a
//! policy file, nobody at all — and it is asked two questions. First, once
//! per connection: who is this? A connection presents a bearer token on its
//! WebSocket upgrade, and the authority says whose it is, or that it is
//! nobody's. Then, once per open: may this subject open this document? The
//! answer is `Read`, `Write`, or `None`, and the server holds the connection
//! to it from then on: a reader's `Push` is refused, a refused subject never
//! receives the history.
//!
//! The first question is what makes the second one worth asking. A subject
//! the client named for itself is a subject anyone can name, and an access
//! decision about it protects nothing. So where there is an authority, the
//! subject is what the authority says and nothing the client sent.
//!
//! This is the seam between two stores that both know about access, and the
//! rule for it is that the answer is *derived* here and *authored* there. A
//! second copy of the membership table inside SwirlDB — a policy file that has
//! to be kept in step with an application's teams — is exactly the thing that
//! leaks the first time the two drift. So the interesting implementation is
//! [`HttpAuthority`], which asks the owning application over HTTP and keeps the
//! answer only briefly. [`PolicyAuthority`] is for deployments where the policy
//! file *is* the authority, and [`OpenToAll`] is the default, which is what
//! every single-document demo and test has always had.

use async_trait::async_trait;
use dashmap::DashMap;
use std::time::{Duration, Instant};
use swirldb_core::policy::{Action, Actor, ActorType, PolicyEngine};
use swirldb_core::protocol::Access;
use tracing::warn;

/// Decides who a connection is and whether a subject may open a document.
#[async_trait]
pub trait Authority: Send + Sync {
    /// Who a connection is. `token` is the bearer token it presented on the
    /// upgrade, if any; `client_id` is the name it gave itself in `Connect`,
    /// which is a routing label, not an identity. `None` refuses the
    /// connection before any document is opened on it.
    async fn subject(&self, token: Option<&str>, client_id: &str) -> Option<Actor>;

    /// `Some(Read)` opens the document read-only, `Some(Write)` opens it for
    /// editing, `None` refuses. The subject is whoever [`Self::subject`]
    /// said the connection is; the document is its id, opaque to SwirlDB.
    async fn may_open(&self, subject: &Actor, document: &str) -> Option<Access>;
}

/// The subject a connection gets when nobody can verify one: anonymous,
/// carrying the id the client chose for itself. This is what every
/// connection was before authentication existed, and it is still right for
/// a laptop and a demo — as long as it is never mistaken for a fact about
/// who is on the other end.
pub fn self_asserted(client_id: &str) -> Actor {
    Actor {
        actor_type: ActorType::Anonymous,
        id: client_id.to_string(),
        org_id: None,
        team_id: None,
        app_id: None,
        role: None,
        claims: Default::default(),
    }
}

/// Everyone may write everything.
///
/// The default. A server with no authority configured is a server whose
/// documents are open to anyone who can reach it, which is what the demos
/// and the test suite want and what nothing else should.
pub struct OpenToAll;

#[async_trait]
impl Authority for OpenToAll {
    async fn subject(&self, token: Option<&str>, client_id: &str) -> Option<Actor> {
        if token.is_some() {
            warn!(
                "{} presented a token, but with no authority configured nothing can verify it",
                client_id
            );
        } else {
            warn!(
                "No authority configured: {} is whoever it says it is",
                client_id
            );
        }
        Some(self_asserted(client_id))
    }

    async fn may_open(&self, _subject: &Actor, _document: &str) -> Option<Access> {
        Some(Access::Write)
    }
}

/// The policy engine as the authority.
///
/// A document id is evaluated as a path, so rules are written over ids:
/// `path_pattern: "pattern.{actor.id}.*"` grants a person their own patterns.
/// `Write` allowed means write; otherwise `Read` allowed means read;
/// otherwise refused. Use this when a policy file is genuinely where access
/// is decided, not as a mirror of a membership table kept elsewhere.
pub struct PolicyAuthority {
    engine: PolicyEngine,
}

impl PolicyAuthority {
    pub fn new(engine: PolicyEngine) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl Authority for PolicyAuthority {
    /// A policy file knows rules, not sessions, so it cannot say whose a
    /// token is. Connections under it are self-asserted and anonymous, and
    /// its rules should be written for `Anonymous` or `Any` actors.
    async fn subject(&self, token: Option<&str>, client_id: &str) -> Option<Actor> {
        if token.is_some() {
            warn!(
                "{} presented a token, but a policy file cannot verify one; the connection is anonymous",
                client_id
            );
        }
        Some(self_asserted(client_id))
    }

    async fn may_open(&self, subject: &Actor, document: &str) -> Option<Access> {
        if self
            .engine
            .evaluate(subject, Action::Write, document)
            .is_allowed()
        {
            return Some(Access::Write);
        }
        if self
            .engine
            .evaluate(subject, Action::Read, document)
            .is_allowed()
        {
            return Some(Access::Read);
        }
        None
    }
}

/// What `HttpAuthority` sends to `/whoami`.
#[derive(Debug, serde::Serialize)]
struct WhoAmIRequest<'a> {
    token: &'a str,
}

/// What `HttpAuthority` expects back from `/whoami`: `{"subject": <actor>}`,
/// where the actor need carry only `actor_type` and `id`.
#[derive(Debug, serde::Deserialize)]
struct WhoAmIResponse {
    subject: Actor,
}

/// What `HttpAuthority` sends to `/may-open`.
#[derive(Debug, serde::Serialize)]
struct MayOpenRequest<'a> {
    subject: &'a Actor,
    document: &'a str,
}

/// What `HttpAuthority` expects back: `{"access": "read" | "write" | "none"}`.
#[derive(Debug, serde::Deserialize)]
struct MayOpenResponse {
    access: AccessAnswer,
}

#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum AccessAnswer {
    Read,
    Write,
    None,
}

impl From<AccessAnswer> for Option<Access> {
    fn from(answer: AccessAnswer) -> Self {
        match answer {
            AccessAnswer::Read => Some(Access::Read),
            AccessAnswer::Write => Some(Access::Write),
            AccessAnswer::None => None,
        }
    }
}

/// Asks the owning application two questions over HTTP.
///
/// `POST <endpoint>/whoami` with `{"token": "<bearer token>"}` is answered by
/// `{"subject": <actor>}` — at least `{"actor_type": "User", "id": "…"}` —
/// or by `401` for a token the application does not recognize. A connection
/// that presents no token is refused without asking: under this authority
/// there is no such thing as an anonymous connection.
///
/// `POST <endpoint>/may-open` with `{"subject": <actor>, "document": "<id>"}`
/// is answered by `{"access": "read" | "write" | "none"}`.
///
/// Both answers are cached for a short time — ten seconds by default: whoami
/// per token, may-open per (subject, document) — so an editor that reconnects
/// or opens the same document twice does not cost the application a query
/// each time, while a revoked session or membership takes effect within that
/// window rather than never. Anything that is not a well-formed `200` is a
/// refusal: an authority that is down must not admit connections or open
/// documents, because failing open here is a disclosure.
pub struct HttpAuthority {
    endpoint: String,
    client: reqwest::Client,
    subjects: DashMap<String, (Option<Actor>, Instant)>,
    cache: DashMap<(String, String), (Option<Access>, Instant)>,
    time_to_live: Duration,
}

/// Upper bound on cached answers before expired ones are swept.
const CACHE_SWEEP_THRESHOLD: usize = 10_000;

impl HttpAuthority {
    /// `endpoint` is the base the application serves `/may-open` under, for
    /// example `http://studio:8080/api/authority`.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self::with_time_to_live(endpoint, Duration::from_secs(10))
    }

    pub fn with_time_to_live(endpoint: impl Into<String>, time_to_live: Duration) -> Self {
        let endpoint = endpoint.into().trim_end_matches('/').to_string();
        Self {
            endpoint,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("an HTTP client with a timeout builds"),
            subjects: DashMap::new(),
            cache: DashMap::new(),
            time_to_live,
        }
    }

    fn cached_subject(&self, token: &str) -> Option<Option<Actor>> {
        let entry = self.subjects.get(token)?;
        let (subject, asked_at) = &*entry;
        if asked_at.elapsed() < self.time_to_live {
            Some(subject.clone())
        } else {
            None
        }
    }

    fn remember_subject(&self, token: &str, subject: Option<Actor>) {
        if self.subjects.len() >= CACHE_SWEEP_THRESHOLD {
            let time_to_live = self.time_to_live;
            self.subjects
                .retain(|_, (_, asked_at)| asked_at.elapsed() < time_to_live);
        }
        self.subjects
            .insert(token.to_string(), (subject, Instant::now()));
    }

    async fn ask_who(&self, token: &str) -> Option<Actor> {
        let url = format!("{}/whoami", self.endpoint);
        let response = match self
            .client
            .post(&url)
            .json(&WhoAmIRequest { token })
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                warn!(
                    "Authority at {} unreachable, refusing connection: {}",
                    url, error
                );
                return None;
            }
        };
        if !response.status().is_success() {
            warn!(
                "Authority at {} answered {}, refusing connection",
                url,
                response.status()
            );
            return None;
        }
        match response.json::<WhoAmIResponse>().await {
            Ok(answer) => Some(answer.subject),
            Err(error) => {
                warn!(
                    "Authority at {} answered badly, refusing connection: {}",
                    url, error
                );
                None
            }
        }
    }

    fn cached(&self, key: &(String, String)) -> Option<Option<Access>> {
        let entry = self.cache.get(key)?;
        let (answer, asked_at) = *entry;
        if asked_at.elapsed() < self.time_to_live {
            Some(answer)
        } else {
            None
        }
    }

    fn remember(&self, key: (String, String), answer: Option<Access>) {
        if self.cache.len() >= CACHE_SWEEP_THRESHOLD {
            let time_to_live = self.time_to_live;
            self.cache
                .retain(|_, (_, asked_at)| asked_at.elapsed() < time_to_live);
        }
        self.cache.insert(key, (answer, Instant::now()));
    }

    async fn ask(&self, subject: &Actor, document: &str) -> Option<Access> {
        let url = format!("{}/may-open", self.endpoint);
        let response = match self
            .client
            .post(&url)
            .json(&MayOpenRequest { subject, document })
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                warn!("Authority at {} unreachable, refusing open: {}", url, error);
                return None;
            }
        };
        if !response.status().is_success() {
            warn!(
                "Authority at {} answered {}, refusing open of {}",
                url,
                response.status(),
                document
            );
            return None;
        }
        match response.json::<MayOpenResponse>().await {
            Ok(answer) => answer.access.into(),
            Err(error) => {
                warn!(
                    "Authority at {} answered badly, refusing open: {}",
                    url, error
                );
                None
            }
        }
    }
}

#[async_trait]
impl Authority for HttpAuthority {
    async fn subject(&self, token: Option<&str>, client_id: &str) -> Option<Actor> {
        let Some(token) = token else {
            warn!("{} presented no token; refusing the connection", client_id);
            return None;
        };
        if let Some(subject) = self.cached_subject(token) {
            return subject;
        }
        let subject = self.ask_who(token).await;
        self.remember_subject(token, subject.clone());
        subject
    }

    async fn may_open(&self, subject: &Actor, document: &str) -> Option<Access> {
        let key = (subject.id.clone(), document.to_string());
        if let Some(answer) = self.cached(&key) {
            return answer;
        }
        let answer = self.ask(subject, document).await;
        self.remember(key, answer);
        answer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swirldb_core::policy::ActorType;

    fn user(id: &str) -> Actor {
        Actor {
            actor_type: ActorType::User,
            id: id.to_string(),
            org_id: None,
            team_id: None,
            app_id: None,
            role: None,
            claims: Default::default(),
        }
    }

    #[tokio::test]
    async fn open_to_all_writes() {
        assert_eq!(
            OpenToAll.may_open(&user("alice"), "anything").await,
            Some(Access::Write)
        );
    }

    #[tokio::test]
    async fn open_to_all_takes_a_connection_at_its_word() {
        let subject = OpenToAll.subject(None, "whoever").await.unwrap();
        assert_eq!(subject.actor_type, ActorType::Anonymous);
        assert_eq!(subject.id, "whoever");
        // A token changes nothing: there is nobody to verify it.
        let with_token = OpenToAll.subject(Some("t"), "whoever").await.unwrap();
        assert_eq!(with_token.id, "whoever");
    }

    #[tokio::test]
    async fn http_authority_refuses_a_connection_without_a_token() {
        let authority = HttpAuthority::new("http://127.0.0.1:9/nowhere");
        assert!(authority.subject(None, "alice").await.is_none());
        // And one whose token it cannot verify.
        assert!(authority.subject(Some("token"), "alice").await.is_none());
    }

    #[tokio::test]
    async fn http_authority_caches_subjects_within_time_to_live() {
        let authority =
            HttpAuthority::with_time_to_live("http://127.0.0.1:9/nowhere", Duration::from_secs(60));
        authority.remember_subject("alice-token", Some(user("alice")));
        assert_eq!(
            authority
                .subject(Some("alice-token"), "anything")
                .await
                .map(|subject| subject.id),
            Some("alice".to_string())
        );

        let expired =
            HttpAuthority::with_time_to_live("http://127.0.0.1:9/nowhere", Duration::ZERO);
        expired.remember_subject("alice-token", Some(user("alice")));
        assert!(expired
            .subject(Some("alice-token"), "anything")
            .await
            .is_none());
    }

    #[tokio::test]
    async fn policy_authority_grades_by_action() {
        let engine = PolicyEngine::from_json(
            r#"{"policies":{"rules":[
                {"priority":10,"actor":{"type":"User"},"action":"Write","path_pattern":"pattern.{actor.id}.*","effect":"Allow"},
                {"priority":20,"actor":{"type":"User"},"action":"Read","path_pattern":"pattern.*.*","effect":"Allow"}
            ]}}"#,
        )
        .unwrap();
        let authority = PolicyAuthority::new(engine);
        let alice = user("alice");
        assert_eq!(
            authority.may_open(&alice, "pattern.alice.1").await,
            Some(Access::Write)
        );
        assert_eq!(
            authority.may_open(&alice, "pattern.bob.1").await,
            Some(Access::Read)
        );
        assert_eq!(authority.may_open(&alice, "palette.bob.1").await, None);
    }

    #[tokio::test]
    async fn http_authority_refuses_when_unreachable() {
        // Nothing listens here; the refusal must be a refusal, not a panic.
        let authority = HttpAuthority::new("http://127.0.0.1:9/nowhere");
        assert_eq!(authority.may_open(&user("alice"), "doc").await, None);
    }

    #[tokio::test]
    async fn http_authority_caches_within_time_to_live() {
        let authority =
            HttpAuthority::with_time_to_live("http://127.0.0.1:9/nowhere", Duration::from_secs(60));
        let key = ("alice".to_string(), "doc".to_string());
        authority.remember(key.clone(), Some(Access::Read));
        assert_eq!(
            authority.may_open(&user("alice"), "doc").await,
            Some(Access::Read)
        );

        let expired =
            HttpAuthority::with_time_to_live("http://127.0.0.1:9/nowhere", Duration::ZERO);
        expired.remember(key, Some(Access::Read));
        assert_eq!(expired.may_open(&user("alice"), "doc").await, None);
    }
}
