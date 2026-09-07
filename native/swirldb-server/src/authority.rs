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

    /// Forget whatever this authority remembers about `subject`: its answer
    /// for `document`, or, with no document named, every answer about the
    /// subject and whose token it is. The next question is then asked
    /// afresh. This is what a revocation calls, so that a subject whose
    /// membership just ended is not admitted again out of a cache. An
    /// authority that remembers nothing has nothing to do, which is the
    /// default.
    async fn forget(&self, _subject: &str, _document: Option<&str>) {}
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
/// The application should not answer these questions to anyone who can reach
/// it, so both requests carry a credential of SwirlDB's own: the shared secret
/// given to [`HttpAuthority::with_secret`], sent as `Authorization: Bearer
/// <secret>`. Before there was a secret, the only credential a deployment
/// could attach was user-info in the endpoint URL, which the HTTP client
/// turns into `Authorization: Basic`; that still works, for one release, and
/// where both are present the user-info is dropped from the URL and the
/// bearer is what the application receives.
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
    /// The endpoint as it appears in the log: user-info removed, so a secret
    /// carried the older way is never printed. The bearer is not in a URL
    /// and is never printed either.
    shown: String,
    /// The shared secret sent as a bearer on every request, if there is one.
    secret: Option<String>,
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
            shown: without_user_info(&endpoint),
            endpoint,
            secret: None,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("an HTTP client with a timeout builds"),
            subjects: DashMap::new(),
            cache: DashMap::new(),
            time_to_live,
        }
    }

    /// Send `secret` as `Authorization: Bearer <secret>` on every request.
    /// This is the credential the application checks before it answers; a
    /// URL with user-info in it is the older way to carry one, and this one
    /// takes precedence: any user-info in the endpoint is dropped, so the
    /// bearer is the only credential on the wire. (The HTTP client would
    /// otherwise send both, and an application reads the first.)
    pub fn with_secret(mut self, secret: impl Into<String>) -> Self {
        self.secret = Some(secret.into());
        self.endpoint = without_user_info(&self.endpoint);
        self
    }

    /// A request to the authority, with the bearer attached if there is one.
    fn post(&self, url: &str) -> reqwest::RequestBuilder {
        let request = self.client.post(url);
        match &self.secret {
            Some(secret) => request.bearer_auth(secret),
            None => request,
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
        let shown = format!("{}/whoami", self.shown);
        let response = match self.post(&url).json(&WhoAmIRequest { token }).send().await {
            Ok(response) => response,
            Err(error) => {
                warn!(
                    "Authority at {} unreachable, refusing connection: {}",
                    shown, error
                );
                return None;
            }
        };
        if !response.status().is_success() {
            warn!(
                "Authority at {} answered {}, refusing connection",
                shown,
                response.status()
            );
            return None;
        }
        match response.json::<WhoAmIResponse>().await {
            Ok(answer) => Some(answer.subject),
            Err(error) => {
                warn!(
                    "Authority at {} answered badly, refusing connection: {}",
                    shown, error
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
        let shown = format!("{}/may-open", self.shown);
        let response = match self
            .post(&url)
            .json(&MayOpenRequest { subject, document })
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                warn!(
                    "Authority at {} unreachable, refusing open: {}",
                    shown, error
                );
                return None;
            }
        };
        if !response.status().is_success() {
            warn!(
                "Authority at {} answered {}, refusing open of {}",
                shown,
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
                    shown, error
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

    async fn forget(&self, subject: &str, document: Option<&str>) {
        match document {
            Some(document) => {
                self.cache
                    .remove(&(subject.to_string(), document.to_string()));
            }
            None => {
                self.cache.retain(|(id, _), _| id != subject);
                self.subjects
                    .retain(|_, (actor, _)| actor.as_ref().is_none_or(|actor| actor.id != subject));
            }
        }
    }
}

/// `url` with any user-info removed: `http://swirldb:secret@host/authority`
/// becomes `http://host/authority`. A URL without a scheme or without
/// user-info comes back as it was, and an `@` in the path or query is not
/// user-info. This is what the server logs where it would otherwise print
/// a URL with a secret in it, and what [`HttpAuthority::with_secret`] asks
/// at once the secret rides as a bearer instead.
pub fn without_user_info(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let authority_start = scheme_end + 3;
    let authority_end = url[authority_start..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |offset| authority_start + offset);
    match url[authority_start..authority_end].rfind('@') {
        Some(at) => format!(
            "{}{}",
            &url[..authority_start],
            &url[authority_start + at + 1..]
        ),
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swirldb_core::policy::ActorType;

    #[test]
    fn user_info_is_stripped_and_nothing_else_is() {
        assert_eq!(
            without_user_info("http://swirldb:s3cret@studio-server:8080/authority"),
            "http://studio-server:8080/authority"
        );
        assert_eq!(
            without_user_info("https://swirldb@studio-server/authority"),
            "https://studio-server/authority"
        );
        assert_eq!(
            without_user_info("http://user:p%40ss@host"),
            "http://host",
            "a percent-encoded @ inside the user-info is not the separator"
        );
        assert_eq!(
            without_user_info("http://studio-server:8080/authority"),
            "http://studio-server:8080/authority"
        );
        assert_eq!(
            without_user_info("http://host/a@b?c=d@e"),
            "http://host/a@b?c=d@e",
            "an @ in the path or query is not user-info"
        );
        assert_eq!(
            without_user_info("http://user:pass@host?x=1"),
            "http://host?x=1",
            "the authority ends at the query when there is no path"
        );
        assert_eq!(
            without_user_info("studio-server:8080/authority"),
            "studio-server:8080/authority"
        );
        assert_eq!(without_user_info(""), "");
    }

    #[test]
    fn the_endpoint_is_shown_without_its_user_info() {
        let authority = HttpAuthority::new("http://swirldb:s3cret@127.0.0.1:9/authority/");
        // Asked with the secret, shown without it.
        assert_eq!(
            authority.endpoint,
            "http://swirldb:s3cret@127.0.0.1:9/authority"
        );
        assert_eq!(authority.shown, "http://127.0.0.1:9/authority");
        assert!(!authority.shown.contains("s3cret"));
    }

    #[test]
    fn a_secret_drops_the_user_info_from_the_endpoint() {
        let authority =
            HttpAuthority::new("http://swirldb:stale@127.0.0.1:9/authority/").with_secret("s3cret");
        assert_eq!(authority.endpoint, "http://127.0.0.1:9/authority");
        assert_eq!(authority.secret.as_deref(), Some("s3cret"));
        // Without a secret the user-info stays: it is the credential.
        let older = HttpAuthority::new("http://swirldb:s3cret@127.0.0.1:9/authority");
        assert_eq!(
            older.endpoint,
            "http://swirldb:s3cret@127.0.0.1:9/authority"
        );
    }

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
    async fn forgetting_a_subject_empties_what_was_cached_about_it() {
        // Nothing listens at the endpoint, so whatever is answered after a
        // forget is answered by asking, and asking refuses.
        let authority =
            HttpAuthority::with_time_to_live("http://127.0.0.1:9/nowhere", Duration::from_secs(60));
        authority.remember(("alice".into(), "doc.1".into()), Some(Access::Write));
        authority.remember(("alice".into(), "doc.2".into()), Some(Access::Write));
        authority.remember(("bob".into(), "doc.1".into()), Some(Access::Read));
        authority.remember_subject("alice-token", Some(user("alice")));
        authority.remember_subject("bob-token", Some(user("bob")));

        // One document: that answer alone is gone.
        authority.forget("alice", Some("doc.1")).await;
        assert_eq!(authority.may_open(&user("alice"), "doc.1").await, None);
        assert_eq!(
            authority.may_open(&user("alice"), "doc.2").await,
            Some(Access::Write)
        );
        assert!(authority.subject(Some("alice-token"), "x").await.is_some());

        // Every document: every answer about the subject, its token too,
        // and nothing about anybody else.
        authority.forget("alice", None).await;
        assert_eq!(authority.may_open(&user("alice"), "doc.2").await, None);
        assert!(authority.subject(Some("alice-token"), "x").await.is_none());
        assert_eq!(
            authority.may_open(&user("bob"), "doc.1").await,
            Some(Access::Read)
        );
        assert!(authority.subject(Some("bob-token"), "x").await.is_some());
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
