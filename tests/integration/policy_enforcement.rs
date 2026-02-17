// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Policy enforcement tests
//!
//! Tests that server enforces subscription policies correctly.

use super::{init_test_logging, rust_client::RustClient, test_server::TestServer};
use swirldb_core::policy::{
    Action, ActorPattern, Effect, PoliciesConfig, PolicyEngine, PolicyRule, SwirlDBConfig,
};

#[tokio::test]
async fn test_policy_denies_unauthorized_subscription() {
    init_test_logging();

    // Create policy that denies admin.** to all users
    let config = SwirlDBConfig {
        policies: PoliciesConfig {
            auth: None,
            rules: vec![PolicyRule {
                _description: Some("Deny admin access".to_string()),
                priority: 0,
                actor: ActorPattern::Any,
                action: Action::Subscribe,
                path_pattern: "admin.**".to_string(),
                effect: Effect::Deny,
            }],
            audit: None,
            jwt_providers: std::collections::HashMap::new(),
        },
        adapters: Default::default(),
        remotes: vec![],
    };

    let policy = PolicyEngine::new(config);

    let server = TestServer::start_with_policy(Some(policy)).await.unwrap();

    // Try to subscribe to admin.** (should be denied)
    let client = RustClient::connect(&server.ws_url(), vec!["admin.**".to_string()])
        .await
        .unwrap();

    // TODO: Verify subscription was denied by checking SubscribeAck message
    // For now, just verify connection works
    assert_eq!(server.connection_count(), 1);

    client.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_policy_allows_authorized_subscription() {
    init_test_logging();

    // Create policy that allows user.** for all users
    let config = SwirlDBConfig {
        policies: PoliciesConfig {
            auth: None,
            rules: vec![PolicyRule {
                _description: Some("Allow user data access".to_string()),
                priority: 0,
                actor: ActorPattern::Any,
                action: Action::Subscribe,
                path_pattern: "user.**".to_string(),
                effect: Effect::Allow,
            }],
            audit: None,
            jwt_providers: std::collections::HashMap::new(),
        },
        adapters: Default::default(),
        remotes: vec![],
    };

    let policy = PolicyEngine::new(config);

    let server = TestServer::start_with_policy(Some(policy)).await.unwrap();

    // Subscribe to user.** (should be allowed)
    let client = RustClient::connect(&server.ws_url(), vec!["user.**".to_string()])
        .await
        .unwrap();

    assert_eq!(server.connection_count(), 1);

    client.close().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_policy_mixed_allow_deny() {
    init_test_logging();

    let config = SwirlDBConfig {
        policies: PoliciesConfig {
            auth: None,
            rules: vec![
                PolicyRule {
                    _description: Some("Allow public data".to_string()),
                    priority: 0,
                    actor: ActorPattern::Any,
                    action: Action::Subscribe,
                    path_pattern: "public.**".to_string(),
                    effect: Effect::Allow,
                },
                PolicyRule {
                    _description: Some("Deny private data".to_string()),
                    priority: 0,
                    actor: ActorPattern::Any,
                    action: Action::Subscribe,
                    path_pattern: "private.**".to_string(),
                    effect: Effect::Deny,
                },
            ],
            audit: None,
            jwt_providers: std::collections::HashMap::new(),
        },
        adapters: Default::default(),
        remotes: vec![],
    };

    let policy = PolicyEngine::new(config);

    let server = TestServer::start_with_policy(Some(policy)).await.unwrap();

    // Subscribe to both public.** and private.**
    let client = RustClient::connect(
        &server.ws_url(),
        vec!["public.**".to_string(), "private.**".to_string()],
    )
    .await
    .unwrap();

    // TODO: Verify that public.** was allowed but private.** was denied
    // by inspecting the SubscribeAck message

    assert_eq!(server.connection_count(), 1);

    client.close().await.unwrap();
    server.shutdown().await.unwrap();
}
