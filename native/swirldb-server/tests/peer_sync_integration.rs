// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Integration tests: two peers syncing CRDT state over LanTransport.
//!
//! These tests use real TCP/UDP sockets (localhost) to verify the full
//! stack: LanTransport → PeerManager → SwirlDB sync protocol.

use std::sync::Arc;
use swirldb_core::core::SwirlDB;
use swirldb_core::protocol::Message;
use swirldb_core::transport::PeerAddr;
use swirldb_server::peer_manager::{PeerEvent, PeerManager, PeerManagerConfig};
use swirldb_server::transport::LanTransport;
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn new_db() -> Arc<RwLock<SwirlDB>> {
    Arc::new(RwLock::new(SwirlDB::new()))
}

fn loopback(addr: std::net::SocketAddr) -> String {
    format!("127.0.0.1:{}", addr.port())
}

/// Drain events from both managers until both sides have emitted PeerSynced.
/// The bidirectional handshake produces PeerSynced on each side, plus possibly
/// ChangesApplied events from the sync data. We drain everything so the test
/// can focus on the behavior it's actually testing.
async fn drain_until_synced(mgr_a: &PeerManager, mgr_b: &PeerManager) {
    // Wait for both sides to complete the sync handshake.
    // Use separate sequential polls instead of select! to avoid
    // issues with concurrent Mutex acquisition.
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    let mut a_synced = false;
    let mut b_synced = false;

    while !(a_synced && b_synced) && tokio::time::Instant::now() < deadline {
        // Try A first, then B, with short timeouts
        if !a_synced {
            if let Ok(Ok(event)) =
                tokio::time::timeout(Duration::from_millis(100), mgr_a.next_event()).await
            {
                if matches!(event, PeerEvent::PeerSynced { .. }) {
                    a_synced = true;
                }
                continue;
            }
        }
        if !b_synced {
            if let Ok(Ok(event)) =
                tokio::time::timeout(Duration::from_millis(100), mgr_b.next_event()).await
            {
                if matches!(event, PeerEvent::PeerSynced { .. }) {
                    b_synced = true;
                }
                continue;
            }
        }
        // Neither had events — small sleep and retry
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Drain any trailing events
    while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(100), mgr_a.next_event()).await
    {
    }
    while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(100), mgr_b.next_event()).await
    {
    }
}

#[tokio::test]
async fn test_two_peers_initial_sync() {
    // Peer A has some data
    let db_a = new_db();
    {
        let db = db_a.read().await;
        db.set_path("settings.bpm", 120.into()).unwrap();
        db.set_path("settings.pattern", "rainbow".into()).unwrap();
    }

    // Peer B is empty
    let db_b = new_db();

    // Create transports
    let transport_a = LanTransport::bind("peer-a", 0, Some(0)).await.unwrap();
    let transport_b = LanTransport::bind("peer-b", 0, Some(0)).await.unwrap();

    let a_tcp = loopback(transport_a.tcp_addr());
    let a_udp = loopback(transport_a.udp_addr());

    // Create peer managers
    let config = PeerManagerConfig::default();
    let mgr_a = PeerManager::start(
        config.clone(),
        db_a.clone(),
        transport_a,
        None::<swirldb_core::transport::MockDiscovery>,
        "peer-a",
    );

    let mgr_b = PeerManager::start(
        config,
        db_b.clone(),
        transport_b,
        None::<swirldb_core::transport::MockDiscovery>,
        "peer-b",
    );

    // B connects to A
    let a_addr = PeerAddr::new("peer-a")
        .with_address("tcp", &a_tcp)
        .with_address("udp", &a_udp);
    mgr_b.connect(&a_addr).await.unwrap();

    // Wait for sync to complete on both sides
    // B should get PeerSynced (it initiated, so it sends Connect, A responds with Sync)
    let mut b_synced = false;
    let mut a_synced = false;

    for _ in 0..10 {
        tokio::select! {
            Ok(event) = mgr_b.next_event(), if !b_synced => {
                if matches!(&event, PeerEvent::PeerSynced { .. }) {
                    b_synced = true;
                }
            }
            Ok(event) = mgr_a.next_event(), if !a_synced => {
                if matches!(&event, PeerEvent::PeerSynced { .. }) {
                    a_synced = true;
                }
            }
            _ = tokio::time::sleep(TEST_TIMEOUT) => {
                break;
            }
        }
        if b_synced && a_synced {
            break;
        }
    }

    assert!(b_synced, "Peer B should have synced with A");

    // Verify B now has A's data
    {
        let db = db_b.read().await;
        let bpm = db.get_path("settings.bpm");
        assert_eq!(
            bpm,
            Some(swirldb_core::automerge::ScalarValue::Int(120)),
            "B should have A's bpm setting"
        );

        let pattern = db.get_path("settings.pattern");
        assert_eq!(
            pattern,
            Some(swirldb_core::automerge::ScalarValue::Str("rainbow".into())),
            "B should have A's pattern setting"
        );
    }

    mgr_a.shutdown().await.unwrap();
    mgr_b.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_ongoing_change_propagation() {
    let db_a = new_db();
    let db_b = new_db();

    let transport_a = LanTransport::bind("peer-a", 0, Some(0)).await.unwrap();
    let transport_b = LanTransport::bind("peer-b", 0, Some(0)).await.unwrap();

    let a_tcp = loopback(transport_a.tcp_addr());

    let config = PeerManagerConfig::default();
    let mgr_a = PeerManager::start(
        config.clone(),
        db_a.clone(),
        transport_a,
        None::<swirldb_core::transport::MockDiscovery>,
        "peer-a",
    );

    let mgr_b = PeerManager::start(
        config,
        db_b.clone(),
        transport_b,
        None::<swirldb_core::transport::MockDiscovery>,
        "peer-b",
    );

    // Connect and wait for initial sync
    let a_addr = PeerAddr::new("peer-a").with_address("tcp", &a_tcp);
    mgr_b.connect(&a_addr).await.unwrap();

    // Drain all sync/connect events on both sides until both are synced
    drain_until_synced(&mgr_a, &mgr_b).await;

    // Now write on A and push to peers
    let changes = {
        let db = db_a.read().await;
        let before_heads = db.get_heads();
        db.set_path("settings.brightness", 75.into()).unwrap();
        // Get only the new changes (since before the write)
        db.get_changes_since(&before_heads)
    };

    mgr_a
        .push_local_changes(changes, vec!["settings.brightness".into()])
        .await
        .unwrap();

    // B should receive ChangesApplied (skip any straggling sync events)
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    let mut got_changes = false;
    while tokio::time::Instant::now() < deadline {
        let event = timeout(Duration::from_secs(2), mgr_b.next_event())
            .await
            .expect("timed out waiting for changes")
            .unwrap();
        match event {
            PeerEvent::ChangesApplied {
                from,
                affected_paths,
            } => {
                assert_eq!(from.as_str(), "peer-a");
                assert!(
                    affected_paths.iter().any(|p| p.contains("brightness")),
                    "Should include brightness path, got: {:?}",
                    affected_paths
                );
                got_changes = true;
                break;
            }
            PeerEvent::PeerSynced { .. } => continue,
            other => panic!("Expected ChangesApplied, got: {:?}", other),
        }
    }
    assert!(got_changes, "Never received ChangesApplied");

    // Verify B has the new value
    {
        let db = db_b.read().await;
        let brightness = db.get_path("settings.brightness");
        assert_eq!(
            brightness,
            Some(swirldb_core::automerge::ScalarValue::Int(75)),
            "B should have A's brightness setting"
        );
    }

    mgr_a.shutdown().await.unwrap();
    mgr_b.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_ephemeral_message_delivery() {
    let db_a = new_db();
    let db_b = new_db();

    let transport_a = LanTransport::bind("peer-a", 0, Some(0)).await.unwrap();
    let transport_b = LanTransport::bind("peer-b", 0, Some(0)).await.unwrap();

    let a_tcp = loopback(transport_a.tcp_addr());
    let a_udp = loopback(transport_a.udp_addr());

    let config = PeerManagerConfig::default();
    let mgr_a = PeerManager::start(
        config.clone(),
        db_a,
        transport_a,
        None::<swirldb_core::transport::MockDiscovery>,
        "peer-a",
    );

    let mgr_b = PeerManager::start(
        config,
        db_b,
        transport_b,
        None::<swirldb_core::transport::MockDiscovery>,
        "peer-b",
    );

    // Connect and wait for sync
    let a_addr = PeerAddr::new("peer-a")
        .with_address("tcp", &a_tcp)
        .with_address("udp", &a_udp);
    mgr_b.connect(&a_addr).await.unwrap();

    // Drain all sync/connect events on both sides
    drain_until_synced(&mgr_a, &mgr_b).await;

    // B sends ephemeral beat sync to A
    let beat_msg = Message::EphemeralBatch {
        updates: vec![("beat.bpm".to_string(), vec![0, 120])],
    };
    mgr_b
        .send_ephemeral(
            &swirldb_core::transport::PeerId::new("peer-a"),
            &beat_msg.encode(),
        )
        .unwrap();

    // A should receive EphemeralReceived (skip any straggling sync events)
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    let mut got_ephemeral = false;
    while tokio::time::Instant::now() < deadline {
        let event = timeout(Duration::from_secs(2), mgr_a.next_event())
            .await
            .expect("timed out waiting for ephemeral")
            .unwrap();
        match event {
            PeerEvent::EphemeralReceived { from, updates } => {
                assert_eq!(from.as_str(), "peer-b");
                assert_eq!(updates.len(), 1);
                assert_eq!(updates[0].0, "beat.bpm");
                got_ephemeral = true;
                break;
            }
            PeerEvent::PeerSynced { .. } | PeerEvent::ChangesApplied { .. } => {
                continue; // Skip late sync events
            }
            other => panic!("Expected EphemeralReceived, got: {:?}", other),
        }
    }
    assert!(got_ephemeral, "Never received ephemeral message");

    mgr_a.shutdown().await.unwrap();
    mgr_b.shutdown().await.unwrap();
}
