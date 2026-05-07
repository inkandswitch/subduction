//! Smoke test for the stress-test scaffolding in `tests/common.rs`.
//!
//! Confirms the helpers wire up correctly before the larger stress
//! scenarios are run. If this fails, the scaffolding is broken and every
//! other stress-test file will fail too.

#![allow(clippy::expect_used)]

use std::time::Duration;

use subduction_core::peer::id::PeerId;

#[path = "common.rs"]
mod common;

use common::{
    SETTLE, blob, commit_id, connect_local_pair, connect_sendable_pair, make_local_node,
    make_sendable_node, populate_linear_chain_local, populate_linear_chain_sendable, sed_id,
    signer,
};

#[tokio::test(flavor = "multi_thread")]
async fn sendable_pair_round_trips() {
    let alice = make_sendable_node(1);
    let bob = make_sendable_node(2);

    connect_sendable_pair(&alice, 1, &bob, 2)
        .await
        .expect("connect");

    let id = sed_id(0);
    populate_linear_chain_sendable(&alice, id, 100, 5, 64)
        .await
        .expect("populate");

    tokio::time::sleep(SETTLE).await;

    let bob_peer_id = PeerId::from(signer(2).verifying_key());
    let (had_success, _stats, _call_errs, _io_errs) = alice
        .full_sync_with_peer(&bob_peer_id, true, Some(Duration::from_millis(2_000)))
        .await;
    assert!(had_success, "sync should succeed");

    tokio::time::sleep(Duration::from_millis(50)).await;

    let bob_commits = bob.get_commits(id).await.expect("bob has tree");
    assert_eq!(bob_commits.len(), 5, "bob should have all 5 commits");
}

#[tokio::test(flavor = "current_thread")]
async fn local_pair_round_trips() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let alice = make_local_node(11);
            let bob = make_local_node(12);

            connect_local_pair(&alice, 11, &bob, 12)
                .await
                .expect("connect");

            let id = sed_id(1);
            populate_linear_chain_local(&alice, id, 200, 3, 32)
                .await
                .expect("populate");

            tokio::time::sleep(SETTLE).await;

            let bob_peer_id = PeerId::from(signer(12).verifying_key());
            let (had_success, _stats, _call_errs, _io_errs) = alice
                .full_sync_with_peer(&bob_peer_id, true, Some(Duration::from_millis(2_000)))
                .await;
            assert!(had_success, "local sync should succeed");

            tokio::time::sleep(Duration::from_millis(50)).await;

            let bob_commits = bob.get_commits(id).await.expect("bob has tree");
            assert_eq!(bob_commits.len(), 3, "bob should have all 3 commits");
        })
        .await;
}

#[tokio::test]
async fn workload_helpers_are_deterministic() {
    let s1 = signer(42);
    let s2 = signer(42);
    assert_eq!(s1.verifying_key(), s2.verifying_key());

    assert_eq!(sed_id(7), sed_id(7));
    assert_eq!(commit_id(1, 2), commit_id(1, 2));
    assert_ne!(commit_id(1, 2), commit_id(2, 1));

    let b = blob(99, 64);
    assert_eq!(b.contents().len(), 64);
}


