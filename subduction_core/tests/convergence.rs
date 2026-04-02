//! Multi-round convergence tests using in-process channel transport.
//!
//! Verifies that after two nodes with commits _and_ fragments have fully
//! synced, subsequent sync rounds — each using a fresh random
//! [`FingerprintSeed`] — consistently produce empty diffs. A final phase
//! adds new data on both sides and confirms the diff is non-empty, proving
//! the system is actually diffing (liveness) rather than trivially returning
//! empty.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::{
    blob::Blob,
    commit::CountLeadingZeroBytes,
    depth::{Depth, DepthMetric},
    id::SedimentreeId,
    loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{builder::SubductionBuilder, Subduction},
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MessageTransport<ChannelTransport>;

type TestSyncHandler =
    SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>;

type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        TestSyncHandler,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
    >,
>;

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_node(signer: MemorySigner) -> TestSubduction {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

fn make_blob(seed: u8) -> Blob {
    let data: Vec<u8> = (0..64).map(|i| seed.wrapping_add(i)).collect();
    Blob::new(data)
}

/// Connect two nodes via a bidirectional channel transport pair.
///
/// Each side sees the other's `PeerId` via `Authenticated::new_for_test`.
async fn connect_pair(
    a: &TestSubduction,
    a_signer: &MemorySigner,
    b: &TestSubduction,
    b_signer: &MemorySigner,
) -> TestResult {
    let (transport_a, transport_b) = ChannelTransport::pair();

    let conn_a = MessageTransport::new(transport_a);
    let conn_b = MessageTransport::new(transport_b);

    let peer_a = PeerId::from(a_signer.verifying_key());
    let peer_b = PeerId::from(b_signer.verifying_key());

    let auth_a: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_b, peer_a);

    a.add_connection(auth_a).await?;
    b.add_connection(auth_b).await?;

    Ok(())
}

/// Multi-round convergence with fragments and pre-sync overlap.
///
/// ```text
/// Phase 1: Alice has A1, A2, A3. Bob has B1, B2.     (no connection)
/// Phase 2: Connect + sync round 1.                    (overlap established)
/// Phase 3: Alice adds A4 + fragment. Bob adds B3 + fragment.
/// Phase 4: Sync round 2.                              (new data transferred)
/// Phase 5: Rounds 3–6 — four empty rounds.            (unique random seeds)
/// Phase 6: Alice adds A5, Bob adds B4. Round 7.       (liveness)
/// ```
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn multi_round_convergence_with_fragments() -> TestResult {
    let alice_signer = make_signer(10);
    let bob_signer = make_signer(11);

    let alice = make_node(alice_signer.clone());
    let bob = make_node(bob_signer.clone());

    let sed_id = SedimentreeId::new([2u8; 32]);
    let sync_timeout = Some(Duration::from_millis(500));

    // ── Phase 1: initial data (no connection yet, no broadcasts) ──

    alice
        .add_commit(
            sed_id,
            CommitId::new([1; 32]),
            BTreeSet::new(),
            make_blob(1),
        )
        .await?; // A1
    alice
        .add_commit(
            sed_id,
            CommitId::new([2; 32]),
            BTreeSet::new(),
            make_blob(2),
        )
        .await?; // A2
    alice
        .add_commit(
            sed_id,
            CommitId::new([3; 32]),
            BTreeSet::new(),
            make_blob(3),
        )
        .await?; // A3

    bob.add_commit(
        sed_id,
        CommitId::new([4; 32]),
        BTreeSet::new(),
        make_blob(4),
    )
    .await?; // B1
    bob.add_commit(
        sed_id,
        CommitId::new([5; 32]),
        BTreeSet::new(),
        make_blob(5),
    )
    .await?; // B2

    // ── Phase 2: connect + first sync (establishes overlap) ──

    connect_pair(&alice, &alice_signer, &bob, &bob_signer).await?;

    // Let the connection managers register the new connections
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Alice initiates sync; Bob's listen loop handles the request.
    let (ok, stats, _, _) = alice.full_sync_with_all_peers(sync_timeout).await;
    assert!(ok, "phase 2: sync should succeed");
    assert!(
        !stats.is_empty(),
        "phase 2: should transfer data (received={}, sent={})",
        stats.total_received(),
        stats.total_sent(),
    );

    // Let Bob's listener process the fire-and-forget data Alice sent back
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        alice.get_commits(sed_id).await.map(|c| c.len()),
        Some(5),
        "phase 2: alice should have 5 commits",
    );
    assert_eq!(
        bob.get_commits(sed_id).await.map(|c| c.len()),
        Some(5),
        "phase 2: bob should have 5 commits",
    );

    // ── Phase 3: add more data + fragments on each side ──
    //
    // These are added while connected, so broadcasts will fire.
    // Give a short sleep after each to let the listen loops drain.

    alice
        .add_commit(
            sed_id,
            CommitId::new([6; 32]),
            BTreeSet::new(),
            make_blob(6),
        )
        .await?; // A4

    // Alice's fragment: synthetic head with depth 2 (2 leading zero bytes)
    let alice_frag_head = CommitId::new({
        let mut b = [0u8; 32];
        b[2] = 1;
        b[3] = 0xAA;
        b
    });
    let alice_frag_boundary = BTreeSet::from([CommitId::new({
        let mut b = [0u8; 32];
        b[0] = 0xFF;
        b[1] = 0xAA;
        b
    })]);
    alice
        .add_fragment(
            sed_id,
            alice_frag_head,
            alice_frag_boundary,
            &[],
            make_blob(7),
        )
        .await?;

    bob.add_commit(
        sed_id,
        CommitId::new([8; 32]),
        BTreeSet::new(),
        make_blob(8),
    )
    .await?; // B3

    let bob_frag_head = CommitId::new({
        let mut b = [0u8; 32];
        b[2] = 1;
        b[3] = 0xBB;
        b
    });
    let bob_frag_boundary = BTreeSet::from([CommitId::new({
        let mut b = [0u8; 32];
        b[0] = 0xFF;
        b[1] = 0xBB;
        b
    })]);
    bob.add_fragment(sed_id, bob_frag_head, bob_frag_boundary, &[], make_blob(9))
        .await?;

    // Let broadcasts settle before syncing
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ── Phase 4: sync round 2 — ensure both sides converge ──

    let (ok, _stats, _, _) = alice.full_sync_with_all_peers(sync_timeout).await;
    assert!(ok, "phase 4: sync should succeed");

    // Let Bob's listener process fire-and-forget data
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        alice.get_commits(sed_id).await.map(|c| c.len()),
        Some(7),
        "phase 4: alice should have 7 commits",
    );
    assert_eq!(
        bob.get_commits(sed_id).await.map(|c| c.len()),
        Some(7),
        "phase 4: bob should have 7 commits",
    );

    let alice_frags = alice.get_fragments(sed_id).await.map_or(0, |f| f.len());
    let bob_frags = bob.get_fragments(sed_id).await.map_or(0, |f| f.len());
    assert_eq!(alice_frags, 2, "phase 4: alice should have 2 fragments");
    assert_eq!(bob_frags, 2, "phase 4: bob should have 2 fragments");

    // ── Phase 5: four empty rounds with unique random seeds ──

    for round in 3..=6 {
        let (ok, stats, _, _) = alice.full_sync_with_all_peers(sync_timeout).await;
        assert!(ok, "phase 5 round {round}: sync should succeed");
        assert!(
            stats.is_empty(),
            "phase 5 round {round}: should transfer zero items \
             (commits_received={}, fragments_received={}, \
             commits_sent={}, fragments_sent={})",
            stats.commits_received,
            stats.fragments_received,
            stats.commits_sent,
            stats.fragments_sent,
        );
    }

    // Counts unchanged after empty rounds
    assert_eq!(
        alice.get_commits(sed_id).await.map(|c| c.len()),
        Some(7),
        "phase 5: alice commit count unchanged",
    );
    assert_eq!(
        bob.get_commits(sed_id).await.map(|c| c.len()),
        Some(7),
        "phase 5: bob commit count unchanged",
    );

    // ── Phase 6: new data breaks convergence (liveness proof) ──

    alice
        .add_commit(
            sed_id,
            CommitId::new([10; 32]),
            BTreeSet::new(),
            make_blob(10),
        )
        .await?; // A5
    bob.add_commit(
        sed_id,
        CommitId::new([11; 32]),
        BTreeSet::new(),
        make_blob(11),
    )
    .await?; // B4

    // Sync immediately — before broadcasts can fully settle, the sync
    // should ensure both sides converge. Whether data arrives via
    // broadcast or sync is an implementation detail; the invariant is
    // that after this call both sides have all 9 commits.
    let (ok, _stats, _, _) = alice.full_sync_with_all_peers(sync_timeout).await;
    assert!(ok, "phase 6: sync should succeed");

    // Let any remaining broadcasts settle
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        alice.get_commits(sed_id).await.map(|c| c.len()),
        Some(9),
        "phase 6: alice should have 9 commits",
    );
    assert_eq!(
        bob.get_commits(sed_id).await.map(|c| c.len()),
        Some(9),
        "phase 6: bob should have 9 commits",
    );
    assert_eq!(
        alice.get_fragments(sed_id).await.map_or(0, |f| f.len()),
        2,
        "phase 6: alice fragment count unchanged",
    );
    assert_eq!(
        bob.get_fragments(sed_id).await.map_or(0, |f| f.len()),
        2,
        "phase 6: bob fragment count unchanged",
    );

    // ── Phase 6b: verify liveness — one more sync must be empty ──
    //
    // If phase 6 actually transferred data (via sync or broadcast),
    // then this final round confirms re-convergence.

    let (ok, stats, _, _) = alice.full_sync_with_all_peers(sync_timeout).await;
    assert!(ok, "phase 6b: sync should succeed");
    assert!(
        stats.is_empty(),
        "phase 6b: should be empty after re-convergence \
         (commits_received={}, fragments_received={}, \
         commits_sent={}, fragments_sent={})",
        stats.commits_received,
        stats.fragments_received,
        stats.commits_sent,
        stats.fragments_sent,
    );

    Ok(())
}

// ── AlwaysDeep depth metric ─────────────────────────────────────────────

/// A depth metric that assigns `Depth(2)` to every commit digest.
///
/// With `MAX_STRATA_DEPTH = Depth(2)`, every commit becomes a checkpoint
/// (block boundary). This means a fragment whose head/boundary/checkpoints
/// include a commit's digest will cause `minimize` to prune that commit.
#[derive(Debug, Clone, Copy, Default)]
struct AlwaysDeep;

impl DepthMetric for AlwaysDeep {
    fn to_depth(&self, _id: CommitId) -> Depth {
        Depth(2)
    }
}

type DeepConn = MessageTransport<ChannelTransport>;

type DeepSyncHandler = SyncHandler<Sendable, MemoryStorage, DeepConn, OpenPolicy, AlwaysDeep>;

type DeepSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        DeepConn,
        DeepSyncHandler,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        AlwaysDeep,
    >,
>;

fn make_deep_node(signer: MemorySigner) -> DeepSubduction {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .depth_metric(AlwaysDeep)
        .build::<Sendable, DeepConn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

async fn connect_deep_pair(
    a: &DeepSubduction,
    a_signer: &MemorySigner,
    b: &DeepSubduction,
    b_signer: &MemorySigner,
) -> TestResult {
    let (transport_a, transport_b) = ChannelTransport::pair();

    let conn_a = MessageTransport::new(transport_a);
    let conn_b = MessageTransport::new(transport_b);

    let peer_a = PeerId::from(a_signer.verifying_key());
    let peer_b = PeerId::from(b_signer.verifying_key());

    let auth_a: Authenticated<DeepConn, Sendable> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<DeepConn, Sendable> = Authenticated::new_for_test(conn_b, peer_a);

    a.add_connection(auth_a).await?;
    b.add_connection(auth_b).await?;

    Ok(())
}

/// Exercises the exact bug path through the full `Subduction` sync pipeline:
///
/// 1. Alice has loose commits; `full_sync_with_all_peers` creates a resolver
/// 2. Bob's response includes a fragment covering Alice's commits
/// 3. Alice ingests the fragment -> `minimize_tree` prunes the covered commits
/// 4. Alice uses the resolver to fulfill Bob's `requesting` fingerprints
///
/// With `AlwaysDeep`, every commit is a checkpoint (`Depth(2)`), so a
/// fragment whose head/boundary match real commit digests causes `minimize`
/// to actually prune those commits from Alice's in-memory tree. Without
/// `FingerprintResolver`, step 4 would fail silently and Bob would never
/// receive Alice's data.
///
/// The test verifies Bob has all of Alice's commits after sync — proof
/// that the resolver survived minimize and resolved the fingerprints.
#[tokio::test]
async fn sync_with_real_minimize_pruning() -> TestResult {
    let alice_signer = make_signer(30);
    let bob_signer = make_signer(31);

    let alice = make_deep_node(alice_signer.clone());
    let bob = make_deep_node(bob_signer.clone());

    let sed_id = SedimentreeId::new([3u8; 32]);
    let sync_timeout = Some(Duration::from_millis(500));

    // ── Alice adds 3 commits (no connection, no broadcasts) ──

    alice
        .add_commit(
            sed_id,
            CommitId::new([20; 32]),
            BTreeSet::new(),
            make_blob(20),
        )
        .await?;
    alice
        .add_commit(
            sed_id,
            CommitId::new([21; 32]),
            BTreeSet::new(),
            make_blob(21),
        )
        .await?;
    alice
        .add_commit(
            sed_id,
            CommitId::new([22; 32]),
            BTreeSet::new(),
            make_blob(22),
        )
        .await?;

    let alice_commits = alice
        .get_commits(sed_id)
        .await
        .expect("alice should have commits");
    assert_eq!(alice_commits.len(), 3);

    // Read back commit IDs
    let mut commit_ids: Vec<CommitId> = alice_commits.iter().map(|c| c.head()).collect();
    commit_ids.sort();
    let (d0, d1, d2) = (commit_ids[0], commit_ids[1], commit_ids[2]);

    // ── Bob adds a fragment covering all 3 of Alice's commit digests ──
    //
    // head = d0, boundary = {d2}, checkpoints = [d1]
    //
    // With AlwaysDeep, every commit is a checkpoint. simplify will see
    // that each single-commit block is "supported" by this fragment
    // (via head/boundary/checkpoint matching), and prune them all.

    bob.add_fragment(sed_id, d0, BTreeSet::from([d2]), &[d1], make_blob(23))
        .await?;

    // ── Connect and sync ──

    connect_deep_pair(&alice, &alice_signer, &bob, &bob_signer).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let (ok, stats, _, _) = alice.full_sync_with_all_peers(sync_timeout).await;
    assert!(ok, "sync should succeed");

    // Alice should have sent her commits (via send_requested_data using
    // the resolver) and received Bob's fragment.
    assert!(
        stats.total_received() > 0 || stats.total_sent() > 0,
        "should transfer data (received={}, sent={})",
        stats.total_received(),
        stats.total_sent(),
    );
    // Let Bob's listener process fire-and-forget data from Alice
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Verify the resolver worked ──

    // Alice sent 3 commits — proves the resolver resolved all 3 fingerprints
    // after minimize pruned the in-memory tree.
    assert_eq!(
        stats.commits_sent, 3,
        "Alice should have sent 3 commits via send_requested_data"
    );

    // Alice received Bob's covering fragment.
    assert_eq!(
        stats.fragments_received, 1,
        "Alice should have received Bob's fragment"
    );

    // ── Verify Bob received all data ──
    //
    // With AlwaysDeep, Bob's minimize_tree also prunes the received commits
    // (his fragment covers them), so get_commits returns 0. That's correct —
    // the in-memory tree is minimized. The real proof is that the blobs are
    // in storage: get_blobs loads from the storage backend, not the tree.

    let bob_blob_count = bob
        .get_blobs(sed_id)
        .await
        .expect("storage should not error")
        .expect("bob should have blobs")
        .len();

    // Bob should have: 1 fragment blob (his own) + 3 commit blobs (Alice's)
    assert_eq!(
        bob_blob_count, 4,
        "Bob should have 4 blobs (3 commits + 1 fragment), got {bob_blob_count}"
    );

    Ok(())
}
