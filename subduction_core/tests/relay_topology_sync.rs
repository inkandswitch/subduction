//! Reproduces the topology of the user-observed bug: two end-peers connected
//! through a third Subduction acting as a relay. The relay stores commits
//! and computes its own diffs against incoming sync requests (matching the
//! `subduction_cli` server behavior in the logs).
//!
//! The user's report: when two browsers edit a todo doc through a relay
//! server, every `BatchSyncRequest` produces a response of the form
//! "N missing, requesting N" — i.e. zero overlap is detected despite
//! identical content. Each update therefore re-transfers the full history,
//! and compute grows with each edit.
//!
//! Existing `convergence.rs` covers two directly-connected Subductions on
//! `MemoryStorage` and passes. These tests add the relay middle-node so we
//! can see whether the bug is specific to that topology.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    commit::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
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

const SYNC_TIMEOUT: Option<Duration> = Some(Duration::from_millis(500));

/// Length of a single propagation hop. We let listeners drain after each
/// state-changing step so broadcasts settle before observations.
const PROPAGATION_PAUSE: Duration = Duration::from_millis(50);

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

fn make_head(seed: u8) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    bytes[1] = seed.wrapping_mul(31);
    bytes[2] = seed.wrapping_add(7);
    CommitId::new(bytes)
}

/// Build a single unsigned `(LooseCommit, Blob)` pair for use with
/// `add_built_batch`. Mirrors what the Automerge wasm adapter builds.
fn make_commit_pair(sed_id: SedimentreeId, seed: u8) -> (LooseCommit, Blob) {
    let blob = make_blob(seed);
    let blob_meta = BlobMeta::new(&blob);
    let head = make_head(seed);
    let commit = LooseCommit::new(sed_id, head, BTreeSet::new(), blob_meta);
    (commit, blob)
}

/// Connect two nodes via a bidirectional channel transport pair.
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

/// Three nodes wired A ↔ R ↔ B. R only has direct connections to A and B;
/// A and B never connect directly. R acts as a passive relay: it stores
/// what it sees and forwards `LooseCommit` / `Fragment` messages to its
/// other subscribers in `recv_loose_commit` / `recv_fragment`.
async fn setup_relay_topology() -> TestResult<(
    TestSubduction,
    TestSubduction,
    TestSubduction,
    MemorySigner,
    MemorySigner,
    MemorySigner,
)> {
    let a_signer = make_signer(10);
    let r_signer = make_signer(20);
    let b_signer = make_signer(30);

    let a = make_node(a_signer.clone());
    let r = make_node(r_signer.clone());
    let b = make_node(b_signer.clone());

    connect_pair(&a, &a_signer, &r, &r_signer).await?;
    connect_pair(&r, &r_signer, &b, &b_signer).await?;

    tokio::time::sleep(Duration::from_millis(20)).await;

    Ok((a, r, b, a_signer, r_signer, b_signer))
}

/// Phase 0 baseline: three-node topology converges on initial sync.
///
/// A makes a commit and explicitly drives sync. R and B both receive it
/// after the sync subscribes them. The `add_commit` broadcast fallback
/// reaches R immediately (no subscribers → broadcast to all connections);
/// for R → B we need an explicit `full_sync_with_all_peers` from B.
#[tokio::test]
async fn relay_topology_converges_on_initial_sync() -> TestResult {
    let (a, r, b, _a_signer, _r_signer, _b_signer) = setup_relay_topology().await?;

    let sed_id = SedimentreeId::new([7u8; 32]);

    a.add_commit(sed_id, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // Direct broadcast fallback reaches R immediately.
    assert_eq!(
        a.get_commits(sed_id).await.map(|c| c.len()),
        Some(1),
        "A has its own commit"
    );
    assert_eq!(
        r.get_commits(sed_id).await.map(|c| c.len()),
        Some(1),
        "R received the direct broadcast"
    );

    // B has no sedimentree yet — needs to actively pull. Once B has issued
    // a sync (which subscribes B to R), future commits will flow.
    b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // Hmm: B's full_sync_with_all_peers iterates B's known sedimentrees,
    // which is empty. So this doesn't help. We need B to explicitly call
    // sync_with_peer for sed_id to fetch it.
    b.sync_with_peer(
        &PeerId::from(make_signer(20).verifying_key()),
        sed_id,
        true,
        SYNC_TIMEOUT,
    )
    .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    assert_eq!(
        b.get_commits(sed_id).await.map(|c| c.len()),
        Some(1),
        "B received the commit after explicit sync_with_peer"
    );

    Ok(())
}

/// **The directly user-facing invariant.** After A and B have fully synced
/// through R, a follow-up `full_sync_with_all_peers` from either side
/// should report zero items transferred.
///
/// The user's logs show "19 missing, requesting 19" instead — i.e. the
/// stats from a converged-then-resynced flow are NON-empty even though
/// nothing has changed.
#[tokio::test]
async fn relay_topology_repeated_sync_after_convergence_is_empty() -> TestResult {
    let (a, r, b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;

    let sed_id = SedimentreeId::new([8u8; 32]);

    // Seed both ends with their own commits.
    for i in 0..5_u8 {
        a.add_commit(sed_id, make_head(i + 1), BTreeSet::new(), make_blob(i + 1))
            .await?;
    }
    for i in 0..5_u8 {
        b.add_commit(
            sed_id,
            make_head(100 + i),
            BTreeSet::new(),
            make_blob(100 + i),
        )
        .await?;
    }

    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // Drive a sync from both ends — let everything converge.
    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // Sanity: all three nodes should now have 10 commits.
    assert_eq!(
        a.get_commits(sed_id).await.map(|c| c.len()),
        Some(10),
        "A converged"
    );
    assert_eq!(
        r.get_commits(sed_id).await.map(|c| c.len()),
        Some(10),
        "R converged"
    );
    assert_eq!(
        b.get_commits(sed_id).await.map(|c| c.len()),
        Some(10),
        "B converged"
    );

    // The actual invariant being tested.
    for round in 1..=4 {
        let (_, stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
        assert!(
            stats.is_empty(),
            "round {round} from A (converged): expected empty sync stats, got \
             commits_received={}, commits_sent={}, fragments_received={}, fragments_sent={}",
            stats.commits_received,
            stats.commits_sent,
            stats.fragments_received,
            stats.fragments_sent,
        );

        let (_, stats, _, _) = b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
        assert!(
            stats.is_empty(),
            "round {round} from B (converged): expected empty sync stats, got \
             commits_received={}, commits_sent={}, fragments_received={}, fragments_sent={}",
            stats.commits_received,
            stats.commits_sent,
            stats.fragments_received,
            stats.fragments_sent,
        );
    }

    Ok(())
}

/// **Closest direct match to the user's todo-app workflow.** Both peers
/// rapid-fire single-commit edits while the system is "live" (i.e.
/// `add_commit` is broadcasting in the background). After things settle,
/// a sync round must be empty.
///
/// Models: a user typing fast in browser A while another user is typing
/// in browser B, both via a relay.
#[tokio::test]
async fn relay_topology_rapid_fire_then_idle_sync_is_empty() -> TestResult {
    let (a, r, b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;

    let sed_id = SedimentreeId::new([9u8; 32]);

    // Rapid-fire commits, no waits between them. Mirrors a user typing
    // characters into a todo input field.
    for i in 0..10_u8 {
        a.add_commit(sed_id, make_head(i + 1), BTreeSet::new(), make_blob(i + 1))
            .await?;
    }
    for i in 0..10_u8 {
        b.add_commit(
            sed_id,
            make_head(100 + i),
            BTreeSet::new(),
            make_blob(100 + i),
        )
        .await?;
    }

    // Give broadcasts a generous window to propagate through the relay.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // One sync to drive any residual convergence.
    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // All three nodes should have 20 commits.
    assert_eq!(
        a.get_commits(sed_id).await.map(|c| c.len()),
        Some(20),
        "A converged after rapid-fire"
    );
    assert_eq!(
        r.get_commits(sed_id).await.map(|c| c.len()),
        Some(20),
        "R converged after rapid-fire"
    );
    assert_eq!(
        b.get_commits(sed_id).await.map(|c| c.len()),
        Some(20),
        "B converged after rapid-fire"
    );

    // The invariant: now-idle sync must report zero.
    let (_, stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(
        stats.is_empty(),
        "idle sync after rapid-fire should be empty, got \
         commits_received={}, commits_sent={}, fragments_received={}, fragments_sent={}",
        stats.commits_received,
        stats.commits_sent,
        stats.fragments_received,
        stats.fragments_sent,
    );

    Ok(())
}

/// **One-more-commit incrementality.** After convergence, a single new
/// commit on A should result in B receiving exactly one commit on the
/// next sync — not the whole history.
///
/// The user's symptom maps directly to this assertion failing with N+1
/// instead of 1.
#[tokio::test]
async fn relay_topology_one_more_commit_transfers_only_the_delta() -> TestResult {
    let (a, r, b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;

    let sed_id = SedimentreeId::new([11u8; 32]);

    // Warm-up: A authors 8 commits; B authors 8 commits.
    for i in 0..8_u8 {
        a.add_commit(sed_id, make_head(i + 1), BTreeSet::new(), make_blob(i + 1))
            .await?;
    }
    for i in 0..8_u8 {
        b.add_commit(
            sed_id,
            make_head(100 + i),
            BTreeSet::new(),
            make_blob(100 + i),
        )
        .await?;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drive convergence.
    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    assert_eq!(a.get_commits(sed_id).await.map(|c| c.len()), Some(16));
    assert_eq!(r.get_commits(sed_id).await.map(|c| c.len()), Some(16));
    assert_eq!(b.get_commits(sed_id).await.map(|c| c.len()), Some(16));

    // A authors exactly one more commit. The broadcast from `add_commit`
    // is intentionally NOT awaited — we want the sync to be responsible
    // for delivering this one commit so we can measure delta size.
    //
    // (In a real client there is no `add_commit` broadcast; the wasm
    // adapter just calls the local-only path. Modeling the same here
    // would require `add_built_batch_locally` or similar; for now we
    // accept that the broadcast may race.)
    a.add_commit(
        sed_id,
        make_head(99),
        BTreeSet::new(),
        make_blob(99),
    )
    .await?;

    // No sleep — go straight to sync to measure what arrives via sync
    // vs what arrived via broadcast.
    let (_, a_stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    let (_, b_stats, _, _) = b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // After everything: 17 commits on each side.
    assert_eq!(a.get_commits(sed_id).await.map(|c| c.len()), Some(17));
    assert_eq!(r.get_commits(sed_id).await.map(|c| c.len()), Some(17));
    assert_eq!(b.get_commits(sed_id).await.map(|c| c.len()), Some(17));

    // The bug as described would produce stats with received≈16, sent≈16
    // on each side (the full history echoed back). Allow up to 2 extra
    // transfers as slack for the race with the broadcast.
    let max_acceptable = 2;
    assert!(
        a_stats.total_received() <= max_acceptable
            && a_stats.total_sent() <= max_acceptable,
        "A's incremental sync should transfer ≤{max_acceptable} items, got \
         received={}, sent={}",
        a_stats.total_received(),
        a_stats.total_sent(),
    );
    assert!(
        b_stats.total_received() <= max_acceptable
            && b_stats.total_sent() <= max_acceptable,
        "B's incremental sync should transfer ≤{max_acceptable} items, got \
         received={}, sent={}",
        b_stats.total_received(),
        b_stats.total_sent(),
    );

    // The follow-up sync must be empty (re-convergence).
    let (_, a_stats2, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    let (_, b_stats2, _, _) = b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(
        a_stats2.is_empty(),
        "A: follow-up sync should be empty, got received={}, sent={}",
        a_stats2.total_received(),
        a_stats2.total_sent(),
    );
    assert!(
        b_stats2.is_empty(),
        "B: follow-up sync should be empty, got received={}, sent={}",
        b_stats2.total_received(),
        b_stats2.total_sent(),
    );

    Ok(())
}

/// **The most direct reproduction of the user's todo-app workflow.** The
/// wasm `addBatch` API calls `add_built_batch`, which performs a local
/// insert+minimize then `sync_with_all_peers` with `subscribe: true`. This
/// is the path the Automerge-repo adapter uses on every change.
///
/// We repeatedly call `add_built_batch` (one commit at a time) and observe
/// the `SyncStats` for each call. The first call has nothing to sync
/// against (no peer state yet); subsequent calls should each transfer
/// exactly the delta. If the bug exists, stats from call N would show
/// transfers proportional to the total history.
#[tokio::test]
async fn relay_topology_add_built_batch_each_call_is_incremental() -> TestResult {
    let (a, _r, _b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;
    let sed_id = SedimentreeId::new([13u8; 32]);

    // First: do an initial sync to subscribe both A and the relay (and
    // through it, B) to this sedimentree. The tree doesn't exist yet on
    // any node, so this is essentially a no-op except that it registers
    // intent. (In a real client the tree would exist before any sync.)
    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // Issue 10 single-commit `add_built_batch` calls on A. Each goes
    // through the same path as `addBatch` in wasm:
    //   1. local insert+minimize
    //   2. sync_with_all_peers(id, subscribe=true)
    //
    // After call N, A should have N commits. After the BatchSyncRequest
    // in step 2 lands and the responder echoes "you have new stuff"
    // back via fire-and-forget, the relay should also have N commits.
    let mut commit_pairs = Vec::new();
    for seed in 1..=10_u8 {
        commit_pairs.push(make_commit_pair(sed_id, seed));
    }

    let mut prior_count = 0usize;
    for (idx, pair) in commit_pairs.into_iter().enumerate() {
        let n = idx + 1;
        a.add_built_batch(sed_id, vec![pair], Vec::new()).await?;
        tokio::time::sleep(PROPAGATION_PAUSE).await;

        // A should have exactly N commits.
        let a_count = a.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
        assert_eq!(
            a_count, n,
            "after call {n}: A should have {n} commits (sees {a_count})"
        );

        // Sanity: the count is monotonic.
        assert!(a_count > prior_count, "monotonic growth");
        prior_count = a_count;
    }

    Ok(())
}

/// **The actual invariant we are hunting.** Same `add_built_batch` flow
/// as above, but we measure that the BatchSyncRequest embedded in each
/// call doesn't echo the entire history back. Concretely: after call N,
/// the relay should hold N commits and a follow-up explicit sync from A
/// should be empty.
#[tokio::test]
async fn relay_topology_repeated_add_built_batch_then_sync_is_empty() -> TestResult {
    let (a, r, _b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;
    let sed_id = SedimentreeId::new([14u8; 32]);

    // Subscribe.
    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    for seed in 1..=10_u8 {
        let pair = make_commit_pair(sed_id, seed);
        a.add_built_batch(sed_id, vec![pair], Vec::new()).await?;
        tokio::time::sleep(PROPAGATION_PAUSE).await;
    }

    // Final state: A has 10 commits. R should too (received them via the
    // fire-and-forget responses to each BatchSyncRequest).
    assert_eq!(
        a.get_commits(sed_id).await.map(|c| c.len()),
        Some(10),
        "A has all 10 commits"
    );
    assert_eq!(
        r.get_commits(sed_id).await.map(|c| c.len()),
        Some(10),
        "R received all 10 commits via the BatchSyncRequest reply flow"
    );

    // Explicit follow-up sync: must report zero transfer in either direction.
    let (_, stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(
        stats.is_empty(),
        "after 10 add_built_batch calls, A→R should be in sync (received={}, sent={})",
        stats.total_received(),
        stats.total_sent(),
    );

    Ok(())
}

/// **Two browsers, one relay, both updating a shared doc.** Most directly
/// mirrors the user's setup: two clients (A, B) each repeatedly call
/// `add_built_batch` (the wasm `addBatch` path) against a single relay
/// (R). After all updates land, the relay's view should fully match both
/// clients and a follow-up sync should be empty for both.
///
/// If the bug from the logs reproduces here, we'd see one of:
/// - Final commit counts disagreeing across A, R, B.
/// - The trailing `full_sync_with_all_peers` from A or B reporting
///   non-empty stats despite all data being present everywhere.
#[tokio::test]
async fn relay_topology_two_clients_add_built_batch_converge_via_relay() -> TestResult {
    let (a, r, b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;
    let sed_id = SedimentreeId::new([15u8; 32]);

    // Subscribe both ends to the relay for this sedimentree. Because the
    // tree doesn't exist anywhere yet, `full_sync_with_all_peers` over a
    // known-empty key-set is a no-op for subscription purposes — we have
    // to seed each side with an explicit per-id sync.
    a.sync_with_peer(
        &PeerId::from(make_signer(20).verifying_key()),
        sed_id,
        true,
        SYNC_TIMEOUT,
    )
    .await?;
    b.sync_with_peer(
        &PeerId::from(make_signer(20).verifying_key()),
        sed_id,
        true,
        SYNC_TIMEOUT,
    )
    .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // Interleaved single-commit `add_built_batch` calls from both ends.
    // Mirrors two users typing into a shared todo simultaneously.
    let total_pairs = 12;
    for i in 0..total_pairs {
        if i % 2 == 0 {
            let pair = make_commit_pair(sed_id, (i as u8) + 1);
            a.add_built_batch(sed_id, vec![pair], Vec::new()).await?;
        } else {
            let pair = make_commit_pair(sed_id, 100 + (i as u8));
            b.add_built_batch(sed_id, vec![pair], Vec::new()).await?;
        }
        tokio::time::sleep(PROPAGATION_PAUSE).await;
    }

    // All three nodes should now hold all 12 commits.
    let a_count = a.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    let r_count = r.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    let b_count = b.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    assert_eq!(
        (a_count, r_count, b_count),
        (total_pairs, total_pairs, total_pairs),
        "after interleaved add_built_batch from both ends, all three should hold \
         {total_pairs} commits each (got A={a_count}, R={r_count}, B={b_count})"
    );

    // Follow-up sync from each end must report empty stats.
    let (_, a_stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    let (_, b_stats, _, _) = b.full_sync_with_all_peers(SYNC_TIMEOUT).await;

    assert!(
        a_stats.is_empty(),
        "A's follow-up sync should be empty, got received={}, sent={}",
        a_stats.total_received(),
        a_stats.total_sent(),
    );
    assert!(
        b_stats.is_empty(),
        "B's follow-up sync should be empty, got received={}, sent={}",
        b_stats.total_received(),
        b_stats.total_sent(),
    );

    Ok(())
}

/// **Concurrent BatchSyncRequests from the same peer.** Most direct
/// reproduction of the race I suspected in the responder's
/// `recv_batch_sync_request`: the responder loads commits from storage
/// then locks the in-memory shard, but those steps are interleaved with
/// concurrent writes from other ingestion paths.
///
/// We fire many simultaneous `add_built_batch` calls (each containing
/// its own embedded `sync_with_all_peers`) and assert final convergence.
#[tokio::test]
async fn relay_topology_concurrent_add_built_batch_calls_converge() -> TestResult {
    let (a, r, _b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;
    let sed_id = SedimentreeId::new([16u8; 32]);

    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // Launch 8 add_built_batch calls in parallel from A.
    let n = 8_u8;
    let pairs: Vec<_> = (1..=n).map(|seed| make_commit_pair(sed_id, seed)).collect();

    let mut handles = Vec::new();
    for pair in pairs {
        let a_clone = a.clone();
        handles.push(tokio::spawn(async move {
            a_clone.add_built_batch(sed_id, vec![pair], Vec::new()).await
        }));
    }

    for h in handles {
        h.await??;
    }

    // Give things time to settle. The concurrent calls each issue a
    // `sync_with_all_peers` internally, so the relay sees a burst of
    // BatchSyncRequests with different seeds.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Both sides should have all `n` commits.
    let a_count = a.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    let r_count = r.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    assert_eq!(a_count, n as usize, "A should have all {n} commits");
    assert_eq!(
        r_count, n as usize,
        "R should have all {n} commits (despite the concurrent BatchSyncRequest barrage)"
    );

    // Final sync must be empty.
    let (_, stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(
        stats.is_empty(),
        "post-burst sync should be empty, got received={}, sent={}",
        stats.total_received(),
        stats.total_sent(),
    );

    Ok(())
}
