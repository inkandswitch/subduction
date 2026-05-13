//! Three-node A↔R↔B sync over `MemoryStorage` + `ChannelTransport`.
//! The middle relay only has direct connections to A and B; the end
//! peers never connect directly — i.e. clients talking through a
//! relay server.

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

/// Unsigned `(LooseCommit, Blob)` pair for `add_built_batch`. Mirrors
/// what the Automerge wasm adapter builds.
fn make_commit_pair(sed_id: SedimentreeId, seed: u8) -> (LooseCommit, Blob) {
    let blob = make_blob(seed);
    let blob_meta = BlobMeta::new(&blob);
    let head = make_head(seed);
    let commit = LooseCommit::new(sed_id, head, BTreeSet::new(), blob_meta);
    (commit, blob)
}

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

#[tokio::test]
async fn relay_topology_converges_on_initial_sync() -> TestResult {
    let (a, r, b, _a_signer, _r_signer, _b_signer) = setup_relay_topology().await?;

    let sed_id = SedimentreeId::new([7u8; 32]);

    a.add_commit(sed_id, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // `add_commit`'s broadcast fallback (no subscribers → broadcast to all
    // connections) reaches R immediately.
    assert_eq!(a.get_commits(sed_id).await.map(|c| c.len()), Some(1));
    assert_eq!(r.get_commits(sed_id).await.map(|c| c.len()), Some(1));

    // B has no sedimentree yet, so `full_sync_with_all_peers` (which
    // iterates B's known sedimentrees) is a no-op for this id. Force a
    // per-id sync to subscribe B and pull the data.
    b.sync_with_peer(
        &PeerId::from(make_signer(20).verifying_key()),
        sed_id,
        true,
        SYNC_TIMEOUT,
    )
    .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    assert_eq!(b.get_commits(sed_id).await.map(|c| c.len()), Some(1));

    Ok(())
}

/// After A and B have fully converged through R, repeated
/// `full_sync_with_all_peers` from either side must report empty stats.
#[tokio::test]
async fn relay_topology_repeated_sync_after_convergence_is_empty() -> TestResult {
    let (a, r, b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;

    let sed_id = SedimentreeId::new([8u8; 32]);

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

    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    assert_eq!(a.get_commits(sed_id).await.map(|c| c.len()), Some(10));
    assert_eq!(r.get_commits(sed_id).await.map(|c| c.len()), Some(10));
    assert_eq!(b.get_commits(sed_id).await.map(|c| c.len()), Some(10));

    for round in 1..=4 {
        let (_, stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
        assert!(stats.is_empty(), "round {round} from A: {stats:?}",);
        let (_, stats, _, _) = b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
        assert!(stats.is_empty(), "round {round} from B: {stats:?}",);
    }

    Ok(())
}

/// Two peers rapid-fire single-commit edits, then an idle sync round
/// must be empty. Models two browsers typing into a shared doc.
#[tokio::test]
async fn relay_topology_rapid_fire_then_idle_sync_is_empty() -> TestResult {
    let (a, r, b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;

    let sed_id = SedimentreeId::new([9u8; 32]);

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
    tokio::time::sleep(Duration::from_millis(200)).await;

    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    assert_eq!(a.get_commits(sed_id).await.map(|c| c.len()), Some(20));
    assert_eq!(r.get_commits(sed_id).await.map(|c| c.len()), Some(20));
    assert_eq!(b.get_commits(sed_id).await.map(|c| c.len()), Some(20));

    let (_, stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(stats.is_empty(), "idle sync after rapid-fire: {stats:?}");

    Ok(())
}

/// After convergence, one new commit on A propagates to the relay and
/// to B by the time both peers explicitly sync, and the sync itself
/// transfers at most that one commit (never the whole history). The
/// follow-up round must be exactly empty.
#[tokio::test]
async fn relay_topology_one_more_commit_transfers_only_the_delta() -> TestResult {
    let (a, r, b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;

    let sed_id = SedimentreeId::new([11u8; 32]);

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

    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    assert_eq!(a.get_commits(sed_id).await.map(|c| c.len()), Some(16));
    assert_eq!(r.get_commits(sed_id).await.map(|c| c.len()), Some(16));
    assert_eq!(b.get_commits(sed_id).await.map(|c| c.len()), Some(16));

    // Author one new commit on A and let the broadcast propagate before
    // measuring delta-sync behavior. `add_commit` broadcasts via the
    // subscription path; without the pause its delivery races with the
    // explicit sync we're about to invoke.
    a.add_commit(sed_id, make_head(99), BTreeSet::new(), make_blob(99))
        .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let (_, a_stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    let (_, b_stats, _, _) = b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    assert_eq!(a.get_commits(sed_id).await.map(|c| c.len()), Some(17));
    assert_eq!(r.get_commits(sed_id).await.map(|c| c.len()), Some(17));
    assert_eq!(b.get_commits(sed_id).await.map(|c| c.len()), Some(17));

    // Either the broadcast won (0 transferred during sync) or the sync
    // delivered the new commit (1 transferred) — never the full history.
    assert!(
        a_stats.total_received() <= 1 && a_stats.total_sent() <= 1,
        "A delta: {a_stats:?}",
    );
    assert!(
        b_stats.total_received() <= 1 && b_stats.total_sent() <= 1,
        "B delta: {b_stats:?}",
    );

    let (_, a_stats2, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    let (_, b_stats2, _, _) = b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(a_stats2.is_empty(), "A follow-up: {a_stats2:?}");
    assert!(b_stats2.is_empty(), "B follow-up: {b_stats2:?}");

    Ok(())
}

/// `add_built_batch` is the wasm `addBatch` path: local insert+minimize
/// then `sync_with_all_peers(subscribe=true)`. Each call should leave A
/// with monotonically growing commit count.
#[tokio::test]
async fn relay_topology_add_built_batch_each_call_is_incremental() -> TestResult {
    let (a, _r, _b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;
    let sed_id = SedimentreeId::new([13u8; 32]);

    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let mut prior_count = 0usize;
    for seed in 1..=10_u8 {
        let n = seed as usize;
        a.add_built_batch(sed_id, vec![make_commit_pair(sed_id, seed)], Vec::new())
            .await?;
        tokio::time::sleep(PROPAGATION_PAUSE).await;

        let a_count = a.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
        assert_eq!(a_count, n);
        assert!(a_count > prior_count);
        prior_count = a_count;
    }

    Ok(())
}

/// After repeated `add_built_batch` calls, A and R must hold the same
/// commit set, and an explicit follow-up sync must be empty.
#[tokio::test]
async fn relay_topology_repeated_add_built_batch_then_sync_is_empty() -> TestResult {
    let (a, r, _b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;
    let sed_id = SedimentreeId::new([14u8; 32]);

    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    for seed in 1..=10_u8 {
        a.add_built_batch(sed_id, vec![make_commit_pair(sed_id, seed)], Vec::new())
            .await?;
        tokio::time::sleep(PROPAGATION_PAUSE).await;
    }

    assert_eq!(a.get_commits(sed_id).await.map(|c| c.len()), Some(10));
    assert_eq!(r.get_commits(sed_id).await.map(|c| c.len()), Some(10));

    let (_, stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(stats.is_empty(), "{stats:?}");

    Ok(())
}

/// Interleaved `add_built_batch` from A and B; the relay must end up
/// with the union, and a follow-up sync from either client must be empty.
#[tokio::test]
async fn relay_topology_two_clients_add_built_batch_converge_via_relay() -> TestResult {
    let (a, r, b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;
    let sed_id = SedimentreeId::new([15u8; 32]);

    let r_peer = PeerId::from(make_signer(20).verifying_key());
    a.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;
    b.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

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

    let a_count = a.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    let r_count = r.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    let b_count = b.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    assert_eq!(
        (a_count, r_count, b_count),
        (total_pairs, total_pairs, total_pairs)
    );

    let (_, a_stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    let (_, b_stats, _, _) = b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(a_stats.is_empty(), "A: {a_stats:?}");
    assert!(b_stats.is_empty(), "B: {b_stats:?}");

    Ok(())
}

/// Many concurrent `add_built_batch` calls from A. Each issues an
/// embedded `BatchSyncRequest`, so the relay sees a burst of concurrent
/// requests interleaved with `LooseCommit` pushes. Probes the
/// load-vs-save race window in the responder.
#[tokio::test]
async fn relay_topology_concurrent_add_built_batch_calls_converge() -> TestResult {
    let (a, r, _b, _a_s, _r_s, _b_s) = setup_relay_topology().await?;
    let sed_id = SedimentreeId::new([16u8; 32]);

    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let n = 8_u8;
    let pairs: Vec<_> = (1..=n).map(|seed| make_commit_pair(sed_id, seed)).collect();

    let mut handles = Vec::new();
    for pair in pairs {
        let a_clone = a.clone();
        handles.push(tokio::spawn(async move {
            a_clone
                .add_built_batch(sed_id, vec![pair], Vec::new())
                .await
        }));
    }
    for h in handles {
        h.await??;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        a.get_commits(sed_id).await.map(|c| c.len()),
        Some(n as usize)
    );
    assert_eq!(
        r.get_commits(sed_id).await.map(|c| c.len()),
        Some(n as usize)
    );

    let (_, stats, _, _) = a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(stats.is_empty(), "{stats:?}");

    Ok(())
}
