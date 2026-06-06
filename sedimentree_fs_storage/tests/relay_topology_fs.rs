//! Relay-topology sync over `FsStorage` (vs. the `MemoryStorage`
//! version in `subduction_core/tests/relay_topology_sync.rs`). If
//! these fail while the in-memory tests pass, the issue is in
//! `FsStorage`'s concurrency model rather than the sync protocol.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    commit::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use sedimentree_fs_storage::FsStorage;
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    subduction::{Subduction, builder::SubductionBuilder},
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use tempfile::TempDir;
use testresult::TestResult;

type Conn = MessageTransport<ChannelTransport>;
type TestSyncHandler = SyncHandler<Sendable, FsStorage, Conn, OpenPolicy, CountLeadingZeroBytes>;
type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        FsStorage,
        Conn,
        TestSyncHandler,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

const SYNC_TIMEOUT: Option<Duration> = Some(Duration::from_secs(1));
const PROPAGATION_PAUSE: Duration = Duration::from_millis(80);

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_blob(seed: u8) -> Blob {
    let data: Vec<u8> = (0..64).map(|i| seed.wrapping_add(i)).collect();
    Blob::new(data)
}

const fn make_head(seed: u8) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    bytes[1] = seed.wrapping_mul(31);
    bytes[2] = seed.wrapping_add(7);
    CommitId::new(bytes)
}

fn make_commit_pair(sed_id: SedimentreeId, seed: u8) -> (LooseCommit, Blob) {
    let blob = make_blob(seed);
    let blob_meta = BlobMeta::new(&blob);
    let head = make_head(seed);
    let commit = LooseCommit::new(sed_id, head, BTreeSet::new(), blob_meta);
    (commit, blob)
}

fn make_node(signer: MemorySigner, tempdir: &TempDir) -> TestResult<TestSubduction> {
    let storage = FsStorage::new(tempdir.path().to_path_buf())?;

    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(storage, Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    Ok(sd)
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

struct RelayHarness {
    a: TestSubduction,
    r: TestSubduction,
    b: TestSubduction,
    /// Held to keep storage dirs alive for the duration of the test.
    _dirs: [TempDir; 3],
}

async fn setup_relay() -> TestResult<RelayHarness> {
    let dir_a = tempfile::tempdir()?;
    let dir_r = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;

    let a_signer = make_signer(10);
    let r_signer = make_signer(20);
    let b_signer = make_signer(30);

    let a = make_node(a_signer.clone(), &dir_a)?;
    let r = make_node(r_signer.clone(), &dir_r)?;
    let b = make_node(b_signer.clone(), &dir_b)?;

    connect_pair(&a, &a_signer, &r, &r_signer).await?;
    connect_pair(&r, &r_signer, &b, &b_signer).await?;

    tokio::time::sleep(Duration::from_millis(30)).await;

    Ok(RelayHarness {
        a,
        r,
        b,
        _dirs: [dir_a, dir_r, dir_b],
    })
}

/// Poll until all three nodes hold exactly `expected` commits, or
/// `timeout` elapses. Returns the last observed counts.
///
/// Used by tests whose `assert_eq!` would otherwise race against
/// broadcast propagation. The per-iteration `PROPAGATION_PAUSE` is
/// occasionally insufficient under heavy CPU contention (e.g., when
/// `cargo test` runs all four FS relay tests in parallel within one
/// process). The real correctness invariant is "convergence is reached
/// in bounded time", not "convergence is reached within exactly the
/// last `PROPAGATION_PAUSE` window".
async fn wait_for_relay_convergence(
    h: &RelayHarness,
    sed_id: SedimentreeId,
    expected: usize,
    timeout: Duration,
) -> (usize, usize, usize) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let a = h.a.get_commits(sed_id).await.map_or(0, |c| c.len());
        let r = h.r.get_commits(sed_id).await.map_or(0, |c| c.len());
        let b = h.b.get_commits(sed_id).await.map_or(0, |c| c.len());
        if (a, r, b) == (expected, expected, expected) || tokio::time::Instant::now() >= deadline {
            return (a, r, b);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Poll until A and R both hold exactly `expected` commits, or
/// `timeout` elapses. Returns `(a, r)` counts at exit.
///
/// Variant of [`wait_for_relay_convergence`] for tests that don't
/// exercise B (e.g., single-client and concurrent-writer scenarios).
async fn wait_for_ar_convergence(
    h: &RelayHarness,
    sed_id: SedimentreeId,
    expected: usize,
    timeout: Duration,
) -> (usize, usize) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let a = h.a.get_commits(sed_id).await.map_or(0, |c| c.len());
        let r = h.r.get_commits(sed_id).await.map_or(0, |c| c.len());
        if (a, r) == (expected, expected) || tokio::time::Instant::now() >= deadline {
            return (a, r);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs_relay_single_client_repeated_add_built_batch_converges() -> TestResult {
    let h = setup_relay().await?;
    let sed_id = SedimentreeId::new([0xA1; 32]);

    h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let total = 10_u8;
    for seed in 1..=total {
        let pair = make_commit_pair(sed_id, seed);
        h.a.add_built_batch(sed_id, vec![pair], Vec::new(), None)
            .await?;
        tokio::time::sleep(PROPAGATION_PAUSE).await;
    }

    let expected = total as usize;
    let (a_count, r_count) =
        wait_for_ar_convergence(&h, sed_id, expected, Duration::from_secs(2)).await;
    assert_eq!(a_count, expected, "A count");
    assert_eq!(r_count, expected, "R count");

    let (_, stats, _, _) = h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(stats.is_empty(), "{stats:?}");

    Ok(())
}

/// Two clients writing through a single relay: A and B each
/// `add_built_batch` repeatedly; final state must agree and a follow-up
/// sync must be empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs_relay_two_clients_add_built_batch_converge_via_relay() -> TestResult {
    let h = setup_relay().await?;
    let sed_id = SedimentreeId::new([0xA2; 32]);

    let r_peer = PeerId::from(make_signer(20).verifying_key());
    h.a.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;
    h.b.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let total_pairs: u8 = 12;
    for i in 0..total_pairs {
        if i % 2 == 0 {
            let pair = make_commit_pair(sed_id, i + 1);
            h.a.add_built_batch(sed_id, vec![pair], Vec::new(), None)
                .await?;
        } else {
            let pair = make_commit_pair(sed_id, 100 + i);
            h.b.add_built_batch(sed_id, vec![pair], Vec::new(), None)
                .await?;
        }
        tokio::time::sleep(PROPAGATION_PAUSE).await;
    }

    let total = total_pairs as usize;
    let (a_count, r_count, b_count) =
        wait_for_relay_convergence(&h, sed_id, total, Duration::from_secs(2)).await;
    assert_eq!((a_count, r_count, b_count), (total, total, total));

    let (_, a_stats, _, _) = h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    let (_, b_stats, _, _) = h.b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(a_stats.is_empty(), "A: {a_stats:?}");
    assert!(b_stats.is_empty(), "B: {b_stats:?}");

    Ok(())
}

/// Rapid-fire from both clients, no inter-call pause. Concurrent
/// `BatchSyncRequest`s arrive while writes are still in flight — the
/// load-vs-save race window is widest here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs_relay_two_clients_rapid_fire_converges() -> TestResult {
    let h = setup_relay().await?;
    let sed_id = SedimentreeId::new([0xA3; 32]);

    let r_peer = PeerId::from(make_signer(20).verifying_key());
    h.a.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;
    h.b.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let total_u8: u8 = 16;
    let total = total_u8 as usize;
    for i in 0..total_u8 {
        if i % 2 == 0 {
            let pair = make_commit_pair(sed_id, i + 1);
            h.a.add_built_batch(sed_id, vec![pair], Vec::new(), None)
                .await?;
        } else {
            let pair = make_commit_pair(sed_id, 100 + i);
            h.b.add_built_batch(sed_id, vec![pair], Vec::new(), None)
                .await?;
        }
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    h.b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let a_count = h.a.get_commits(sed_id).await.map_or(0, |c| c.len());
    let r_count = h.r.get_commits(sed_id).await.map_or(0, |c| c.len());
    let b_count = h.b.get_commits(sed_id).await.map_or(0, |c| c.len());
    assert_eq!((a_count, r_count, b_count), (total, total, total));

    let (_, a_stats, _, _) = h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    let (_, b_stats, _, _) = h.b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(a_stats.is_empty(), "A: {a_stats:?}");
    assert!(b_stats.is_empty(), "B: {b_stats:?}");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs_relay_concurrent_add_built_batch_calls_converge() -> TestResult {
    let h = setup_relay().await?;
    let sed_id = SedimentreeId::new([0xA4; 32]);

    h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let n = 12_u8;
    let pairs: Vec<_> = (1..=n).map(|seed| make_commit_pair(sed_id, seed)).collect();

    let mut handles = Vec::new();
    for pair in pairs {
        let a_clone = h.a.clone();
        handles.push(tokio::spawn(async move {
            a_clone
                .add_built_batch(sed_id, vec![pair], Vec::new(), None)
                .await
        }));
    }
    for handle in handles {
        handle.await??;
    }

    // Give in-flight broadcasts and embedded syncs a moment to drain,
    // then drive an explicit sync to deterministically flush any state
    // still pending on the relay (FsStorage I/O can outlast the burst).
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (_, stats, _, _) = h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;

    let total = n as usize;
    let (a_count, r_count) =
        wait_for_ar_convergence(&h, sed_id, total, Duration::from_secs(2)).await;
    assert_eq!(a_count, total, "A count after burst");
    assert_eq!(
        r_count, total,
        "R count after burst; drive sync stats: {stats:?}"
    );

    // Re-converged: a follow-up sync must be empty.
    let (_, stats2, _, _) = h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(stats2.is_empty(), "follow-up sync: {stats2:?}");

    Ok(())
}
