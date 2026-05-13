//! Relay-topology sync test using **`FsStorage`** instead of `MemoryStorage`.
//!
//! The equivalent test in `subduction_core/tests/relay_topology_sync.rs`
//! passes with `MemoryStorage`. The user-observed bug only reproduces with
//! the `subduction_cli` server, which uses `MetricsStorage<FsStorage>`.
//! If this test fails while the `MemoryStorage` version passes, the bug
//! is in `FsStorage` (most likely a concurrency / load-vs-save race that
//! the synchronous in-memory map doesn't have).

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
    >,
>;

const SYNC_TIMEOUT: Option<Duration> = Some(Duration::from_millis(1000));
const PROPAGATION_PAUSE: Duration = Duration::from_millis(80);

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
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

fn make_commit_pair(sed_id: SedimentreeId, seed: u8) -> (LooseCommit, Blob) {
    let blob = make_blob(seed);
    let blob_meta = BlobMeta::new(&blob);
    let head = make_head(seed);
    let commit = LooseCommit::new(sed_id, head, BTreeSet::new(), blob_meta);
    (commit, blob)
}

/// Build a Subduction backed by `FsStorage` rooted at `tempdir`.
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
    // Held to keep storage dirs alive for the duration of the test.
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

/// Single-client: repeatedly `add_built_batch` (the wasm `addBatch` path)
/// against a relay backed by `FsStorage`. Each call inserts one commit,
/// then internally syncs with the relay. After all calls, both sides must
/// have all commits and a follow-up sync must be empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs_relay_single_client_repeated_add_built_batch_converges() -> TestResult {
    let h = setup_relay().await?;
    let sed_id = SedimentreeId::new([0xA1; 32]);

    h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let total = 10_u8;
    for seed in 1..=total {
        let pair = make_commit_pair(sed_id, seed);
        h.a.add_built_batch(sed_id, vec![pair], Vec::new()).await?;
        tokio::time::sleep(PROPAGATION_PAUSE).await;
    }

    assert_eq!(
        h.a.get_commits(sed_id).await.map(|c| c.len()),
        Some(total as usize),
        "A: all commits present"
    );
    assert_eq!(
        h.r.get_commits(sed_id).await.map(|c| c.len()),
        Some(total as usize),
        "R: all commits propagated"
    );

    let (_, stats, _, _) = h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(
        stats.is_empty(),
        "follow-up sync should be empty, got received={}, sent={}",
        stats.total_received(),
        stats.total_sent(),
    );

    Ok(())
}

/// Two clients writing through a relay, each calling `add_built_batch`
/// repeatedly. This is the closest analogue to the user's two-browser
/// todo-app workflow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs_relay_two_clients_add_built_batch_converge_via_relay() -> TestResult {
    let h = setup_relay().await?;
    let sed_id = SedimentreeId::new([0xA2; 32]);

    let r_peer = PeerId::from(make_signer(20).verifying_key());

    // Subscribe each client to the relay for this sed_id.
    h.a.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;
    h.b.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let total_pairs = 12;
    for i in 0..total_pairs {
        if i % 2 == 0 {
            let pair = make_commit_pair(sed_id, (i as u8) + 1);
            h.a.add_built_batch(sed_id, vec![pair], Vec::new()).await?;
        } else {
            let pair = make_commit_pair(sed_id, 100 + (i as u8));
            h.b.add_built_batch(sed_id, vec![pair], Vec::new()).await?;
        }
        tokio::time::sleep(PROPAGATION_PAUSE).await;
    }

    let a_count = h.a.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    let r_count = h.r.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    let b_count = h.b.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    assert_eq!(
        (a_count, r_count, b_count),
        (total_pairs, total_pairs, total_pairs),
        "after interleaved add_built_batch from both ends, all three should hold \
         {total_pairs} commits each (got A={a_count}, R={r_count}, B={b_count})"
    );

    let (_, a_stats, _, _) = h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    let (_, b_stats, _, _) = h.b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
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

/// **Rapid-fire stress.** Two clients fire commits as fast as possible,
/// with no waits between calls. Each commit involves disk I/O via
/// `FsStorage`. With concurrent `BatchSyncRequest`s arriving while writes
/// are still in flight, this is the most plausible setup for a
/// load-vs-save race on the responder.
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

    let total = 16usize;
    // Each side authors `total / 2` commits in rapid succession. The
    // `await` is required at each step (single-task issuance), but no
    // PROPAGATION_PAUSE between them.
    for i in 0..total {
        if i % 2 == 0 {
            let pair = make_commit_pair(sed_id, (i as u8) + 1);
            h.a.add_built_batch(sed_id, vec![pair], Vec::new()).await?;
        } else {
            let pair = make_commit_pair(sed_id, 100 + (i as u8));
            h.b.add_built_batch(sed_id, vec![pair], Vec::new()).await?;
        }
    }

    // Give I/O + propagation a generous chance to settle.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Drive convergence explicitly.
    h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    h.b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let a_count = h.a.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    let r_count = h.r.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    let b_count = h.b.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    assert_eq!(
        (a_count, r_count, b_count),
        (total, total, total),
        "rapid-fire: all three should hold {total} commits each \
         (got A={a_count}, R={r_count}, B={b_count})"
    );

    let (_, a_stats, _, _) = h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    let (_, b_stats, _, _) = h.b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(
        a_stats.is_empty(),
        "A's follow-up sync after rapid-fire should be empty, got received={}, sent={}",
        a_stats.total_received(),
        a_stats.total_sent(),
    );
    assert!(
        b_stats.is_empty(),
        "B's follow-up sync after rapid-fire should be empty, got received={}, sent={}",
        b_stats.total_received(),
        b_stats.total_sent(),
    );

    Ok(())
}

/// **Concurrent add_built_batch from a single client.** Spawns multiple
/// tasks that each call `add_built_batch` simultaneously against the
/// relay. Each `add_built_batch` internally issues a `BatchSyncRequest`,
/// so the relay sees a burst of concurrent requests interleaved with
/// `LooseCommit` fire-and-forget pushes. With `FsStorage`'s real I/O
/// latency, the storage-load-vs-in-memory-shard race window is widest.
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
            a_clone.add_built_batch(sed_id, vec![pair], Vec::new()).await
        }));
    }
    for handle in handles {
        handle.await??;
    }

    tokio::time::sleep(Duration::from_millis(400)).await;

    let a_count = h.a.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    let r_count = h.r.get_commits(sed_id).await.map(|c| c.len()).unwrap_or(0);
    assert_eq!(
        a_count, n as usize,
        "A: all {n} commits present after concurrent burst"
    );
    assert_eq!(
        r_count, n as usize,
        "R: all {n} commits propagated despite concurrent BatchSyncRequest barrage \
         (got R={r_count})"
    );

    let (_, stats, _, _) = h.a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    assert!(
        stats.is_empty(),
        "post-burst sync should be empty, got received={}, sent={}",
        stats.total_received(),
        stats.total_sent(),
    );

    Ok(())
}
