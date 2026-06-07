//! Three-node A↔R↔B sync over `MemoryStorage` + `ChannelTransport`.
//! The middle relay only has direct connections to A and B; the end
//! peers never connect directly — i.e. clients talking through a
//! relay server.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use core::convert::Infallible;
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    crypto::fingerprint::FingerprintSeed,
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::FingerprintSummary,
};

use futures::{FutureExt, future::BoxFuture};
use subduction_core::{
    authenticated::Authenticated,
    connection::{
        message::{
            BatchSyncRequest, BatchSyncResponse, RequestId, RequestedData, SyncDiff, SyncMessage,
            SyncResult,
        },
        test_utils::{
            ChannelMockConnection, ChannelMockConnectionHandle, ChannelTransport, InstantTimeout,
            TokioSpawn,
        },
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, open::OpenPolicy, storage::StoragePolicy},
    remote_heads::RemoteHeads,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::{signer::memory::MemorySigner, verified_author::VerifiedAuthor};
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
        TokioSpawn,
    >,
>;

const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(500);
const PROPAGATION_PAUSE: Duration = Duration::from_millis(50);

/// Fail-fast cap for [`wait_until`] polling; tests converge well within it.
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Poll `cond` until it holds (returns `true`) or [`WAIT_TIMEOUT`]
/// elapses (returns `false`), avoiding fixed `sleep`-then-assert delays.
async fn wait_until<F, Fut>(mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: core::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        if cond().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

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

const fn make_head(seed: u8) -> CommitId {
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
        assert!(stats.is_empty(), "round {round} from A: {stats:?}");
        let (_, stats, _, _) = b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
        assert!(stats.is_empty(), "round {round} from B: {stats:?}");
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
    assert!(stats.is_empty(), "{stats:?}");

    Ok(())
}

/// When A subscribes to R for a sedimentree that R doesn't yet know
/// about, R must propagate that subscription upstream to B so any
/// future commits B pushes can be forwarded back through R to A.
///
/// This is the symmetric counterpart of the existing outbound
/// broadcast in `SyncHandler::recv_commit` / `recv_fragment`:
/// forwarding updates and forwarding subscription requests are now
/// both done by every node.
#[tokio::test]
async fn relay_topology_propagates_subscriptions_upstream() -> TestResult {
    let (a, r, b, _a_s, r_signer, b_signer) = setup_relay_topology().await?;

    let sed_id = SedimentreeId::new([42u8; 32]);

    // A subscribes to R for `sed_id` (R has no data yet).
    let r_peer = PeerId::from(r_signer.verifying_key());
    a.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;

    // R propagates to B; poll until the subscription is recorded.
    let b_peer = PeerId::from(b_signer.verifying_key());
    let r_subscribed = wait_until(|| {
        let r = Arc::clone(&r);
        async move { r.get_peer_subscriptions(b_peer).await.contains(&sed_id) }
    })
    .await;
    assert!(
        r_subscribed,
        "R failed to propagate A's subscription upstream to B: {:?}",
        r.get_peer_subscriptions(b_peer).await
    );

    // End-to-end: a commit pushed at B must reach A through R, even
    // though A never subscribed directly to B and B never knew about
    // A. This exercises the full update path:
    //   B --LooseCommit--> R (because R subscribed to B above)
    //   R --LooseCommit--> A (because A subscribed to R first)
    b.add_commit(sed_id, make_head(99), BTreeSet::new(), make_blob(99))
        .await?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(b.get_commits(sed_id).await.map(|c| c.len()), Some(1));
    assert_eq!(
        r.get_commits(sed_id).await.map(|c| c.len()),
        Some(1),
        "R did not receive commit from B"
    );
    assert_eq!(
        a.get_commits(sed_id).await.map(|c| c.len()),
        Some(1),
        "A did not receive commit relayed from B via R"
    );

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
        a.add_built_batch(
            sed_id,
            vec![make_commit_pair(sed_id, seed)],
            Vec::new(),
            CallTimeout::Default,
        )
        .await?;
        tokio::time::sleep(PROPAGATION_PAUSE).await;

        let a_count = a.get_commits(sed_id).await.map_or(0, |c| c.len());
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
        a.add_built_batch(
            sed_id,
            vec![make_commit_pair(sed_id, seed)],
            Vec::new(),
            CallTimeout::Default,
        )
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

    let total_pairs: u8 = 12;
    for i in 0..total_pairs {
        if i % 2 == 0 {
            let pair = make_commit_pair(sed_id, i + 1);
            a.add_built_batch(sed_id, vec![pair], Vec::new(), CallTimeout::Default)
                .await?;
        } else {
            let pair = make_commit_pair(sed_id, 100 + i);
            b.add_built_batch(sed_id, vec![pair], Vec::new(), CallTimeout::Default)
                .await?;
        }
        tokio::time::sleep(PROPAGATION_PAUSE).await;
    }

    let total = total_pairs as usize;
    let a_count = a.get_commits(sed_id).await.map_or(0, |c| c.len());
    let r_count = r.get_commits(sed_id).await.map_or(0, |c| c.len());
    let b_count = b.get_commits(sed_id).await.map_or(0, |c| c.len());
    assert_eq!((a_count, r_count, b_count), (total, total, total));

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
                .add_built_batch(sed_id, vec![pair], Vec::new(), CallTimeout::Default)
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

/// Storage policy that rejects fetch unless the requesting peer matches
/// `allowed_fetcher` (put is always allowed), to exercise the listen
/// loop's `authorize_fetch` gate on upstream propagation.
#[derive(Clone, Copy)]
struct RestrictiveFetchPolicy {
    allowed_fetcher: PeerId,
}

#[derive(Debug, Clone, Copy)]
struct FetchRejected;

impl core::fmt::Display for FetchRejected {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "fetch rejected by policy")
    }
}

impl core::error::Error for FetchRejected {}

impl ConnectionPolicy<Sendable> for RestrictiveFetchPolicy {
    type ConnectionDisallowed = Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async { Ok(()) }.boxed()
    }
}

impl StoragePolicy<Sendable> for RestrictiveFetchPolicy {
    type FetchDisallowed = FetchRejected;
    type PutDisallowed = Infallible;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        let allowed = self.allowed_fetcher;
        async move {
            if peer == allowed {
                Ok(())
            } else {
                Err(FetchRejected)
            }
        }
        .boxed()
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        _author: VerifiedAuthor,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        async { Ok(()) }.boxed()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        let allowed = self.allowed_fetcher;
        async move { if peer == allowed { ids } else { Vec::new() } }.boxed()
    }
}

// Wire-counting harness: R runs on a `ChannelMockConnection` toward "B"
// so tests count the upstream messages R actually emits, rather than
// inferring from recorded subscription state (which a re-propagating or
// never-propagating bug leaves unchanged). The responder replies so R's
// `sync_with_peer` completes and the claim/rollback path runs.

type MockConn = ChannelMockConnection<SyncMessage>;

#[allow(clippy::type_complexity)]
type OpenMockSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        MockConn,
        SyncHandler<Sendable, MemoryStorage, MockConn, OpenPolicy, CountLeadingZeroBytes>,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

#[allow(clippy::type_complexity)]
type RestrictiveMockSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        MockConn,
        SyncHandler<
            Sendable,
            MemoryStorage,
            MockConn,
            RestrictiveFetchPolicy,
            CountLeadingZeroBytes,
        >,
        RestrictiveFetchPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

fn make_open_mock_node(signer: MemorySigner) -> OpenMockSubduction {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, MockConn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

fn make_restrictive_mock_node(
    signer: MemorySigner,
    allowed_fetcher: PeerId,
) -> RestrictiveMockSubduction {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(
            MemoryStorage::new(),
            Arc::new(RestrictiveFetchPolicy { allowed_fetcher }),
        )
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, MockConn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

/// Register a [`ChannelMockConnection`] as `peer`, returning the
/// test-side handle for injecting inbound and observing outbound
/// messages. The closure adapts over each relay node's policy type.
async fn attach_mock_peer<S>(
    add_connection: impl FnOnce(
        Authenticated<MockConn, Sendable>,
    ) -> BoxFuture<'static, Result<bool, S>>,
    peer: PeerId,
) -> Result<ChannelMockConnectionHandle<SyncMessage>, S> {
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer);
    let auth: Authenticated<MockConn, Sendable> = Authenticated::new_for_test(conn, peer);
    add_connection(auth).await?;
    Ok(handle)
}

/// An empty [`SyncDiff`] for an `Ok` upstream response that carries no
/// data (the responder simply accepts the subscription).
fn empty_sync_diff() -> SyncDiff {
    SyncDiff {
        missing_commits: Vec::new(),
        missing_fragments: Vec::new(),
        requesting: RequestedData::default(),
    }
}

/// Build a subscribing `BatchSyncRequest` as it would arrive from a
/// downstream peer `from` for sedimentree `id`.
const fn subscribing_request(from: PeerId, id: SedimentreeId) -> SyncMessage {
    SyncMessage::BatchSyncRequest(BatchSyncRequest {
        id,
        req_id: RequestId {
            requestor: from,
            nonce: 1,
        },
        fingerprint_summary: FingerprintSummary::new(
            FingerprintSeed::new(0, 0),
            std::collections::BTreeSet::new(),
            std::collections::BTreeSet::new(),
        ),
        subscribe: true,
    })
}

/// Spawn a task that counts every subscribing `BatchSyncRequest` R sends
/// and replies with a `BatchSyncResponse` (carrying `result`) so R's
/// `sync_with_peer` completes. The count is shared via the returned `Arc`.
fn spawn_upstream_responder(
    handle: ChannelMockConnectionHandle<SyncMessage>,
    result_factory: impl Fn() -> SyncResult + Send + 'static,
) -> Arc<std::sync::atomic::AtomicUsize> {
    use std::sync::atomic::Ordering;

    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count_for_task = Arc::clone(&count);

    tokio::spawn(async move {
        while let Ok(msg) = handle.outbound_rx.recv().await {
            if let SyncMessage::BatchSyncRequest(req) = &msg {
                if req.subscribe {
                    count_for_task.fetch_add(1, Ordering::SeqCst);
                }

                // Reply so the upstream `sync_with_peer` resolves.
                let response = SyncMessage::BatchSyncResponse(BatchSyncResponse {
                    req_id: req.req_id,
                    id: req.id,
                    result: result_factory(),
                    responder_heads: RemoteHeads::default(),
                });

                if handle.inbound_tx.send(response).await.is_err() {
                    break;
                }
            }
        }
    });

    count
}

/// Two inbound subscribes from A for the same sedimentree must cause R
/// to send **exactly one** upstream `BatchSyncRequest { subscribe: true }`
/// to B (a re-propagating bug pushes the count to 2).
#[tokio::test]
async fn relay_topology_repeated_subscribe_sends_exactly_one_upstream_request() -> TestResult {
    let a_signer = make_signer(13);
    let r_signer = make_signer(23);
    let b_signer = make_signer(33);

    let a_peer = PeerId::from(a_signer.verifying_key());
    let b_peer = PeerId::from(b_signer.verifying_key());

    let r = make_open_mock_node(r_signer.clone());

    let r_for_a = Arc::clone(&r);
    let a_handle = attach_mock_peer(
        move |auth| Box::pin(async move { r_for_a.add_connection(auth).await }),
        a_peer,
    )
    .await?;
    let r_for_b = Arc::clone(&r);
    let b_handle = attach_mock_peer(
        move |auth| Box::pin(async move { r_for_b.add_connection(auth).await }),
        b_peer,
    )
    .await?;

    let sed_id = SedimentreeId::new([43u8; 32]);

    // B accepts the upstream subscribe, so the first propagation sticks.
    let upstream_count = spawn_upstream_responder(b_handle, || SyncResult::Ok(empty_sync_diff()));

    a_handle
        .inbound_tx
        .send(subscribing_request(a_peer, sed_id))
        .await?;
    let saw_first = wait_until(|| {
        let c = Arc::clone(&upstream_count);
        async move { c.load(std::sync::atomic::Ordering::SeqCst) >= 1 }
    })
    .await;
    assert!(
        saw_first,
        "R never sent the first upstream BatchSyncRequest to B"
    );

    a_handle
        .inbound_tx
        .send(subscribing_request(a_peer, sed_id))
        .await?;

    // Wait until R has processed the second subscribe (its recorded
    // subscription is present), then confirm no second send landed.
    let stable = wait_until(|| {
        let r = Arc::clone(&r);
        async move { r.get_peer_subscriptions(b_peer).await.contains(&sed_id) }
    })
    .await;
    assert!(stable, "R lost its recorded subscription to B");

    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let final_count = upstream_count.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        final_count, 1,
        "R must send exactly one upstream BatchSyncRequest across two \
         inbound subscribes; sent {final_count} (a value > 1 means the \
         idempotency guard re-propagated)"
    );

    Ok(())
}

/// An unauthorized subscriber U must cause **zero** upstream
/// `BatchSyncRequest`s to B. Counted on the wire, not via the recorded
/// subscription set, so a propagating-but-unrecorded leak is still
/// caught. (The counter reaching a non-zero value under an authorized
/// subscribe is covered by
/// `relay_topology_repeated_subscribe_sends_exactly_one_upstream_request`.)
#[tokio::test]
async fn relay_topology_unauthorized_subscribe_sends_zero_upstream_requests() -> TestResult {
    let u_signer = make_signer(14);
    let r_signer = make_signer(24);
    let b_signer = make_signer(34);
    let allowed_signer = make_signer(44);

    let u_peer = PeerId::from(u_signer.verifying_key());
    let b_peer = PeerId::from(b_signer.verifying_key());
    let allowed_peer = PeerId::from(allowed_signer.verifying_key());

    // U is the unauthorized subscriber; only `allowed_peer` (never
    // connected) may fetch, guaranteeing U's subscribe is rejected at
    // R's listen-loop gate.
    assert_ne!(u_peer, allowed_peer);
    assert_ne!(u_peer, b_peer);

    let r = make_restrictive_mock_node(r_signer.clone(), allowed_peer);

    let r_for_u = Arc::clone(&r);
    let u_handle = attach_mock_peer(
        move |auth| Box::pin(async move { r_for_u.add_connection(auth).await }),
        u_peer,
    )
    .await?;
    let r_for_b = Arc::clone(&r);
    let b_handle = attach_mock_peer(
        move |auth| Box::pin(async move { r_for_b.add_connection(auth).await }),
        b_peer,
    )
    .await?;

    let sed_id = SedimentreeId::new([88u8; 32]);

    let upstream_count = spawn_upstream_responder(b_handle, || SyncResult::Ok(empty_sync_diff()));

    u_handle
        .inbound_tx
        .send(subscribing_request(u_peer, sed_id))
        .await?;

    // Wait until R answers U, so a zero count means "suppressed", not
    // "not yet processed".
    let u_answered = wait_until(|| {
        let rx = u_handle.outbound_rx.clone();
        async move {
            while let Ok(msg) = rx.try_recv() {
                if matches!(
                    &msg,
                    SyncMessage::BatchSyncResponse(BatchSyncResponse { id, .. }) if *id == sed_id
                ) {
                    return true;
                }
            }
            false
        }
    })
    .await;
    assert!(
        u_answered,
        "R never answered U's unauthorized subscribe; a zero upstream \
         count cannot distinguish 'suppressed' from 'not yet processed'"
    );

    // Allow any erroneous upstream propagation to land before counting.
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let upstream = upstream_count.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        upstream, 0,
        "R propagated an unauthorized subscribe upstream to B \
         ({upstream} upstream BatchSyncRequest(s) sent; expected 0)"
    );

    Ok(())
}

/// Regression for the claim rollback when the *upstream* sync does not
/// actually establish a subscription.
///
/// `propagate_subscription` pre-claims `(peer, id)` in
/// `outgoing_subscriptions` before awaiting `sync_with_peer`. An upstream
/// `Unauthorized` (here) returns `Ok((false, ..))` with nothing tracked;
/// the original rollback fired only on `Err`, leaving a phantom claim
/// that blocks future re-propagation and is replayed on reconnect.
///
/// The test confirms R *attempted* the send (so the rollback path ran,
/// not skipped) before asserting the claim was rolled back.
#[tokio::test]
async fn relay_topology_unauthorized_upstream_response_rolls_back_claim() -> TestResult {
    let a_signer = make_signer(15);
    let r_signer = make_signer(25);
    let b_signer = make_signer(35);

    let a_peer = PeerId::from(a_signer.verifying_key());
    let b_peer = PeerId::from(b_signer.verifying_key());

    // R is open (so it authorizes A and starts propagation), but B
    // answers every upstream subscribe `Unauthorized`.
    let r = make_open_mock_node(r_signer.clone());

    let r_for_a = Arc::clone(&r);
    let a_handle = attach_mock_peer(
        move |auth| Box::pin(async move { r_for_a.add_connection(auth).await }),
        a_peer,
    )
    .await?;
    let r_for_b = Arc::clone(&r);
    let b_handle = attach_mock_peer(
        move |auth| Box::pin(async move { r_for_b.add_connection(auth).await }),
        b_peer,
    )
    .await?;

    let sed_id = SedimentreeId::new([77u8; 32]);

    let upstream_count = spawn_upstream_responder(b_handle, || SyncResult::Unauthorized);

    a_handle
        .inbound_tx
        .send(subscribing_request(a_peer, sed_id))
        .await?;

    // Precondition: R attempted the send, so the rollback path ran.
    let attempted = wait_until(|| {
        let c = Arc::clone(&upstream_count);
        async move { c.load(std::sync::atomic::Ordering::SeqCst) >= 1 }
    })
    .await;
    assert!(
        attempted,
        "R never sent an upstream BatchSyncRequest, so the rollback path \
         was never exercised — the absence check below would be vacuous"
    );

    // Poll until the claim is gone (it may briefly exist before rollback).
    let rolled_back = wait_until(|| {
        let r = Arc::clone(&r);
        async move { !r.get_peer_subscriptions(b_peer).await.contains(&sed_id) }
    })
    .await;
    assert!(
        rolled_back,
        "claim rollback failed: R kept an outgoing subscription to B \
         even though B answered Unauthorized (R→B subscriptions: {:?})",
        r.get_peer_subscriptions(b_peer).await
    );

    Ok(())
}
