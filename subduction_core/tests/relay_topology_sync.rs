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
    commit::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};

use futures::{FutureExt, future::BoxFuture};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, open::OpenPolicy, storage::StoragePolicy},
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
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

    // A subscribes to R for `sed_id`. R has no data yet — this is
    // purely a subscription registration.
    let r_peer = PeerId::from(r_signer.verifying_key());
    a.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // The propagation runs after the handler returns, so give the
    // listen loop a chance to dispatch it.
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // R should now be subscribed to B for `sed_id` as a result of the
    // propagation.
    let b_peer = PeerId::from(b_signer.verifying_key());
    let r_subscribed_to_b = r.get_peer_subscriptions(b_peer).await;
    assert!(
        r_subscribed_to_b.contains(&sed_id),
        "R failed to propagate A's subscription upstream to B: {r_subscribed_to_b:?}"
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

/// Idempotency: a second subscribe from A for the same sedimentree
/// must NOT cause R to re-issue its upstream subscribe to B (which
/// would be wasted wire traffic). We rely on
/// `outgoing_subscriptions` having recorded the first propagation.
#[tokio::test]
async fn relay_topology_repeated_subscribe_does_not_re_propagate() -> TestResult {
    let (a, r, _b, _a_s, r_signer, b_signer) = setup_relay_topology().await?;
    let sed_id = SedimentreeId::new([43u8; 32]);
    let r_peer = PeerId::from(r_signer.verifying_key());
    let b_peer = PeerId::from(b_signer.verifying_key());

    a.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;
    assert!(r.get_peer_subscriptions(b_peer).await.contains(&sed_id));

    // Second subscribe from A. Propagation should short-circuit because
    // R already has `(B, sed_id)` in `outgoing_subscriptions`. We can't
    // easily count wire messages here, but verifying the subscription
    // set hasn't grown (still exactly the same sedimentree) is a
    // proxy.
    a.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let r_subs = r.get_peer_subscriptions(b_peer).await;
    assert_eq!(r_subs.len(), 1);
    assert!(r_subs.contains(&sed_id));

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

    let total_pairs: u8 = 12;
    for i in 0..total_pairs {
        if i % 2 == 0 {
            let pair = make_commit_pair(sed_id, i + 1);
            a.add_built_batch(sed_id, vec![pair], Vec::new()).await?;
        } else {
            let pair = make_commit_pair(sed_id, 100 + i);
            b.add_built_batch(sed_id, vec![pair], Vec::new()).await?;
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

// ─── Policy gate on upstream propagation ────────────────────────────────

/// Storage policy that rejects fetch unless the requesting peer matches
/// `allowed_fetcher`. Put is always allowed. Used to exercise the
/// `authorize_fetch` gate the listen loop applies before propagating
/// inbound subscribe requests upstream.
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

#[allow(clippy::type_complexity)]
type RestrictiveSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<Sendable, MemoryStorage, Conn, RestrictiveFetchPolicy, CountLeadingZeroBytes>,
        RestrictiveFetchPolicy,
        MemorySigner,
        InstantTimeout,
    >,
>;

fn make_restrictive_node(signer: MemorySigner, allowed_fetcher: PeerId) -> RestrictiveSubduction {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(
            MemoryStorage::new(),
            Arc::new(RestrictiveFetchPolicy { allowed_fetcher }),
        )
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

async fn connect_restrictive_pair_a_r(
    a: &TestSubduction,
    a_signer: &MemorySigner,
    r: &RestrictiveSubduction,
    r_signer: &MemorySigner,
) -> TestResult {
    let (transport_a, transport_r) = ChannelTransport::pair();

    let conn_a = MessageTransport::new(transport_a);
    let conn_r = MessageTransport::new(transport_r);

    let peer_a = PeerId::from(a_signer.verifying_key());
    let peer_r = PeerId::from(r_signer.verifying_key());

    let auth_a: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_a, peer_r);
    let auth_r: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_r, peer_a);

    a.add_connection(auth_a).await?;
    r.add_connection(auth_r).await?;

    Ok(())
}

async fn connect_restrictive_pair_r_b(
    r: &RestrictiveSubduction,
    r_signer: &MemorySigner,
    b: &TestSubduction,
    b_signer: &MemorySigner,
) -> TestResult {
    let (transport_r, transport_b) = ChannelTransport::pair();

    let conn_r = MessageTransport::new(transport_r);
    let conn_b = MessageTransport::new(transport_b);

    let peer_r = PeerId::from(r_signer.verifying_key());
    let peer_b = PeerId::from(b_signer.verifying_key());

    let auth_r: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_r, peer_b);
    let auth_b: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_b, peer_r);

    r.add_connection(auth_r).await?;
    b.add_connection(auth_b).await?;

    Ok(())
}

/// Regression for the policy gate on upstream-subscription propagation.
///
/// When A subscribes to R for a sedimentree that R's policy rejects A
/// from fetching, R must NOT propagate the subscription upstream to B.
/// Without the gate, an unauthorized peer could cause R to enrol in
/// upstream subscriptions whose traffic the egress filter would only
/// drop on the return path — wasted bandwidth and dangling upstream
/// subscription state.
///
/// Pairs with `relay_topology_propagates_subscriptions_upstream` (which
/// proves propagation does happen under `OpenPolicy`).
#[tokio::test]
async fn relay_topology_unauthorized_subscribe_does_not_propagate() -> TestResult {
    let a_signer = make_signer(11);
    let r_signer = make_signer(21);
    let b_signer = make_signer(31);

    let a_peer = PeerId::from(a_signer.verifying_key());
    let r_peer = PeerId::from(r_signer.verifying_key());
    let b_peer = PeerId::from(b_signer.verifying_key());

    // Sanity: A is not the allowed fetcher, so this test is exercising
    // the rejection path. (If A == b_peer accidentally, the propagation
    // assertion below would still pass for the wrong reason.)
    assert_ne!(a_peer, b_peer);

    // R only allows B to fetch. A is unauthorized.
    let a = make_node(a_signer.clone());
    let r = make_restrictive_node(r_signer.clone(), b_peer);
    let b = make_node(b_signer.clone());

    connect_restrictive_pair_a_r(&a, &a_signer, &r, &r_signer).await?;
    connect_restrictive_pair_r_b(&r, &r_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let sed_id = SedimentreeId::new([99u8; 32]);

    // A's subscribe reaches R. `recv_batch_sync_request` will get a
    // policy rejection from `get_fetcher` and send back
    // `SyncResult::Unauthorized`. The handler returns `Ok` and
    // `propagate_subscription` would run if the policy gate weren't
    // there. We deliberately don't propagate `?` because the request
    // surfaces an `Unauthorized` response, not a transport-level error.
    let _outcome = a.sync_with_peer(&r_peer, sed_id, true, SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    // The gate must have suppressed propagation: R has no outgoing
    // subscription to B for `sed_id`.
    let r_to_b_subs = r.get_peer_subscriptions(b_peer).await;
    assert!(
        !r_to_b_subs.contains(&sed_id),
        "policy gate failed: R propagated A's unauthorized subscribe to B \
         (R→B subscriptions: {r_to_b_subs:?})"
    );

    Ok(())
}
