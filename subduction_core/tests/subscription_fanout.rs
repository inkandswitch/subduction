//! The push invariant for locally authored data (see
//! `design/sync/subscriptions.md` § Push Invariant):
//!
//! > `add_commit` / `add_fragment` push for tree T to exactly
//! > `wants(T) ∩ may_fetch(T)`.
//!
//! An unsubscribed peer receives nothing; a subscribed-but-unauthorized peer
//! receives nothing. Each negative assertion has a positive control on the
//! same harness so a pass cannot be vacuous.

use core::{convert::Infallible, fmt};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};
use sedimentree_core::{
    blob::Blob, crypto::fingerprint::FingerprintSeed, depth::CountLeadingZeroBytes,
    id::SedimentreeId, loose_commit::id::CommitId, sedimentree::FingerprintSummary,
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::{
        message::{BatchSyncRequest, RequestId, SyncMessage},
        test_utils::{ChannelMockConnection, ChannelTransport, InstantTimeout, TokioSpawn},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, open::OpenPolicy, storage::StoragePolicy},
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::{signer::memory::MemorySigner, verified_author::VerifiedAuthor};
use testresult::TestResult;

type Conn = MessageTransport<ChannelTransport>;

type Node<P> = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<Sendable, MemoryStorage, Conn, P, CountLeadingZeroBytes, TokioSpawn>,
        P,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(500);

/// Time allowed for any (unwanted) push to land before asserting it did
/// not. Generous relative to in-process channel latency.
const PROPAGATION_PAUSE: Duration = Duration::from_millis(100);

const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

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

/// Allows connections and puts; allows fetches only for the listed peers.
/// Any peer may subscribe (the subscription is recorded), but only listed
/// peers may be pushed data.
#[derive(Clone)]
struct AllowFetchFor(BTreeSet<PeerId>);

#[derive(Debug, Clone, Copy)]
struct FetchRejected;

impl fmt::Display for FetchRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fetch rejected by policy")
    }
}

impl core::error::Error for FetchRejected {}

impl ConnectionPolicy<Sendable> for AllowFetchFor {
    type ConnectionDisallowed = Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async { Ok(()) }.boxed()
    }
}

impl StoragePolicy<Sendable> for AllowFetchFor {
    type FetchDisallowed = FetchRejected;
    type PutDisallowed = Infallible;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        let allowed = self.0.contains(&peer);
        async move { allowed.then_some(()).ok_or(FetchRejected) }.boxed()
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
        let allowed = self.0.contains(&peer);
        async move { if allowed { ids } else { Vec::new() } }.boxed()
    }
}

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

/// Policy bounds `Subduction` needs in a `Sendable` (tokio) test harness.
trait TestPolicy:
    ConnectionPolicy<Sendable>
    + StoragePolicy<Sendable, FetchDisallowed: Send, PutDisallowed: Send>
    + Send
    + Sync
    + 'static
{
}

impl<P> TestPolicy for P where
    P: ConnectionPolicy<Sendable>
        + StoragePolicy<Sendable, FetchDisallowed: Send, PutDisallowed: Send>
        + Send
        + Sync
        + 'static
{
}

fn make_node<P: TestPolicy>(signer: MemorySigner, policy: P) -> Node<P> {
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(policy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

fn make_blob(seed: u8) -> Blob {
    Blob::new((0..64).map(|i| seed.wrapping_add(i)).collect())
}

const fn make_head(seed: u8) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    bytes[1] = seed.wrapping_mul(31);
    CommitId::new(bytes)
}

/// Connect two nodes (of possibly different policy types) over an
/// in-process channel pair. `a` dials `b`.
async fn connect<PA: TestPolicy, PB: TestPolicy>(
    a: &Node<PA>,
    a_signer: &MemorySigner,
    b: &Node<PB>,
    b_signer: &MemorySigner,
) -> TestResult {
    let (t_a, t_b) = ChannelTransport::pair();
    let peer_a = PeerId::from(a_signer.verifying_key());
    let peer_b = PeerId::from(b_signer.verifying_key());
    let auth_a: Authenticated<Conn, Sendable> =
        Authenticated::new_for_test(MessageTransport::new(t_a), peer_b, Direction::Dialed);
    let auth_b: Authenticated<Conn, Sendable> =
        Authenticated::new_for_test(MessageTransport::new(t_b), peer_a, Direction::Accepted);
    a.add_connection(auth_a).await?;
    b.add_connection(auth_b).await?;
    Ok(())
}

async fn commit_count<P: TestPolicy>(node: &Node<P>, id: SedimentreeId) -> usize {
    node.get_commits(id).await.map_or(0, |c| c.len())
}

async fn fragment_count<P: TestPolicy>(node: &Node<P>, id: SedimentreeId) -> usize {
    node.get_fragments(id).await.map_or(0, |f| f.len())
}

/// Two connected peers, no subscriptions. A locally authored commit stays
/// on A. Positive control: once B subscribes, the data flows.
#[tokio::test]
async fn add_commit_without_subscribers_pushes_nothing() -> TestResult {
    let (a_s, b_s) = (make_signer(1), make_signer(2));
    let a = make_node(a_s.clone(), OpenPolicy);
    let b = make_node(b_s.clone(), OpenPolicy);
    connect(&b, &b_s, &a, &a_s).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let id = SedimentreeId::new([1u8; 32]);
    a.add_commit(id, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;

    tokio::time::sleep(PROPAGATION_PAUSE).await;
    assert_eq!(commit_count(&a, id).await, 1);
    assert_eq!(
        commit_count(&b, id).await,
        0,
        "B never subscribed to {id:?}; add_commit must not push to it"
    );

    // Positive control.
    b.sync_with_peer(&PeerId::from(a_s.verifying_key()), id, true, SYNC_TIMEOUT)
        .await?;
    assert!(
        wait_until(|| async { commit_count(&b, id).await == 1 }).await,
        "after subscribing, B must receive the commit"
    );
    Ok(())
}

/// Same contract for fragments.
#[tokio::test]
async fn add_fragment_without_subscribers_pushes_nothing() -> TestResult {
    let (a_s, b_s) = (make_signer(3), make_signer(4));
    let a = make_node(a_s.clone(), OpenPolicy);
    let b = make_node(b_s.clone(), OpenPolicy);
    connect(&b, &b_s, &a, &a_s).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let id = SedimentreeId::new([2u8; 32]);
    let boundary = BTreeSet::from([make_head(200)]);
    a.add_fragment(id, make_head(1), boundary, &[], make_blob(1))
        .await?;

    tokio::time::sleep(PROPAGATION_PAUSE).await;
    assert_eq!(fragment_count(&a, id).await, 1);
    assert_eq!(
        fragment_count(&b, id).await,
        0,
        "B never subscribed to {id:?}; add_fragment must not push to it"
    );

    // Positive control.
    b.sync_with_peer(&PeerId::from(a_s.verifying_key()), id, true, SYNC_TIMEOUT)
        .await?;
    assert!(
        wait_until(|| async { fragment_count(&b, id).await == 1 }).await,
        "after subscribing, B must receive the fragment"
    );
    Ok(())
}

/// Two subscribers, one authorized and one not: the push goes to exactly the
/// intersection. Policy is evaluated on the pushing node (A), the only node
/// whose `filter_authorized_fetch` matters for A's pushes.
#[tokio::test]
async fn add_commit_pushes_only_to_authorized_subscribers() -> TestResult {
    let (a_s, b_s, c_s) = (make_signer(5), make_signer(6), make_signer(7));
    let b_peer = PeerId::from(b_s.verifying_key());
    let c_peer = PeerId::from(c_s.verifying_key());

    let a = make_node(a_s.clone(), AllowFetchFor(BTreeSet::from([b_peer])));
    let b = make_node(b_s.clone(), OpenPolicy);
    let c = make_node(c_s.clone(), OpenPolicy);
    connect(&b, &b_s, &a, &a_s).await?;
    connect(&c, &c_s, &a, &a_s).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let id = SedimentreeId::new([3u8; 32]);
    let a_peer = PeerId::from(a_s.verifying_key());

    // Both subscribe. A records both subscriptions (that happens before the
    // fetch-policy check); C's request is answered `Unauthorized`, which
    // `sync_with_peer` reports as `Ok((false, ..))`, not an error.
    b.sync_with_peer(&a_peer, id, true, SYNC_TIMEOUT).await?;
    c.sync_with_peer(&a_peer, id, true, SYNC_TIMEOUT).await?;
    assert!(
        wait_until(|| async {
            let subs = a.get_subscribers(id).await;
            subs.contains(&b_peer) && subs.contains(&c_peer)
        })
        .await,
        "A should have recorded both subscriptions"
    );

    a.add_commit(id, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;

    assert!(
        wait_until(|| async { commit_count(&b, id).await == 1 }).await,
        "authorized subscriber B must receive the push"
    );
    tokio::time::sleep(PROPAGATION_PAUSE).await;
    assert_eq!(
        commit_count(&c, id).await,
        0,
        "C is subscribed but not authorized; it must not be pushed"
    );
    Ok(())
}

/// Wire-level count: once a peer is subscribed, each `add_commit` produces
/// exactly one outbound `LooseCommit` frame to it, with no sync round.
///
/// Uses a mock connection whose outbound frames are observable, since a
/// receiving node deduplicates by commit identity and its stored count
/// cannot distinguish one delivery from several.
#[tokio::test]
async fn add_commit_sends_one_frame_per_commit_to_a_subscriber() -> TestResult {
    let (a, _handler, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
        .signer(make_signer(8))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();
    tokio::spawn(listener);
    tokio::spawn(manager);

    let id = SedimentreeId::new([4u8; 32]);
    let b_peer = PeerId::new([9u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(b_peer);
    a.add_connection(conn.authenticated()).await?;

    // B subscribes (empty fingerprints: B has nothing).
    handle
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(BatchSyncRequest {
            id,
            req_id: RequestId {
                requestor: b_peer,
                nonce: 1,
            },
            fingerprint_summary: FingerprintSummary::new(
                FingerprintSeed::new(0, 0),
                BTreeSet::new(),
                BTreeSet::new(),
            ),
            subscribe: true,
        }))
        .await?;
    assert!(
        wait_until(|| async { a.get_subscribers(id).await.contains(&b_peer) }).await,
        "A should have recorded B's subscription"
    );
    // Discard the BatchSyncResponse so only pushes remain on the wire.
    tokio::time::sleep(PROPAGATION_PAUSE).await;
    while handle.outbound_rx.try_recv().is_ok() {}

    for n in 1..=3u8 {
        a.add_commit(id, make_head(n), BTreeSet::new(), make_blob(n))
            .await?;
    }
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let mut frames = Vec::new();
    while let Ok(msg) = handle.outbound_rx.try_recv() {
        frames.push(msg);
    }
    let pushed: Vec<_> = frames
        .iter()
        .filter(|m| matches!(m, SyncMessage::LooseCommit { .. }))
        .collect();
    assert_eq!(
        pushed.len(),
        3,
        "expected exactly one LooseCommit frame per add_commit; wire had {frames:?}"
    );
    assert!(
        frames
            .iter()
            .all(|m| matches!(m, SyncMessage::LooseCommit { .. })),
        "add_commit must push, not open a sync round; wire had {frames:?}"
    );
    Ok(())
}

/// The same intersection governs data a node pulls *as a requester*. Relay
/// R (policy: only A may fetch) has subscribers A and C; R pulls X from its
/// dialed upstream B and forwards it to A only.
///
/// ```text
///   A ──dials──▸ R ──dials──▸ B (holds X)
///   C ──dials──▸ R
/// ```
#[tokio::test]
async fn requester_pulled_data_is_pushed_only_to_authorized_subscribers() -> TestResult {
    let (a_s, c_s, r_s, b_s) = (
        make_signer(10),
        make_signer(11),
        make_signer(12),
        make_signer(13),
    );
    let a_peer = PeerId::from(a_s.verifying_key());
    let r_peer = PeerId::from(r_s.verifying_key());

    let a = make_node(a_s.clone(), OpenPolicy);
    let c = make_node(c_s.clone(), OpenPolicy);
    let r = make_node(r_s.clone(), AllowFetchFor(BTreeSet::from([a_peer])));
    let b = make_node(b_s.clone(), OpenPolicy);
    connect(&a, &a_s, &r, &r_s).await?;
    connect(&c, &c_s, &r, &r_s).await?;
    connect(&r, &r_s, &b, &b_s).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let id = SedimentreeId::new([5u8; 32]);
    b.store_commit(id, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;

    // Both subscribe on R. R has nothing yet, propagates upstream to B, and
    // pulls X in the response.
    a.sync_with_peer(&r_peer, id, true, SYNC_TIMEOUT).await?;
    drop(c.sync_with_peer(&r_peer, id, true, SYNC_TIMEOUT).await);

    assert!(
        wait_until(|| async { commit_count(&r, id).await == 1 }).await,
        "R should pull X from B"
    );
    assert!(
        wait_until(|| async { commit_count(&a, id).await == 1 }).await,
        "authorized subscriber A must receive what R pulled"
    );
    tokio::time::sleep(PROPAGATION_PAUSE).await;
    assert_eq!(
        commit_count(&c, id).await,
        0,
        "C is subscribed but not authorized on R; it must not receive X"
    );
    Ok(())
}
