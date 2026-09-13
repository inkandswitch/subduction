//! The push invariant for locally authored data:
//!
//! > `add_commit` / `add_fragment` push to peer P for tree T **iff** P is in
//! > `subscriptions[T]` **and** `filter_authorized_fetch(P, [T])` keeps T.
//!
//! In particular there is no "nobody subscribed, so tell everyone" fallback:
//! an unsubscribed peer receives nothing, and a subscribed-but-unauthorized
//! peer receives nothing. Each negative case is paired with a positive
//! control on the same harness so a pass cannot be vacuous.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use core::{convert::Infallible, fmt};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
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

/// Allows connections and puts; denies every fetch. A peer may subscribe
/// (subscription is recorded) but must never be pushed data.
#[derive(Clone, Copy)]
struct RejectFetchPolicy;

#[derive(Debug, Clone, Copy)]
struct FetchRejected;

impl fmt::Display for FetchRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fetch rejected by policy")
    }
}

impl core::error::Error for FetchRejected {}

impl ConnectionPolicy<Sendable> for RejectFetchPolicy {
    type ConnectionDisallowed = Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async { Ok(()) }.boxed()
    }
}

impl StoragePolicy<Sendable> for RejectFetchPolicy {
    type FetchDisallowed = FetchRejected;
    type PutDisallowed = Infallible;

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        async { Err(FetchRejected) }.boxed()
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
        _peer: PeerId,
        _ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async { Vec::new() }.boxed()
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
/// in-process channel pair.
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
        Authenticated::new_for_test(MessageTransport::new(t_a), peer_b);
    let auth_b: Authenticated<Conn, Sendable> =
        Authenticated::new_for_test(MessageTransport::new(t_b), peer_a);
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
    connect(&a, &a_s, &b, &b_s).await?;
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
    connect(&a, &a_s, &b, &b_s).await?;
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

/// A peer that *is* subscribed but fails `filter_authorized_fetch` must not
/// be pushed data. Before the fix, an all-unauthorized subscriber set looked
/// like "no subscribers" and triggered a policy-free broadcast.
#[tokio::test]
async fn add_commit_with_only_unauthorized_subscribers_pushes_nothing() -> TestResult {
    let (a_s, b_s) = (make_signer(5), make_signer(6));
    // A enforces the policy (it decides who may fetch from it).
    let a = make_node(a_s.clone(), RejectFetchPolicy);
    let b = make_node(b_s.clone(), OpenPolicy);
    connect(&a, &a_s, &b, &b_s).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let id = SedimentreeId::new([3u8; 32]);
    let a_peer = PeerId::from(a_s.verifying_key());
    let b_peer = PeerId::from(b_s.verifying_key());

    // B subscribes. A records the subscription (that happens before the
    // fetch policy check) but answers `Unauthorized`, so the sync result is
    // irrelevant here.
    drop(b.sync_with_peer(&a_peer, id, true, SYNC_TIMEOUT).await);
    assert!(
        wait_until(|| async { a.get_subscribers(id).await.contains(&b_peer) }).await,
        "A should have recorded B's subscription"
    );

    a.add_commit(id, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;

    tokio::time::sleep(PROPAGATION_PAUSE).await;
    assert_eq!(commit_count(&a, id).await, 1);
    assert_eq!(
        commit_count(&b, id).await,
        0,
        "B is subscribed but unauthorized; add_commit must not push to it"
    );
    Ok(())
}

/// Once B is subscribed, each `add_commit` on A is pushed to B exactly once
/// (no duplicate deliveries, no reliance on a later sync round).
#[tokio::test]
async fn add_commit_pushes_to_each_subscriber_once() -> TestResult {
    let (a_s, b_s) = (make_signer(7), make_signer(8));
    let a = make_node(a_s.clone(), OpenPolicy);
    let b = make_node(b_s.clone(), OpenPolicy);
    connect(&a, &a_s, &b, &b_s).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let id = SedimentreeId::new([4u8; 32]);
    let a_peer = PeerId::from(a_s.verifying_key());
    let b_peer = PeerId::from(b_s.verifying_key());

    // B subscribes to an (as yet empty) tree on A.
    b.sync_with_peer(&a_peer, id, true, SYNC_TIMEOUT).await?;
    assert!(
        wait_until(|| async { a.get_subscribers(id).await.contains(&b_peer) }).await,
        "A should have recorded B's subscription"
    );

    for n in 1..=3u8 {
        a.add_commit(id, make_head(n), BTreeSet::new(), make_blob(n))
            .await?;
        assert!(
            wait_until(|| async { commit_count(&b, id).await == usize::from(n) }).await,
            "B should have {n} commit(s) after the {n}th push, has {}",
            commit_count(&b, id).await
        );
    }

    // Settle, then confirm nothing was delivered twice.
    tokio::time::sleep(PROPAGATION_PAUSE).await;
    assert_eq!(commit_count(&b, id).await, 3);
    Ok(())
}
