//! Heads reach the observer only for watched sedimentrees.
//!
//! A watch is established by `WatchHeads`, answered with a snapshot, and
//! followed by a `HeadsUpdate` on every change. Syncing or being pushed to
//! does not imply a watch, so an application never learns the existence of
//! trees it did not ask about.

#![allow(clippy::expect_used, clippy::panic)]

use core::{convert::Infallible, time::Duration};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, open::OpenPolicy, storage::StoragePolicy},
    remote_heads::{RemoteHeads, RemoteHeadsObserver},
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
        SyncHandler<
            Sendable,
            MemoryStorage,
            Conn,
            P,
            CountLeadingZeroBytes,
            TokioSpawn,
            256,
            RecordingObserver,
        >,
        P,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

/// Refuses every fetch.
#[derive(Debug, Clone, Copy)]
struct DenyFetch;

#[derive(Debug, thiserror::Error)]
#[error("fetches are refused")]
struct FetchRefused;

impl ConnectionPolicy<Sendable> for DenyFetch {
    type ConnectionDisallowed = Infallible;

    fn authorize_connect(&self, _peer: PeerId) -> BoxFuture<'_, Result<(), Infallible>> {
        Box::pin(async { Ok(()) })
    }
}

impl StoragePolicy<Sendable> for DenyFetch {
    type FetchDisallowed = FetchRefused;
    type PutDisallowed = Infallible;

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), FetchRefused>> {
        Box::pin(async { Err(FetchRefused) })
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        _author: VerifiedAuthor,
        _id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Infallible>> {
        Box::pin(async { Ok(()) })
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        _ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        Box::pin(async { Vec::new() })
    }
}

#[derive(Clone, Debug, Default)]
struct RecordingObserver(Arc<Mutex<Vec<(SedimentreeId, PeerId, RemoteHeads)>>>);

impl RecordingObserver {
    fn deliveries(&self) -> Vec<(SedimentreeId, PeerId, RemoteHeads)> {
        self.0.lock().expect("poisoned").clone()
    }

    fn count(&self) -> usize {
        self.0.lock().expect("poisoned").len()
    }

    fn heads_for(&self, id: SedimentreeId) -> Vec<Vec<CommitId>> {
        self.deliveries()
            .into_iter()
            .filter(|(tree, _, _)| *tree == id)
            .map(|(_, _, h)| h.heads)
            .collect()
    }
}

impl RemoteHeadsObserver for RecordingObserver {
    fn on_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
        self.0.lock().expect("poisoned").push((id, peer, heads));
    }
}

fn signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn peer_id(seed: u8) -> PeerId {
    PeerId::from(signer(seed).verifying_key())
}

fn node<P>(seed: u8, policy: P) -> (Node<P>, RecordingObserver)
where
    P: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Clone + Send + Sync + 'static,
    <P as StoragePolicy<Sendable>>::PutDisallowed: Send + Sync + 'static,
    <P as StoragePolicy<Sendable>>::FetchDisallowed: Send + Sync + 'static,
    <P as ConnectionPolicy<Sendable>>::ConnectionDisallowed: Send + Sync + 'static,
{
    let observer = RecordingObserver::default();
    let (sd, _handler, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
        .signer(signer(seed))
        .storage(MemoryStorage::new(), Arc::new(policy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .heads_observer(observer.clone())
        .build::<Sendable, Conn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    (sd, observer)
}

async fn connect_nodes<PA, PB>(a: &Node<PA>, a_seed: u8, b: &Node<PB>, b_seed: u8) -> TestResult
where
    PA: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    <PA as StoragePolicy<Sendable>>::PutDisallowed: Send + Sync + 'static,
    <PA as StoragePolicy<Sendable>>::FetchDisallowed: Send + Sync + 'static,
    <PA as ConnectionPolicy<Sendable>>::ConnectionDisallowed: Send + Sync + 'static,
    PB: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    <PB as StoragePolicy<Sendable>>::PutDisallowed: Send + Sync + 'static,
    <PB as StoragePolicy<Sendable>>::FetchDisallowed: Send + Sync + 'static,
    <PB as ConnectionPolicy<Sendable>>::ConnectionDisallowed: Send + Sync + 'static,
{
    let (ta, tb) = ChannelTransport::pair();
    a.add_connection(Authenticated::new_for_test(
        MessageTransport::new(ta),
        peer_id(b_seed),
        Direction::Dialed,
    ))
    .await?;
    b.add_connection(Authenticated::new_for_test(
        MessageTransport::new(tb),
        peer_id(a_seed),
        Direction::Accepted,
    ))
    .await?;
    Ok(())
}

async fn wait_until(mut predicate: impl FnMut() -> bool, failure: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(tokio::time::Instant::now() < deadline, "{failure}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Long enough for any in-flight message to have been dispatched.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

const fn commit(n: u8) -> CommitId {
    CommitId::new([n; 32])
}

async fn add_commit<P>(node: &Node<P>, doc: SedimentreeId, n: u8) -> TestResult
where
    P: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    <P as StoragePolicy<Sendable>>::PutDisallowed: Send + Sync + 'static,
    <P as StoragePolicy<Sendable>>::FetchDisallowed: Send + Sync + 'static,
    <P as ConnectionPolicy<Sendable>>::ConnectionDisallowed: Send + Sync + 'static,
{
    node.add_commit(doc, commit(n), BTreeSet::new(), Blob::new(vec![n; 8]))
        .await?;
    Ok(())
}

const DOC: SedimentreeId = SedimentreeId::new([0xD0; 32]);
const OTHER: SedimentreeId = SedimentreeId::new([0x0E; 32]);

/// Neither syncing a tree, being pushed one, nor receiving a push ack
/// reports heads for an unwatched tree.
#[tokio::test]
async fn unwatched_trees_never_reach_the_observer() -> TestResult {
    let (a, a_obs) = node(1, OpenPolicy);
    let (b, b_obs) = node(2, OpenPolicy);
    connect_nodes(&a, 1, &b, 2).await?;

    add_commit(&b, DOC, 1).await?;

    // A syncs and subscribes: responder_heads arrive, and B is now a
    // subscriber of A.
    a.sync_with_peer(&peer_id(2), DOC, true, CallTimeout::TimeoutMillis(500))
        .await?;
    // B pushes a change: sender_heads arrive at A.
    add_commit(&b, DOC, 2).await?;
    // A pushes a change: B acks with HeadsUpdate.
    add_commit(&a, DOC, 3).await?;
    settle().await;

    assert!(
        a_obs.deliveries().is_empty(),
        "A was told about an unwatched tree: {:?}",
        a_obs.deliveries()
    );
    assert!(
        b_obs.deliveries().is_empty(),
        "B was told about an unwatched tree: {:?}",
        b_obs.deliveries()
    );
    Ok(())
}

/// A watch is answered with the peer's current heads.
#[tokio::test]
async fn watch_delivers_a_snapshot() -> TestResult {
    let (a, a_obs) = node(3, OpenPolicy);
    let (b, _) = node(4, OpenPolicy);
    connect_nodes(&a, 3, &b, 4).await?;

    add_commit(&b, DOC, 1).await?;
    a.watch_heads(DOC).await;

    wait_until(|| a_obs.count() >= 1, "snapshot never arrived").await;
    assert_eq!(
        a_obs.deliveries().first().map(|(_, peer, _)| *peer),
        Some(peer_id(4))
    );
    assert_eq!(a_obs.heads_for(DOC), vec![vec![commit(1)]]);
    Ok(())
}

/// A watch on a tree the peer does not hold yet is answered with empty
/// heads, then followed by the first commit.
#[tokio::test]
async fn watch_on_absent_tree_reports_empty_then_first_commit() -> TestResult {
    let (a, a_obs) = node(5, OpenPolicy);
    let (b, _) = node(6, OpenPolicy);
    connect_nodes(&a, 5, &b, 6).await?;

    a.watch_heads(DOC).await;
    wait_until(|| a_obs.count() >= 1, "snapshot never arrived").await;
    assert_eq!(a_obs.heads_for(DOC), vec![Vec::<CommitId>::new()]);

    add_commit(&b, DOC, 1).await?;
    wait_until(|| a_obs.count() >= 2, "change never arrived").await;
    assert_eq!(a_obs.heads_for(DOC), vec![vec![], vec![commit(1)]]);
    Ok(())
}

/// Every change to a watched tree is reported exactly once, whether the
/// heads ride a subscription push or a standalone `HeadsUpdate`.
#[tokio::test]
async fn changes_are_reported_once_with_or_without_subscription() -> TestResult {
    for subscribe in [false, true] {
        let (a, a_obs) = node(7, OpenPolicy);
        let (b, _) = node(8, OpenPolicy);
        connect_nodes(&a, 7, &b, 8).await?;

        add_commit(&b, DOC, 1).await?;
        if subscribe {
            a.sync_with_peer(&peer_id(8), DOC, true, CallTimeout::TimeoutMillis(500))
                .await?;
        }
        a.watch_heads(DOC).await;
        wait_until(|| a_obs.count() >= 1, "snapshot never arrived").await;

        for n in 2..=4u8 {
            add_commit(&b, DOC, n).await?;
            wait_until(
                || {
                    a_obs
                        .heads_for(DOC)
                        .last()
                        .is_some_and(|h| h.contains(&commit(n)))
                },
                "change never arrived",
            )
            .await;
        }
        settle().await;

        assert_eq!(
            a_obs.count(),
            4,
            "subscribe={subscribe}: one snapshot plus three changes, got {:?}",
            a_obs.deliveries()
        );
    }
    Ok(())
}

#[tokio::test]
async fn unwatch_stops_delivery() -> TestResult {
    let (a, a_obs) = node(9, OpenPolicy);
    let (b, _) = node(10, OpenPolicy);
    connect_nodes(&a, 9, &b, 10).await?;

    a.watch_heads(DOC).await;
    wait_until(|| a_obs.count() >= 1, "snapshot never arrived").await;

    a.unwatch_heads(DOC).await;
    settle().await;
    add_commit(&b, DOC, 1).await?;
    settle().await;

    assert_eq!(
        a_obs.count(),
        1,
        "heads arrived after unwatch: {:?}",
        a_obs.deliveries()
    );
    Ok(())
}

/// Watching one tree does not open a path for another.
#[tokio::test]
async fn watch_is_per_tree() -> TestResult {
    let (a, a_obs) = node(11, OpenPolicy);
    let (b, _) = node(12, OpenPolicy);
    connect_nodes(&a, 11, &b, 12).await?;

    a.watch_heads(DOC).await;
    wait_until(|| a_obs.count() >= 1, "snapshot never arrived").await;

    add_commit(&b, OTHER, 1).await?;
    a.sync_with_peer(&peer_id(12), OTHER, true, CallTimeout::TimeoutMillis(500))
        .await?;
    add_commit(&b, OTHER, 2).await?;
    settle().await;

    assert!(
        a_obs.heads_for(OTHER).is_empty(),
        "unwatched tree leaked: {:?}",
        a_obs.deliveries()
    );
    Ok(())
}

/// A peer that refuses the fetch records no watcher and sends nothing.
#[tokio::test]
async fn unauthorized_watch_is_refused() -> TestResult {
    let (a, a_obs) = node(13, OpenPolicy);
    let (b, _) = node(14, DenyFetch);
    connect_nodes(&a, 13, &b, 14).await?;

    a.watch_heads(DOC).await;
    settle().await;
    add_commit(&b, DOC, 1).await?;
    settle().await;

    assert!(a_obs.deliveries().is_empty(), "{:?}", a_obs.deliveries());
    Ok(())
}

/// Watches survive the peer's session: reconnecting re-sends them.
#[tokio::test]
async fn watch_is_replayed_on_reconnect() -> TestResult {
    let (a, a_obs) = node(15, OpenPolicy);
    let (b, _) = node(16, OpenPolicy);
    connect_nodes(&a, 15, &b, 16).await?;

    add_commit(&b, DOC, 1).await?;
    a.watch_heads(DOC).await;
    wait_until(|| a_obs.count() >= 1, "snapshot never arrived").await;

    a.disconnect_from_peer(&peer_id(16)).await?;
    b.disconnect_from_peer(&peer_id(15)).await?;
    connect_nodes(&a, 15, &b, 16).await?;

    // The new session reports the (unchanged) snapshot once more, and then
    // keeps reporting changes.
    wait_until(|| a_obs.count() >= 2, "snapshot not replayed").await;
    add_commit(&b, DOC, 2).await?;
    wait_until(
        || a_obs.count() >= 3,
        "change after reconnect never arrived",
    )
    .await;
    Ok(())
}
