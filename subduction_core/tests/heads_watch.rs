//! Heads reach the observer only for watched sedimentrees; see
//! `design/sync/subscriptions.md` § Heads Watches.

#![allow(clippy::expect_used, clippy::panic, clippy::indexing_slicing)]

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

/// Settling time for negative assertions on in-process channels.
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

/// Covers the three unwatched paths: `responder_heads`, `sender_heads`, and
/// the push ack.
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

/// After `unwatch_heads`, the requester drops any report for the tree.
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

/// Every write API reports to watchers. Each row is one `HeadsChanged`
/// producer; a new write path that forgets `propagate` fails to compile, and a
/// new API that bypasses `HeadsChanged` shows up here as a missing row.
mod every_write_path_notifies_watchers {
    use sedimentree_core::{
        blob::BlobMeta, fragment::Fragment, loose_commit::LooseCommit, sedimentree::Sedimentree,
    };
    use subduction_core::subduction::fragment_batch_item::FragmentBatchItem;

    use super::*;

    type B = Node<OpenPolicy>;

    async fn watched_pair() -> Result<(B, RecordingObserver), testresult::TestError> {
        let (a, a_obs) = node(30, OpenPolicy);
        let (b, _) = node(31, OpenPolicy);
        connect_nodes(&a, 30, &b, 31).await?;
        a.watch_heads(DOC).await;
        wait_until(|| a_obs.count() >= 1, "snapshot never arrived").await;
        Ok((b, a_obs))
    }

    async fn expect_report<F, Fut>(write: F) -> TestResult
    where
        F: FnOnce(B) -> Fut,
        Fut: core::future::Future<Output = TestResult>,
    {
        let (b, a_obs) = watched_pair().await?;
        let before = a_obs.count();
        write(b).await?;
        wait_until(
            || a_obs.count() > before,
            "watcher was not told about the change",
        )
        .await;
        Ok(())
    }

    fn blob(n: u8) -> Blob {
        Blob::new(vec![n; 8])
    }

    #[tokio::test]
    async fn add_commit() -> TestResult {
        expect_report(|b| async move {
            b.add_commit(DOC, commit(1), BTreeSet::new(), blob(1))
                .await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn add_fragment() -> TestResult {
        expect_report(|b| async move {
            b.add_fragment(DOC, commit(1), BTreeSet::new(), &[], blob(1))
                .await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn add_commits_batch() -> TestResult {
        expect_report(|b| async move {
            b.add_commits_batch(
                DOC,
                vec![(commit(1), BTreeSet::new(), blob(1))],
                CallTimeout::TimeoutMillis(500),
            )
            .await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn add_fragments_batch() -> TestResult {
        expect_report(|b| async move {
            b.add_fragments_batch(
                DOC,
                vec![FragmentBatchItem {
                    head: commit(1),
                    boundary: BTreeSet::new(),
                    checkpoints: Vec::new(),
                    blob: blob(1),
                }],
                CallTimeout::TimeoutMillis(500),
            )
            .await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn add_built_batch() -> TestResult {
        expect_report(|b| async move {
            let blob = blob(1);
            let c = LooseCommit::new(DOC, commit(1), BTreeSet::new(), BlobMeta::new(&blob));
            b.add_built_batch(
                DOC,
                vec![(c, blob)],
                Vec::new(),
                CallTimeout::TimeoutMillis(500),
            )
            .await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn add_sedimentree() -> TestResult {
        expect_report(|b| async move {
            let blob = blob(1);
            let c = LooseCommit::new(DOC, commit(1), BTreeSet::new(), BlobMeta::new(&blob));
            let f = Fragment::new(DOC, commit(2), BTreeSet::new(), &[], BlobMeta::new(&blob));
            b.add_sedimentree(
                DOC,
                Sedimentree::new(vec![f], vec![c]),
                vec![blob],
                CallTimeout::TimeoutMillis(500),
            )
            .await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn remove_sedimentree() -> TestResult {
        let (b, a_obs) = watched_pair().await?;
        b.add_commit(DOC, commit(1), BTreeSet::new(), blob(1))
            .await?;
        wait_until(|| a_obs.count() >= 2, "commit never reported").await;

        b.remove_sedimentree(DOC).await?;
        wait_until(
            || a_obs.heads_for(DOC).last().is_some_and(Vec::is_empty),
            "removal never reported as empty heads",
        )
        .await;
        Ok(())
    }

    /// `store_*` is documented as local-only; the watcher hears nothing until
    /// a later sync. Positive control: `add_commit` on the same pair reports.
    #[tokio::test]
    async fn store_is_local_only() -> TestResult {
        let (b, a_obs) = watched_pair().await?;
        let before = a_obs.count();
        b.store_commits_batch(DOC, vec![(commit(1), BTreeSet::new(), blob(1))])
            .await?;
        settle().await;
        assert_eq!(a_obs.count(), before, "{:?}", a_obs.deliveries());

        b.add_commit(DOC, commit(2), BTreeSet::new(), blob(2))
            .await?;
        wait_until(|| a_obs.count() > before, "control write not reported").await;
        Ok(())
    }
}

/// Wire-level view from a mock peer: exactly one heads-carrying frame per
/// change, whichever shape it takes.
mod on_the_wire {
    use sedimentree_core::{
        blob::BlobMeta, crypto::fingerprint::FingerprintSeed, loose_commit::LooseCommit,
        sedimentree::FingerprintSummary,
    };
    use subduction_core::{
        connection::{
            message::{
                BatchSyncRequest, RequestId, SyncMessage, UnwatchHeads, WatchHeads,
                WatchHeadsResponse, WatchOutcome, WatchResult,
            },
            test_utils::{ChannelMockConnection, ChannelMockConnectionHandle},
        },
        remote_heads::watches::MAX_WATCHERS_PER_PEER,
    };
    use subduction_crypto::signed::Signed;

    use super::*;

    type Mock = ChannelMockConnection<SyncMessage>;
    type Handle = ChannelMockConnectionHandle<SyncMessage>;
    type MockNode = Arc<
        Subduction<
            'static,
            Sendable,
            MemoryStorage,
            Mock,
            SyncHandler<
                Sendable,
                MemoryStorage,
                Mock,
                OpenPolicy,
                CountLeadingZeroBytes,
                TokioSpawn,
                256,
                RecordingObserver,
            >,
            OpenPolicy,
            MemorySigner,
            InstantTimeout,
            TokioSpawn,
        >,
    >;

    /// A real node with one mock peer attached.
    async fn node_with_mock(
        seed: u8,
        mock_seed: u8,
    ) -> Result<(MockNode, Handle), Box<dyn std::error::Error>> {
        let (sd, _handler, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(signer(seed))
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .heads_observer(RecordingObserver::default())
            .build::<Sendable, Mock>();
        tokio::spawn(listener);
        tokio::spawn(manager);

        let (conn, handle) = Mock::new_with_handle(peer_id(mock_seed));
        sd.add_connection(conn.authenticated()).await?;
        Ok((sd, handle))
    }

    /// Wait for the first frame matching `want`, then let the round settle and
    /// return everything received.
    async fn frames_after(
        handle: &Handle,
        want: impl Fn(&SyncMessage) -> bool,
    ) -> Vec<SyncMessage> {
        let mut frames = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(msg) = handle.outbound_rx.try_recv() {
                let done = want(&msg);
                frames.push(msg);
                if done {
                    break;
                }
            } else {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "expected frame never arrived: {frames:?}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        settle().await;
        while let Ok(msg) = handle.outbound_rx.try_recv() {
            frames.push(msg);
        }
        frames
    }

    fn heads_frames(frames: &[SyncMessage]) -> Vec<&SyncMessage> {
        frames
            .iter()
            .filter(|f| matches!(f, SyncMessage::HeadsUpdate { .. }))
            .collect()
    }

    const fn is_heads_update(f: &SyncMessage) -> bool {
        matches!(f, SyncMessage::HeadsUpdate { .. })
    }

    async fn watch(
        handle: &Handle,
        ids: Vec<SedimentreeId>,
    ) -> Result<Vec<WatchResult>, Box<dyn std::error::Error>> {
        handle.inbound_tx.send(WatchHeads { ids }.into()).await?;
        let frames =
            frames_after(handle, |f| matches!(f, SyncMessage::WatchHeadsResponse(_))).await;
        let resp = frames
            .into_iter()
            .find_map(|f| {
                if let SyncMessage::WatchHeadsResponse(WatchHeadsResponse { results }) = f {
                    Some(results)
                } else {
                    None
                }
            })
            .expect("response");
        Ok(resp)
    }

    #[tokio::test]
    async fn one_heads_frame_per_change_in_every_shape() -> TestResult {
        let (a, b) = node_with_mock(40, 41).await?;
        let b_peer = peer_id(41);

        let results = watch(&b, vec![DOC]).await?;
        assert!(matches!(
            results.as_slice(),
            [WatchResult { id, outcome: WatchOutcome::Watching(h) }] if *id == DOC && h.heads.is_empty()
        ));

        // Watch only: a standalone HeadsUpdate.
        a.add_commit(DOC, commit(1), BTreeSet::new(), Blob::new(vec![1; 8]))
            .await?;
        let frames = frames_after(&b, is_heads_update).await;
        assert_eq!(heads_frames(&frames).len(), 1, "{frames:?}");
        assert_eq!(
            frames.len(),
            1,
            "no pushes without a subscription: {frames:?}"
        );

        // Watch + subscribe: heads ride the push, no separate HeadsUpdate.
        b.inbound_tx
            .send(SyncMessage::BatchSyncRequest(BatchSyncRequest {
                id: DOC,
                req_id: RequestId {
                    requestor: b_peer,
                    nonce: 1,
                },
                fingerprint_summary: FingerprintSummary::new(
                    FingerprintSeed::new(1, 2),
                    BTreeSet::new(),
                    BTreeSet::new(),
                ),
                subscribe: true,
            }))
            .await?;
        frames_after(&b, |f| matches!(f, SyncMessage::BatchSyncResponse(_))).await;
        a.add_commit(DOC, commit(2), BTreeSet::new(), Blob::new(vec![2; 8]))
            .await?;
        let frames = frames_after(&b, |f| matches!(f, SyncMessage::LooseCommit { .. })).await;
        assert_eq!(frames.len(), 1, "one push and nothing else: {frames:?}");
        assert!(heads_frames(&frames).is_empty(), "{frames:?}");

        // B originates: the ack is the only heads frame.
        let blob = Blob::new(vec![3; 8]);
        let c = LooseCommit::new(DOC, commit(3), BTreeSet::new(), BlobMeta::new(&blob));
        let signed = Signed::seal::<Sendable, _>(&signer(41), c)
            .await
            .into_signed();
        b.inbound_tx
            .send(SyncMessage::LooseCommit {
                id: DOC,
                commit: signed,
                blob,
                sender_heads: RemoteHeads {
                    counter: 1,
                    heads: vec![commit(3)],
                },
            })
            .await?;
        let frames = frames_after(&b, is_heads_update).await;
        assert_eq!(frames.len(), 1, "exactly the ack: {frames:?}");

        Ok(())
    }

    /// `UnwatchHeads` stops `HeadsUpdate`s to that peer while another watcher
    /// (the positive control) keeps receiving them.
    #[tokio::test]
    async fn unwatch_stops_the_peer_being_told() -> TestResult {
        let (a, b) = node_with_mock(42, 43).await?;
        let (c_conn, c) = Mock::new_with_handle(peer_id(44));
        a.add_connection(c_conn.authenticated()).await?;

        watch(&b, vec![DOC]).await?;
        watch(&c, vec![DOC]).await?;

        b.inbound_tx
            .send(UnwatchHeads { ids: vec![DOC] }.into())
            .await?;
        settle().await;

        a.add_commit(DOC, commit(1), BTreeSet::new(), Blob::new(vec![1; 8]))
            .await?;
        let c_frames = frames_after(&c, is_heads_update).await;
        assert_eq!(
            heads_frames(&c_frames).len(),
            1,
            "control watcher: {c_frames:?}"
        );

        let mut b_frames = Vec::new();
        while let Ok(f) = b.outbound_rx.try_recv() {
            b_frames.push(f);
        }
        assert!(
            heads_frames(&b_frames).is_empty(),
            "unwatched peer still told: {b_frames:?}"
        );
        Ok(())
    }

    /// Past the cap, ids are answered `AtCapacity` and not recorded; unwatching
    /// frees a slot.
    #[tokio::test]
    async fn watches_past_cap_are_refused_until_a_slot_frees() -> TestResult {
        let (a, b) = node_with_mock(45, 46).await?;
        let ids: Vec<SedimentreeId> = (0..=MAX_WATCHERS_PER_PEER)
            .map(|n| {
                let mut bytes = [0u8; 32];
                bytes[..8].copy_from_slice(&n.to_le_bytes());
                SedimentreeId::new(bytes)
            })
            .collect();
        let extra = ids[MAX_WATCHERS_PER_PEER];

        let results = watch(&b, ids.clone()).await?;
        assert_eq!(results.len(), ids.len());
        let (watching, refused): (Vec<_>, Vec<_>) = results
            .iter()
            .partition(|r| matches!(r.outcome, WatchOutcome::Watching(_)));
        assert_eq!(watching.len(), MAX_WATCHERS_PER_PEER);
        assert!(
            matches!(refused.as_slice(), [WatchResult { id, outcome: WatchOutcome::AtCapacity }] if *id == extra)
        );

        // Not recorded: a change to the refused tree is not reported.
        a.add_commit(extra, commit(1), BTreeSet::new(), Blob::new(vec![1; 8]))
            .await?;
        settle().await;
        let mut frames = Vec::new();
        while let Ok(f) = b.outbound_rx.try_recv() {
            frames.push(f);
        }
        assert!(heads_frames(&frames).is_empty(), "{frames:?}");

        // Free one slot and retry.
        b.inbound_tx
            .send(UnwatchHeads { ids: vec![ids[0]] }.into())
            .await?;
        let results = watch(&b, vec![extra]).await?;
        assert!(matches!(
            results.as_slice(),
            [WatchResult {
                outcome: WatchOutcome::Watching(_),
                ..
            }]
        ));
        a.add_commit(extra, commit(2), BTreeSet::new(), Blob::new(vec![2; 8]))
            .await?;
        let frames = frames_after(&b, is_heads_update).await;
        assert_eq!(heads_frames(&frames).len(), 1, "{frames:?}");
        Ok(())
    }
}
