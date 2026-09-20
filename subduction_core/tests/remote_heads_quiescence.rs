//! Heads notifications must quiesce once two peers have converged.
//!
//! An observer that syncs on every notification must terminate once peers
//! converge: each sync answers with `responder_heads`, and reporting
//! unchanged heads would restart the loop.
//!
//! Unit tests of the filter itself live in `subduction_core::remote_heads`;
//! these tests exercise the wiring through a running node.

#![allow(clippy::expect_used, clippy::panic)]

use core::convert::Infallible;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::{
        message::SyncMessage,
        test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    },
    handler::{Handler, sync::SyncHandler},
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, open::OpenPolicy, storage::StoragePolicy},
    remote_heads::{RemoteHeads, RemoteHeadsObserver},
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::{
    signed::Signed, signer::memory::MemorySigner, verified_author::VerifiedAuthor,
};
use testresult::TestResult;

type Conn = MessageTransport<ChannelTransport>;

type Node<P, R> = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<Sendable, MemoryStorage, Conn, P, CountLeadingZeroBytes, TokioSpawn, 256, R>,
        P,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

type NodeHandler<P, R> =
    Arc<SyncHandler<Sendable, MemoryStorage, Conn, P, CountLeadingZeroBytes, TokioSpawn, 256, R>>;

/// Refuses every write.
#[derive(Debug, Clone, Copy)]
struct DenyWrites;

#[derive(Debug, thiserror::Error)]
#[error("writes are refused")]
struct WritesRefused;

impl ConnectionPolicy<Sendable> for DenyWrites {
    type ConnectionDisallowed = Infallible;

    fn authorize_connect(&self, _peer: PeerId) -> BoxFuture<'_, Result<(), Infallible>> {
        Box::pin(async { Ok(()) })
    }
}

impl StoragePolicy<Sendable> for DenyWrites {
    type FetchDisallowed = Infallible;
    type PutDisallowed = WritesRefused;

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Infallible>> {
        Box::pin(async { Ok(()) })
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        _author: VerifiedAuthor,
        _id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), WritesRefused>> {
        Box::pin(async { Err(WritesRefused) })
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        Box::pin(async move { ids })
    }
}

/// Remembers every delivery.
#[derive(Clone, Debug, Default)]
struct RecordingObserver(Arc<Mutex<Vec<(SedimentreeId, PeerId, RemoteHeads)>>>);

impl RecordingObserver {
    fn deliveries(&self) -> Vec<(SedimentreeId, PeerId, RemoteHeads)> {
        self.0.lock().expect("poisoned").clone()
    }

    fn count(&self) -> usize {
        self.0.lock().expect("poisoned").len()
    }

    fn heads(&self) -> Vec<Vec<CommitId>> {
        self.deliveries()
            .into_iter()
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

fn node<R: RemoteHeadsObserver + Send + Sync + 'static>(
    seed: u8,
    observer: R,
) -> Node<OpenPolicy, R> {
    node_with_policy(seed, observer, OpenPolicy).0
}

fn node_with_policy<P, R>(seed: u8, observer: R, policy: P) -> (Node<P, R>, NodeHandler<P, R>)
where
    P: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Clone + Send + Sync + 'static,
    <P as StoragePolicy<Sendable>>::PutDisallowed: Send + Sync + 'static,
    <P as StoragePolicy<Sendable>>::FetchDisallowed: Send + Sync + 'static,
    <P as ConnectionPolicy<Sendable>>::ConnectionDisallowed: Send + Sync + 'static,
    R: RemoteHeadsObserver + Send + Sync + 'static,
{
    let (sd, handler, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
        .signer(signer(seed))
        .storage(MemoryStorage::new(), Arc::new(policy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .heads_observer(observer)
        .build::<Sendable, Conn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    (sd, handler)
}

async fn connect_nodes<PA, RA, PB, RB>(
    a: &Node<PA, RA>,
    a_seed: u8,
    b: &Node<PB, RB>,
    b_seed: u8,
) -> TestResult
where
    PA: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    <PA as StoragePolicy<Sendable>>::PutDisallowed: Send + Sync + 'static,
    <PA as StoragePolicy<Sendable>>::FetchDisallowed: Send + Sync + 'static,
    <PA as ConnectionPolicy<Sendable>>::ConnectionDisallowed: Send + Sync + 'static,
    PB: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    <PB as StoragePolicy<Sendable>>::PutDisallowed: Send + Sync + 'static,
    <PB as StoragePolicy<Sendable>>::FetchDisallowed: Send + Sync + 'static,
    <PB as ConnectionPolicy<Sendable>>::ConnectionDisallowed: Send + Sync + 'static,
    RA: RemoteHeadsObserver + Send + Sync + 'static,
    RB: RemoteHeadsObserver + Send + Sync + 'static,
{
    let (ta, tb) = ChannelTransport::pair();

    // Direction only affects subscription propagation, which is not exercised here.
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

/// `sync_with_peer` reports `responder_heads` before returning, so no settling
/// is needed between rounds.
#[tokio::test]
async fn repeated_sync_of_converged_trees_stops_notifying() -> TestResult {
    let observer = RecordingObserver::default();
    let a = node(1, observer.clone());
    let b = node(2, RecordingObserver::default());
    connect_nodes(&a, 1, &b, 2).await?;

    let doc = SedimentreeId::new([9u8; 32]);

    b.add_commit(
        doc,
        CommitId::new([1u8; 32]),
        BTreeSet::new(),
        Blob::new(b"only commit".to_vec()),
    )
    .await?;

    a.sync_with_peer(&peer_id(2), doc, true, CallTimeout::TimeoutMillis(500))
        .await?;
    let after_first = observer.count();
    assert!(after_first >= 1, "the first sync reports B's heads");

    for _ in 0..5 {
        a.sync_with_peer(&peer_id(2), doc, true, CallTimeout::TimeoutMillis(500))
            .await?;
    }

    let distinct: BTreeSet<Vec<CommitId>> = observer.heads().into_iter().collect();
    assert_eq!(distinct.len(), 1, "one distinct heads value: {distinct:?}");
    assert_eq!(
        observer.count(),
        after_first,
        "syncing a converged tree re-reported unchanged heads"
    );

    Ok(())
}

#[tokio::test]
async fn changed_heads_still_notify() -> TestResult {
    let observer = RecordingObserver::default();
    let a = node(3, observer.clone());
    let b = node(4, RecordingObserver::default());
    connect_nodes(&a, 3, &b, 4).await?;

    let doc = SedimentreeId::new([8u8; 32]);

    for i in 1..=3u8 {
        b.add_commit(
            doc,
            CommitId::new([i; 32]),
            BTreeSet::new(),
            Blob::new(vec![i; 16]),
        )
        .await?;
        a.sync_with_peer(&peer_id(4), doc, true, CallTimeout::TimeoutMillis(500))
            .await?;
    }

    let distinct: BTreeSet<Vec<CommitId>> = observer.heads().into_iter().collect();
    assert_eq!(distinct.len(), 3, "each new commit is a distinct report");
    Ok(())
}

/// A proactive disconnect must reach `FilteredHeadsNotifier::remove_peer`
/// through `Handler::on_peer_disconnect`, so a reconnecting peer's unchanged
/// heads are reported again.
#[tokio::test]
async fn disconnect_through_the_api_clears_filter_state() -> TestResult {
    let observer = RecordingObserver::default();
    let a = node(5, observer.clone());
    let b = node(6, RecordingObserver::default());
    connect_nodes(&a, 5, &b, 6).await?;

    let doc = SedimentreeId::new([3u8; 32]);

    b.add_commit(
        doc,
        CommitId::new([1u8; 32]),
        BTreeSet::new(),
        Blob::new(b"before".to_vec()),
    )
    .await?;
    a.sync_with_peer(&peer_id(6), doc, true, CallTimeout::TimeoutMillis(500))
        .await?;
    let before = observer.count();
    assert!(before >= 1);

    a.disconnect_from_peer(&peer_id(6)).await?;

    connect_nodes(&a, 5, &b, 6).await?;
    a.sync_with_peer(&peer_id(6), doc, true, CallTimeout::TimeoutMillis(500))
        .await?;

    assert!(
        observer.count() > before,
        "after a disconnect the peer's heads are reported again"
    );
    Ok(())
}

/// Heads on a push the storage policy refuses never reach the observer.
/// Drives the handler directly so the outcome is observable without settling.
mod policy_denied_pushes {
    use super::*;

    const DOC: SedimentreeId = SedimentreeId::new([13u8; 32]);
    const REFUSED: CommitId = CommitId::new([1u8; 32]);

    fn sender_heads() -> RemoteHeads {
        RemoteHeads {
            counter: 1,
            heads: vec![REFUSED],
        }
    }

    async fn assert_not_notified(message: SyncMessage) -> TestResult {
        let observer = RecordingObserver::default();
        let (locked, handler) = node_with_policy(21, observer.clone(), DenyWrites);
        let (transport, _far_end) = ChannelTransport::pair();
        let conn = Authenticated::new_for_test(
            MessageTransport::new(transport),
            peer_id(22),
            Direction::Accepted,
        );

        handler.handle(&conn, message).await?;

        assert_eq!(
            locked.get_commits(DOC).await.map_or(0, |c| c.len()),
            0,
            "the policy should have refused the write"
        );
        assert!(
            observer.deliveries().is_empty(),
            "heads from a refused push reached the observer: {:?}",
            observer.deliveries()
        );
        Ok(())
    }

    #[tokio::test]
    async fn commit() -> TestResult {
        let blob = Blob::new(b"refused".to_vec());
        let commit = LooseCommit::new(DOC, REFUSED, BTreeSet::new(), BlobMeta::new(&blob));
        let signed = Signed::seal::<Sendable, _>(&signer(22), commit).await;

        assert_not_notified(SyncMessage::LooseCommit {
            id: DOC,
            commit: signed.into_signed(),
            blob,
            sender_heads: sender_heads(),
        })
        .await
    }

    #[tokio::test]
    async fn fragment() -> TestResult {
        let blob = Blob::new(b"refused".to_vec());
        let fragment = Fragment::new(DOC, REFUSED, BTreeSet::new(), &[], BlobMeta::new(&blob));
        let signed = Signed::seal::<Sendable, _>(&signer(22), fragment).await;

        assert_not_notified(SyncMessage::Fragment {
            id: DOC,
            fragment: signed.into_signed(),
            blob,
            sender_heads: sender_heads(),
        })
        .await
    }
}
