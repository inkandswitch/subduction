//! A commit or fragment pushed to a node reaches the node's other subscribers
//! without waiting for the node to save it; the sender's `HeadsUpdate` ack
//! waits for the save.
//!
//! The node's storage holds every commit and fragment save until the test
//! opens a gate, standing in for an object store's write latency.
//!
//! ```text
//!   A ──LooseCommit──▸ S ──LooseCommit──▸ B (subscribed)
//!                      │
//!                      └── save held … then HeadsUpdate ──▸ A
//! ```
#![allow(clippy::expect_used)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    collections::Set,
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnectionHandle, InstantTimeout, TokioSpawn, test_signer},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::{memory::MemoryStorage, traits::Storage},
    subduction::{Subduction, builder::SubductionBuilder},
    test_utils::{MockConn, make_signer, subscribe_request, wait_until},
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner, verified_meta::VerifiedMeta};
use testresult::TestResult;
use tokio::sync::watch;

/// How long to wait for a frame that must arrive.
const WIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// [`MemoryStorage`] whose commit and fragment saves wait until the gate opens.
#[derive(Debug, Clone)]
struct GatedStorage {
    inner: MemoryStorage,
    open: watch::Receiver<bool>,
}

impl GatedStorage {
    async fn wait(&self) {
        let mut open = self.open.clone();
        open.wait_for(|open| *open)
            .await
            .expect("the gate's sender outlives the node");
    }
}

impl Storage<Sendable> for GatedStorage {
    type Error = <MemoryStorage as Storage<Sendable>>::Error;

    fn save_sedimentree_id(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::save_sedimentree_id(&self.inner, id)
    }

    fn delete_sedimentree_id(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::delete_sedimentree_id(&self.inner, id)
    }

    fn load_all_sedimentree_ids(&self) -> BoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Storage::<Sendable>::load_all_sedimentree_ids(&self.inner)
    }

    fn contains_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        Storage::<Sendable>::contains_sedimentree_id(&self.inner, id)
    }

    fn save_loose_commit(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            self.wait().await;
            Storage::<Sendable>::save_loose_commit(&self.inner, id, verified).await
        })
    }

    fn list_commit_ids(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Storage::<Sendable>::list_commit_ids(&self.inner, id)
    }

    fn load_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Storage::<Sendable>::load_loose_commits(&self.inner, id)
    }

    fn load_loose_commit_metas(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        Storage::<Sendable>::load_loose_commit_metas(&self.inner, id)
    }

    fn load_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> BoxFuture<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Storage::<Sendable>::load_loose_commit(&self.inner, id, commit_id)
    }

    fn delete_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::delete_loose_commit(&self.inner, id, commit_id)
    }

    fn delete_loose_commits(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::delete_loose_commits(&self.inner, id)
    }

    fn save_fragment(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            self.wait().await;
            Storage::<Sendable>::save_fragment(&self.inner, id, verified).await
        })
    }

    fn load_fragment(
        &self,
        id: SedimentreeId,
        fragment_head: CommitId,
    ) -> BoxFuture<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Storage::<Sendable>::load_fragment(&self.inner, id, fragment_head)
    }

    fn list_fragment_ids(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Storage::<Sendable>::list_fragment_ids(&self.inner, id)
    }

    fn load_fragments(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Storage::<Sendable>::load_fragments(&self.inner, id)
    }

    fn load_fragment_metas(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        Storage::<Sendable>::load_fragment_metas(&self.inner, id)
    }

    fn delete_fragment(
        &self,
        id: SedimentreeId,
        fragment_head: CommitId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::delete_fragment(&self.inner, id, fragment_head)
    }

    fn delete_fragments(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::delete_fragments(&self.inner, id)
    }

    fn save_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> BoxFuture<'_, Result<usize, Self::Error>> {
        Box::pin(async move {
            self.wait().await;
            Storage::<Sendable>::save_batch(&self.inner, id, commits, fragments).await
        })
    }
}

type Node = Arc<
    Subduction<
        'static,
        Sendable,
        GatedStorage,
        MockConn,
        SyncHandler<Sendable, GatedStorage, MockConn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn>,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

/// A node whose saves wait for the returned gate to be opened.
fn spawn_gated_node() -> (Node, watch::Sender<bool>) {
    let (gate, open) = watch::channel(false);
    let storage = GatedStorage {
        inner: MemoryStorage::new(),
        open,
    };
    let (node, _handler, listener, manager) = SubductionBuilder::new()
        .signer(make_signer(100))
        .storage(storage, Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, MockConn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    (node, gate)
}

async fn attach(node: &Node, peer: PeerId) -> ChannelMockConnectionHandle<SyncMessage> {
    let (conn, handle) = MockConn::new_with_handle(peer);
    node.add_connection(Authenticated::new_for_test(conn, peer, Direction::Accepted))
        .await
        .expect("node should accept the mock connection");
    handle
}

/// The first frame on `mock` that `pick` accepts.
async fn next_matching<T>(
    mock: &ChannelMockConnectionHandle<SyncMessage>,
    mut pick: impl FnMut(SyncMessage) -> Option<T>,
) -> T {
    loop {
        let msg = tokio::time::timeout(WIRE_TIMEOUT, mock.outbound_rx.recv())
            .await
            .expect("the frame should arrive")
            .expect("mock outbound is open");
        if let Some(found) = pick(msg) {
            return found;
        }
    }
}

/// Everything on `mock` up to the node's answer to a subscribe for
/// `sentinel`: whatever the node had sent before then.
async fn drain_until_answered(
    mock: &ChannelMockConnectionHandle<SyncMessage>,
    peer: PeerId,
    sentinel: SedimentreeId,
) -> Vec<SyncMessage> {
    mock.inbound_tx
        .send(subscribe_request(peer, sentinel))
        .await
        .expect("mock inbound is open");
    let mut frames = Vec::new();
    loop {
        let msg = tokio::time::timeout(WIRE_TIMEOUT, mock.outbound_rx.recv())
            .await
            .expect("the node should answer the sentinel subscribe")
            .expect("mock outbound is open");
        if matches!(&msg, SyncMessage::BatchSyncResponse(r) if r.id == sentinel) {
            return frames;
        }
        frames.push(msg);
    }
}

/// The heads a forwarded commit or fragment for `id` carries.
fn pushed_heads(msg: SyncMessage, id: SedimentreeId) -> Option<Vec<CommitId>> {
    match msg {
        SyncMessage::LooseCommit {
            id: pushed,
            sender_heads,
            ..
        }
        | SyncMessage::Fragment {
            id: pushed,
            sender_heads,
            ..
        } if pushed == id => Some(sender_heads.heads),
        _ => None,
    }
}

/// Send `message` (a commit or fragment for `id`) from A to a node B is
/// subscribed to, and check the push, the held save and the ack.
async fn forwards_before_saving(id: SedimentreeId, message: SyncMessage) -> TestResult {
    let (node, gate) = spawn_gated_node();
    let a_peer = PeerId::new([1u8; 32]);
    let b_peer = PeerId::new([2u8; 32]);
    let a = attach(&node, a_peer).await;
    let b = attach(&node, b_peer).await;

    b.inbound_tx.send(subscribe_request(b_peer, id)).await?;
    assert!(
        wait_until(|| async { node.get_subscribers(id).await.contains(&b_peer) }).await,
        "the node should record B's subscription"
    );

    a.inbound_tx.send(message).await?;

    // B hears about it while the save is still held.
    let pushed = next_matching(&b, |msg| pushed_heads(msg, id)).await;
    assert!(
        node.get_commits(id).await.is_none_or(|c| c.is_empty())
            && node.get_fragments(id).await.is_none_or(|f| f.is_empty()),
        "nothing is stored yet"
    );

    // A is not acked while the save is held.
    let before_save = drain_until_answered(&a, a_peer, SedimentreeId::new([0xEE; 32])).await;
    assert!(
        !before_save
            .iter()
            .any(|msg| matches!(msg, SyncMessage::HeadsUpdate { id: acked, .. } if *acked == id)),
        "no ack before the save: {before_save:?}"
    );

    gate.send(true)?;
    let acked = next_matching(&a, |msg| match msg {
        SyncMessage::HeadsUpdate {
            id: acked,
            heads: RemoteHeads { heads, .. },
        } if acked == id => Some(heads),
        _ => None,
    })
    .await;
    assert_eq!(
        pushed, acked,
        "the push carried the heads the node has once it is saved"
    );

    Ok(())
}

#[tokio::test]
async fn commit_reaches_subscribers_before_it_is_saved() -> TestResult {
    let id = SedimentreeId::new([9u8; 32]);
    let blob = Blob::new(b"typed".to_vec());
    let commit = LooseCommit::new(
        id,
        CommitId::new([0xA1; 32]),
        BTreeSet::new(),
        BlobMeta::new(&blob),
    );
    let signed = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
    let message = SyncMessage::LooseCommit {
        id,
        commit: signed.into_signed(),
        blob,
        sender_heads: RemoteHeads::default(),
    };

    forwards_before_saving(id, message).await?;
    Ok(())
}

#[tokio::test]
async fn fragment_reaches_subscribers_before_it_is_saved() -> TestResult {
    let id = SedimentreeId::new([10u8; 32]);
    let blob = Blob::new(b"compacted".to_vec());
    let fragment = Fragment::new(
        id,
        CommitId::new([1u8; 32]),
        BTreeSet::from([CommitId::new([2u8; 32])]),
        &[],
        BlobMeta::new(&blob),
    );
    let signed = Signed::seal::<Sendable, _>(&test_signer(), fragment).await;
    let message = SyncMessage::Fragment {
        id,
        fragment: signed.into_signed(),
        blob,
        sender_heads: RemoteHeads::default(),
    };

    forwards_before_saving(id, message).await?;
    Ok(())
}
