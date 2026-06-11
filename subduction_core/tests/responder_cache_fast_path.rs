//! Verifies that the sync responder serves cache-resident trees without
//! bulk storage scans.
//!
//! `recv_batch_sync_request` previously reloaded *every* commit and fragment
//! for the tree from storage on every request, even when the tree was
//! resident in the sedimentree cache (the cache holds payload metadata only,
//! and the response needs signed bytes + blobs). The fix diffs against the
//! cached minimized tree and fetches only the items the requestor is missing
//! via targeted point reads.
//!
//! This test counts bulk loads (`load_loose_commits` / `load_fragments`) vs
//! point reads (`load_loose_commit` / `load_fragment`) on the responder's
//! storage and asserts a sync request for a cache-resident tree performs
//! zero bulk loads.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloc::vec::Vec;
use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::Blob,
    collections::Set,
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::{memory::MemoryStorage, traits::Storage},
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};
use testresult::TestResult;

extern crate alloc;

type Conn = MessageTransport<ChannelTransport>;

/// Wraps `MemoryStorage`, counting bulk scans vs targeted point reads.
#[derive(Debug, Clone)]
struct CallCountingStorage {
    inner: MemoryStorage,
    bulk_loads: Arc<AtomicUsize>,
    point_reads: Arc<AtomicUsize>,
}

impl CallCountingStorage {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            bulk_loads: Arc::new(AtomicUsize::new(0)),
            point_reads: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Total `load_loose_commits` + `load_fragments` calls.
    fn bulk_loads(&self) -> usize {
        self.bulk_loads.load(Ordering::SeqCst)
    }

    /// Total `load_loose_commit` + `load_fragment` calls.
    fn point_reads(&self) -> usize {
        self.point_reads.load(Ordering::SeqCst)
    }
}

impl Storage<Sendable> for CallCountingStorage {
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
        Storage::<Sendable>::save_loose_commit(&self.inner, id, verified)
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
        self.bulk_loads.fetch_add(1, Ordering::SeqCst);
        Storage::<Sendable>::load_loose_commits(&self.inner, id)
    }

    fn load_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> BoxFuture<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        self.point_reads.fetch_add(1, Ordering::SeqCst);
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
        Storage::<Sendable>::save_fragment(&self.inner, id, verified)
    }

    fn load_fragment(
        &self,
        id: SedimentreeId,
        fragment_head: CommitId,
    ) -> BoxFuture<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        self.point_reads.fetch_add(1, Ordering::SeqCst);
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
        self.bulk_loads.fetch_add(1, Ordering::SeqCst);
        Storage::<Sendable>::load_fragments(&self.inner, id)
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
        Storage::<Sendable>::save_batch(&self.inner, id, commits, fragments)
    }
}

type CountingSyncHandler =
    SyncHandler<Sendable, CallCountingStorage, Conn, OpenPolicy, CountLeadingZeroBytes>;

type CountingSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        CallCountingStorage,
        Conn,
        CountingSyncHandler,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

fn make_node(signer: MemorySigner, storage: CallCountingStorage) -> CountingSubduction {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(storage, Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

async fn connect_pair(
    a: &CountingSubduction,
    a_signer: &MemorySigner,
    b: &CountingSubduction,
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

/// A sync request against a cache-resident responder tree must not trigger
/// bulk storage scans — only targeted point reads for the items the
/// requestor is missing.
#[tokio::test]
async fn responder_serves_resident_tree_without_bulk_scans() -> TestResult {
    let alice_signer = MemorySigner::from_bytes(&[60u8; 32]);
    let bob_signer = MemorySigner::from_bytes(&[61u8; 32]);

    let alice_storage = CallCountingStorage::new();
    let bob_storage = CallCountingStorage::new();

    let alice = make_node(alice_signer.clone(), alice_storage.clone());
    let bob = make_node(bob_signer.clone(), bob_storage.clone());

    let sed_id = SedimentreeId::new([0x77; 32]);

    // Both sides hold one commit the other lacks. `add_commit` writes
    // through the sedimentree cache, so the tree is resident on both nodes.
    alice
        .add_commit(
            sed_id,
            CommitId::new([0xA1; 32]),
            BTreeSet::new(),
            Blob::new(vec![1; 64]),
        )
        .await?;
    bob.add_commit(
        sed_id,
        CommitId::new([0xB1; 32]),
        BTreeSet::new(),
        Blob::new(vec![2; 64]),
    )
    .await?;

    connect_pair(&alice, &alice_signer, &bob, &bob_signer).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let bob_bulk_before = bob_storage.bulk_loads();
    let bob_point_before = bob_storage.point_reads();

    let bob_peer_id = PeerId::from(bob_signer.verifying_key());
    let (synced, _stats, send_errors) = alice
        .sync_with_peer(
            &bob_peer_id,
            sed_id,
            false,
            CallTimeout::TimeoutMillis(2_000),
        )
        .await?;
    assert!(synced, "sync should reach Bob");
    assert!(send_errors.is_empty(), "no send errors expected");

    // Let fire-and-forget data messages drain.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let bob_bulk = bob_storage.bulk_loads() - bob_bulk_before;
    let bob_points = bob_storage.point_reads() - bob_point_before;

    assert_eq!(
        bob_bulk, 0,
        "responder must not bulk-scan storage for a cache-resident tree \
         (saw {bob_bulk} bulk loads)"
    );
    assert!(
        bob_points > 0,
        "responder should point-read the items the requestor is missing"
    );

    // Sanity: the sync actually converged — Alice received Bob's commit.
    let alice_commits = alice
        .get_commits(sed_id)
        .await
        .expect("Alice's tree must exist");
    assert!(
        alice_commits
            .iter()
            .any(|c| c.head() == CommitId::new([0xB1; 32])),
        "Alice must have received Bob's commit"
    );

    Ok(())
}
