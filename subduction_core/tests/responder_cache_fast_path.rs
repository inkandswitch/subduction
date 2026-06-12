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

/// A cold clone (fresh peer missing the whole tree) of a cache-resident
/// tree must use the bulk-scan crossover, not thousands of point reads.
#[tokio::test]
async fn cold_clone_of_resident_tree_uses_bulk_scan() -> TestResult {
    const N: usize = 300; // missing = 300 > crossover = max(300/4, 32) = 75

    let alice_signer = MemorySigner::from_bytes(&[62u8; 32]);
    let bob_signer = MemorySigner::from_bytes(&[63u8; 32]);

    let alice_storage = CallCountingStorage::new();
    let bob_storage = CallCountingStorage::new();

    let alice = make_node(alice_signer.clone(), alice_storage.clone());
    let bob = make_node(bob_signer.clone(), bob_storage.clone());

    let sed_id = SedimentreeId::new([0x78; 32]);

    // Bob holds a large tree (resident in his cache via the write path);
    // Alice has nothing.
    for i in 0..N {
        let mut head = [0u8; 32];
        head[..8].copy_from_slice(&(i as u64).to_be_bytes());
        bob.add_commit(
            sed_id,
            CommitId::new(head),
            BTreeSet::new(),
            Blob::new(vec![1; 16]),
        )
        .await?;
    }

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
            CallTimeout::TimeoutMillis(5_000),
        )
        .await?;
    assert!(synced, "sync should reach Bob");
    assert!(send_errors.is_empty(), "no send errors expected");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let bob_bulk = bob_storage.bulk_loads() - bob_bulk_before;
    let bob_points = bob_storage.point_reads() - bob_point_before;

    assert!(
        bob_points < N,
        "cold clone must not point-read every item (saw {bob_points} point reads for {N} items)"
    );
    assert!(
        bob_bulk >= 1,
        "cold clone above the crossover must bulk-scan (saw {bob_bulk} bulk loads)"
    );

    // Convergence: Alice received the whole tree.
    let alice_commits = alice
        .get_commits(sed_id)
        .await
        .expect("Alice's tree must exist after the clone");
    assert_eq!(alice_commits.len(), N, "Alice must receive all commits");

    Ok(())
}

/// The slow path warms the cache: the first request for a tree that lives
/// only in storage (not resident — e.g. after a restart) takes the
/// bulk-scan slow path, and that scan installs the tree into the cache so
/// the *second* request takes the zero-bulk fast path.
#[tokio::test]
async fn slow_path_warms_cache_for_subsequent_requests() -> TestResult {
    let alice_signer = MemorySigner::from_bytes(&[66u8; 32]);
    let bob_signer = MemorySigner::from_bytes(&[67u8; 32]);

    let alice_storage = CallCountingStorage::new();
    let bob_storage = CallCountingStorage::new();

    let alice = make_node(alice_signer.clone(), alice_storage.clone());
    let bob = make_node(bob_signer.clone(), bob_storage.clone());

    let sed_id = SedimentreeId::new([0x7A; 32]);

    // Write Bob's tree *directly to storage*, bypassing the node: the tree
    // is durable but not cache-resident — the restart shape.
    for i in 0..4u8 {
        let verified = VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
            &bob_signer,
            (sed_id, CommitId::new([i; 32]), BTreeSet::new()),
            sedimentree_core::blob::verified::VerifiedBlobMeta::new(Blob::new(vec![i; 32])),
        )
        .await;
        Storage::<Sendable>::save_loose_commit(&bob_storage, sed_id, verified).await?;
    }

    connect_pair(&alice, &alice_signer, &bob, &bob_signer).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let bob_peer_id = PeerId::from(bob_signer.verifying_key());

    // First request: cache miss ⇒ slow path ⇒ at least one bulk scan.
    let bulk_before_first = bob_storage.bulk_loads();
    let (synced, _stats, send_errors) = alice
        .sync_with_peer(
            &bob_peer_id,
            sed_id,
            false,
            CallTimeout::TimeoutMillis(2_000),
        )
        .await?;
    assert!(synced, "first sync should reach Bob");
    assert!(send_errors.is_empty(), "no send errors expected");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let first_bulk = bob_storage.bulk_loads() - bulk_before_first;
    assert!(
        first_bulk >= 1,
        "the first request for a non-resident tree must take the bulk-scan \
         slow path (saw {first_bulk} bulk loads)"
    );

    // Second request: the slow path warmed the cache ⇒ zero bulk scans.
    let bulk_before_second = bob_storage.bulk_loads();
    let (synced, _stats, send_errors) = alice
        .sync_with_peer(
            &bob_peer_id,
            sed_id,
            false,
            CallTimeout::TimeoutMillis(2_000),
        )
        .await?;
    assert!(synced, "second sync should reach Bob");
    assert!(send_errors.is_empty(), "no send errors expected");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let second_bulk = bob_storage.bulk_loads() - bulk_before_second;
    assert_eq!(
        second_bulk, 0,
        "the second request must take the cache fast path \
         (saw {second_bulk} bulk loads — the slow path did not warm the cache)"
    );

    // Convergence sanity: Alice received the whole tree.
    let alice_commits = alice
        .get_commits(sed_id)
        .await
        .expect("Alice's tree must exist");
    assert_eq!(alice_commits.len(), 4, "Alice must receive all commits");

    Ok(())
}

/// A moderate diff on a large tree must stay on point reads: the scan
/// fallback is a *fraction* of tree size (total/4, floor 32), not an
/// absolute count. A fixed threshold here would bulk-scan 800 records to
/// serve 150 — measured ~9–12x slower than the point reads.
#[tokio::test]
async fn moderate_diff_on_large_tree_stays_on_point_reads() -> TestResult {
    const SHARED: usize = 650;
    const BOB_ONLY: usize = 150; // > old fixed floor region, ≤ total/4 (= 200)

    let alice_signer = MemorySigner::from_bytes(&[64u8; 32]);
    let bob_signer = MemorySigner::from_bytes(&[65u8; 32]);

    let alice_storage = CallCountingStorage::new();
    let bob_storage = CallCountingStorage::new();

    let alice = make_node(alice_signer.clone(), alice_storage.clone());
    let bob = make_node(bob_signer.clone(), bob_storage.clone());

    let sed_id = SedimentreeId::new([0x79; 32]);

    // Both peers hold the shared prefix (same heads + blobs, so the
    // fingerprints match); Bob additionally holds BOB_ONLY items.
    for i in 0..(SHARED + BOB_ONLY) {
        let mut head = [0u8; 32];
        head[..8].copy_from_slice(&(i as u64).to_be_bytes());
        let commit_id = CommitId::new(head);
        let content = Blob::new(i.to_le_bytes().to_vec());

        bob.add_commit(sed_id, commit_id, BTreeSet::new(), content.clone())
            .await?;
        if i < SHARED {
            alice
                .add_commit(sed_id, commit_id, BTreeSet::new(), content)
                .await?;
        }
    }

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
            CallTimeout::TimeoutMillis(5_000),
        )
        .await?;
    assert!(synced, "sync should reach Bob");
    assert!(send_errors.is_empty(), "no send errors expected");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let bob_bulk = bob_storage.bulk_loads() - bob_bulk_before;
    let bob_points = bob_storage.point_reads() - bob_point_before;

    assert_eq!(
        bob_bulk, 0,
        "a diff under total/4 must not bulk-scan (saw {bob_bulk} bulk loads)"
    );
    assert!(
        bob_points > 0,
        "the missing items must be served via point reads"
    );

    let alice_commits = alice
        .get_commits(sed_id)
        .await
        .expect("Alice's tree must exist");
    assert_eq!(
        alice_commits.len(),
        SHARED + BOB_ONLY,
        "Alice must converge to the full tree"
    );

    Ok(())
}
