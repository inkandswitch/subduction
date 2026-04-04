//! Verifies that `full_sync_with_peer` syncs multiple documents concurrently
//! rather than sequentially, avoiding head-of-line blocking.
//!
//! Uses a storage wrapper that tracks the high-water mark of concurrent
//! storage load calls across `load_loose_commits` and `load_fragments`.
//! If documents are synced concurrently, multiple handler invocations will
//! perform those loads simultaneously, pushing the high-water mark above 1.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::StreamExt;

use alloc::vec::Vec;
use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::Blob,
    collections::Set,
    commit::CountLeadingZeroBytes,
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
    transport::message::MessageTransport,
};
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};
use testresult::TestResult;

extern crate alloc;

type Conn = MessageTransport<ChannelTransport>;

// ---------------------------------------------------------------------------
// ConcurrencyTrackingStorage — wraps MemoryStorage, tracks peak concurrency
// ---------------------------------------------------------------------------

/// A storage wrapper that tracks the peak number of concurrent
/// storage load calls (`load_loose_commits` and `load_fragments`)
/// via an atomic high-water mark.
///
/// Each tracked load call:
/// 1. Increments the in-flight counter
/// 2. Updates the high-water mark if the counter exceeds it
/// 3. Yields to the executor (allowing other tasks to start their loads)
/// 4. Delegates to the inner storage
/// 5. Decrements the in-flight counter
#[derive(Debug, Clone)]
struct ConcurrencyTrackingStorage {
    inner: MemoryStorage,
    in_flight: Arc<AtomicUsize>,
    high_water: Arc<AtomicUsize>,
}

impl ConcurrencyTrackingStorage {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            in_flight: Arc::new(AtomicUsize::new(0)),
            high_water: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The peak number of concurrent `load_loose_commits` calls observed.
    fn high_water_mark(&self) -> usize {
        self.high_water.load(Ordering::SeqCst)
    }
}

/// Helper: increment in-flight counter and update high-water mark.
fn enter_tracking(in_flight: &AtomicUsize, high_water: &AtomicUsize) {
    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    high_water.fetch_max(current, Ordering::SeqCst);
}

/// Helper: decrement in-flight.
fn exit_tracking(in_flight: &AtomicUsize) {
    in_flight.fetch_sub(1, Ordering::SeqCst);
}

impl Storage<Sendable> for ConcurrencyTrackingStorage {
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
        let in_flight = self.in_flight.clone();
        let high_water = self.high_water.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            enter_tracking(&in_flight, &high_water);
            // Yield to let other concurrent tasks start their loads
            tokio::task::yield_now().await;
            let result = Storage::<Sendable>::load_loose_commits(&inner, id).await;
            exit_tracking(&in_flight);
            result
        })
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
        Storage::<Sendable>::save_fragment(&self.inner, id, verified)
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
        let in_flight = self.in_flight.clone();
        let high_water = self.high_water.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            enter_tracking(&in_flight, &high_water);
            tokio::task::yield_now().await;
            let result = Storage::<Sendable>::load_fragments(&inner, id).await;
            exit_tracking(&in_flight);
            result
        })
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

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

type TrackingSyncHandler =
    SyncHandler<Sendable, ConcurrencyTrackingStorage, Conn, OpenPolicy, CountLeadingZeroBytes>;

type TrackingSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        ConcurrencyTrackingStorage,
        Conn,
        TrackingSyncHandler,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
    >,
>;

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_tracking_node(
    signer: MemorySigner,
    storage: ConcurrencyTrackingStorage,
) -> TrackingSubduction {
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

fn make_blob(seed: u8) -> Blob {
    let data: Vec<u8> = (0..64).map(|i| seed.wrapping_add(i)).collect();
    Blob::new(data)
}

async fn connect_pair(
    a: &TrackingSubduction,
    a_signer: &MemorySigner,
    b: &TrackingSubduction,
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

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

/// Regression test: verify that `full_sync_with_peer` processes multiple
/// sedimentrees concurrently, not sequentially.
///
/// This test was added because the original implementation used a sequential
/// `for id in tree_ids { sync_with_peer(..).await }` loop, causing
/// head-of-line blocking where each document waited for the previous one's
/// round trip to complete. The fix uses `FuturesUnordered` to fire all
/// sync requests concurrently.
///
/// The test creates two nodes, adds one commit to each of N different
/// sedimentree IDs on Alice, then syncs with Bob. The storage on Bob's
/// side tracks the high-water mark of concurrent `load_loose_commits`
/// calls. If sync is concurrent, multiple handler invocations will be
/// active simultaneously, pushing the high-water mark above 1.
///
/// This test covers the `Sendable` (tokio) path. The `Local` (Wasm) path
/// uses identical code — `full_sync_with_peer` is generic over `FutureForm`
/// — so the concurrency pattern is the same for both.
#[tokio::test]
async fn full_sync_with_peer_is_concurrent() -> TestResult {
    const NUM_DOCUMENTS: u8 = 20;

    let alice_signer = make_signer(50);
    let bob_signer = make_signer(51);

    let alice_storage = ConcurrencyTrackingStorage::new();
    let bob_storage = ConcurrencyTrackingStorage::new();

    let alice = make_tracking_node(alice_signer.clone(), alice_storage.clone());
    let bob = make_tracking_node(bob_signer.clone(), bob_storage.clone());

    // Add one commit to each of N different sedimentree IDs on Alice
    for i in 0..NUM_DOCUMENTS {
        let sed_id = SedimentreeId::new({
            let mut b = [0u8; 32];
            b[0] = i;
            b
        });
        let commit_id = CommitId::new({
            let mut b = [0u8; 32];
            b[0] = i;
            b[1] = 0xCC;
            b
        });
        alice
            .add_commit(sed_id, commit_id, BTreeSet::new(), make_blob(i))
            .await?;

        // Bob also needs to know about this sedimentree ID so the handler
        // will process sync requests for it. Add a different commit.
        let bob_commit_id = CommitId::new({
            let mut b = [0u8; 32];
            b[0] = i;
            b[1] = 0xBB;
            b
        });
        bob.add_commit(sed_id, bob_commit_id, BTreeSet::new(), make_blob(i + 100))
            .await?;
    }

    // Connect the two nodes
    connect_pair(&alice, &alice_signer, &bob, &bob_signer).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let bob_peer_id = PeerId::from(bob_signer.verifying_key());
    let sync_timeout = Some(Duration::from_millis(2000));

    // Sync all documents with Bob
    let (ok, stats, _call_errs, _io_errs) = alice
        .full_sync_with_peer(&bob_peer_id, true, sync_timeout)
        .await;

    assert!(ok, "sync should succeed");

    // Let fire-and-forget messages complete
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The high-water mark on Bob's storage should be > 1, proving
    // that multiple sync requests were handled concurrently.
    let bob_hwm = bob_storage.high_water_mark();
    assert!(
        bob_hwm > 1,
        "Bob's storage should see concurrent load_loose_commits calls \
         (high-water mark = {bob_hwm}, expected > 1). \
         If this is 1, full_sync_with_peer is sequential."
    );

    // Sanity: data actually synced
    assert!(
        stats.total_received() > 0 || stats.total_sent() > 0,
        "should have transferred some data"
    );

    Ok(())
}

/// Regression test: verify that many independent `sync_with_peer` calls
/// (simulating JS `Promise.all` over N documents) run concurrently.
///
/// Unlike [`full_sync_with_peer_is_concurrent`] which tests the internal
/// `FuturesUnordered` inside `full_sync_with_peer`, this test fires N
/// independent `sync_with_peer` calls from outside — the same code path
/// that a Wasm caller hits when doing:
///
/// ```js
/// await Promise.all(docIds.map(id => syncer.syncWithPeer(peerId, id, true, timeout)))
/// ```
///
/// The responses flow through the unbounded response channel (not the
/// bounded request queue), so all N pending oneshots can be resolved
/// without backpressure. If the response channel were removed and
/// responses shared the bounded request queue, backpressure would
/// serialize response processing and reduce concurrency.
#[tokio::test]
async fn many_independent_sync_with_peer_calls_are_concurrent() -> TestResult {
    const NUM_DOCUMENTS: u8 = 20;

    let alice_signer = make_signer(60);
    let bob_signer = make_signer(61);

    let alice_storage = ConcurrencyTrackingStorage::new();
    let bob_storage = ConcurrencyTrackingStorage::new();

    let alice = make_tracking_node(alice_signer.clone(), alice_storage.clone());
    let bob = make_tracking_node(bob_signer.clone(), bob_storage.clone());

    // Collect sedimentree IDs for later
    let mut sed_ids = Vec::new();

    for i in 0..NUM_DOCUMENTS {
        let sed_id = SedimentreeId::new({
            let mut b = [0u8; 32];
            b[0] = i;
            b[1] = 0xDD;
            b
        });
        sed_ids.push(sed_id);

        let alice_commit = CommitId::new({
            let mut b = [0u8; 32];
            b[0] = i;
            b[1] = 0xAA;
            b
        });
        alice
            .add_commit(sed_id, alice_commit, BTreeSet::new(), make_blob(i))
            .await?;

        let bob_commit = CommitId::new({
            let mut b = [0u8; 32];
            b[0] = i;
            b[1] = 0xBB;
            b
        });
        bob.add_commit(sed_id, bob_commit, BTreeSet::new(), make_blob(i + 100))
            .await?;
    }

    // Connect the two nodes
    connect_pair(&alice, &alice_signer, &bob, &bob_signer).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let bob_peer_id = PeerId::from(bob_signer.verifying_key());
    let sync_timeout = Some(Duration::from_millis(2000));

    // Fire N independent sync_with_peer calls concurrently — simulates
    // what JS Promise.all would do from the Wasm bindings.
    let mut sync_futures: futures::stream::FuturesUnordered<_> = sed_ids
        .iter()
        .map(|&sed_id| alice.sync_with_peer(&bob_peer_id, sed_id, true, sync_timeout))
        .collect();

    let mut success_count = 0;
    while let Some(result) = StreamExt::next(&mut sync_futures).await {
        if let Ok((true, _stats, _errs)) = result {
            success_count += 1;
        }
    }

    assert!(success_count > 0, "at least some syncs should succeed");

    // Let fire-and-forget messages complete
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The high-water mark on Bob's storage should be > 1, proving
    // that multiple independent sync requests were handled concurrently.
    let bob_hwm = bob_storage.high_water_mark();
    assert!(
        bob_hwm > 1,
        "Bob's storage should see concurrent load_loose_commits calls \
         from independent sync_with_peer requests \
         (high-water mark = {bob_hwm}, expected > 1). \
         If this is 1, responses are being serialized."
    );

    Ok(())
}
