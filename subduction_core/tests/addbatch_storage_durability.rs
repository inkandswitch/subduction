//! Durability must not be held hostage by a wedged peer.
//!
//! These tests port PR #198's `addbatch_storage_durability` coverage to
//! this branch's `store_*` / `add_*` API split:
//!
//! * [`store_built_batch`](Subduction::store_built_batch) is the durable,
//!   peer-independent write. With a byte-connected-but-wedged peer attached,
//!   it must return well under the configured per-call timeout — proving it
//!   never touches the network. (This is the exact regression #198's Fix B
//!   targeted, re-expressed via the persist-only verb.)
//! * [`add_built_batch`](Subduction::add_built_batch) is the convenience
//!   combinator that *does* await the broadcast. Against a wedged peer it must
//!   bound at the per-call timeout (not hang forever) and surface that peer in
//!   the returned [`PerPeerSync`] as `succeeded = false` — proving the durable
//!   write still landed and the propagation outcome is observable.
//!
//! The wedged peer is simulated with [`PausableChannelTransport::pause`]: it
//! stays byte-connected but never reads inbound messages, so A's
//! `BatchSyncRequest` sits in the channel unanswered.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    commit::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{PausableChannelTransport, TokioSpawn, TokioTimeout},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{FragmentBatchItem, Subduction, builder::SubductionBuilder},
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MessageTransport<PausableChannelTransport>;

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
        TokioTimeout,
        TokioSpawn,
    >,
>;

/// Wired into the node via [`SubductionBuilder::roundtrip_timeout`] in
/// [`make_node`], so it *is* the node's configured per-call timeout (not just
/// a constant referenced in messages). Long enough that a `store_built_batch`
/// which (wrongly) waited on the peer would block for the full
/// [`LONG_PER_CALL_TIMEOUT`], far past [`BOUND`] — making a regression into a
/// network roundtrip impossible to miss.
const LONG_PER_CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// Upper bound for the durable-write path: `store_built_batch` must return
/// far faster than [`LONG_PER_CALL_TIMEOUT`], even with a wedged peer.
const BOUND: Duration = Duration::from_secs(3);

/// Per-call timeout used for the `add_built_batch` combinator test, kept
/// short so the bounded broadcast resolves the test quickly. The combinator
/// is *expected* to wait roughly this long against a wedged peer.
const SHORT_PER_CALL_TIMEOUT: Duration = Duration::from_millis(500);

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_node(signer: MemorySigner) -> TestSubduction {
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        // Make the documented margin real: any (regressed) network roundtrip
        // from store_built_batch would inherit this 60s default and blow past
        // BOUND. The add_* combinator tests override it per-call.
        .roundtrip_timeout(LONG_PER_CALL_TIMEOUT)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

async fn connect_pair(
    a: &TestSubduction,
    a_signer: &MemorySigner,
    b: &TestSubduction,
    b_signer: &MemorySigner,
) -> TestResult<(PausableChannelTransport, PausableChannelTransport)> {
    let (t_a, t_b) = PausableChannelTransport::pair();

    let conn_a = MessageTransport::new(t_a.clone());
    let conn_b = MessageTransport::new(t_b.clone());

    let peer_a = PeerId::from(a_signer.verifying_key());
    let peer_b = PeerId::from(b_signer.verifying_key());

    let auth_a: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_b, peer_a);

    a.add_connection(auth_a).await?;
    b.add_connection(auth_b).await?;

    Ok((t_a, t_b))
}

fn make_blob(seed: u8) -> Blob {
    let data: Vec<u8> = (0..64).map(|i| seed.wrapping_add(i)).collect();
    Blob::new(data)
}

/// Build a `(LooseCommit, Blob)` pair whose `BlobMeta` matches its blob.
fn make_commit(id: SedimentreeId, head: u8, blob_seed: u8) -> (LooseCommit, Blob) {
    let blob = make_blob(blob_seed);
    let blob_meta = BlobMeta::new(&blob);
    let commit = LooseCommit::new(id, CommitId::new([head; 32]), BTreeSet::new(), blob_meta);
    (commit, blob)
}

/// `store_built_batch` is the durability path: with a wedged peer attached,
/// it must complete far under the long per-call timeout. A pass can only come
/// from the write never touching the network, not from the timeout firing.
#[tokio::test(flavor = "current_thread")]
async fn store_built_batch_does_not_stall_on_wedged_peer() -> TestResult {
    let a_signer = make_signer(40);
    let b_signer = make_signer(50);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    let (_t_a, t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Wedge B: byte-connected but never reads inbound messages.
    t_b.pause();

    let sed_id = SedimentreeId::new([1u8; 32]);
    let commits: Vec<(LooseCommit, Blob)> =
        (0..5).map(|i| make_commit(sed_id, i + 100, i)).collect();

    let a_clone = a.clone();
    let store_handle =
        tokio::spawn(async move { a_clone.store_built_batch(sed_id, commits, Vec::new()).await });

    let result = tokio::time::timeout(BOUND, store_handle).await;
    assert!(
        result.is_ok(),
        "store_built_batch did not return within {BOUND:?} with a wedged \
         peer connected; the durable write must never wait on the network \
         (it was probably blocked behind the {LONG_PER_CALL_TIMEOUT:?} \
         per-call timeout)"
    );
    result.expect("join error").expect("task panicked")?;

    // The write must actually be durable.
    let stored = a.get_commits(sed_id).await;
    assert_eq!(
        stored.as_ref().map(Vec::len),
        Some(5),
        "all commits should be persisted by store_built_batch"
    );

    Ok(())
}

/// `add_built_batch` is the store-then-broadcast combinator. Against a wedged
/// peer it must bound at the per-call timeout (not hang forever), persist the
/// data, and report the wedged peer in the returned `PerPeerSync` as failed.
#[tokio::test(flavor = "current_thread")]
async fn add_built_batch_bounds_at_timeout_and_reports_wedged_peer() -> TestResult {
    let a_signer = make_signer(41);
    let b_signer = make_signer(51);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    let (_t_a, t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    t_b.pause();

    let sed_id = SedimentreeId::new([2u8; 32]);
    let commits: Vec<(LooseCommit, Blob)> =
        (0..5).map(|i| make_commit(sed_id, i + 100, i)).collect();

    let a_clone = a.clone();
    let add_handle = tokio::spawn(async move {
        a_clone
            .add_built_batch(sed_id, commits, Vec::new(), Some(SHORT_PER_CALL_TIMEOUT))
            .await
    });

    // Generous outer bound: the combinator awaits the broadcast, which is
    // capped by SHORT_PER_CALL_TIMEOUT. It must resolve well before BOUND.
    let result = tokio::time::timeout(BOUND, add_handle).await;
    assert!(
        result.is_ok(),
        "add_built_batch did not return within {BOUND:?}; the bounded \
         broadcast should resolve by ~{SHORT_PER_CALL_TIMEOUT:?}"
    );
    let per_peer = result.expect("join error").expect("task panicked")?;

    // Durability still holds even though the broadcast could not reach B.
    let stored = a.get_commits(sed_id).await;
    assert_eq!(
        stored.as_ref().map(Vec::len),
        Some(5),
        "add_built_batch must persist the batch even when the peer is wedged"
    );

    // The wedged peer must be reported as not-succeeded in the per-peer map,
    // rather than silently dropped or causing an Err.
    let b_peer = PeerId::from(b_signer.verifying_key());
    let (success, _stats, _conn_errs) = per_peer
        .get(&b_peer)
        .expect("wedged peer must be present in the per-peer result map");
    assert!(
        !success,
        "wedged peer must be reported as failed in the returned PerPeerSync"
    );

    Ok(())
}

/// `add_commits_batch` is the parts-based store-then-broadcast combinator.
/// Against a wedged peer it must bound at the per-call timeout, persist the
/// commits, and report the peer as failed in the returned `PerPeerSync`.
#[tokio::test(flavor = "current_thread")]
async fn add_commits_batch_bounds_at_timeout_and_reports_wedged_peer() -> TestResult {
    let a_signer = make_signer(42);
    let b_signer = make_signer(52);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    let (_t_a, t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    t_b.pause();

    let sed_id = SedimentreeId::new([3u8; 32]);
    let commits: Vec<(CommitId, BTreeSet<CommitId>, Blob)> = (0..5u8)
        .map(|i| (CommitId::new([i + 100; 32]), BTreeSet::new(), make_blob(i)))
        .collect();

    let a_clone = a.clone();
    let add_handle = tokio::spawn(async move {
        a_clone
            .add_commits_batch(sed_id, commits, Some(SHORT_PER_CALL_TIMEOUT))
            .await
    });

    let result = tokio::time::timeout(BOUND, add_handle).await;
    assert!(
        result.is_ok(),
        "add_commits_batch did not return within {BOUND:?}; the bounded \
         broadcast should resolve by ~{SHORT_PER_CALL_TIMEOUT:?}"
    );
    let per_peer = result.expect("join error").expect("task panicked")?;

    assert_eq!(
        a.get_commits(sed_id).await.as_ref().map(Vec::len),
        Some(5),
        "add_commits_batch must persist the batch even when the peer is wedged"
    );

    let b_peer = PeerId::from(b_signer.verifying_key());
    let (success, _stats, _conn_errs) = per_peer
        .get(&b_peer)
        .expect("wedged peer must be present in the per-peer result map");
    assert!(
        !success,
        "wedged peer must be reported as failed in the returned PerPeerSync"
    );

    Ok(())
}

/// `add_fragments_batch` mirror of the commits combinator test above.
#[tokio::test(flavor = "current_thread")]
async fn add_fragments_batch_bounds_at_timeout_and_reports_wedged_peer() -> TestResult {
    let a_signer = make_signer(43);
    let b_signer = make_signer(53);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    let (_t_a, t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    t_b.pause();

    let sed_id = SedimentreeId::new([4u8; 32]);
    let fragments: Vec<FragmentBatchItem> = (0..3u8)
        .map(|i| FragmentBatchItem {
            head: CommitId::new([i + 50; 32]),
            boundary: BTreeSet::from([CommitId::new([i + 150; 32])]),
            checkpoints: Vec::new(),
            blob: make_blob(i),
        })
        .collect();

    let a_clone = a.clone();
    let add_handle = tokio::spawn(async move {
        a_clone
            .add_fragments_batch(sed_id, fragments, Some(SHORT_PER_CALL_TIMEOUT))
            .await
    });

    let result = tokio::time::timeout(BOUND, add_handle).await;
    assert!(
        result.is_ok(),
        "add_fragments_batch did not return within {BOUND:?}; the bounded \
         broadcast should resolve by ~{SHORT_PER_CALL_TIMEOUT:?}"
    );
    let per_peer = result.expect("join error").expect("task panicked")?;

    assert_eq!(
        a.get_fragments(sed_id).await.as_ref().map(Vec::len),
        Some(3),
        "add_fragments_batch must persist the batch even when the peer is wedged"
    );

    let b_peer = PeerId::from(b_signer.verifying_key());
    let (success, _stats, _conn_errs) = per_peer
        .get(&b_peer)
        .expect("wedged peer must be present in the per-peer result map");
    assert!(
        !success,
        "wedged peer must be reported as failed in the returned PerPeerSync"
    );

    Ok(())
}
