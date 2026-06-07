//! The defining contract of the `store_*` tier: it never touches the network.
//!
//! Two responsive nodes A and B are connected over `ChannelTransport`. After
//! `store_built_batch` on A, B must still hold nothing for that sedimentree —
//! proving the durable write did not broadcast. As a positive control (so the
//! "B got nothing" assertion can't pass merely because the harness never
//! propagates anything), the same data is then made to reach B via an explicit
//! `sync_with_peer`, which must succeed.

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
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MessageTransport<ChannelTransport>;

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
        InstantTimeout,
        TokioSpawn,
    >,
>;

const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(500);

/// Time allowed for any (unwanted) broadcast to land before we assert it
/// did not. Generous relative to the in-process channel latency so a
/// "B got nothing" pass reflects no-broadcast, not a too-short wait.
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

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_node(signer: MemorySigner) -> TestSubduction {
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
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

fn make_commit(id: SedimentreeId, head: u8, blob_seed: u8) -> (LooseCommit, Blob) {
    let blob = make_blob(blob_seed);
    let blob_meta = BlobMeta::new(&blob);
    let commit = LooseCommit::new(id, CommitId::new([head; 32]), BTreeSet::new(), blob_meta);
    (commit, blob)
}

async fn connect_pair(
    a: &TestSubduction,
    a_signer: &MemorySigner,
    b: &TestSubduction,
    b_signer: &MemorySigner,
) -> TestResult {
    let (t_a, t_b) = ChannelTransport::pair();

    let conn_a = MessageTransport::new(t_a);
    let conn_b = MessageTransport::new(t_b);

    let peer_a = PeerId::from(a_signer.verifying_key());
    let peer_b = PeerId::from(b_signer.verifying_key());

    let auth_a: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_b, peer_a);

    a.add_connection(auth_a).await?;
    b.add_connection(auth_b).await?;

    Ok(())
}

/// `store_built_batch` must not propagate: with a responsive peer connected,
/// B receives nothing. The positive control then pulls the data via
/// `sync_with_peer`, proving the harness *can* observe propagation (so the
/// negative assertion is meaningful).
#[tokio::test]
async fn store_built_batch_does_not_broadcast() -> TestResult {
    let a_signer = make_signer(60);
    let b_signer = make_signer(70);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let sed_id = SedimentreeId::new([1u8; 32]);
    let commits: Vec<(LooseCommit, Blob)> =
        (0..5).map(|i| make_commit(sed_id, i + 100, i)).collect();

    // Durable, local-only write.
    a.store_built_batch(sed_id, commits, Vec::new()).await?;

    // A holds the data.
    assert_eq!(
        a.get_commits(sed_id).await.as_ref().map(Vec::len),
        Some(5),
        "A must hold the stored commits"
    );

    // Give any (unwanted) broadcast ample time to land, then assert B is
    // still empty for this sedimentree.
    tokio::time::sleep(PROPAGATION_PAUSE).await;
    assert!(
        b.get_commits(sed_id).await.is_none(),
        "store_built_batch must NOT broadcast: B should have received nothing"
    );

    // Positive control: an explicit sync from B pulls the data, confirming
    // the link works and the negative assertion above wasn't vacuous.
    let a_peer = PeerId::from(a_signer.verifying_key());
    b.sync_with_peer(&a_peer, sed_id, true, SYNC_TIMEOUT)
        .await?;

    let converged =
        wait_until(|| async { b.get_commits(sed_id).await.as_ref().map(Vec::len) == Some(5) })
            .await;
    assert!(
        converged,
        "after an explicit sync_with_peer, B must receive all 5 commits \
         (positive control for the no-broadcast assertion)"
    );

    Ok(())
}

/// `store_commit` (single, local-only) likewise must not broadcast.
#[tokio::test]
async fn store_commit_does_not_broadcast() -> TestResult {
    let a_signer = make_signer(61);
    let b_signer = make_signer(71);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let sed_id = SedimentreeId::new([2u8; 32]);

    // Non-boundary head so this is a plain loose commit.
    a.store_commit(
        sed_id,
        CommitId::new([0xAB; 32]),
        BTreeSet::new(),
        make_blob(1),
    )
    .await?;

    assert_eq!(
        a.get_commits(sed_id).await.as_ref().map(Vec::len),
        Some(1),
        "A must hold the stored commit"
    );

    tokio::time::sleep(PROPAGATION_PAUSE).await;
    assert!(
        b.get_commits(sed_id).await.is_none(),
        "store_commit must NOT broadcast: B should have received nothing"
    );

    Ok(())
}
