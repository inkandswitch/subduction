//! `add_built_batch` storage-vs-broadcast decoupling:
//! `add_built_batch` (the underlying entrypoint for the wasm
//! `addBatch`) must return as soon as the local store has accepted the
//! commits/fragments. It must not block awaiting acks from
//! protocol-unresponsive peers.
//!
//! ## Reproduction shape
//!
//! 1. Connect A↔B over [`PausableChannelTransport`].
//! 2. Pause B's transport so B will never process A's
//!    `BatchSyncRequest`.
//! 3. Time-bound `a.add_built_batch(...)`. Today this blocks for the
//!    full per-call timeout (default 30 s) inside the trailing
//!    `sync_with_all_peers` broadcast. After the fix it returns as
//!    soon as `add_built_batch_locally` settles.
//!
//! The 2-second bound is large enough to absorb tokio runtime
//! scheduling, channel/wire framing, and the storage write, but well
//! below the 30 s per-call timeout that the broadcast would otherwise
//! wait out.

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
    subduction::{Subduction, builder::SubductionBuilder},
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
    >,
>;

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_node(signer: MemorySigner) -> TestSubduction {
    let (sd, _h, listener, manager, mut broadcast_seed) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);

    let abort_reg = broadcast_seed
        .take_abort_registration()
        .expect("broadcast worker abort registration consumed twice");
    let sd_for_worker = sd.clone();
    tokio::spawn(async move {
        let worker = sd_for_worker.run_broadcast_worker(broadcast_seed);
        let _ = futures::future::Abortable::new(worker, abort_reg).await;
    });

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

fn make_commit_pair(sed_id: SedimentreeId, seed: u8) -> (LooseCommit, Blob) {
    let blob = Blob::new((0..64).map(|i| seed.wrapping_add(i)).collect::<Vec<u8>>());
    let blob_meta = BlobMeta::new(&blob);
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    let head = CommitId::new(bytes);
    let commit = LooseCommit::new(sed_id, head, BTreeSet::new(), blob_meta);
    (commit, blob)
}

const BOUND: Duration = Duration::from_secs(2);

#[tokio::test(flavor = "current_thread")]
async fn add_built_batch_returns_when_storage_durable_even_if_peer_wedged() -> TestResult {
    let a_signer = make_signer(10);
    let b_signer = make_signer(20);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    let (_t_a, t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Wedge B: it stays "connected" but never reads inbound messages,
    // so A's BatchSyncRequest sits in the channel unanswered.
    t_b.pause();

    let sed_id = SedimentreeId::new([7u8; 32]);
    let commits = vec![make_commit_pair(sed_id, 1)];

    let start = std::time::Instant::now();
    let result =
        tokio::time::timeout(BOUND, a.add_built_batch(sed_id, commits, Vec::new())).await;
    let elapsed = start.elapsed();

    assert!(
        result.is_ok(),
        "add_built_batch did not return within {BOUND:?} with a wedged peer; \
         elapsed={elapsed:?}. It was probably waiting on the trailing \
         sync_with_all_peers broadcast for the full per-call timeout."
    );
    result.expect("inner timeout").expect("add_built_batch errored");

    // Storage must have absorbed the write regardless of broadcast outcome.
    assert_eq!(
        a.get_commits(sed_id).await.map(|c| c.len()),
        Some(1),
        "commit should be durable in A's local store"
    );

    Ok(())
}
