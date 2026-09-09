//! The redb file lock is released by RAII, not by `Subduction::stop()`.
//!
//! A caller who tears a node down and wants to reopen the same database
//! (e.g. "clear project, start over") must `stop().await` and then drop every
//! handle that transitively holds the `RedbStorage`. This test pins that
//! sequence end-to-end: while the node is alive the path is locked; after
//! teardown it reopens and the data written through the node is still there.

#![allow(clippy::panic, clippy::expect_used, clippy::indexing_slicing)]

use std::{collections::BTreeSet, sync::Arc};

use core::time::Duration;
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn, test_signer},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::traits::Storage,
    subduction::builder::SubductionBuilder,
};
use subduction_crypto::signed::Signed;
use subduction_redb_storage::RedbStorage;
use testresult::TestResult;

const LOOP_EXIT_BUDGET: Duration = Duration::from_secs(5);
const TREES: u8 = 4;

async fn make_commit(id: &SedimentreeId, data: &[u8]) -> (Signed<LooseCommit>, Blob) {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(&blob);
    let head = CommitId::new({
        let mut bytes = [0u8; 32];
        let len = data.len().min(32);
        bytes[..len].copy_from_slice(&data[..len]);
        bytes
    });
    let commit = LooseCommit::new(*id, head, BTreeSet::new(), blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
    (verified.into_signed(), blob)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redb_reopens_after_shutdown_and_last_drop() -> TestResult {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_path_buf();

    let (subduction, handler, listener_fut, manager_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(RedbStorage::new(&root)?, Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
    subduction.add_connection(conn.authenticated()).await?;

    let manager_task = tokio::spawn(manager_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Write through the node so the redb group-commit writer is spun up and
    // has real work in flight.
    for i in 0..TREES {
        let id = SedimentreeId::new([i; 32]);
        let (commit, blob) = make_commit(&id, format!("commit {i}").as_bytes()).await;
        handle
            .inbound_tx
            .send(SyncMessage::LooseCommit {
                id,
                commit,
                blob,
                sender_heads: RemoteHeads::default(),
            })
            .await?;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(subduction.sedimentree_ids().await.len(), TREES as usize);

    // While the node is alive the database is locked: a second open fails.
    assert!(
        RedbStorage::new(&root).is_err(),
        "redb must refuse a second open while the node still holds it"
    );

    // Teardown: stop the runtime, then drop every holder.
    tokio::time::timeout(LOOP_EXIT_BUDGET, subduction.stop())
        .await
        .expect("stop() must resolve once the loops exit");
    tokio::time::timeout(LOOP_EXIT_BUDGET, listener_task)
        .await??
        .expect("listener must exit gracefully");
    tokio::time::timeout(LOOP_EXIT_BUDGET, manager_task)
        .await??
        .expect("manager must exit gracefully");

    // Stop alone is not enough: the node still holds the database.
    assert!(
        RedbStorage::new(&root).is_err(),
        "stop() must not release storage while handles are alive"
    );

    drop(subduction);
    drop(handler); // the handler holds its own StoragePowerbox clone
    drop(handle);

    // The writer task holds only a `Weak<Database>`, so once the last strong
    // storage handle is gone the lock releases without waiting on it.
    let reopened = RedbStorage::new(&root).expect("database must reopen after teardown");

    for i in 0..TREES {
        let id = SedimentreeId::new([i; 32]);
        let commits = Storage::<Sendable>::load_loose_commits(&reopened, id).await?;
        assert_eq!(commits.len(), 1, "tree {i} must survive teardown");
    }

    Ok(())
}
