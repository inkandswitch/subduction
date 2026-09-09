//! Lifecycle contract: `stop` ends the node; dropping the last `Arc` ends
//! the resources.
//!
//! After `stop().await` the core must not retain any reference to the
//! `Arc<Subduction>` — not in a spawned dispatch task, not in the manager, not in
//! a connection loop. Otherwise a caller who drops their own `Arc` expecting
//! storage to be released (e.g. to reopen a file-locked database at the same
//! path) is silently stuck.

#![allow(clippy::panic, clippy::expect_used)]

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
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
};
use subduction_crypto::signed::Signed;
use testresult::TestResult;

const BUDGET: Duration = Duration::from_secs(5);

#[allow(clippy::indexing_slicing)]
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

/// Happy path: traffic flows, `stop().await`, the caller's handle is dropped,
/// and nothing in the core still holds the node.
#[tokio::test]
async fn stop_then_drop_releases_every_handle() -> TestResult {
    let (subduction, handler, listener_fut, manager_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
    subduction.add_connection(conn.authenticated()).await?;

    let manager_task = tokio::spawn(manager_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Push enough inbound traffic that dispatch tasks (which each hold a
    // handle to the node) have actually been spawned and run.
    for i in 0..8u8 {
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
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(subduction.sedimentree_ids().await.len(), 8);
    assert!(!subduction.is_stopped());

    let weak = Arc::downgrade(&subduction);

    tokio::time::timeout(BUDGET, subduction.stop())
        .await
        .expect("stop() must resolve once the loops exit");
    assert!(subduction.is_stopped());

    // Both loops exited on their own via the channel-close path — no abort.
    tokio::time::timeout(BUDGET, listener_task)
        .await??
        .expect("listener must exit gracefully, not via abort");
    tokio::time::timeout(BUDGET, manager_task)
        .await??
        .expect("manager must exit gracefully, not via abort");

    // Storage is still reachable after stop: the node is offline, not gone.
    assert_eq!(subduction.sedimentree_ids().await.len(), 8);

    drop(subduction);
    drop(handler);
    drop(handle);

    assert!(
        weak.upgrade().is_none(),
        "core still holds a reference to the node after stop; strong_count = {}",
        weak.strong_count()
    );

    Ok(())
}

/// `request_stop()` before the loops were ever polled must still let them
/// exit promptly once driven, and `stopped()` must observe that.
#[tokio::test]
async fn request_stop_before_loops_start_still_releases() -> TestResult {
    let (subduction, handler, listener_fut, manager_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let weak = Arc::downgrade(&subduction);
    subduction.request_stop();
    assert!(
        !subduction.is_stopped(),
        "a stop *request* is not the same as the loops having exited"
    );

    let listener_task = tokio::spawn(listener_fut);
    let manager_task = tokio::spawn(manager_fut);

    tokio::time::timeout(BUDGET, subduction.stopped())
        .await
        .expect("stopped() must resolve once the loops exit");
    assert!(subduction.is_stopped());

    tokio::time::timeout(BUDGET, listener_task)
        .await??
        .expect("listener must exit gracefully");
    tokio::time::timeout(BUDGET, manager_task)
        .await??
        .expect("manager must exit gracefully");

    drop(subduction);
    drop(handler);

    assert!(weak.upgrade().is_none());
    Ok(())
}

/// Dropping the loop futures unpolled counts as "gone": `stopped()` must not
/// hang waiting on a listener that will never run.
#[tokio::test]
async fn stopped_resolves_when_loop_futures_are_discarded() -> TestResult {
    let (subduction, _handler, listener_fut, manager_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    drop(listener_fut);
    assert!(!subduction.is_stopped(), "manager future is still alive");
    drop(manager_fut);
    assert!(subduction.is_stopped());

    tokio::time::timeout(BUDGET, subduction.stopped())
        .await
        .expect("stopped() must resolve immediately once both futures are gone");
    Ok(())
}

/// `stop` and `request_stop` are idempotent.
#[tokio::test]
async fn stop_is_idempotent() -> TestResult {
    let (subduction, _handler, listener_fut, manager_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let listener_task = tokio::spawn(listener_fut);
    let manager_task = tokio::spawn(manager_fut);

    subduction.request_stop();
    subduction.request_stop();
    tokio::time::timeout(BUDGET, subduction.stop()).await?;
    tokio::time::timeout(BUDGET, subduction.stop()).await?;

    tokio::time::timeout(BUDGET, listener_task)
        .await??
        .expect("listener must exit gracefully");
    tokio::time::timeout(BUDGET, manager_task)
        .await??
        .expect("manager must exit gracefully");
    Ok(())
}
