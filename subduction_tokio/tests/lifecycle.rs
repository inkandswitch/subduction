//! `TokioSubduction` lifecycle: one owner, deterministic teardown.
//!
//! The wrapper exists to make the following true without the caller having
//! to think about it:
//!
//! - `stop().await` returns only when *everything* the node owns has exited
//!   (loops, connection readers, dispatch, `spawn`ed tasks).
//! - Dropping the owner without `stop` still brings the node down.
//! - A loop dying behind the wrapper's back is noticed, reported, and takes
//!   the rest of the node with it.
//! - Cancelling a shared token stops the node; stopping the node cancels
//!   the shared token.
//! - After `stop().await` + drop, nothing holds the `Arc<Subduction>` — so
//!   a file-locked storage backend can be reopened at the same path.

#![allow(clippy::panic, clippy::expect_used, clippy::indexing_slicing)]

use std::{collections::BTreeSet, sync::Arc};

use core::{future::pending, time::Duration};
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnection, test_signer},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::{memory::MemoryStorage, traits::Storage},
    subduction::builder::SubductionBuilder,
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use subduction_redb_storage::RedbStorage;
use subduction_tokio::{
    node::{Parts, TokioSubduction},
    spawn::TrackedTokioSpawn,
    timeout::TimeoutTokio,
};
use testresult::TestResult;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const BUDGET: Duration = Duration::from_secs(5);

type Conn = ChannelMockConnection<SyncMessage>;
type Handler = SyncHandler<
    Sendable,
    MemoryStorage,
    Conn,
    OpenPolicy,
    CountLeadingZeroBytes,
    TrackedTokioSpawn,
>;
type MemoryParts = Parts<MemoryStorage, Conn, Handler, OpenPolicy, MemorySigner>;

fn build_memory(spawner: TrackedTokioSpawn, _cancel: CancellationToken) -> MemoryParts {
    let (sd, _handler, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(spawner)
        .timer(TimeoutTokio)
        .build::<Sendable, Conn>();
    (sd, listener, manager)
}

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

/// `stop().await` drains everything: loops, dispatch, and `spawn`ed tasks
/// holding their own `Arc<Subduction>`. Nothing survives to pin the node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_then_drop_releases_every_arc() -> TestResult {
    let node = TokioSubduction::start(build_memory);

    let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
    node.add_connection(conn.authenticated()).await?;

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
    assert_eq!(node.sedimentree_ids().await.len(), 8);

    // A background task that would pin the node forever if the wrapper
    // didn't cancel it.
    let pinned = Arc::clone(node.subduction());
    let task = node.spawn(async move {
        let _keep_alive = pinned;
        pending::<()>().await;
    });

    let weak = Arc::downgrade(node.subduction());
    assert!(!node.is_stopped());

    tokio::time::timeout(BUDGET, node.stop())
        .await
        .expect("stop() must resolve");
    assert!(node.is_stopped());
    assert!(!node.exited_unexpectedly());
    assert_eq!(
        tokio::time::timeout(BUDGET, task).await??,
        None,
        "spawned task must have been cancelled, not completed"
    );

    // Offline, not gone: storage still answers.
    assert_eq!(node.sedimentree_ids().await.len(), 8);

    drop(node);
    drop(handle);
    assert!(
        weak.upgrade().is_none(),
        "something still holds the node; strong_count = {}",
        weak.strong_count()
    );
    Ok(())
}

/// The owner going away without `stop` is a stop request. It cannot wait,
/// but everything still comes down shortly after.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_without_stop_cancels_tasks_and_stops_loops() -> TestResult {
    let node = TokioSubduction::start(build_memory);
    let tracker = node.tracker();
    let weak = Arc::downgrade(node.subduction());
    let token = node.cancellation_token();

    let task = node.spawn(pending::<()>());

    drop(node);

    tokio::time::timeout(BUDGET, token.cancelled())
        .await
        .expect("drop must cancel the node's token");
    assert_eq!(tokio::time::timeout(BUDGET, task).await??, None);
    tokio::time::timeout(BUDGET, tracker.wait())
        .await
        .expect("drop must close the tracker and every task must exit");
    assert!(weak.upgrade().is_none());
    Ok(())
}

/// A loop dying behind the wrapper's back is a failure: reported, and the
/// rest of the node is brought down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_reports_unexpected_loop_exit() -> TestResult {
    let node = TokioSubduction::start(build_memory);
    let token = node.cancellation_token();

    // Stop the *core* directly, bypassing the wrapper — as if the loops had
    // died on their own.
    node.subduction().request_stop();

    tokio::time::timeout(BUDGET, token.cancelled())
        .await
        .expect("supervisor must cancel the node when a loop exits unexpectedly");
    assert!(node.exited_unexpectedly());

    // And the wrapper's own stop still converges afterwards.
    tokio::time::timeout(BUDGET, node.stop()).await?;
    Ok(())
}

/// An orderly `stop` is not a failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orderly_stop_is_not_a_failure() -> TestResult {
    let node = TokioSubduction::start(build_memory);
    tokio::time::timeout(BUDGET, node.stop()).await?;
    assert!(!node.exited_unexpectedly());

    // Idempotent.
    node.request_stop();
    tokio::time::timeout(BUDGET, node.stop()).await?;
    Ok(())
}

/// The build closure's token exists so handler-side loops that never end on
/// their own can still be torn down: tracked via the spawner, cancelled by
/// `stop`. Without it, `stop` would wait on this task forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_closure_token_cancels_tracked_loops_on_stop() -> TestResult {
    let node = TokioSubduction::start(|spawner, cancel| {
        spawner
            .tracker()
            .spawn(cancel.run_until_cancelled_owned(std::future::pending::<()>()));
        build_memory(spawner, CancellationToken::new())
    });

    tokio::time::timeout(BUDGET, node.stop()).await?;
    Ok(())
}

/// With `start_with`, the shared token is a two-way street: cancelling it
/// from outside stops the node, quietly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_shared_token_stops_node() -> TestResult {
    let root = CancellationToken::new();
    let tracker = TaskTracker::new();
    let (node, ()) =
        TokioSubduction::start_with(tracker.clone(), root.clone(), |spawner, cancel| {
            (build_memory(spawner, cancel), ())
        });
    let weak = Arc::downgrade(node.subduction());

    root.cancel();

    tokio::time::timeout(BUDGET, node.stopped())
        .await
        .expect("external cancel must stop the loops");
    assert!(
        !node.exited_unexpectedly(),
        "a stop via the shared token is orderly, not a failure"
    );
    tokio::time::timeout(BUDGET, tracker.wait())
        .await
        .expect("external cancel must let the tracker drain");

    drop(node);
    assert!(weak.upgrade().is_none());
    Ok(())
}

/// ...and stopping the node cancels the shared token.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_node_cancels_shared_token() -> TestResult {
    let root = CancellationToken::new();
    let (node, ()) =
        TokioSubduction::start_with(TaskTracker::new(), root.clone(), |spawner, cancel| {
            (build_memory(spawner, cancel), ())
        });

    tokio::time::timeout(BUDGET, node.stop()).await?;
    assert!(root.is_cancelled());
    Ok(())
}

/// The end-to-end reason this crate exists: tear down a node over redb and
/// reopen the same path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redb_reopens_after_stop_and_drop() -> TestResult {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_path_buf();
    let storage = RedbStorage::new(&root)?;

    let node = TokioSubduction::start(move |spawner, _cancel| {
        let (sd, _handler, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(storage, Arc::new(OpenPolicy))
            .spawner(spawner)
            .timer(TimeoutTokio)
            .build::<Sendable, Conn>();
        (sd, listener, manager)
    });

    let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
    node.add_connection(conn.authenticated()).await?;

    let id = SedimentreeId::new([7u8; 32]);
    let (commit, blob) = make_commit(&id, b"persisted").await;
    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id,
            commit,
            blob,
            sender_heads: RemoteHeads::default(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(node.sedimentree_ids().await.len(), 1);

    assert!(
        RedbStorage::new(&root).is_err(),
        "locked while the node is alive"
    );

    tokio::time::timeout(BUDGET, node.stop()).await?;
    assert!(
        RedbStorage::new(&root).is_err(),
        "stop() alone must not release storage while the owner is alive"
    );

    drop(node);
    drop(handle);

    let reopened = RedbStorage::new(&root).expect("must reopen after stop + drop");
    let commits = Storage::<Sendable>::load_loose_commits(&reopened, id).await?;
    assert_eq!(commits.len(), 1);
    Ok(())
}
