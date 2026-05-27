//! Lifecycle regression test for the background broadcast worker
//! that decouples local storage durability from network broadcast.
//!
//! `Subduction::shutdown` must terminate a running worker promptly.
//! Before this fix the worker would hang because it held an
//! `Arc<Self>` clone that kept the `broadcast_tx` sender alive, so
//! closing the channel from the outside was a no-op, and there was
//! no `AbortHandle` for the worker stored inside `Subduction`. The
//! fix:
//!
//! 1. `Subduction` now stores `abort_broadcast_handle` and fires it
//!    from both `shutdown` and `Drop`.
//! 2. `shutdown` also closes `broadcast_tx`, so even without the
//!    abort the worker exits on its next `recv()`.
//! 3. The worker downgrades its received `Arc<Self>` to a `Weak`
//!    immediately, so it never extends `Subduction`'s lifetime.
//!
//! The test spawns the worker future on a `tokio` task and asserts
//! that the join handle resolves within a tight time bound after
//! `shutdown` is called.
//!
//! Note: dropping the last external `Subduction` `Arc` does *not* on
//! its own terminate the worker, because the listener and manager
//! tasks (spawned by `make_node`) hold their own `Arc<Self>` clones.
//! Cleaning those up requires an explicit `shutdown` (or aborting
//! their handles individually). That broader cycle is out of scope
//! for this worker-lifecycle fix.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::commit::CountLeadingZeroBytes;
use subduction_core::{
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnection, TokioSpawn, TokioTimeout, test_signer},
    },
    handler::sync::SyncHandler,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = ChannelMockConnection<SyncMessage>;
type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>,
        OpenPolicy,
        MemorySigner,
        TokioTimeout,
    >,
>;

const BOUND: Duration = Duration::from_secs(2);

fn make_node() -> (TestSubduction, tokio::task::JoinHandle<()>) {
    let (sd, _h, listener, manager, broadcast_seed) = SubductionBuilder::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);

    let sd_for_worker = sd.clone();
    let worker_handle = tokio::spawn(sd_for_worker.run_broadcast_worker(broadcast_seed));

    (sd, worker_handle)
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_terminates_broadcast_worker_promptly() -> TestResult {
    let (sd, worker_handle) = make_node();

    // Give the worker a moment to actually start awaiting recv().
    tokio::time::sleep(Duration::from_millis(20)).await;

    sd.shutdown();

    // Worker should exit within the bound — it is either aborted via
    // the abort handle or returns naturally because the broadcast
    // channel was closed.
    let result = tokio::time::timeout(BOUND, worker_handle).await;
    assert!(
        result.is_ok(),
        "broadcast worker did not terminate within {BOUND:?} after shutdown(); \
         the worker is probably still holding an Arc<Self> cycle \
         (memory-leak regression)"
    );

    Ok(())
}
