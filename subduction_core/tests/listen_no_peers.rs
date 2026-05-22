//! Reproducer: does `Subduction::listen()` panic when spawned with no
//! connected peers (i.e., `in_flight: FuturesUnordered` starts empty)?
//!
//! This is the exact scenario Alex described in PR #174:
//! > "select_next_some() on an empty FuturesUnordered panics ... on a
//! > freshly-created Subduction instance."
//!
//! If the panic is real, this test will crash. If not, it idles and
//! then exits cleanly when shutdown fires.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_docs_in_private_items,
    clippy::type_complexity
)]

use std::{sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::commit::CountLeadingZeroBytes;
use subduction_core::{
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MessageTransport<ChannelTransport>;
type SH = SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>;
type SD = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SH,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
    >,
>;

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_node() -> (
    SD,
    tokio::task::JoinHandle<Result<(), futures::future::Aborted>>,
    tokio::task::JoinHandle<Result<(), futures::future::Aborted>>,
) {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(make_signer(1))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();

    let listener_handle = tokio::spawn(listener);
    let manager_handle = tokio::spawn(manager);
    (sd, listener_handle, manager_handle)
}

/// Spin the listener for a real beat (a few scheduler ticks) with no
/// peers attached. Then shut down. If `select_next_some()` on an empty
/// `FuturesUnordered` panics, the listener task will abort and joining
/// will surface a `JoinError::is_panic()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listen_with_no_peers_does_not_panic() -> TestResult {
    let (sd, listener_handle, _manager_handle) = make_node();

    // Let the listener actually run several scheduler turns. With
    // `select_next_some()` on empty `in_flight`, this is enough for the
    // wake_by_ref re-poll cycle to manifest the alleged panic.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Listener should still be alive — not crashed.
    assert!(
        !listener_handle.is_finished(),
        "listener task finished early — possible panic"
    );

    // Idempotent graceful shutdown closes the response_queue + msg_queue
    // Receivers, which triggers the listener's clean-exit branches.
    sd.shutdown();

    // The listener's `select_biased!` is supposed to break on the
    // response_queue close branch.
    let join_result = tokio::time::timeout(Duration::from_secs(2), listener_handle).await;
    let listener_outcome = join_result.expect("listener should exit within 2s");
    assert!(
        listener_outcome.is_ok(),
        "listener task panicked or was cancelled: {listener_outcome:?}"
    );
    Ok(())
}

/// Same as above, but holds the listener longer (200ms) and verifies
/// the WAKER-driven repeat-poll cycle of `SelectNextSome` against an
/// empty stream doesn't trip the assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listen_with_no_peers_extended_idle() -> TestResult {
    let (sd, listener_handle, _manager_handle) = make_node();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!listener_handle.is_finished(), "listener died during idle");

    sd.shutdown();
    let join_result = tokio::time::timeout(Duration::from_secs(2), listener_handle).await;
    assert!(
        join_result.expect("listener should exit within 2s").is_ok(),
        "listener panicked during shutdown"
    );
    Ok(())
}
