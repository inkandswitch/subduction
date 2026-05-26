//! Bug 5: an in-flight `sync_with_all_peers` against a wedged peer must
//! abort as soon as the peer is removed/disconnected — it must not
//! continue waiting for the per-call timeout to expire.
//!
//! ## Reproduction shape
//!
//! 1. Connect A↔B over [`PausableChannelTransport`].
//! 2. After the connection is registered, pause B's `recv_bytes`. B's
//!    listener loop never picks up A's `BatchSyncRequest`, so no response
//!    is produced.
//! 3. Spawn `a.sync_with_all_peers(sed_id, true, Some(60s))` — a long
//!    per-call timeout so a passing run can only be explained by the
//!    disconnect plumbing.
//! 4. After a short sleep (long enough for A's request to be queued),
//!    call `a.disconnect_from_peer(&b_peer)`. This removes the peer
//!    from A's connection map.
//! 5. Assert the spawned future resolves in well under the 60s timeout.
//!
//! Currently this hangs for the full timeout because cancelling a peer
//! does not clear its `Multiplexer` pending map; in-flight `call()`s sit
//! on a oneshot receiver that nobody will ever resolve.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::{commit::CountLeadingZeroBytes, id::SedimentreeId};
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
    let (sd, _h, listener, manager, _broadcast_seed) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
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

/// Long enough that the per-call default timeout (30 s by builder default)
/// cannot mask a missing disconnect-cancellation. If the test passes only
/// thanks to the timeout firing, it would still take well over 5 s.
const LONG_PER_CALL_TIMEOUT: Duration = Duration::from_secs(60);
const BOUND: Duration = Duration::from_secs(3);

#[tokio::test(flavor = "current_thread")]
async fn disconnect_cancels_in_flight_sync_with_all_peers() -> TestResult {
    let a_signer = make_signer(10);
    let b_signer = make_signer(20);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    let (_t_a, t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Wedge B: it stays byte-connected but never reads inbound messages,
    // so A's BatchSyncRequest sits in the channel unanswered.
    t_b.pause();

    let sed_id = SedimentreeId::new([1u8; 32]);
    let a_clone = a.clone();
    let sync_handle = tokio::spawn(async move {
        a_clone
            .sync_with_all_peers(sed_id, true, Some(LONG_PER_CALL_TIMEOUT))
            .await
    });

    // Let the request get queued into B's transport.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let b_peer = PeerId::from(b_signer.verifying_key());
    a.disconnect_from_peer(&b_peer).await?;

    let result = tokio::time::timeout(BOUND, sync_handle).await;
    assert!(
        result.is_ok(),
        "sync_with_all_peers did not return within {BOUND:?} after \
         peer was disconnected; it was probably waiting for the \
         {LONG_PER_CALL_TIMEOUT:?} per-call timeout"
    );
    let result = result.expect("join error").expect("task panicked")?;

    // Either the peer's slot is gone (no entry in the result map) or it
    // is present with `success=false` and a CallError. Both are correct
    // outcomes; what we explicitly forbid is hanging for the full
    // per-call timeout.
    if let Some((success, _stats, conn_errs)) = result.get(&b_peer) {
        assert!(!success, "peer should not have succeeded");
        assert!(
            !conn_errs.is_empty(),
            "expected at least one CallError for the wedged peer"
        );
    }

    Ok(())
}
