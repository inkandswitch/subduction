//! Disconnect cancels in-flight sync calls: an in-flight
//! `sync_with_all_peers` against a wedged peer must resolve as soon as
//! the peer is disconnected, not wait out the per-call timeout.
//!
//! The tests connect A to a wedged B over [`PausableChannelTransport`],
//! spawn a sync with a deliberately long (60s) per-call timeout, then
//! tear down the connection and assert the sync resolves well under that
//! timeout. The long timeout means a pass can only come from teardown
//! dropping the mux's pending senders, not from the timeout firing.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::{commit::CountLeadingZeroBytes, id::SedimentreeId};
use subduction_core::{
    authenticated::Authenticated,
    connection::{
        managed::CallError,
        test_utils::{PausableChannelTransport, TokioSpawn, TokioTimeout},
    },
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
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

/// Pull out the single connection registered for `peer` so we can
/// hand it to `disconnect()` (the per-connection variant).
async fn single_conn(sd: &TestSubduction, peer: PeerId) -> Authenticated<Conn, Sendable> {
    sd.get_connection(&peer)
        .await
        .expect("peer connection must be registered")
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

/// Long enough that the per-call default timeout cannot mask a missing
/// disconnect-cancellation. If the test passes only thanks to the
/// timeout firing, it would still take well over [`BOUND`].
const LONG_PER_CALL_TIMEOUT: Duration = Duration::from_secs(60);
const BOUND: Duration = Duration::from_secs(3);

#[tokio::test(flavor = "current_thread")]
async fn disconnect_from_peer_cancels_in_flight_sync_with_all_peers() -> TestResult {
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

    // The peer is in the result map because `sync_with_all_peers`
    // snapshots connections before `disconnect_from_peer` runs. Require
    // `ResponseDropped` (cancelled via the dropped mux sender), not
    // `Timeout`, so a slow timeout can't masquerade as a pass.
    let (success, _stats, conn_errs) = result
        .get(&b_peer)
        .expect("wedged peer must be present in the result map");
    assert!(!success, "peer should not have succeeded");
    assert_eq!(
        conn_errs.len(),
        1,
        "expected exactly one failed connection for the wedged peer"
    );
    assert!(
        matches!(conn_errs[0].1, CallError::ResponseDropped),
        "in-flight call must be cancelled via dropped mux sender \
         (ResponseDropped), not resolved by timeout; got {:?}",
        conn_errs[0].1
    );

    Ok(())
}

/// The per-connection `disconnect()` variant must also drop pending
/// multiplexer calls when it removes the peer's last connection, so
/// in-flight `sync_with_all_peers` callers don't hang. Exercises the
/// `remove_connection`/`disconnect` teardown path (distinct from
/// `disconnect_from_peer` above).
#[tokio::test(flavor = "current_thread")]
async fn disconnect_single_conn_when_last_cancels_in_flight_sync() -> TestResult {
    let a_signer = make_signer(11);
    let b_signer = make_signer(21);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    let (_t_a, t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    t_b.pause();

    let sed_id = SedimentreeId::new([2u8; 32]);
    let a_clone = a.clone();
    let sync_handle = tokio::spawn(async move {
        a_clone
            .sync_with_all_peers(sed_id, true, Some(LONG_PER_CALL_TIMEOUT))
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let b_peer = PeerId::from(b_signer.verifying_key());
    let b_conn = single_conn(&a, b_peer).await;
    a.disconnect(&b_conn).await?;

    let result = tokio::time::timeout(BOUND, sync_handle).await;
    assert!(
        result.is_ok(),
        "single-conn disconnect did not cancel in-flight sync within {BOUND:?}; \
         it was probably waiting for the {LONG_PER_CALL_TIMEOUT:?} per-call timeout"
    );
    // Pin the mechanism: the call was cancelled via the dropped mux
    // sender (`ResponseDropped`), not resolved by the per-call timeout.
    let map = result.expect("join error").expect("task panicked")?;
    let (success, _stats, conn_errs) = map
        .get(&b_peer)
        .expect("wedged peer must be present in the result map");
    assert!(!success, "peer should not have succeeded");
    assert!(
        matches!(conn_errs.first().map(|e| &e.1), Some(CallError::ResponseDropped)),
        "expected ResponseDropped from the dropped mux sender, got {:?}",
        conn_errs.first().map(|e| &e.1)
    );
    Ok(())
}

/// Contract test for `remove_connection`'s tri-state return AND its
/// cancellation side effect.
///
/// 1. With an in-flight call registered on the peer's mux, removing the
///    peer's last connection returns `Some(true)` and cancels that call
///    (the in-flight sync resolves promptly).
/// 2. A second removal of the same already-gone connection returns
///    `None` and is a clean no-op (no panic, no mux leak resurrected).
#[tokio::test(flavor = "current_thread")]
async fn remove_connection_cancels_in_flight_then_second_removal_is_noop() -> TestResult {
    let a_signer = make_signer(12);
    let b_signer = make_signer(22);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    let (_t_a, t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    t_b.pause();

    let b_peer = PeerId::from(b_signer.verifying_key());

    // Put a real in-flight call on B's mux so the cancellation has
    // something observable to act on.
    let sed_id = SedimentreeId::new([3u8; 32]);
    let a_clone = a.clone();
    let sync_handle = tokio::spawn(async move {
        a_clone
            .sync_with_all_peers(sed_id, true, Some(LONG_PER_CALL_TIMEOUT))
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let b_conn = single_conn(&a, b_peer).await;

    // First removal: peer's last connection → Some(true), cancels muxes.
    let res1 = a.remove_connection(&b_conn).await;
    assert_eq!(res1, Some(true));

    // The cancellation must resolve the in-flight sync promptly.
    let result = tokio::time::timeout(BOUND, sync_handle).await;
    assert!(
        result.is_ok(),
        "remove_connection did not cancel the in-flight sync within {BOUND:?}"
    );
    result.expect("join error").expect("task panicked")?;

    // And the mux must be gone afterwards (no leak).
    assert_eq!(
        a.mux_count(&b_peer).await,
        0,
        "mux entry must be removed once the peer's last connection is gone"
    );

    // Second removal of the already-gone connection: clean None no-op.
    let res2 = a.remove_connection(&b_conn).await;
    assert_eq!(res2, None);
    assert_eq!(a.mux_count(&b_peer).await, 0, "no mux must be resurrected");

    Ok(())
}

// ---------------------------------------------------------------------------
// Invariant: connections ⟺ multiplexers
// ---------------------------------------------------------------------------

/// Direct assertion of the `connections` ⟺ `multiplexers` invariant that
/// the three `.expect("multiplexer exists for every connected peer")`
/// sites rely on: a connected peer always has a multiplexer, and a
/// fully-disconnected peer has none.
#[tokio::test(flavor = "current_thread")]
async fn connected_peer_always_has_a_multiplexer() -> TestResult {
    let a_signer = make_signer(30);
    let b_signer = make_signer(40);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    let b_peer = PeerId::from(b_signer.verifying_key());

    // Before connecting: neither map has the peer.
    assert_eq!(a.connection_count(&b_peer).await, 0);
    assert_eq!(a.mux_count(&b_peer).await, 0);

    let (_t_a, _t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;

    // After connecting: exactly one connection and one mux, in lockstep.
    assert_eq!(a.connection_count(&b_peer).await, 1, "one connection");
    assert_eq!(a.mux_count(&b_peer).await, 1, "one mux for the connection");

    // After teardown: both gone.
    a.disconnect_from_peer(&b_peer).await?;
    assert_eq!(a.connection_count(&b_peer).await, 0, "connection removed");
    assert_eq!(a.mux_count(&b_peer).await, 0, "mux removed in lockstep");

    Ok(())
}

// ---------------------------------------------------------------------------
// Reconnect-during-teardown races
// ---------------------------------------------------------------------------

/// Repeatedly race `add_connection` (a fresh connection for the same
/// peer) against `remove_connection` (the peer's existing last
/// connection). The lock-nesting fix must guarantee that the freshly
/// added mux is never clobbered, so the invariant holds every iteration:
/// a peer present in `connections` is present in `multiplexers`, and
/// `sync_with_peer` therefore never hits its `.expect()` panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_during_remove_connection_never_clobbers_mux() -> TestResult {
    let a_signer = make_signer(50);
    let b_signer = make_signer(60);
    let a = make_node(a_signer.clone());
    // No `b` node: this test drives only `a`, building connections to
    // `b_peer` manually so it can race add/remove without a live remote.
    let b_peer = PeerId::from(b_signer.verifying_key());

    for i in 0..64u8 {
        // Establish a connection to remove.
        let (t_a, _t_b) = PausableChannelTransport::pair();
        let old_conn: Authenticated<Conn, Sendable> =
            Authenticated::new_for_test(MessageTransport::new(t_a), b_peer);
        a.add_connection(old_conn.clone()).await?;

        // Race: a reconnect (new distinct connection) concurrently with
        // removing the old one.
        let (t_a2, _t_b2) = PausableChannelTransport::pair();
        let new_conn: Authenticated<Conn, Sendable> =
            Authenticated::new_for_test(MessageTransport::new(t_a2), b_peer);

        let a1 = a.clone();
        let a2 = a.clone();
        let new_conn_for_task = new_conn.clone();
        let add = tokio::spawn(async move { a1.add_connection(new_conn_for_task).await });
        let remove = tokio::spawn(async move { a2.remove_connection(&old_conn).await });
        let _ = add.await.expect("add task panicked");
        let _ = remove.await.expect("remove task panicked");

        // Load-bearing invariant: if the peer is still connected it MUST
        // have a mux (else `sync_with_peer`'s `.expect()` would panic).
        // The reconnect race must never leave the fresh mux clobbered.
        assert!(
            a.conn_mux_invariant_holds(&b_peer).await,
            "iteration {i}: connections⟹multiplexers invariant violated after \
             add/remove race ({} conns, {} muxes) — reconnect race clobbered the mux",
            a.connection_count(&b_peer).await,
            a.mux_count(&b_peer).await,
        );

        // Clean up for the next iteration.
        a.disconnect_from_peer(&b_peer).await?;
        assert_eq!(a.connection_count(&b_peer).await, 0);
        assert_eq!(a.mux_count(&b_peer).await, 0);
    }

    Ok(())
}

/// Same race as above, but against the `disconnect_from_peer` teardown
/// path rather than `remove_connection`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_during_disconnect_from_peer_never_clobbers_mux() -> TestResult {
    let a_signer = make_signer(51);
    let b_signer = make_signer(61);
    let a = make_node(a_signer.clone());
    // No `b` node: drives only `a` (see sibling test above).
    let b_peer = PeerId::from(b_signer.verifying_key());

    for i in 0..64u8 {
        let (t_a, _t_b) = PausableChannelTransport::pair();
        let old_conn: Authenticated<Conn, Sendable> =
            Authenticated::new_for_test(MessageTransport::new(t_a), b_peer);
        a.add_connection(old_conn).await?;

        let (t_a2, _t_b2) = PausableChannelTransport::pair();
        let new_conn: Authenticated<Conn, Sendable> =
            Authenticated::new_for_test(MessageTransport::new(t_a2), b_peer);

        let a1 = a.clone();
        let a2 = a.clone();
        let add = tokio::spawn(async move { a1.add_connection(new_conn).await });
        let disc = tokio::spawn(async move { a2.disconnect_from_peer(&b_peer).await });
        let _ = add.await.expect("add task panicked");
        let _ = disc.await.expect("disconnect task panicked");

        assert!(
            a.conn_mux_invariant_holds(&b_peer).await,
            "iteration {i}: connections⟹multiplexers invariant violated after \
             add/disconnect race ({} conns, {} muxes)",
            a.connection_count(&b_peer).await,
            a.mux_count(&b_peer).await,
        );

        a.disconnect_from_peer(&b_peer).await?;
        assert_eq!(a.mux_count(&b_peer).await, 0);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// disconnect_all
// ---------------------------------------------------------------------------

/// `disconnect_all` must cancel in-flight calls for every peer (not just
/// one), resolving them promptly rather than at the per-call timeout, and
/// must empty both the connection and multiplexer maps.
#[tokio::test(flavor = "current_thread")]
async fn disconnect_all_cancels_in_flight_sync_for_every_peer() -> TestResult {
    let a_signer = make_signer(70);
    let b_signer = make_signer(80);
    let c_signer = make_signer(90);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());
    let c = make_node(c_signer.clone());

    let (_t_a_b, t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    let (_t_a_c, t_c) = connect_pair(&a, &a_signer, &c, &c_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Wedge both peers.
    t_b.pause();
    t_c.pause();

    let b_peer = PeerId::from(b_signer.verifying_key());
    let c_peer = PeerId::from(c_signer.verifying_key());

    let sed_id = SedimentreeId::new([4u8; 32]);
    let a_clone = a.clone();
    let sync_handle = tokio::spawn(async move {
        a_clone
            .sync_with_all_peers(sed_id, true, Some(LONG_PER_CALL_TIMEOUT))
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    a.disconnect_all().await?;

    let result = tokio::time::timeout(BOUND, sync_handle).await;
    assert!(
        result.is_ok(),
        "disconnect_all did not cancel in-flight syncs within {BOUND:?}; \
         a wedged peer was probably waiting out the {LONG_PER_CALL_TIMEOUT:?} timeout"
    );
    let map = result.expect("join error").expect("task panicked")?;

    // Every peer present in the snapshot must report ResponseDropped.
    for peer in [b_peer, c_peer] {
        if let Some((success, _stats, conn_errs)) = map.get(&peer) {
            assert!(!success, "peer {peer:?} should not have succeeded");
            assert!(
                matches!(
                    conn_errs.first().map(|e| &e.1),
                    Some(CallError::ResponseDropped)
                ),
                "peer {peer:?}: expected ResponseDropped, got {:?}",
                conn_errs.first().map(|e| &e.1)
            );
        }
    }

    // Both maps emptied.
    assert_eq!(a.connection_count(&b_peer).await, 0);
    assert_eq!(a.connection_count(&c_peer).await, 0);
    assert_eq!(a.mux_count(&b_peer).await, 0);
    assert_eq!(a.mux_count(&c_peer).await, 0);

    Ok(())
}

// ---------------------------------------------------------------------------
// remove_connection Some(false): peer keeps other connections
// ---------------------------------------------------------------------------

/// Removing one of several connections to a peer returns `Some(false)`
/// and must NOT cancel the peer's pending calls — a surviving connection
/// may still service them. Only when the LAST connection drops do we
/// cancel.
#[tokio::test(flavor = "current_thread")]
async fn remove_non_last_connection_does_not_cancel_pending_calls() -> TestResult {
    let a_signer = make_signer(13);
    let b_signer = make_signer(23);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());
    let b_peer = PeerId::from(b_signer.verifying_key());

    // First connection (also wires up B's side so the handshake-free
    // test transport is symmetric).
    let (_t_a, t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;

    // Second, distinct connection to the SAME peer.
    let (t_a2, _t_b2) = PausableChannelTransport::pair();
    let conn_a2: Authenticated<Conn, Sendable> =
        Authenticated::new_for_test(MessageTransport::new(t_a2), b_peer);
    a.add_connection(conn_a2.clone()).await?;

    assert_eq!(a.connection_count(&b_peer).await, 2, "two connections");
    tokio::time::sleep(Duration::from_millis(20)).await;
    t_b.pause();

    // In-flight sync against the (now wedged) peer.
    let sed_id = SedimentreeId::new([5u8; 32]);
    let a_clone = a.clone();
    let sync_handle = tokio::spawn(async move {
        a_clone
            .sync_with_all_peers(sed_id, true, Some(LONG_PER_CALL_TIMEOUT))
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Remove ONE of the two connections (the second one we added).
    let res = a.remove_connection(&conn_a2).await;
    assert_eq!(res, Some(false), "peer still has another connection");

    // The peer is still connected (one connection remains), so the
    // load-bearing invariant must still hold: it still has a multiplexer
    // and `sync_with_peer` would not panic.
    assert_eq!(
        a.connection_count(&b_peer).await,
        1,
        "one connection should remain after removing the non-last one"
    );
    assert!(
        a.conn_mux_invariant_holds(&b_peer).await,
        "a still-connected peer must still satisfy connections⟹multiplexers"
    );

    // The behavioral assertion that matters: the in-flight sync must
    // still be PENDING. Removing a non-last connection must NOT cancel
    // the peer's pending calls — a surviving connection may yet service
    // them. We wait a short window and assert it has NOT completed.
    //
    // (Note: `remove_connection`'s non-last path does not touch the
    // multiplexer map at all, so the pending oneshot sender is untouched;
    // this asserts that observable behavior rather than the internal
    // mux count, which is positional modeling debt.)
    let outcome = tokio::time::timeout(Duration::from_millis(300), sync_handle).await;
    assert!(
        outcome.is_err(),
        "in-flight sync resolved after removing a non-last connection; \
         a still-connected peer's pending calls must not be cancelled"
    );

    Ok(())
}
