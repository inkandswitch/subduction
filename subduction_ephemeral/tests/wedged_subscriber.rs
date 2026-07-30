//! Regression tests for the detached, capped ephemeral fan-out.
//!
//! Scenario:
//!
//! 1. `handle` returns promptly even when a subscriber's send never
//!    completes (the fan-out runs on a detached task, not the dispatch
//!    path), and healthy subscribers keep receiving messages.
//! 2. The per-peer in-flight cap
//!    ([`MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER`]) bounds how many detached
//!    sends can park against a wedged peer; further payloads to that peer
//!    are dropped (fire-and-forget) instead of accumulating tasks.

#![allow(clippy::panic)]

use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_lock::Mutex;
use future_form::{FutureForm, Sendable};
use nonempty::NonEmpty;
use sedimentree_core::collections::Map;
use subduction_core::{
    authenticated::Authenticated,
    connection::{Connection, test_utils::TokioSpawn},
    handler::Handler,
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use subduction_ephemeral::{
    clock::fake::FakeClock,
    config::{EphemeralConfig, EphemeralEvent},
    handler::{EphemeralHandler, MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER},
    message::{EphemeralMessage, EphemeralPayload},
    policy::OpenEphemeralPolicy,
    topic::Topic,
};
use testresult::TestResult;

// ── Wedgeable mock connection ───────────────────────────────────────────

/// A mock connection that either delivers sends to an unbounded channel
/// (healthy) or counts the attempt and parks forever (wedged) — modelling
/// a peer that stopped reading its socket.
#[derive(Clone)]
struct WedgeableConn {
    peer_id: PeerId,
    mode: Mode,
}

#[derive(Clone)]
enum Mode {
    Healthy(async_channel::Sender<EphemeralMessage>),
    Wedged(Arc<AtomicUsize>),
}

impl WedgeableConn {
    fn healthy(peer_id: PeerId) -> (Self, async_channel::Receiver<EphemeralMessage>) {
        let (tx, rx) = async_channel::unbounded();
        (
            Self {
                peer_id,
                mode: Mode::Healthy(tx),
            },
            rx,
        )
    }

    fn wedged(peer_id: PeerId) -> (Self, Arc<AtomicUsize>) {
        let attempts = Arc::new(AtomicUsize::new(0));
        (
            Self {
                peer_id,
                mode: Mode::Wedged(attempts.clone()),
            },
            attempts,
        )
    }
}

impl PartialEq for WedgeableConn {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id
    }
}

impl Connection<Sendable, EphemeralMessage> for WedgeableConn {
    type DisconnectionError = Infallible;
    type SendError = async_channel::SendError<EphemeralMessage>;
    type RecvError = async_channel::RecvError;

    fn disconnect(
        &self,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        Sendable::from_future(async { Ok(()) })
    }

    fn send(
        &self,
        message: &EphemeralMessage,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        let mode = self.mode.clone();
        let message = message.clone();
        Sendable::from_future(async move {
            match mode {
                Mode::Healthy(tx) => tx.send(message).await,
                Mode::Wedged(attempts) => {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    futures::future::pending::<()>().await;
                    unreachable!("pending() never resolves")
                }
            }
        })
    }

    fn recv(
        &self,
    ) -> <Sendable as FutureForm>::Future<'_, Result<EphemeralMessage, Self::RecvError>> {
        Sendable::from_future(async { futures::future::pending().await })
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

type Auth = Authenticated<WedgeableConn, Sendable>;
type Connections = Arc<Mutex<Map<PeerId, NonEmpty<Auth>>>>;
type OpenHandler =
    Arc<EphemeralHandler<Sendable, WedgeableConn, OpenEphemeralPolicy, FakeClock, TokioSpawn>>;

const TEST_CLOCK_SECS: TimestampSeconds = TimestampSeconds::new(1_000);

const fn peer(n: u8) -> PeerId {
    PeerId::new([n; 32])
}

fn make_handler(
    connections: Connections,
) -> (OpenHandler, async_channel::Receiver<EphemeralEvent>) {
    let (handler, rx) = EphemeralHandler::new(
        connections,
        OpenEphemeralPolicy,
        EphemeralConfig::default(),
        FakeClock::new(TEST_CLOCK_SECS),
        TokioSpawn,
    );
    (Arc::new(handler), rx)
}

async fn register(connections: &Connections, conn: WedgeableConn) -> Auth {
    let peer_id = conn.peer_id;
    let auth = Authenticated::new_for_test(conn, peer_id);
    connections
        .lock()
        .await
        .insert(peer_id, NonEmpty::new(auth.clone()));
    auth
}

fn rand_nonce() -> u64 {
    let mut buf = [0u8; 8];
    #[allow(
        clippy::expect_used,
        reason = "getrandom is infallible on test platforms"
    )]
    getrandom::getrandom(&mut buf).expect("getrandom failed");
    u64::from_le_bytes(buf)
}

async fn make_signed_ephemeral(
    signer: &MemorySigner,
    id: Topic,
    payload: Vec<u8>,
) -> EphemeralMessage {
    let ep = EphemeralPayload {
        id,
        nonce: rand_nonce(),
        timestamp: TEST_CLOCK_SECS,
        payload,
    };
    let verified = Signed::seal::<Sendable, _>(signer, ep).await;
    EphemeralMessage::Ephemeral(Box::new(verified.into_signed()))
}

/// Yield to the (current-thread) runtime so detached fan-out tasks make
/// progress. All healthy-path operations are immediately ready, so a
/// bounded number of yields settles them deterministically.
async fn settle() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

/// A subscriber whose send never completes must not stall `handle` (the
/// dispatch path) nor delivery to healthy subscribers, and parked sends
/// toward it must stop at the in-flight cap.
#[tokio::test]
async fn wedged_subscriber_does_not_stall_dispatch_and_is_capped() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _event_rx) = make_handler(connections.clone());

    let topic = Topic::new([0xAA; 32]);

    // The relay peer that inbound messages arrive through.
    let (relay_conn, _relay_rx) = WedgeableConn::healthy(peer(1));
    let auth_relay = register(&connections, relay_conn).await;

    // A healthy subscriber and a wedged one.
    let (healthy_conn, healthy_rx) = WedgeableConn::healthy(peer(2));
    let auth_healthy = register(&connections, healthy_conn).await;
    let (wedged_conn, wedged_attempts) = WedgeableConn::wedged(peer(3));
    let auth_wedged = register(&connections, wedged_conn).await;

    for auth in [&auth_healthy, &auth_wedged] {
        handler
            .handle(
                auth,
                EphemeralMessage::Subscribe {
                    topics: NonEmpty::new(topic),
                },
            )
            .await?;
    }

    // Relay strictly more messages than the cap. Each `handle` must return
    // promptly: the fan-out (including the never-completing send to the
    // wedged peer) runs on a detached task, not the dispatch path. Settle
    // between messages so healthy sends complete (modelling paced arrival);
    // the wedged peer's sends park regardless.
    let originator = MemorySigner::generate();
    let total = MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER + 4;
    for i in 0..total {
        let msg = make_signed_ephemeral(&originator, topic, vec![u8::try_from(i)?]).await;
        tokio::time::timeout(Duration::from_secs(5), handler.handle(&auth_relay, msg))
            .await
            .map_err(|_| "handle() stalled on a wedged subscriber (fan-out not detached?)")??;
        settle().await;
    }

    // Healthy subscriber received every payload despite its wedged sibling.
    let mut healthy_received = 0;
    while let Ok(msg) = healthy_rx.try_recv() {
        if matches!(msg, EphemeralMessage::Ephemeral(_)) {
            healthy_received += 1;
        }
    }
    assert_eq!(
        healthy_received, total,
        "healthy subscriber must receive all payloads"
    );

    // Sends toward the wedged peer stopped at the cap: the excess payloads
    // were dropped instead of accumulating parked tasks.
    assert_eq!(
        wedged_attempts.load(Ordering::SeqCst),
        MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER,
        "parked sends toward a wedged peer must stop at the in-flight cap"
    );

    Ok(())
}

/// Cancelling `publish` mid-flight must not leak in-flight budget.
///
/// `publish` awaits its fan-out inline, so a caller wrapping it in a
/// timeout drops the fan-out future while sends toward a wedged peer are
/// still parked. The RAII `InflightGuard`s must release those slots on
/// drop: each subsequent publish re-admits the peer, so send *attempts*
/// keep accruing past the cap. A leaky implementation (increment at
/// admit, decrement only on send completion) pins attempts at the cap and
/// silently locks the peer out of ephemera.
#[tokio::test]
async fn cancelled_publish_does_not_leak_inflight_budget() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _event_rx) = make_handler(connections.clone());

    let topic = Topic::new([0xCC; 32]);

    // One wedged subscriber; publishes fan out to it and park forever.
    let (wedged_conn, wedged_attempts) = WedgeableConn::wedged(peer(2));
    let auth_wedged = register(&connections, wedged_conn).await;
    handler
        .handle(
            &auth_wedged,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(topic),
            },
        )
        .await?;

    let publisher = MemorySigner::generate();
    let total = MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER + 4;
    for i in 0..total {
        let msg = make_signed_ephemeral(&publisher, topic, vec![u8::try_from(i)?]).await;
        // Each publish parks on the wedged send; cancel it via timeout.
        // Dropping the fan-out future must release the admitted slot.
        let cancelled = tokio::time::timeout(Duration::from_millis(20), handler.publish(msg))
            .await
            .is_err();
        // This is the leak detector: a leaky implementation stops admitting
        // at the cap, `admit_fan_out` returns None, publish completes
        // instantly, and this assert fails at iteration cap+1.
        assert!(
            cancelled,
            "publish returned early — admission returned None (leaked in-flight slot?)"
        );
    }

    // Every publish was admitted (slot freed by the previous cancellation),
    // so the wedged mock saw `total` attempts. A budget leak pins this at
    // the cap and drops the rest at admission.
    assert_eq!(
        wedged_attempts.load(Ordering::SeqCst),
        total,
        "cancelled publishes must release their in-flight slots"
    );

    Ok(())
}

/// Disconnecting a wedged peer clears its in-flight accounting: after
/// `on_peer_disconnect`, a fresh (healthy) connection for the same peer
/// receives payloads again rather than being treated as still-at-cap.
#[tokio::test]
async fn disconnect_resets_inflight_accounting() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _event_rx) = make_handler(connections.clone());

    let topic = Topic::new([0xBB; 32]);

    let (relay_conn, _relay_rx) = WedgeableConn::healthy(peer(1));
    let auth_relay = register(&connections, relay_conn).await;

    // Wedge peer(2) and drive it to its cap.
    let (wedged_conn, wedged_attempts) = WedgeableConn::wedged(peer(2));
    let auth_wedged = register(&connections, wedged_conn).await;
    handler
        .handle(
            &auth_wedged,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(topic),
            },
        )
        .await?;

    let originator = MemorySigner::generate();
    for i in 0..(MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER + 2) {
        let msg = make_signed_ephemeral(&originator, topic, vec![u8::try_from(i)?]).await;
        handler.handle(&auth_relay, msg).await?;
    }
    settle().await;
    assert_eq!(
        wedged_attempts.load(Ordering::SeqCst),
        MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER,
        "precondition: peer(2) is at its cap"
    );

    // The keepalive reaper (or teardown) removes the connection; the
    // handler is told about it. This must clear the in-flight counter.
    connections.lock().await.remove(&peer(2));
    handler.on_peer_disconnect(peer(2)).await;

    // Peer(2) reconnects healthy and re-subscribes.
    let (fresh_conn, fresh_rx) = WedgeableConn::healthy(peer(2));
    let auth_fresh = register(&connections, fresh_conn).await;
    handler
        .handle(
            &auth_fresh,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(topic),
            },
        )
        .await?;

    let msg = make_signed_ephemeral(&originator, topic, vec![0xFF]).await;
    handler.handle(&auth_relay, msg).await?;
    settle().await;

    assert!(
        matches!(fresh_rx.try_recv(), Ok(EphemeralMessage::Ephemeral(_))),
        "reconnected peer must receive payloads (stale in-flight count not cleared?)"
    );

    Ok(())
}
