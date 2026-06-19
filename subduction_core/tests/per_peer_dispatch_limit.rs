//! Per-peer dispatch limiting: one peer saturating its in-flight budget must
//! not starve another peer, and a saturated peer backpressures its *own*
//! transport rather than the shared dispatch queue.
//!
//! A handler parks every message from a designated "slow" peer (holding its
//! per-peer dispatch permit) until the test releases it, and counts messages
//! from a "fast" peer. We flood the slow peer past its per-peer cap so its
//! semaphore is fully held and its connection reader stops draining — observable
//! as a backlog left in the slow peer's (unbounded) inbound channel — then
//! assert the fast peer's message is still dispatched promptly.
//!
//! With the old single global cap, a peer holding the whole cap would block
//! dispatch for everyone; the per-peer semaphore is what keeps the fast peer
//! flowing here.

#![allow(clippy::expect_used, clippy::panic)]

use core::time::Duration;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use future_form::Sendable;
use sedimentree_core::{depth::CountLeadingZeroBytes, id::SedimentreeId};
use subduction_core::{
    authenticated::Authenticated,
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn, test_signer},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::{RemoteHeads, RemoteHeadsNotifier},
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
};
use testresult::TestResult;

type Conn = ChannelMockConnection<SyncMessage>;
type InnerHandler = SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>;

/// Must match `MAX_INFLIGHT_DISPATCH_PER_PEER` in `connection::manager`. That
/// constant is crate-private, so we pin it here; if it changes, the
/// `entered_slow >= PER_PEER_CAP` wait below times out and flags the drift.
const PER_PEER_CAP: usize = 512;

/// Parks every message from `slow_peer` (holding its dispatch permit) until
/// `release` is closed, and counts every message from any other peer. Always
/// returns `Ok` so connections are never torn down.
struct GatedHandler {
    inner: Arc<InnerHandler>,
    slow_peer: PeerId,
    entered_slow: Arc<AtomicUsize>,
    fast_handled: Arc<AtomicUsize>,
    release: async_channel::Receiver<()>,
}

impl subduction_core::handler::Handler<Sendable, Conn> for GatedHandler {
    type Message = SyncMessage;
    type HandlerError =
        <InnerHandler as subduction_core::handler::Handler<Sendable, Conn>>::HandlerError;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<Conn, Sendable>,
        _message: Self::Message,
    ) -> <Sendable as future_form::FutureForm>::Future<'a, Result<(), Self::HandlerError>> {
        let is_slow = conn.peer_id() == self.slow_peer;
        <Sendable as future_form::FutureForm>::from_future(async move {
            if is_slow {
                // Mark the slot occupied, then park until released. The dispatch
                // task holds this peer's permit for the whole time we're parked,
                // so the slow peer's semaphore fills and stays full.
                self.entered_slow.fetch_add(1, Ordering::SeqCst);
                let _released = self.release.recv().await;
            } else {
                self.fast_handled.fetch_add(1, Ordering::SeqCst);
            }

            Ok(())
        })
    }

    fn on_peer_disconnect(
        &self,
        peer: PeerId,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, ()> {
        self.inner.on_peer_disconnect(peer)
    }
}

impl RemoteHeadsNotifier for GatedHandler {
    fn notify_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
        self.inner.notify_remote_heads(id, peer, heads);
    }
}

/// Poll `cond` until it holds or `limit` elapses; returns whether it held.
async fn wait_for(limit: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < limit {
        if cond() {
            return true;
        }

        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    cond()
}

fn heads_update(id: [u8; 32]) -> SyncMessage {
    SyncMessage::HeadsUpdate {
        id: SedimentreeId::new(id),
        heads: RemoteHeads::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_peer_does_not_starve_another_peer() -> TestResult {
    // Flood the slow peer past its per-peer cap. Its unbounded inbound channel
    // absorbs the flood; the reader admits only `PER_PEER_CAP` before its
    // semaphore is exhausted and it stops pulling.
    const SLOW_FLOOD: usize = PER_PEER_CAP + 200;

    let slow_peer = PeerId::new([0xA1u8; 32]);
    let fast_peer = PeerId::new([0xB2u8; 32]);

    let entered_slow = Arc::new(AtomicUsize::new(0));
    let fast_handled = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = async_channel::bounded::<()>(1);

    let entered_for_handler = Arc::clone(&entered_slow);
    let fast_for_handler = Arc::clone(&fast_handled);

    let (sd, listener, manager, ()) = SubductionBuilder::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build_composed::<Sendable, Conn, GatedHandler, ()>(|sync_handler| {
            (
                Arc::new(GatedHandler {
                    inner: sync_handler,
                    slow_peer,
                    entered_slow: entered_for_handler,
                    fast_handled: fast_for_handler,
                    release: release_rx,
                }),
                (),
            )
        });

    let listener_task = tokio::spawn(listener);
    let manager_task = tokio::spawn(manager);

    let (slow_conn, slow_handle) = ChannelMockConnection::new_with_handle(slow_peer);
    let (fast_conn, fast_handle) = ChannelMockConnection::new_with_handle(fast_peer);
    sd.add_connection(slow_conn.authenticated()).await?;
    sd.add_connection(fast_conn.authenticated()).await?;

    for _ in 0..SLOW_FLOOD {
        slow_handle
            .inbound_tx
            .send(heads_update([0xCCu8; 32]))
            .await?;
    }

    // Wait until the slow peer holds its full per-peer budget in flight. The
    // semaphore allows exactly `PER_PEER_CAP` concurrent handlers, so this
    // plateaus at the cap — the next request is held at the reader's `acquire`.
    let saturated = wait_for(Duration::from_secs(20), || {
        entered_slow.load(Ordering::SeqCst) >= PER_PEER_CAP
    })
    .await;
    assert!(
        saturated,
        "slow peer never reached its per-peer cap: entered_slow = {} (want {PER_PEER_CAP})",
        entered_slow.load(Ordering::SeqCst)
    );
    assert_eq!(
        entered_slow.load(Ordering::SeqCst),
        PER_PEER_CAP,
        "slow peer ran more than its per-peer cap of {PER_PEER_CAP} handlers concurrently"
    );

    // Backpressure: once the cap was hit the reader stopped draining, so the
    // flood's remainder is still queued in the slow peer's inbound channel.
    assert!(
        !slow_handle.inbound_tx.is_empty(),
        "a saturated peer must backpressure its own inbound channel, not drain it"
    );

    // Isolation: a different peer's message is still dispatched promptly even
    // though the slow peer is pinned at its cap.
    fast_handle.inbound_tx.send(heads_update([7u8; 32])).await?;
    let fast_progressed = wait_for(Duration::from_secs(5), || {
        fast_handled.load(Ordering::SeqCst) >= 1
    })
    .await;
    assert!(
        fast_progressed,
        "fast peer was starved by the saturated peer: fast_handled = {}",
        fast_handled.load(Ordering::SeqCst)
    );

    // Release the parked slow handlers so teardown is clean.
    release_tx.close();
    listener_task.abort();
    manager_task.abort();
    Ok(())
}
