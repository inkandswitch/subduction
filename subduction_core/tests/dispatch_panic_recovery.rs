//! A panicking dispatch task must not wedge the listener.
//!
//! The listener bounds in-flight dispatch tasks with a counter and stops
//! pulling new messages once it reaches `MAX_INFLIGHT_DISPATCH`. Each spawned
//! task releases its slot through a completion guard that fires on drop — on the
//! normal path *and* on unwind. So even a flood of panicking handler tasks
//! cannot ratchet the in-flight count up permanently, and the listener keeps
//! processing.
//!
//! This test drives more than `MAX_INFLIGHT_DISPATCH` panicking messages
//! through a real listener, then a normal message, and asserts the normal one
//! is still handled. Without the completion guard, the panics would never
//! release their slots, the count would pin at the cap, and the final message
//! would never be processed.

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

/// The sedimentree id whose `HeadsUpdate` makes the handler panic.
const POISON_ID: SedimentreeId = SedimentreeId::new([0xFFu8; 32]);

/// A handler that panics on a designated poison message and otherwise delegates
/// to the inner [`SyncHandler`], counting every message it handles to the end.
struct PanicHandler {
    inner: Arc<InnerHandler>,
    handled: Arc<AtomicUsize>,
}

impl subduction_core::handler::Handler<Sendable, Conn> for PanicHandler {
    type Message = SyncMessage;
    type HandlerError =
        <InnerHandler as subduction_core::handler::Handler<Sendable, Conn>>::HandlerError;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<Conn, Sendable>,
        message: Self::Message,
    ) -> <Sendable as future_form::FutureForm>::Future<'a, Result<(), Self::HandlerError>> {
        <Sendable as future_form::FutureForm>::from_future(async move {
            if matches!(&message, SyncMessage::HeadsUpdate { id, .. } if *id == POISON_ID) {
                panic!("poison message: simulated dispatch-task panic");
            }

            let result: Result<(), Self::HandlerError> = self.inner.handle(conn, message).await;
            self.handled.fetch_add(1, Ordering::SeqCst);
            result
        })
    }

    fn on_peer_disconnect(
        &self,
        peer: PeerId,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, ()> {
        self.inner.on_peer_disconnect(peer)
    }
}

impl RemoteHeadsNotifier for PanicHandler {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_dispatch_tasks_do_not_wedge_the_listener() -> TestResult {
    // Comfortably above the listener's `MAX_INFLIGHT_DISPATCH` (1024): without
    // the completion guard, this many un-released slots would pin the gate.
    const POISON_COUNT: usize = 1100;

    let handled = Arc::new(AtomicUsize::new(0));
    let handled_for_handler = Arc::clone(&handled);

    let (sd, listener, manager, ()) = SubductionBuilder::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build_composed::<Sendable, Conn, PanicHandler, ()>(|sync_handler| {
            (
                Arc::new(PanicHandler {
                    inner: sync_handler,
                    handled: handled_for_handler,
                }),
                (),
            )
        });

    let listener_task = tokio::spawn(listener);
    let manager_task = tokio::spawn(manager);

    let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
    sd.add_connection(conn.authenticated()).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Flood the listener with panicking messages. Each spawns a dispatch task
    // that panics; the runtime isolates the panic to that task.
    for _ in 0..POISON_COUNT {
        handle
            .inbound_tx
            .send(SyncMessage::HeadsUpdate {
                id: POISON_ID,
                heads: RemoteHeads::default(),
            })
            .await?;
    }

    // Then a single non-poison message whose handling we can observe.
    let normal_id = SedimentreeId::new([7u8; 32]);
    handle
        .inbound_tx
        .send(SyncMessage::HeadsUpdate {
            id: normal_id,
            heads: RemoteHeads::default(),
        })
        .await?;

    // With the completion guard, the panics release their in-flight slots, so
    // the listener never wedges and the normal message is handled to the end.
    let progressed = wait_for(Duration::from_secs(5), || {
        handled.load(Ordering::SeqCst) >= 1
    })
    .await;
    assert!(
        progressed,
        "listener stopped processing after panicking dispatch tasks: no normal \
         message was handled within the timeout (handled = {}); the in-flight \
         count was not released on panic",
        handled.load(Ordering::SeqCst)
    );

    listener_task.abort();
    manager_task.abort();
    Ok(())
}
