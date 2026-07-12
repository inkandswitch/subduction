//! A [`Spawn`] implementation that *collects* futures instead of detaching
//! them, so the Durable Object can drive them to completion inline.
//!
//! # Why not a real spawner?
//!
//! On native/browser runtimes, [`SyncHandler`] fans subscription pushes out on
//! a detached task (`spawner.spawn(fanout.run())`) so a slow subscriber can't
//! stall inbound dispatch. That model assumes the process keeps running after
//! the handler returns.
//!
//! A hibernatable Durable Object breaks that assumption: once `websocket_message`
//! returns and the microtask queue drains, the runtime may **evict the isolate**.
//! Any future still parked in `wasm_bindgen_futures::spawn_local` would simply
//! never run again — the subscriber push would be silently dropped.
//!
//! [`CollectingSpawner`] instead pushes each future into a shared queue. The
//! Durable Object drains and awaits that queue *before* returning from the
//! message handler (see [`crate::durable_object`]), so every send completes
//! while the isolate is guaranteed alive. It is a deliberate trade: fan-out no
//! longer runs off the dispatch path, but a WebSocket `send` is a cheap enqueue,
//! so the cost is negligible and correctness under hibernation is preserved.
//!
//! [`SyncHandler`]: subduction_core::handler::sync::SyncHandler

use std::{cell::RefCell, rc::Rc};

use future_form::Local;
use futures::{
    future::{Abortable, LocalBoxFuture},
    stream::AbortHandle,
};
use subduction_core::spawn::Spawn;

/// A single-threaded spawner that queues futures for later inline execution.
///
/// Cloneable: every clone shares the same underlying queue (via [`Rc`]), so the
/// clone handed to [`SyncHandler`] and the one held by the Durable Object see
/// the same pending work.
///
/// [`SyncHandler`]: subduction_core::handler::sync::SyncHandler
#[derive(Clone, Default)]
pub struct CollectingSpawner {
    queue: Rc<RefCell<Vec<LocalBoxFuture<'static, ()>>>>,
}

impl CollectingSpawner {
    /// Create an empty spawner.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Remove and return everything queued since the last drain.
    #[must_use]
    pub fn drain(&self) -> Vec<LocalBoxFuture<'static, ()>> {
        std::mem::take(&mut self.queue.borrow_mut())
    }
}

impl Spawn<Local> for CollectingSpawner {
    fn spawn(&self, fut: LocalBoxFuture<'static, ()>) -> AbortHandle {
        // Wire the returned handle to the queued future so the `Spawn`
        // contract holds ("the handle aborts the task"): wrapping in
        // `Abortable` means that if a caller aborts before the Durable Object
        // drains the queue, the future short-circuits to a no-op instead of
        // sending. Our own fan-out never aborts, but honouring the contract
        // avoids surprising a future caller that relies on cancellation.
        let (handle, reg) = AbortHandle::new_pair();
        let abortable = Abortable::new(fut, reg);
        self.queue.borrow_mut().push(Box::pin(async move {
            let _ = abortable.await;
        }));
        handle
    }
}
