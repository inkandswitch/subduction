//! Tokio implementations for [`WebSocket`][crate::websocket::WebSocket]s.

use core::time::Duration;

use future_form::Sendable;
use futures::{
    FutureExt,
    future::BoxFuture,
    stream::{AbortHandle, Abortable},
};
use subduction_core::connection::manager::Spawn;
use tokio_util::task::TaskTracker;

use subduction_core::timeout::{TimedOut, Timeout};

#[cfg(feature = "tokio_client_any")]
pub mod client;

#[cfg(feature = "tokio_server_any")]
pub mod server;

#[cfg(feature = "tokio_server_any")]
pub mod unified;

/// A spawner that uses tokio to spawn detached tasks.
///
/// Each spawned task runs to completion on the tokio runtime, but the
/// returned [`AbortHandle`] is the only way to control it. The
/// underlying `JoinHandle` is discarded, so callers cannot await
/// completion. This is suitable for long-lived servers that don't
/// need deterministic teardown of their connection tasks.
///
/// For short-lived peers (tests, benchmarks) that need to release
/// `Arc<WebSocket>` and `Arc<Subduction>` references promptly between
/// iterations, prefer [`TrackedTokioSpawn`] — it registers each
/// spawned future with a [`TaskTracker`] so the owner can await every
/// task to completion before allocating fresh resources.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioSpawn;

impl Spawn<Sendable> for TokioSpawn {
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> AbortHandle {
        let (handle, reg) = AbortHandle::new_pair();
        tokio::spawn(async move {
            let _ = Abortable::new(fut, reg).await;
        });
        handle
    }
}

/// A spawner that registers every spawned task with a [`TaskTracker`].
///
/// Use this when you need to deterministically wait for every task
/// spawned through the [`Subduction`][subduction_core::subduction::Subduction]
/// connection manager — most commonly per-iteration bench teardown
/// where leftover `connection_loop` tasks parked on a still-alive
/// WebSocket `recv()` retain `Arc<WebSocket>` and pin everything in
/// place.
///
/// The contained tracker is `Clone`able (it's reference-counted
/// internally), so the same tracker can be shared with the
/// [`crate::tokio::server::TokioWebSocketServer`] to await both
/// Subduction-internal tasks (connection loops) and server-internal
/// tasks (accept loop, per-WS listener/sender) under a single
/// [`TaskTracker::wait`] call.
#[derive(Debug, Clone, Default)]
pub struct TrackedTokioSpawn {
    tracker: TaskTracker,
}

impl TrackedTokioSpawn {
    /// Create a spawner backed by the given tracker.
    #[must_use]
    pub const fn new(tracker: TaskTracker) -> Self {
        Self { tracker }
    }

    /// Return a clone of the underlying tracker.
    ///
    /// Useful when the owner of this spawner wants to share the
    /// tracker with other systems (e.g., a `TokioWebSocketServer`).
    #[must_use]
    pub fn tracker(&self) -> TaskTracker {
        self.tracker.clone()
    }
}

impl Spawn<Sendable> for TrackedTokioSpawn {
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> AbortHandle {
        let (handle, reg) = AbortHandle::new_pair();
        self.tracker.spawn(async move {
            let _ = Abortable::new(fut, reg).await;
        });
        handle
    }
}

/// Tokio-backed timeout wrapper.
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeoutTokio;

impl Timeout<Sendable> for TimeoutTokio {
    fn timeout<'a, T: 'a>(
        &'a self,
        dur: Duration,
        fut: BoxFuture<'a, T>,
    ) -> BoxFuture<'a, Result<T, TimedOut>> {
        async move {
            match tokio::time::timeout(dur, fut).await {
                Ok(v) => Ok(v),
                Err(_elapsed) => Err(TimedOut),
            }
        }
        .boxed()
    }
}
