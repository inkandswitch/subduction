//! Tokio implementations for [`WebSocket`][crate::websocket::WebSocket]s.

use core::time::Duration;

use future_form::Sendable;
use futures::{
    FutureExt,
    future::BoxFuture,
    stream::{AbortHandle, Abortable},
};
use subduction_core::spawn::Spawn;
use tokio_util::task::TaskTracker;

use subduction_core::timeout::{TimedOut, Timeout};

#[cfg(feature = "tokio_client_any")]
pub mod client;

#[cfg(feature = "tokio_server_any")]
pub mod server;

#[cfg(feature = "tokio_server_any")]
pub mod unified;

/// Detached `tokio::spawn` spawner. Tasks are controllable only via
/// the returned [`AbortHandle`]; use [`TrackedTokioSpawn`] if you need
/// to await completion.
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

/// Spawner that registers each task with a [`TaskTracker`] so the
/// owner can deterministically await completion (e.g. per-iteration
/// bench teardown).
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

    /// Clone of the underlying tracker, for sharing with other owners.
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
