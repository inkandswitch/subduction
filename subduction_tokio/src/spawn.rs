//! [`Spawn`] implementations backed by Tokio.

use future_form::Sendable;
use futures::{
    future::BoxFuture,
    stream::{AbortHandle, Abortable},
};
use subduction_core::spawn::Spawn;
use tokio_util::task::TaskTracker;

/// Detached `tokio::spawn` spawner.
///
/// Tasks are controllable only via the returned [`AbortHandle`]; nothing
/// records that they exist, so nobody can wait for them. Use
/// [`TrackedTokioSpawn`] if you need to await completion — a
/// [`TokioSubduction`](crate::node::TokioSubduction) always does.
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

/// Spawner that registers each task with a [`TaskTracker`] so the owner can
/// deterministically await completion.
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
