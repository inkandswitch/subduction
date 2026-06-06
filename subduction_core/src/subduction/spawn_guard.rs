//! Abort-on-drop guard for detached spawned tasks.
//!
//! When work is fanned out via [`Spawn::spawn`], the runtime drives each task to
//! completion independently and the driving future joins them only through a
//! results channel. If that driving future is dropped before the join completes
//! — e.g. a cancelled/GC'd JS promise on the wasm `Local` runtime — the spawned
//! tasks would otherwise keep running detached, each holding an `Arc` clone of
//! the node, until they self-terminate.
//!
//! [`AbortOnDrop`] restores structured-concurrency semantics: it retains the
//! [`AbortHandle`] of every spawned task and aborts any that are still running
//! when it is dropped. On the normal path the join loop runs to completion and
//! the guard's surviving handles all point at finished tasks, so the final drop
//! aborts nothing.
//!
//! [`Spawn::spawn`]: crate::connection::manager::Spawn::spawn

use alloc::vec::Vec;

use futures::stream::AbortHandle;

/// Retains [`AbortHandle`]s and aborts any unfinished tasks when dropped.
///
/// Aborting an already-finished task is a no-op, so dropping the guard after a
/// successful join is harmless.
#[derive(Debug, Default)]
pub(crate) struct AbortOnDrop {
    handles: Vec<AbortHandle>,
}

impl AbortOnDrop {
    /// Create an empty guard.
    pub(crate) const fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    /// Track a spawned task's [`AbortHandle`] so it is cancelled if this guard
    /// is dropped before the task completes.
    pub(crate) fn push(&mut self, handle: AbortHandle) {
        self.handles.push(handle);
    }

    /// Drop the handles of tasks already known to have completed.
    ///
    /// An [`AbortHandle`] does not expose task completion, so a long-lived
    /// caller must prune at a point where it knows no tracked task is still
    /// running — typically when its outstanding count returns to zero. Calling
    /// this keeps the handle vector bounded across an unbounded task stream
    /// (e.g. the listen loop) without aborting anything still in flight.
    pub(crate) fn clear(&mut self) {
        self.handles.clear();
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}
