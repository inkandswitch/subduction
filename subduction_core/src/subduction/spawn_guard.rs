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
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use alloc::sync::Arc;
    use core::{
        future::pending,
        sync::atomic::{AtomicBool, Ordering},
    };

    use futures::{FutureExt, future::Abortable};

    use super::*;

    /// Spawn an abortable task that runs `fut` to completion and flips `done`
    /// when (and only when) it finishes without being aborted. Returns the
    /// guard-trackable [`AbortHandle`] and a join handle to drive it.
    fn spawn_tracked<F>(fut: F, done: Arc<AtomicBool>) -> (AbortHandle, tokio::task::JoinHandle<()>)
    where
        F: core::future::Future<Output = ()> + Send + 'static,
    {
        let (handle, reg) = AbortHandle::new_pair();
        let task = Abortable::new(fut, reg).map(move |outcome| {
            // `Ok(())` means the inner future completed; `Err(Aborted)` means
            // the handle fired first.
            if outcome.is_ok() {
                done.store(true, Ordering::SeqCst);
            }
        });

        (handle, tokio::spawn(task))
    }

    /// Dropping the guard aborts a tracked task that is still running: it never
    /// reaches completion.
    #[tokio::test]
    async fn drop_aborts_pending_task() {
        let done = Arc::new(AtomicBool::new(false));

        // A task that never completes on its own, so the only way it can finish
        // is via the guard's abort.
        let (handle, join) = spawn_tracked(pending::<()>(), Arc::clone(&done));

        let mut guard = AbortOnDrop::new();
        guard.push(handle);

        // Let the task reach its pending await point, then drop the guard.
        tokio::task::yield_now().await;
        drop(guard);

        // The join resolves because the abort drove the task to its (aborted)
        // end; `done` must remain false because the inner future never
        // completed.
        join.await.expect("aborted task joins");
        assert!(
            !done.load(Ordering::SeqCst),
            "guard drop must abort the pending task, not let it complete"
        );
    }

    /// A guard with many tracked tasks aborts every one of them on drop.
    #[tokio::test]
    async fn drop_aborts_all_tracked_tasks() {
        const N: usize = 16;

        let mut guard = AbortOnDrop::new();
        let mut joins = alloc::vec::Vec::new();
        let dones: alloc::vec::Vec<_> = (0..N).map(|_| Arc::new(AtomicBool::new(false))).collect();

        for done in &dones {
            let (handle, join) = spawn_tracked(pending::<()>(), Arc::clone(done));
            guard.push(handle);
            joins.push(join);
        }

        tokio::task::yield_now().await;
        drop(guard);

        for join in joins {
            join.await.expect("aborted task joins");
        }
        assert!(
            dones.iter().all(|d| !d.load(Ordering::SeqCst)),
            "every tracked task must be aborted on guard drop"
        );
    }
}
