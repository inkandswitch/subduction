//! Internal task ownership. Registration is serialized with forced shutdown.
use crate::{Error, ErrorKind};
use futures::FutureExt;
use std::{
    collections::BTreeMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Debug, Default)]
struct Registry {
    next: u64,
    sealed: bool,
    tasks: BTreeMap<u64, tokio::task::AbortHandle>,
}

#[derive(Debug)]
struct Inner {
    registry: Mutex<Registry>,
    completed: Mutex<Vec<u64>>,
    runtime: tokio::runtime::Handle,
    tracker: TaskTracker,
    stop: CancellationToken,
    failure: watch::Sender<Option<Error>>,
}

#[derive(Debug, Clone)]
pub(crate) struct Tasks(Arc<Inner>);
impl Tasks {
    pub(crate) fn new(stop: CancellationToken) -> Self {
        Self(Arc::new(Inner {
            registry: Mutex::default(),
            completed: Mutex::default(),
            runtime: tokio::runtime::Handle::current(),
            tracker: TaskTracker::new(),
            stop,
            failure: watch::channel(None).0,
        }))
    }
    pub(crate) fn failure(&self) -> Option<Error> {
        self.0.failure.borrow().clone()
    }
    pub(crate) fn fail(&self, error: Error) {
        self.0.failure.send_if_modified(|slot| {
            if slot.is_none() {
                *slot = Some(error);
                true
            } else {
                false
            }
        });
        self.0.stop.cancel();
    }
    pub(crate) fn spawn(
        &self,
        name: &'static str,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> bool {
        let mut registry = self
            .0
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry.sealed {
            return false;
        }
        // Only reap IDs whose completion raced a registry lock. Admission
        // work is proportional to completions, not the number of live tasks.
        for id in self
            .0
            .completed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
        {
            registry.tasks.remove(&id);
        }
        let id = registry.next;
        registry.next += 1;
        let guard = TaskGuard {
            tasks: self.clone(),
            id,
        };
        let tasks = self.clone();
        let future = self.0.tracker.track_future(async move {
            let _guard = guard;
            if AssertUnwindSafe(future).catch_unwind().await.is_err() {
                tasks.fail(Error::new(
                    ErrorKind::Background,
                    format!("{name} panicked"),
                ));
            }
        });
        let handle = self.0.runtime.spawn(future);
        registry.tasks.insert(id, handle.abort_handle());
        true
    }
    pub(crate) fn seal(&self) {
        self.0
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sealed = true;
    }
    pub(crate) fn abort_all(&self) {
        let mut registry = self
            .0
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.sealed = true;
        for handle in registry.tasks.values() {
            handle.abort();
        }
    }
    pub(crate) async fn wait(&self) {
        self.0.tracker.close();
        self.0.tracker.wait().await;
    }
    pub(crate) fn len(&self) -> usize {
        self.0.tracker.len()
    }
}
struct TaskGuard {
    tasks: Tasks,
    id: u64,
}
impl Drop for TaskGuard {
    fn drop(&mut self) {
        // Spawning on a shutting-down executor can synchronously drop the
        // future while spawn still holds the registry lock. Never reenter it.
        // The completed-ID lock is never held across spawn or task drops.
        if let Ok(mut registry) = self.tasks.0.registry.try_lock() {
            registry.tasks.remove(&self.id);
        } else {
            self.tasks
                .0
                .completed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn spawn_after_executor_shutdown_does_not_deadlock() -> Result<(), std::io::Error> {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let tasks = {
            let _entered = runtime.enter();
            Tasks::new(CancellationToken::new())
        };
        drop(runtime);
        for _ in 0..100 {
            assert!(tasks.spawn("dead executor", std::future::pending()));
            assert_eq!(tasks.len(), 0);
            // Synchronous drop queues this ID before its handle is inserted.
            // Each later spawn must reap it without scanning live handles.
            assert_eq!(
                tasks
                    .0
                    .registry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .tasks
                    .len(),
                1
            );
            assert_eq!(
                tasks
                    .0
                    .completed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len(),
                1
            );
        }
        tasks.abort_all();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_completions_are_reaped_without_losing_live_tasks()
    -> Result<(), Box<dyn std::error::Error>> {
        let tasks = Tasks::new(CancellationToken::new());
        for _ in 0..16 {
            assert!(tasks.spawn("long lived", std::future::pending()));
        }
        let mut workers = Vec::new();
        for _ in 0..4 {
            let tasks = tasks.clone();
            workers.push(tokio::spawn(async move {
                for _ in 0..100 {
                    assert!(tasks.spawn("short lived", async {}));
                    tokio::task::yield_now().await;
                }
            }));
        }
        for worker in workers {
            worker.await?;
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while tasks.len() != 16 {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        assert!(tasks.spawn("reap completions", std::future::pending()));
        {
            let registry = tasks
                .0
                .registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(registry.tasks.len(), 17);
            for id in 0..16 {
                assert!(registry.tasks.contains_key(&id));
            }
        }
        tasks.abort_all();
        tokio::time::timeout(Duration::from_secs(5), tasks.wait()).await?;
        assert_eq!(tasks.len(), 0);
        assert!(!tasks.spawn("sealed", std::future::pending()));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::panic)]
    async fn worker_panic_stops_runtime_and_retains_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let stop = CancellationToken::new();
        let tasks = Tasks::new(stop.clone());
        assert!(tasks.spawn("panic injection", async {
            panic!("injected");
        }));
        tokio::time::timeout(Duration::from_secs(1), stop.cancelled()).await?;
        tasks.wait().await;
        assert_eq!(
            tasks.failure().map(|e| e.kind()),
            Some(ErrorKind::Background)
        );
        assert_eq!(tasks.len(), 0);
        tasks.seal();
        assert!(!tasks.spawn("late", std::future::pending()));
        assert_eq!(tasks.len(), 0);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ManagedSpawn(pub(crate) Tasks);
impl subduction_core::spawn::Spawn<future_form::Sendable> for ManagedSpawn {
    fn spawn(
        &self,
        future: futures::future::BoxFuture<'static, ()>,
    ) -> futures::future::AbortHandle {
        let (handle, reg) = futures::future::AbortHandle::new_pair();
        self.0.spawn("core worker", async move {
            let _result = futures::future::Abortable::new(future, reg).await;
        });
        handle
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Timer;
impl subduction_core::timeout::Timeout<future_form::Sendable> for Timer {
    fn timeout<'a, T: 'a>(
        &'a self,
        duration: std::time::Duration,
        future: futures::future::BoxFuture<'a, T>,
    ) -> futures::future::BoxFuture<'a, Result<T, subduction_core::timeout::TimedOut>> {
        Box::pin(async move {
            tokio::time::timeout(duration, future)
                .await
                .map_err(|_| subduction_core::timeout::TimedOut)
        })
    }
}
