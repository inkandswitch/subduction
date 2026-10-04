//! Closing storage must interrupt initial-read preparation, not just delivery.
#![allow(clippy::expect_used)]

use crate::{
    connection::test_utils::{InstantTimeout, MockConnection, test_signer},
    policy::open::OpenPolicy,
    spawn::Spawn,
    storage::memory::MemoryStorage,
    stored::LoadError,
    subduction::{NodeTasks, builder::SubductionBuilder},
};
use alloc::sync::Arc;
use future_form::Sendable;
use futures::{
    FutureExt, StreamExt,
    future::{AbortHandle, Abortable, BoxFuture},
};
use sedimentree_core::id::SedimentreeId;

/// Let the test poll the actual node-owned stream task deterministically.
#[derive(Clone)]
struct ManualSpawn(async_channel::Sender<BoxFuture<'static, ()>>);
impl Spawn<Sendable> for ManualSpawn {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> AbortHandle {
        let (abort, registration) = AbortHandle::new_pair();
        assert!(
            self.0
                .try_send(alloc::boxed::Box::pin(async move {
                    let _ = Abortable::new(task, registration).await;
                }))
                .is_ok()
        );
        abort
    }
}

#[tokio::test]
async fn shutdown_and_driver_loss_interrupt_preparation_without_certifying_a_read() {
    for lose_driver in [false, true] {
        let (tasks, pending) = async_channel::unbounded();
        let (node, _handler, _listener, _manager) = SubductionBuilder::<_, _, _, _, _, _, 1>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(ManualSpawn(tasks))
            .timer(InstantTimeout)
            .build::<Sendable, MockConnection>();
        let id = SedimentreeId::new([93; 32]);
        let fetcher = node.storage.local_fetcher(id);
        let tracked = fetcher.coordination().track_mutation();
        let mut stream = Sendable::start_stored(node.clone(), id);
        let mut driver = pending.try_recv().expect("node-owned stream task");
        // The snapshot yields once, then waits in prepare_read for the mutation.
        assert!(driver.as_mut().now_or_never().is_none());
        assert!(driver.as_mut().now_or_never().is_none());
        assert!(stream.next().now_or_never().is_none());

        if lose_driver {
            drop(tracked);
        } else {
            // The mutation is still pending; its completion cannot wake us.
            node.storage.state.close();
        }
        assert!(driver.now_or_never().is_some(), "closure wakes the driver");
        // No StorageChecked (or other data) may precede the terminal error.
        assert!(matches!(
            stream.next().now_or_never(),
            Some(Some(Err(LoadError::Closed)))
        ));
        assert!(matches!(stream.next().now_or_never(), Some(None)));
    }
}
