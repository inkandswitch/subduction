//! Common test utilities and helpers.

use crate::{
    Subduction,
    connection::{manager::Spawn, nonce_cache::NonceCache, test_utils::MockConnection},
    crypto::signer::MemorySigner,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
};
use alloc::sync::Arc;
use future_form::Sendable;
use futures::future::{AbortHandle, BoxFuture, LocalBoxFuture};
use sedimentree_core::commit::CountLeadingZeroBytes;

/// Create a test signer with deterministic key bytes.
pub(super) fn test_signer() -> MemorySigner {
    MemorySigner::from_bytes(&[42u8; 32])
}

/// A spawner that doesn't actually spawn (for tests that don't need task execution).
pub(super) struct TestSpawn;

impl Spawn<Sendable> for TestSpawn {
    fn spawn(&self, _fut: BoxFuture<'static, ()>) -> AbortHandle {
        let (handle, _reg) = AbortHandle::new_pair();
        handle
    }
}

impl Spawn<future_form::Local> for TestSpawn {
    fn spawn(&self, _fut: LocalBoxFuture<'static, ()>) -> AbortHandle {
        let (handle, _reg) = AbortHandle::new_pair();
        handle
    }
}

/// A spawner that uses `tokio::spawn` for tests that need actual task execution.
pub(super) struct TokioSpawn;

impl Spawn<Sendable> for TokioSpawn {
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> AbortHandle {
        use futures::future::Abortable;
        let (handle, reg) = AbortHandle::new_pair();
        tokio::spawn(Abortable::new(fut, reg));
        handle
    }
}

impl Spawn<future_form::Local> for TokioSpawn {
    fn spawn(&self, fut: LocalBoxFuture<'static, ()>) -> AbortHandle {
        use futures::future::Abortable;
        let (handle, reg) = AbortHandle::new_pair();
        tokio::task::spawn_local(Abortable::new(fut, reg));
        handle
    }
}

/// Create a new Subduction instance for testing with default settings.
#[allow(clippy::type_complexity)]
pub(super) fn new_test_subduction() -> (
    Arc<
        Subduction<
            'static,
            Sendable,
            MemoryStorage,
            MockConnection,
            OpenPolicy,
            MemorySigner,
            CountLeadingZeroBytes,
        >,
    >,
    impl core::future::Future<Output = Result<(), futures::future::Aborted>>,
    impl core::future::Future<Output = Result<(), futures::future::Aborted>>,
) {
    Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
        None,
        test_signer(),
        MemoryStorage::new(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TestSpawn,
    )
}
