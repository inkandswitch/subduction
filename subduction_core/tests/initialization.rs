//! Tests for Subduction initialization.

use std::sync::Arc;

use async_lock::Mutex;
use future_form::Sendable;
use sedimentree_core::{collections::Map, commit::CountLeadingZeroBytes};
use subduction_core::{
    connection::{
        nonce_cache::NonceCache,
        test_utils::{MockConnection, TestSpawn, new_test_subduction, test_signer},
    },
    handler::sync::SyncHandler,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::{memory::MemoryStorage, powerbox::StoragePowerbox},
    subduction::{
        Subduction,
        pending_blob_requests::{DEFAULT_MAX_PENDING_BLOB_REQUESTS, PendingBlobRequests},
    },
};

#[test]
fn test_new_creates_empty_subduction() {
    let sedimentrees = Arc::new(ShardedMap::with_key(0, 0));
    let connections = Arc::new(Mutex::new(Map::new()));
    let subscriptions = Arc::new(Mutex::new(Map::new()));
    let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
    let pending = Arc::new(Mutex::new(PendingBlobRequests::new(
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    )));

    let handler = Arc::new(SyncHandler::new(
        sedimentrees.clone(),
        connections.clone(),
        subscriptions.clone(),
        storage.clone(),
        pending.clone(),
        CountLeadingZeroBytes,
    ));

    // Verify construction doesn't panic
    let (_subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            handler,
            None,
            test_signer(),
            sedimentrees,
            connections,
            subscriptions,
            storage,
            pending,
            NonceCache::default(),
            CountLeadingZeroBytes,
            TestSpawn,
        );
}

#[tokio::test]
async fn test_new_has_empty_sedimentrees() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let ids = subduction.sedimentree_ids().await;
    assert!(ids.is_empty());
}

#[tokio::test]
async fn test_new_has_no_connections() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let peer_ids = subduction.connected_peer_ids().await;
    assert!(peer_ids.is_empty());
}
