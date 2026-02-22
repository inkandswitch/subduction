//! Tests for Subduction initialization.

use future_form::Sendable;
use sedimentree_core::commit::CountLeadingZeroBytes;
use subduction_core::{
    connection::{
        nonce_cache::NonceCache,
        test_utils::{MockConnection, TestSpawn, new_test_subduction, test_signer},
    },
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
};

#[test]
fn test_new_creates_empty_subduction() {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    // Verify construction doesn't panic
    let (_subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
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
