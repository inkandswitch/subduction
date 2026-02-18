//! Tests for Subduction initialization.

use super::common::{TestSpawn, new_test_subduction, test_keyhive, test_signer};
use crate::{
    connection::{nonce_cache::NonceCache, test_utils::MockConnection},
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
};
use future_form::Sendable;
use sedimentree_core::commit::CountLeadingZeroBytes;
use subduction_keyhive::storage::MemoryKeyhiveStorage;

#[tokio::test]
async fn test_new_creates_empty_subduction() {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;
    let keyhive = test_keyhive().await;

    let (subduction, _listener_fut, _actor_fut) =
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
            keyhive,
            MemoryKeyhiveStorage::default(),
        )
        .await
        .expect("failed to create Subduction");

    // Verify initial state via async runtime would be needed,
    // but we can at least verify construction doesn't panic
    assert!(!subduction.abort_manager_handle.is_aborted());
    assert!(!subduction.abort_listener_handle.is_aborted());
}

#[tokio::test]
async fn test_new_has_empty_sedimentrees() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction().await;

    let ids = subduction.sedimentree_ids().await;
    assert!(ids.is_empty());
}

#[tokio::test]
async fn test_new_has_no_connections() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction().await;

    let peer_ids = subduction.connected_peer_ids().await;
    assert!(peer_ids.is_empty());
}
