//! Tests for Subduction initialization.

use super::common::{TestSpawn, new_test_subduction, test_signer};
use crate::{
    Subduction,
    connection::{nonce_cache::NonceCache, test_utils::MockConnection},
    policy::OpenPolicy,
    sharded_map::ShardedMap,
    storage::MemoryStorage,
};
use future_form::Sendable;
use sedimentree_core::commit::CountLeadingZeroBytes;

#[test]
fn test_new_creates_empty_subduction() {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

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
        );

    // Verify initial state via async runtime would be needed,
    // but we can at least verify construction doesn't panic
    assert!(!subduction.abort_manager_handle.is_aborted());
    assert!(!subduction.abort_listener_handle.is_aborted());
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
