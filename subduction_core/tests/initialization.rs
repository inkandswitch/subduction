//! Tests for Subduction initialization.

use std::sync::Arc;

use future_form::Sendable;
use subduction_core::{
    connection::test_utils::{MockConnection, TestSpawn, new_test_subduction, test_signer},
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::SubductionBuilder,
};

#[test]
fn test_new_creates_empty_subduction() {
    // Verify construction doesn't panic
    let (_subduction, _handler, _listener_fut, _actor_fut) =
        SubductionBuilder::<_, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TestSpawn)
            .build::<Sendable, MockConnection>();
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
