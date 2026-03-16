//! Tests for Subduction initialization.

use subduction_core::connection::test_utils::new_test_subduction;

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
