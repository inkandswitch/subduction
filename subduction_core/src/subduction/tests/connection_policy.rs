//! Tests for connection authorization policy.

use super::common::new_test_subduction;
use crate::{peer::id::PeerId, policy::connection::ConnectionPolicy};

#[tokio::test]
async fn test_allowed_to_connect_allows_all_peers() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let peer_id = PeerId::new([1u8; 32]);
    let result = subduction.authorize_connect(peer_id).await;
    assert!(result.is_ok());
}

// TODO also test when the policy says no
