//! Tests for connection management (register, unregister, disconnect).

use super::common::new_test_subduction;
use crate::connection::test_utils::MockConnection;
use crate::peer::id::PeerId;
use testresult::TestResult;

#[tokio::test]
async fn test_peer_ids_returns_empty_initially() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let peer_ids = subduction.connected_peer_ids().await;
    assert_eq!(peer_ids.len(), 0);
}

#[tokio::test]
async fn test_register_adds_connection() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let conn = MockConnection::new();
    let fresh = subduction.register(conn).await?;

    assert!(fresh);
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_register_same_connection_twice_returns_false() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let conn = MockConnection::new();
    let fresh1 = subduction.register(conn).await?;
    let fresh2 = subduction.register(conn).await?;

    assert!(fresh1);
    assert!(!fresh2);
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_unregister_removes_connection() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let conn = MockConnection::new();
    let _fresh = subduction.register(conn).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    let removed = subduction.unregister(&conn).await;
    assert_eq!(removed, Some(true));
    assert_eq!(subduction.connected_peer_ids().await.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_unregister_nonexistent_returns_false() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    // Unregister a connection that was never registered
    let conn = MockConnection::with_peer_id(PeerId::new([99u8; 32]));
    let removed = subduction.unregister(&conn).await;
    assert_eq!(removed, None);
}

#[tokio::test]
async fn test_register_different_peers_increases_count() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let conn1 = MockConnection::with_peer_id(PeerId::new([1u8; 32]));
    let conn2 = MockConnection::with_peer_id(PeerId::new([2u8; 32]));

    subduction.register(conn1).await?;
    subduction.register(conn2).await?;

    assert_eq!(subduction.connected_peer_ids().await.len(), 2);
    Ok(())
}

#[tokio::test]
async fn test_disconnect_removes_connection() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let conn = MockConnection::new();
    let _fresh = subduction.register(conn).await?;

    let removed = subduction.disconnect(&conn).await?;
    assert!(removed);
    assert_eq!(subduction.connected_peer_ids().await.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_disconnect_nonexistent_returns_false() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let conn = MockConnection::with_peer_id(PeerId::new([99u8; 32]));
    let removed = subduction.disconnect(&conn).await?;
    assert!(!removed);

    Ok(())
}

#[tokio::test]
async fn test_disconnect_all_removes_all_connections() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let conn1 = MockConnection::with_peer_id(PeerId::new([1u8; 32]));
    let conn2 = MockConnection::with_peer_id(PeerId::new([2u8; 32]));

    subduction.register(conn1).await?;
    subduction.register(conn2).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 2);

    subduction.disconnect_all().await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_disconnect_from_peer_removes_specific_peer() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let peer_id1 = PeerId::new([1u8; 32]);
    let peer_id2 = PeerId::new([2u8; 32]);
    let conn1 = MockConnection::with_peer_id(peer_id1);
    let conn2 = MockConnection::with_peer_id(peer_id2);

    subduction.register(conn1).await?;
    subduction.register(conn2).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 2);

    let removed = subduction.disconnect_from_peer(&peer_id1).await?;
    assert!(removed);
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);
    assert!(!subduction.connected_peer_ids().await.contains(&peer_id1));
    assert!(subduction.connected_peer_ids().await.contains(&peer_id2));

    Ok(())
}

#[tokio::test]
async fn test_disconnect_from_nonexistent_peer_returns_false() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let peer_id = PeerId::new([1u8; 32]);
    #[allow(clippy::unwrap_used)]
    let removed = subduction.disconnect_from_peer(&peer_id).await.unwrap();
    assert!(!removed);
}
