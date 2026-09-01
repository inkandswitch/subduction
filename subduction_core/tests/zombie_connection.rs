//! `add_connection` must not leave "zombie" connections behind when the
//! connection-manager handoff fails.
//!
//! A connection registered in the core `connections` map without a
//! `connection_loop` reader is invisible-dead: nobody consumes its inbound
//! messages, and — because closure events are emitted by the reader — nothing
//! will ever remove it from the map.

#![allow(clippy::panic)]

use std::sync::Arc;

use future_form::Sendable;
use subduction_core::{
    connection::{
        id::ConnectionId,
        message::SyncMessage,
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn, test_signer},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
};
use testresult::TestResult;

/// If the manager is gone, `add_connection` errors and rolls back the
/// registration; the peer must not remain tracked without a reader.
#[tokio::test]
async fn failed_manager_handoff_rolls_back_registration() -> TestResult {
    let (subduction, _handler, _listener_fut, actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    // Kill the manager before it ever runs: dropping the future drops the
    // command-channel receiver, so the handoff send fails immediately.
    drop(actor_fut);

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, _handle) = ChannelMockConnection::new_with_handle(peer_id);

    let result = subduction.add_connection(conn.authenticated()).await;
    assert!(
        result.is_err(),
        "add_connection must surface the dead manager, got: {result:?}"
    );

    assert!(
        !subduction.connected_peer_ids().await.contains(&peer_id),
        "a connection that never got a reader must not stay tracked \
         (zombie: nothing will ever emit its closure event)"
    );

    // The rollback must be complete: the multiplexer created alongside the
    // connection has to go too, not just the map entry.
    assert_eq!(
        subduction.mux_count(&peer_id).await,
        0,
        "rollback must detach the connection's multiplexer"
    );

    Ok(())
}

/// `on_reconnect_success` has the same register-before-handoff shape as
/// `add_connection` and must roll back the same way when the manager is
/// gone.
#[tokio::test]
async fn failed_reconnect_handoff_rolls_back_registration() -> TestResult {
    let (subduction, _handler, _listener_fut, actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    drop(actor_fut);

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, _handle) = ChannelMockConnection::new_with_handle(peer_id);

    // Sentinel ID: the manager is dead, so no allocation can collide, but
    // use an unallocatable value in case this test ever grows a manager.
    let result = subduction
        .on_reconnect_success(ConnectionId::new(usize::MAX), conn.authenticated())
        .await;
    assert!(
        result.is_err(),
        "on_reconnect_success must surface the dead manager"
    );

    assert!(
        !subduction.connected_peer_ids().await.contains(&peer_id),
        "a reconnect that never got a reader must not stay tracked"
    );
    assert_eq!(
        subduction.mux_count(&peer_id).await,
        0,
        "reconnect rollback must detach the connection's multiplexer"
    );

    Ok(())
}
