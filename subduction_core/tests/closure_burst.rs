//! The connection manager must survive a mass-disconnect burst even when the
//! listen loop cannot drain closure notifications.
//!
//! Each exiting `connection_loop`'s cleanup sends into the bounded
//! `connection_closed` channel. If that send parks while holding the
//! manager's `tasks` mutex, the command loop deadlocks acquiring it and no
//! connection can ever be added again.

#![allow(clippy::panic)]

use core::time::Duration;
use std::sync::Arc;

use future_form::Sendable;
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn, test_signer},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
};
use testresult::TestResult;

/// Closing far more connections than the `connection_closed` channel can
/// buffer must not deadlock the manager: a fresh connection added
/// afterwards still gets a reader that consumes its inbound messages.
///
/// The listener future is deliberately not spawned; it stands in for a
/// listener too busy (or starved) to drain closure events.
///
/// `current_thread` is load-bearing, not cosmetic: the sleeps below rely on
/// single-threaded quiescence (a sleep yields until all spawned tasks are
/// parked) so the burst's cleanup tasks deterministically reach their park
/// points before the probes run.
#[tokio::test(flavor = "current_thread")]
async fn manager_survives_closure_burst_without_listener_drain() -> TestResult {
    // Must exceed the closure-channel capacity or the test silently stops
    // exercising the wedge.
    const BURST: usize = subduction_core::subduction::CONNECTION_CLOSED_CHANNEL_CAPACITY + 8;

    let (subduction, _handler, _listener_fut, actor_fut) =
        SubductionBuilder::<_, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let actor_task = tokio::spawn(actor_fut);

    // Connect a burst of peers and wait for the manager to spawn their loops.
    let mut handles = Vec::with_capacity(BURST);
    for i in 0..BURST {
        #[allow(clippy::cast_possible_truncation)]
        let peer_id = PeerId::new([i as u8 + 1; 32]);
        let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
        subduction.add_connection(conn.authenticated()).await?;
        handles.push(handle);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Every reader errors out at once. Each exiting connection_loop's
    // cleanup pushes into the bounded `connection_closed` channel; with no
    // listener draining it, the channel fills and the rest park.
    for handle in &handles {
        handle.inbound_tx.close();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The manager must still be alive: fresh connections get
    // connection_loops, observable as their inbound queues being drained.
    //
    // Two probes, asserting on the second: the command loop spawns a
    // connection's reader before it acquires the `tasks` mutex, so a
    // deadlocked manager still gives the first post-burst `Add` a reader —
    // it's the next command that is never processed.
    let sacrificial_peer = PeerId::new([0xFE; 32]);
    let (conn, _sacrificial_handle) = ChannelMockConnection::new_with_handle(sacrificial_peer);
    subduction.add_connection(conn.authenticated()).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let fresh_peer = PeerId::new([0xFF; 32]);
    let (conn, fresh_handle) = ChannelMockConnection::new_with_handle(fresh_peer);
    subduction.add_connection(conn.authenticated()).await?;

    fresh_handle
        .inbound_tx
        .send(SyncMessage::HeadsUpdate {
            id: SedimentreeId::new([7u8; 32]),
            heads: RemoteHeads::default(),
        })
        .await?;

    // Poll rather than sleep: a live manager drains the message almost
    // immediately; a deadlocked one never does. Generous deadline: the pass
    // path exits in milliseconds, so this only bounds the failure case.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if fresh_handle.inbound_tx.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "manager deadlocked: connection added after the closure burst \
             never got a reader (its inbound message was never consumed)"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    actor_task.abort();
    Ok(())
}
