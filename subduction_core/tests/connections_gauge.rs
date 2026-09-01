//! The `connections_active` gauge is set from the connection map itself,
//! so a missed event cannot make it drift: the next refresh heals it.
//!
//! Runs under plain `block_on` with a thread-local `DebuggingRecorder`:
//! nothing here needs a running task (the manager's command channel
//! buffers; `TestSpawn` discards futures).
//!
//! Each assertion follows a deliberate poison, proving the mutation's
//! refresh *healed* the gauge rather than deltas happening to line up.

#![allow(clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use future_form::Sendable;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use subduction_core::{
    connection::{
        id::ConnectionId,
        message::SyncMessage,
        test_utils::{ChannelMockConnection, InstantTimeout, TestSpawn, test_signer},
    },
    metrics::names,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
};

fn gauge(snapshotter: &Snapshotter) -> Option<f64> {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, value)| {
            let (_, key) = key.into_parts();
            if key.name() != names::CONNECTIONS_ACTIVE {
                return None;
            }
            match value {
                DebugValue::Gauge(n) => Some(n.into_inner()),
                DebugValue::Counter(_) | DebugValue::Histogram(_) => None,
            }
        })
}

/// The gauge is healed to map truth by the refresh in every
/// connection-map mutation: `add_connection`, `on_reconnect_success`,
/// `remove_connection`, `disconnect`, `disconnect_from_peer`, and
/// `disconnect_all`.
#[test]
fn connections_gauge_healed_by_every_mutation() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    metrics::with_local_recorder(&recorder, || {
        futures::executor::block_on(async {
            let (subduction, _handler, _listener_fut, _actor_fut) =
                SubductionBuilder::<_, _, _, _, _, _, 256>::new()
                    .signer(test_signer())
                    .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
                    .spawner(TestSpawn)
                    .timer(InstantTimeout)
                    .build::<Sendable, ChannelMockConnection<SyncMessage>>();

            let peer_a = PeerId::new([1u8; 32]);
            let peer_b = PeerId::new([2u8; 32]);
            let poison = || subduction_core::metrics::set_connections_active(999);
            let conn = |peer| {
                ChannelMockConnection::new_with_handle(peer)
                    .0
                    .authenticated()
            };

            // add_connection
            let conn_a = conn(peer_a);
            poison();
            subduction
                .add_connection(conn_a.clone())
                .await
                .expect("add a");
            assert_eq!(gauge(&snapshotter), Some(1.0), "add_connection heals");

            // on_reconnect_success (the manager channel buffers the ReAdd;
            // the sentinel ID is unallocatable, so a future edit spawning
            // the manager cannot model an ID collision)
            let conn_b = conn(peer_b);
            poison();
            subduction
                .on_reconnect_success(ConnectionId::new(usize::MAX), conn_b.clone())
                .await
                .expect("readd b");
            assert_eq!(gauge(&snapshotter), Some(2.0), "on_reconnect_success heals");

            // remove_connection
            poison();
            subduction.remove_connection_for_test(&conn_b).await;
            assert_eq!(gauge(&snapshotter), Some(1.0), "remove_connection heals");

            // disconnect
            poison();
            subduction.disconnect(&conn_a).await.expect("disconnect a");
            assert_eq!(gauge(&snapshotter), Some(0.0), "disconnect heals");

            // disconnect_from_peer
            subduction
                .add_connection(conn(peer_a))
                .await
                .expect("re-add a");
            poison();
            subduction
                .disconnect_from_peer(&peer_a)
                .await
                .expect("disconnect_from_peer");
            assert_eq!(gauge(&snapshotter), Some(0.0), "disconnect_from_peer heals");

            // disconnect_all
            subduction
                .add_connection(conn(peer_a))
                .await
                .expect("re-add a");
            subduction
                .add_connection(conn(peer_b))
                .await
                .expect("re-add b");
            poison();
            subduction.disconnect_all().await.expect("disconnect_all");
            assert_eq!(gauge(&snapshotter), Some(0.0), "disconnect_all heals");
        });
    });
}
