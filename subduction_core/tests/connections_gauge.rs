//! The `connections_active` gauge is set from the connection map itself
//! rather than maintained with increment/decrement deltas, so a missed
//! event cannot make it drift: the next refresh heals it.
//!
//! Uses a thread-local `DebuggingRecorder` and a plain `block_on` executor:
//! none of the operations under test need a running task (the manager loop
//! is never spawned; `TestSpawn` discards futures), and the recorder is
//! only visible on the installing thread.

#![allow(clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use future_form::Sendable;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use subduction_core::{
    connection::{
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

/// The gauge mirrors the map through adds and removals, and a poisoned
/// value is healed by the next mutation's refresh.
#[test]
fn connections_gauge_tracks_map_truth() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    metrics::with_local_recorder(&recorder, || {
        futures::executor::block_on(async {
            let (subduction, _handler, _listener_fut, _actor_fut) =
                SubductionBuilder::<_, _, _, _, _, 256>::new()
                    .signer(test_signer())
                    .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
                    .spawner(TestSpawn)
                    .timer(InstantTimeout)
                    .build::<Sendable, ChannelMockConnection<SyncMessage>>();

            let peer_a = PeerId::new([1u8; 32]);
            let peer_b = PeerId::new([2u8; 32]);
            let (conn_a, _handle_a) = ChannelMockConnection::new_with_handle(peer_a);
            let (conn_b, _handle_b) = ChannelMockConnection::new_with_handle(peer_b);
            let conn_a = conn_a.authenticated();

            subduction
                .add_connection(conn_a.clone())
                .await
                .expect("add a");
            assert_eq!(gauge(&snapshotter), Some(1.0), "one connection tracked");

            subduction
                .add_connection(conn_b.authenticated())
                .await
                .expect("add b");
            assert_eq!(gauge(&snapshotter), Some(2.0), "two connections tracked");

            subduction.remove_connection_for_test(&conn_a).await;
            assert_eq!(gauge(&snapshotter), Some(1.0), "removal refreshes");

            // Poison the gauge, then verify the next mutation's refresh
            // heals it to map truth — the actual set-from-truth claim.
            subduction_core::metrics::set_connections_active(999);
            assert_eq!(gauge(&snapshotter), Some(999.0), "poisoned");

            subduction.disconnect_all().await.expect("disconnect_all");
            assert_eq!(
                gauge(&snapshotter),
                Some(0.0),
                "refresh must heal the gauge to observed truth"
            );
        });
    });
}
