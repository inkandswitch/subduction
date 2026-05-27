//! Shared peer-management helpers used by both [`Subduction`] and [`SyncHandler`].
//!
//! These free functions handle connection tracking, subscription
//! bookkeeping, and policy-filtered subscriber lookups. Both
//! `Subduction` and `SyncHandler` delegate to these functions through
//! thin `&self` wrappers.
//!
//! [`Subduction`]: super::Subduction
//! [`SyncHandler`]: crate::handler::sync::SyncHandler

use alloc::{sync::Arc, vec::Vec};
use async_lock::Mutex;
use future_form::FutureForm;
use nonempty::NonEmpty;
use sedimentree_core::{
    collections::{
        Map, Set,
        nonempty_ext::{NonEmptyExt, RemoveResult},
    },
    id::SedimentreeId,
};

use crate::{
    authenticated::Authenticated,
    connection::Connection,
    multiplexer::Multiplexer,
    peer::id::PeerId,
    policy::storage::StoragePolicy,
    storage::{powerbox::StoragePowerbox, traits::Storage},
};
use sedimentree_core::codec::{decode::Decode, encode::Encode};

/// Record that `peer_id` is subscribed to `sedimentree_id`.
pub(crate) async fn add_subscription(
    subscriptions: &Mutex<Map<SedimentreeId, Set<PeerId>>>,
    peer_id: PeerId,
    sedimentree_id: SedimentreeId,
) {
    let mut guard = subscriptions.lock().await;
    guard.entry(sedimentree_id).or_default().insert(peer_id);
}

/// Remove `peer_id` from all subscription sets.
///
/// Called when the last connection for a peer drops.
/// Empty subscription entries are pruned.
pub(crate) async fn remove_peer_from_subscriptions(
    subscriptions: &Mutex<Map<SedimentreeId, Set<PeerId>>>,
    peer_id: PeerId,
) {
    let mut guard = subscriptions.lock().await;
    guard.retain(|_id, peers| {
        peers.remove(&peer_id);
        !peers.is_empty()
    });
}

/// Get connections for subscribers authorized to receive updates for
/// a sedimentree, excluding a specific peer.
///
/// For each subscriber, checks policy to confirm they are allowed to
/// fetch this sedimentree before including their connections.
pub(crate) async fn get_authorized_subscriber_conns<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, WireMsg> + PartialEq + Clone + 'static,
    WireMsg: Encode + Decode,
    Auth: StoragePolicy<Async>,
>(
    subscriptions: &Mutex<Map<SedimentreeId, Set<PeerId>>>,
    storage: &StoragePowerbox<Store, Auth>,
    connections: &Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>,
    sedimentree_id: SedimentreeId,
    exclude_peer: &PeerId,
) -> Vec<Authenticated<Conn, Async>> {
    let subscriber_ids: Vec<PeerId> = {
        let guard = subscriptions.lock().await;
        guard
            .get(&sedimentree_id)
            .map(|peers| peers.iter().copied().collect())
            .unwrap_or_default()
    };

    if subscriber_ids.is_empty() {
        return Vec::new();
    }

    let mut authorized_peers = Vec::new();
    for peer_id in subscriber_ids {
        if peer_id == *exclude_peer {
            continue;
        }
        let can_fetch = storage
            .policy()
            .filter_authorized_fetch(peer_id, alloc::vec![sedimentree_id])
            .await;
        if !can_fetch.is_empty() {
            authorized_peers.push(peer_id);
        }
    }

    let guard = connections.lock().await;
    authorized_peers
        .into_iter()
        .flat_map(|pid| {
            guard
                .get(&pid)
                .map(|conns| conns.iter().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
        })
        .collect()
}

/// Remove a connection from tracking, cleaning up subscriptions if it
/// was the peer's last connection.
///
/// Returns:
/// - `Some(false)` — connection removed, peer still has other connections
/// - `Some(true)` — connection removed, was the peer's last connection
/// - `None` — connection was not found
///
/// # Defensive multiplexer cleanup on the `None` branch
///
/// When `multiplexers` is `Some` and the peer has no entry in
/// `connections` but **does** have an entry in `multiplexers`, this
/// function still drops the orphaned multiplexers. This handles the
/// race where [`SyncHandler::remove_connection`] (which only has access
/// to the connections map) ran first and removed the peer from
/// `connections`, then the listener loop calls
/// [`Subduction::remove_connection`] for the same peer via the
/// `connection_closed` channel. Without this defensive sweep, the
/// multiplexer entry would persist with pending oneshot senders, and
/// any concurrent `sync_with_peer` against that peer would hang for
/// the per-call timeout.
///
/// [`SyncHandler::remove_connection`]: crate::handler::sync::SyncHandler::remove_connection
/// [`Subduction::remove_connection`]: super::Subduction::remove_connection
#[allow(clippy::type_complexity)]
pub(crate) async fn remove_connection<
    Async: FutureForm,
    Conn: Connection<Async, WireMsg> + PartialEq + Clone + 'static,
    WireMsg: Encode + Decode,
>(
    connections: &Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>,
    subscriptions: &Mutex<Map<SedimentreeId, Set<PeerId>>>,
    multiplexers: Option<&Mutex<Map<PeerId, Vec<Arc<Multiplexer>>>>>,
    conn: &Authenticated<Conn, Async>,
) -> Option<bool> {
    let peer_id = conn.peer_id();
    let mut guard = connections.lock().await;

    let outcome = if let Some(peer_conns) = guard.remove(&peer_id) {
        match peer_conns.remove_item(conn) {
            RemoveResult::Removed(remaining) => {
                guard.insert(peer_id, remaining);
                Some(false)
            }
            RemoveResult::WasLast(_) => Some(true),
            RemoveResult::NotFound(original) => {
                guard.insert(peer_id, original);
                None
            }
        }
    } else {
        None
    };
    drop(guard);

    match outcome {
        Some(false) => {
            #[cfg(feature = "metrics")]
            crate::metrics::connection_closed();
        }
        Some(true) => {
            remove_peer_from_subscriptions(subscriptions, peer_id).await;
            cancel_peer_multiplexers(multiplexers, peer_id).await;

            #[cfg(feature = "metrics")]
            crate::metrics::connection_closed();
        }
        None => {
            // Defensive sweep — see the function-level docs above.
            // If the connection wasn't in the map but the peer still
            // has multiplexer entries, drop them. This catches the
            // race between SyncHandler-side removal (no muxes) and
            // listener-side removal.
            cancel_peer_multiplexers_if_orphaned(connections, multiplexers, peer_id).await;
        }
    }

    outcome
}

/// Drop every multiplexer for `peer_id`, cancelling all pending calls.
#[allow(clippy::type_complexity)]
async fn cancel_peer_multiplexers(
    multiplexers: Option<&Mutex<Map<PeerId, Vec<Arc<Multiplexer>>>>>,
    peer_id: PeerId,
) {
    if let Some(muxes_map) = multiplexers {
        let muxes_to_cancel: Vec<Arc<Multiplexer>> = {
            let mut mguard = muxes_map.lock().await;
            mguard.remove(&peer_id).unwrap_or_default()
        };
        for mux in muxes_to_cancel {
            mux.cancel_all_pending().await;
        }
    }
}

/// Drop every multiplexer for `peer_id` *only* if `peer_id` is no
/// longer present in `connections`. This is the post-race recovery
/// path: a different code path may already have removed the
/// connection without touching multiplexers.
#[allow(clippy::type_complexity)]
async fn cancel_peer_multiplexers_if_orphaned<
    Async: FutureForm,
    Conn: Connection<Async, WireMsg> + PartialEq + Clone + 'static,
    WireMsg: Encode + Decode,
>(
    connections: &Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>,
    multiplexers: Option<&Mutex<Map<PeerId, Vec<Arc<Multiplexer>>>>>,
    peer_id: PeerId,
) {
    let Some(muxes_map) = multiplexers else {
        return;
    };
    // Re-acquire the connections lock briefly to check for the peer.
    // Doing this *before* touching the multiplexer lock keeps lock
    // ordering consistent with the rest of the codebase
    // (connections then multiplexers).
    let still_connected = connections.lock().await.contains_key(&peer_id);
    if still_connected {
        return;
    }
    let muxes_to_cancel: Vec<Arc<Multiplexer>> = {
        let mut mguard = muxes_map.lock().await;
        mguard.remove(&peer_id).unwrap_or_default()
    };
    if !muxes_to_cancel.is_empty() {
        tracing::debug!(
            ?peer_id,
            "cleaning up {} orphaned multiplexer(s) (SyncHandler-vs-listener race recovery)",
            muxes_to_cancel.len()
        );
        for mux in muxes_to_cancel {
            mux.cancel_all_pending().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::sync::Arc;
    use async_lock::Mutex;
    use core::time::Duration;
    use future_form::{FutureForm, Sendable};
    use futures::future::BoxFuture;
    use nonempty::NonEmpty;
    use sedimentree_core::{
        codec::{decode::Decode, encode::Encode},
        collections::Map,
    };

    use crate::{
        authenticated::Authenticated,
        connection::Connection,
        connection::message::SyncMessage,
        multiplexer::Multiplexer,
        peer::id::PeerId,
    };

    /// Minimal no-op connection used to populate the connections map in tests.
    #[derive(Clone, PartialEq, Debug)]
    struct NullConn(PeerId);

    impl Connection<Sendable, SyncMessage> for NullConn {
        type DisconnectionError = core::fmt::Error;
        type SendError = core::fmt::Error;
        type RecvError = core::fmt::Error;

        fn disconnect(&self) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
            Sendable::from_future(async { Ok(()) })
        }

        fn send(&self, _: &SyncMessage) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SendError>> {
            Sendable::from_future(async { Ok(()) })
        }

        fn recv(&self) -> <Sendable as FutureForm>::Future<'_, Result<SyncMessage, Self::RecvError>> {
            Sendable::from_future(async { Err(core::fmt::Error) })
        }
    }

    #[tokio::test]
    async fn orphaned_multiplexer_pending_calls_are_cancelled() {
        let peer_id = PeerId::new([42u8; 32]);

        let mux = Arc::new(Multiplexer::new(peer_id, Duration::from_secs(5)));
        let req_id = mux.next_request_id();
        let rx = mux.register_pending(req_id).await;

        // Peer is not in the connections map (orphaned).
        let connections: Mutex<Map<PeerId, NonEmpty<Authenticated<NullConn, Sendable>>>> =
            Mutex::new(Map::new());

        let mut mux_map = Map::new();
        mux_map.insert(peer_id, vec![Arc::clone(&mux)]);
        let multiplexers: Mutex<Map<PeerId, Vec<Arc<Multiplexer>>>> = Mutex::new(mux_map);

        cancel_peer_multiplexers_if_orphaned(&connections, Some(&multiplexers), peer_id).await;

        assert!(
            rx.await.is_err(),
            "pending call must be cancelled for an orphaned multiplexer"
        );
    }

    #[tokio::test]
    async fn connected_peer_multiplexer_is_not_cancelled() {
        let peer_id = PeerId::new([43u8; 32]);

        let mux = Arc::new(Multiplexer::new(peer_id, Duration::from_secs(5)));
        let req_id = mux.next_request_id();
        let _rx = mux.register_pending(req_id).await;

        // Peer IS in the connections map — should not be touched.
        let conn = Authenticated::new_for_test(NullConn(peer_id), peer_id);
        let mut conn_map: Map<PeerId, NonEmpty<Authenticated<NullConn, Sendable>>> = Map::new();
        conn_map.insert(peer_id, NonEmpty::new(conn));
        let connections = Mutex::new(conn_map);

        let mut mux_map = Map::new();
        mux_map.insert(peer_id, vec![Arc::clone(&mux)]);
        let multiplexers: Mutex<Map<PeerId, Vec<Arc<Multiplexer>>>> = Mutex::new(mux_map);

        cancel_peer_multiplexers_if_orphaned(&connections, Some(&multiplexers), peer_id).await;

        // The mux entry must remain untouched because the peer is still connected.
        assert!(
            multiplexers.lock().await.contains_key(&peer_id),
            "multiplexer must not be removed for a still-connected peer"
        );
    }
}
