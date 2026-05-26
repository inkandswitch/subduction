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
/// # Bug 5: defensive multiplexer cleanup on the `None` branch
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
            "cleaning up {} orphaned multiplexer(s) (Bug 5 race recovery)",
            muxes_to_cancel.len()
        );
        for mux in muxes_to_cancel {
            mux.cancel_all_pending().await;
        }
    }
}
