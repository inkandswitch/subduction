//! Shared peer-management helpers used by both [`Subduction`] and [`SyncHandler`].
//!
//! These free functions handle connection tracking, subscription
//! bookkeeping, and policy-filtered subscriber lookups. Both
//! `Subduction` and `SyncHandler` delegate to these functions through
//! thin `&self` wrappers.
//!
//! [`Subduction`]: super::Subduction
//! [`SyncHandler`]: crate::handler::sync::SyncHandler

use alloc::vec::Vec;
use async_lock::Mutex;
use future_form::FutureForm;
use nonempty::NonEmpty;
use sedimentree_core::collections::{
    Map, Set,
    nonempty_ext::{NonEmptyExt, RemoveResult},
};
use sedimentree_core::id::SedimentreeId;

use crate::{
    connection::{Connection, authenticated::Authenticated},
    peer::id::PeerId,
    policy::storage::StoragePolicy,
    storage::{powerbox::StoragePowerbox, traits::Storage},
};

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
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
>(
    subscriptions: &Mutex<Map<SedimentreeId, Set<PeerId>>>,
    storage: &StoragePowerbox<S, P>,
    connections: &Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>,
    sedimentree_id: SedimentreeId,
    exclude_peer: &PeerId,
) -> Vec<Authenticated<C, F>> {
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
pub(crate) async fn unregister<
    F: FutureForm,
    C: Connection<F> + PartialEq + Clone + 'static,
>(
    connections: &Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>,
    subscriptions: &Mutex<Map<SedimentreeId, Set<PeerId>>>,
    conn: &Authenticated<C, F>,
) -> Option<bool> {
    let peer_id = conn.peer_id();
    let mut guard = connections.lock().await;

    if let Some(peer_conns) = guard.remove(&peer_id) {
        match peer_conns.remove_item(conn) {
            RemoveResult::Removed(remaining) => {
                guard.insert(peer_id, remaining);

                #[cfg(feature = "metrics")]
                crate::metrics::connection_closed();

                Some(false)
            }
            RemoveResult::WasLast(_) => {
                drop(guard);
                remove_peer_from_subscriptions(subscriptions, peer_id).await;

                #[cfg(feature = "metrics")]
                crate::metrics::connection_closed();

                Some(true)
            }
            RemoveResult::NotFound(original) => {
                guard.insert(peer_id, original);
                None
            }
        }
    } else {
        None
    }
}
