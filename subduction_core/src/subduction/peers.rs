//! Shared peer-management helpers used by both [`Subduction`] and [`SyncHandler`].
//!
//! These free functions handle connection tracking, subscription
//! bookkeeping, policy-filtered subscriber lookups, and building the push
//! frames sent to those subscribers. Both
//! `Subduction` and `SyncHandler` delegate to these functions through
//! thin `&self` wrappers.
//!
//! [`Subduction`]: super::Subduction
//! [`SyncHandler`]: crate::handler::sync::SyncHandler

use alloc::vec::Vec;
use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable, future_form};
use nonempty::NonEmpty;
use sedimentree_core::{
    collections::{Map, Set},
    id::SedimentreeId,
    loose_commit::id::CommitId,
};

use crate::{
    authenticated::Authenticated,
    connection::{Connection, message::SyncMessage},
    peer::{counter::PeerCounter, id::PeerId},
    policy::storage::StoragePolicy,
    remote_heads::RemoteHeads,
    storage::{powerbox::StoragePowerbox, traits::Storage},
};

use super::ingest::Ingested;
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

/// Build one push frame per ingested item per connection, stamping each
/// peer's send counter in order. Both push paths (`Subduction` for local
/// writes and requester-side ingest, `SyncHandler` for inbound data) use
/// this so the wire format and counter discipline cannot drift.
pub(crate) async fn build_pushes<Conn: Clone, Async: FutureForm>(
    id: SedimentreeId,
    heads: &[CommitId],
    send_counter: &PeerCounter,
    conns: &[Authenticated<Conn, Async>],
    ingested: &Ingested,
) -> Pushes<Conn, Async, SyncMessage> {
    let per_conn = ingested.commits.len() + ingested.fragments.len();
    let mut out = Vec::with_capacity(conns.len() * per_conn);
    for conn in conns {
        let peer_id = conn.peer_id();
        for (commit, blob) in &ingested.commits {
            let sender_heads = RemoteHeads {
                counter: send_counter.next(peer_id).await,
                heads: heads.to_vec(),
            };
            out.push((
                conn.clone(),
                SyncMessage::LooseCommit {
                    id,
                    commit: commit.clone(),
                    blob: blob.clone(),
                    sender_heads,
                },
            ));
        }
        for (fragment, blob) in &ingested.fragments {
            let sender_heads = RemoteHeads {
                counter: send_counter.next(peer_id).await,
                heads: heads.to_vec(),
            };
            out.push((
                conn.clone(),
                SyncMessage::Fragment {
                    id,
                    fragment: fragment.clone(),
                    blob: blob.clone(),
                    sender_heads,
                },
            ));
        }
    }
    out
}

/// Push frames addressed to one peer's connection, in send order.
pub(crate) type Pushes<Conn, Async, WireMsg> = Vec<(Authenticated<Conn, Async>, WireMsg)>;

/// Sends built push frames on a task of the right [`FutureForm`], so generic
/// code can detach a fan-out without naming `Send` bounds. Mirrors
/// [`RunManager`](crate::connection::manager::RunManager).
pub trait SendPushes<Conn, WireMsg: Encode + Decode>: FutureForm + Sized {
    /// Send frames to each peer in order, peers concurrently. A failed send
    /// is logged and that peer's remaining frames are skipped; the transport
    /// is left for the read loop's canonical teardown so `on_peer_disconnect`
    /// still fires.
    fn send_pushes(pushes: Pushes<Conn, Self, WireMsg>) -> Self::Future<'static, ()>
    where
        Conn: Connection<Self, WireMsg> + Clone + 'static;
}

#[future_form(
    Sendable where
        Conn: Connection<Sendable, WireMsg> + Clone + Send + Sync + 'static,
        WireMsg: Send + Sync + 'static,
    Local where
        Conn: Connection<Local, WireMsg> + Clone + 'static,
        WireMsg: 'static
)]
impl<Async: FutureForm, Conn, WireMsg: Encode + Decode> SendPushes<Conn, WireMsg> for Async {
    fn send_pushes(pushes: Pushes<Conn, Self, WireMsg>) -> Self::Future<'static, ()> {
        Async::from_future(async move {
            let mut by_peer: Map<PeerId, Pushes<Conn, Self, WireMsg>> = Map::new();
            for (conn, msg) in pushes {
                by_peer.entry(conn.peer_id()).or_default().push((conn, msg));
            }
            futures::future::join_all(by_peer.into_values().map(|frames| async move {
                for (conn, msg) in frames {
                    if let Err(e) = conn.send(&msg).await {
                        tracing::warn!(peer = %conn.peer_id(), error = %e, "peer disconnected");
                        break;
                    }
                }
            }))
            .await;
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::connection::test_utils::MockConnection;
    use future_form::Sendable;
    use sedimentree_core::{
        blob::{Blob, BlobMeta},
        loose_commit::LooseCommit,
    };
    use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};

    fn signed_commit(id: SedimentreeId, head: u8) -> (Signed<LooseCommit>, Blob) {
        let blob = Blob::new(alloc::vec![head; 8]);
        let commit = LooseCommit::new(
            id,
            CommitId::new([head; 32]),
            alloc::collections::BTreeSet::default(),
            BlobMeta::new(&blob),
        );
        let signed = futures::executor::block_on(Signed::seal::<Sendable, _>(
            &MemorySigner::from_bytes(&[7u8; 32]),
            commit,
        ))
        .into_signed();
        (signed, blob)
    }

    /// One frame per (connection, item); per-peer counters strictly increase
    /// in output order; every frame carries the same heads.
    #[test]
    #[cfg(feature = "bolero")]
    fn prop_build_pushes_is_a_product_with_ordered_counters() {
        let id = SedimentreeId::new([1u8; 32]);
        let heads = alloc::vec![CommitId::new([0xAA; 32]), CommitId::new([0xBB; 32])];
        let pool: Vec<_> = (0..4u8).map(|h| signed_commit(id, h)).collect();

        bolero::check!()
            .with_arbitrary::<(Vec<u8>, u8)>()
            .for_each(|(peer_seeds, n_items)| {
                let peers: Vec<PeerId> = peer_seeds
                    .iter()
                    .copied()
                    .collect::<Set<_>>()
                    .into_iter()
                    .take(6)
                    .map(|s| PeerId::new([s; 32]))
                    .collect();
                let n_items = usize::from(*n_items % 5);
                let ingested = Ingested {
                    commits: pool[..n_items].to_vec(),
                    fragments: Vec::new(),
                };
                let conns: Vec<Authenticated<MockConnection, Sendable>> = peers
                    .iter()
                    .map(|p| MockConnection::with_peer_id(*p).authenticated())
                    .collect();
                let counter = PeerCounter::default();

                let pushes = futures::executor::block_on(build_pushes(
                    id, &heads, &counter, &conns, &ingested,
                ));

                assert_eq!(pushes.len(), peers.len() * n_items);

                let mut last: Map<PeerId, u64> = Map::new();
                let mut seen: Set<(PeerId, CommitId)> = Set::new();
                for (conn, msg) in &pushes {
                    let SyncMessage::LooseCommit {
                        id: got_id,
                        commit,
                        sender_heads,
                        ..
                    } = msg
                    else {
                        panic!("expected LooseCommit, got {msg:?}");
                    };
                    assert_eq!(*got_id, id);
                    assert_eq!(sender_heads.heads, heads);
                    let p = conn.peer_id();
                    if let Some(prev) = last.insert(p, sender_heads.counter) {
                        assert!(
                            sender_heads.counter > prev,
                            "counters for {p} not increasing"
                        );
                    }
                    let head = commit
                        .try_verify()
                        .expect("signed in test")
                        .payload()
                        .head();
                    assert!(seen.insert((p, head)), "duplicate (peer, item) frame");
                }
            });
    }
}
