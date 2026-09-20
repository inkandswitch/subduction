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
    depth::DepthMetric,
    id::SedimentreeId,
    loose_commit::id::CommitId,
};

use crate::{
    authenticated::{Authenticated, Direction},
    collections::bounded_sharded_map::BoundedShardedMap,
    connection::{Connection, message::SyncMessage},
    peer::{counter::PeerCounter, id::PeerId},
    policy::storage::StoragePolicy,
    remote_heads::{RemoteHeads, watches::HeadsWatches},
    storage::{powerbox::StoragePowerbox, traits::Storage},
};

use super::ingest::{self, HeadsChanged, Ingested};
use sedimentree_core::{
    codec::{decode::Decode, encode::Encode},
    sedimentree::minimized::MinimizedSedimentree,
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

/// Turn a [`HeadsChanged`] witness into the frames that tell everyone who
/// should know: the 1.5-RTT ack to `ack_to` (the peer whose push caused the
/// change, if any), one push per ingested item to each authorized
/// subscriber other than `origin`, and a `HeadsUpdate` to each watcher not
/// already covered by one of those. Per-peer send counters are stamped here,
/// in order; the caller sends the frames off the dispatch path.
///
/// This is the only consumer of `HeadsChanged`, so every tree mutation is
/// propagated the same way.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn propagate<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, WireMsg> + PartialEq + Clone + 'static,
    WireMsg: Encode + Decode,
    Auth: StoragePolicy<Async>,
    Metric: DepthMetric,
    const SHARDS: usize,
>(
    change: HeadsChanged,
    origin: &PeerId,
    ack_to: Option<&Authenticated<Conn, Async>>,
    sedimentrees: &BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>,
    storage: &StoragePowerbox<Store, Auth>,
    depth_metric: &Metric,
    connections: &Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>,
    subscriptions: &Mutex<Map<SedimentreeId, Set<PeerId>>>,
    watches: &HeadsWatches,
    send_counter: &PeerCounter,
) -> Pushes<Conn, Async, SyncMessage> {
    let HeadsChanged {
        id,
        ingested,
        heads,
    } = change;

    // On a heads read failure report empty heads rather than drop the
    // frames; the heads field is advisory.
    let heads = match heads {
        Some(heads) => heads,
        None => ingest::heads_or_hydrate(sedimentrees, storage, depth_metric, id)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(tree = ?id, error = %e, "could not read heads; reporting none");
                Vec::new()
            }),
    };

    let mut out = Vec::new();
    if let Some(conn) = ack_to {
        out.push((
            conn.clone(),
            SyncMessage::HeadsUpdate {
                id,
                heads: RemoteHeads {
                    counter: send_counter.next(conn.peer_id()).await,
                    heads: heads.clone(),
                },
            },
        ));
    }

    if !ingested.is_empty() {
        let conns =
            get_authorized_subscriber_conns(subscriptions, storage, connections, id, origin).await;
        out.extend(build_pushes(id, &heads, send_counter, &conns, &ingested).await);
    }

    let covered = push_recipients(&out, []);
    out.extend(
        build_watcher_heads_updates(
            watches,
            storage,
            connections,
            send_counter,
            id,
            &heads,
            &covered,
        )
        .await,
    );
    out
}

/// Build one `HeadsUpdate` per peer watching `id` that will not learn these
/// heads another way this round: `exclude` names the peers already getting
/// them on a push or an ack. Watchers are re-checked against the fetch policy
/// so a revoked peer stops hearing heads without a disconnect.
pub(crate) async fn build_watcher_heads_updates<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, WireMsg> + PartialEq + Clone + 'static,
    WireMsg: Encode + Decode,
    Auth: StoragePolicy<Async>,
>(
    watches: &HeadsWatches,
    storage: &StoragePowerbox<Store, Auth>,
    connections: &Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>,
    send_counter: &PeerCounter,
    id: SedimentreeId,
    heads: &[CommitId],
    exclude: &Set<PeerId>,
) -> Pushes<Conn, Async, SyncMessage> {
    let candidates: Vec<PeerId> = watches
        .watchers_of(id)
        .await
        .into_iter()
        .filter(|peer| !exclude.contains(peer))
        .collect();
    if candidates.is_empty() {
        return Vec::new();
    }

    let mut authorized = Vec::with_capacity(candidates.len());
    for peer in candidates {
        if !storage
            .policy()
            .filter_authorized_fetch(peer, alloc::vec![id])
            .await
            .is_empty()
        {
            authorized.push(peer);
        }
    }

    // One connection per peer suffices: a heads report is per peer, not per
    // connection. A watcher with no connection is a teardown straggler (its
    // watch arrived after `remove_peer`); drop it here.
    let mut conns = Vec::with_capacity(authorized.len());
    let mut orphaned = Vec::new();
    {
        let guard = connections.lock().await;
        for peer in authorized {
            match guard.get(&peer) {
                Some(peer_conns) => conns.push(peer_conns.first().clone()),
                None => orphaned.push(peer),
            }
        }
    }
    for peer in orphaned {
        watches.remove_watcher(peer, &[id]).await;
    }

    let mut out = Vec::with_capacity(conns.len());
    for conn in conns {
        let msg = SyncMessage::HeadsUpdate {
            id,
            heads: RemoteHeads {
                counter: send_counter.next(conn.peer_id()).await,
                heads: heads.to_vec(),
            },
        };
        out.push((conn, msg));
    }
    out
}

/// Peers already covered by a set of pushes, plus `also`.
pub(crate) fn push_recipients<Conn: Clone, Async: FutureForm, WireMsg>(
    pushes: &Pushes<Conn, Async, WireMsg>,
    also: impl IntoIterator<Item = PeerId>,
) -> Set<PeerId> {
    pushes
        .iter()
        .map(|(conn, _)| conn.peer_id())
        .chain(also)
        .collect()
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
            let per_peer =
                futures::future::join_all(by_peer.into_values().map(|frames| async move {
                    let total = frames.len() as u64;
                    let mut sent = 0u64;
                    for (conn, msg) in frames {
                        if let Err(e) = conn.send(&msg).await {
                            tracing::warn!(peer = %conn.peer_id(), error = %e, "peer disconnected");
                            break;
                        }
                        sent += 1;
                    }
                    (sent, total - sent)
                }))
                .await;

            let (ok, failed) = per_peer
                .iter()
                .fold((0, 0), |(ok, failed), (s, f)| (ok + s, failed + f));
            tracing::trace!(ok, failed, "pushes sent");
            #[cfg(feature = "metrics")]
            crate::metrics::subscription_pushes(ok, failed);
        })
    }
}

/// The peers a subscribe from `originator` is forwarded to: every peer other
/// than `originator` with at least one connection this node dialed. Upstream
/// is a property of the peer, not of any one connection.
pub fn upstream_peers<D: IntoIterator<Item = Direction>>(
    peers: impl IntoIterator<Item = (PeerId, D)>,
    originator: PeerId,
) -> Vec<PeerId> {
    peers
        .into_iter()
        .filter(|(peer, _)| *peer != originator)
        .filter_map(|(peer, dirs)| {
            dirs.into_iter()
                .any(|d| d == Direction::Dialed)
                .then_some(peer)
        })
        .collect()
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
    /// Allows fetches only for the listed peers.
    struct AllowFetchFor(Set<PeerId>);

    #[derive(Debug, thiserror::Error)]
    #[error("fetch refused")]
    struct FetchRefused;

    impl StoragePolicy<Sendable> for AllowFetchFor {
        type FetchDisallowed = FetchRefused;
        type PutDisallowed = core::convert::Infallible;

        fn authorize_fetch(
            &self,
            peer: PeerId,
            _id: SedimentreeId,
        ) -> futures::future::BoxFuture<'_, Result<(), FetchRefused>> {
            let ok = self.0.contains(&peer);
            Box::pin(async move { ok.then_some(()).ok_or(FetchRefused) })
        }

        fn authorize_put(
            &self,
            _requestor: PeerId,
            _author: subduction_crypto::verified_author::VerifiedAuthor,
            _id: SedimentreeId,
        ) -> futures::future::BoxFuture<'_, Result<(), core::convert::Infallible>> {
            Box::pin(async { Ok(()) })
        }

        fn filter_authorized_fetch(
            &self,
            peer: PeerId,
            ids: Vec<SedimentreeId>,
        ) -> futures::future::BoxFuture<'_, Vec<SedimentreeId>> {
            let ok = self.0.contains(&peer);
            Box::pin(async move { if ok { ids } else { Vec::new() } })
        }
    }

    /// `heads_only_targets = (watchers ∩ connected ∩ may_fetch) \ exclude`,
    /// one `HeadsUpdate` per peer carrying the same heads; watchers with no
    /// connection are dropped from the table.
    #[test]
    #[cfg(feature = "bolero")]
    fn prop_watcher_heads_updates_is_watchers_minus_exclude() {
        use crate::storage::memory::MemoryStorage;

        let id = SedimentreeId::new([1u8; 32]);
        let heads = alloc::vec![CommitId::new([0xAA; 32])];
        let peer = |s: &u8| PeerId::new([s % 6; 32]);

        bolero::check!()
            .with_arbitrary::<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)>()
            .for_each(|(watchers, connected, allowed, exclude)| {
                let watches = HeadsWatches::new();
                for p in watchers.iter().map(peer) {
                    futures::executor::block_on(watches.add_watcher(p, id)).expect("below cap");
                }
                let connections = Mutex::new(
                    connected
                        .iter()
                        .map(peer)
                        .map(|p| {
                            (
                                p,
                                NonEmpty::new(MockConnection::with_peer_id(p).authenticated()),
                            )
                        })
                        .collect::<Map<_, _>>(),
                );
                let allowed: Set<PeerId> = allowed.iter().map(peer).collect();
                let storage = StoragePowerbox::new(
                    MemoryStorage::new(),
                    alloc::sync::Arc::new(AllowFetchFor(allowed.clone())),
                );
                let exclude: Set<PeerId> = exclude.iter().map(peer).collect();

                let out = futures::executor::block_on(build_watcher_heads_updates(
                    &watches,
                    &storage,
                    &connections,
                    &PeerCounter::default(),
                    id,
                    &heads,
                    &exclude,
                ));

                let connected: Set<PeerId> = connected.iter().map(peer).collect();
                let want: Set<PeerId> = watchers
                    .iter()
                    .map(peer)
                    .filter(|p| {
                        connected.contains(p) && allowed.contains(p) && !exclude.contains(p)
                    })
                    .collect();
                let got: Set<PeerId> = out.iter().map(|(c, _)| c.peer_id()).collect();
                assert_eq!(got, want);
                assert_eq!(out.len(), got.len(), "one frame per peer");
                for (_, msg) in &out {
                    let SyncMessage::HeadsUpdate {
                        id: got_id,
                        heads: h,
                    } = msg
                    else {
                        panic!("{msg:?}");
                    };
                    assert_eq!((*got_id, &h.heads), (id, &heads));
                }

                // Disconnected, authorized, non-excluded watchers were pruned.
                let remaining: Set<PeerId> = futures::executor::block_on(watches.watchers_of(id))
                    .into_iter()
                    .collect();
                for p in watchers.iter().map(peer) {
                    if allowed.contains(&p) && !exclude.contains(&p) && !connected.contains(&p) {
                        assert!(!remaining.contains(&p), "orphan watcher {p} kept");
                    }
                }
            });
    }
}

#[cfg(all(test, feature = "metrics"))]
mod metrics_tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use sedimentree_core::id::SedimentreeId;

    use super::*;
    use crate::{
        connection::{message::DataRequestRejected, test_utils::FailingSendMockConnection},
        metrics::names,
    };

    /// Pins which `send_pushes` outcome feeds which label of
    /// `subscription_pushes_total`; a swapped classification would corrupt
    /// the dead-connection push signal while every render-level test passes.
    #[test]
    #[allow(clippy::expect_used)]
    fn send_pushes_counts_outcomes() {
        // Asymmetric counts (2 ok, 1 failed) so a swap cannot pass by symmetry.
        let ok_a = FailingSendMockConnection::with_peer_id_failing(PeerId::new([1u8; 32]), false)
            .authenticated();
        let ok_b = FailingSendMockConnection::with_peer_id_failing(PeerId::new([2u8; 32]), false)
            .authenticated();
        let failing = FailingSendMockConnection::with_peer_id_failing(PeerId::new([3u8; 32]), true)
            .authenticated();
        let msg = SyncMessage::DataRequestRejected(DataRequestRejected {
            id: SedimentreeId::new([0u8; 32]),
        });
        let pushes = alloc::vec![(ok_a, msg.clone()), (ok_b, msg.clone()), (failing, msg)];

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(Sendable::send_pushes(pushes));
        });

        let mut counts: Map<String, u64> = Map::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            let (_, key) = key.into_parts();
            if key.name() != names::SUBSCRIPTION_PUSHES_TOTAL {
                continue;
            }
            let outcome = key
                .labels()
                .find(|label| label.key() == "outcome")
                .map(|label| label.value().to_owned())
                .expect("outcome label");
            if let DebugValue::Counter(n) = value {
                counts.insert(outcome, n);
            }
        }

        assert_eq!(counts.get("ok"), Some(&2));
        assert_eq!(counts.get("failed"), Some(&1));
    }
}
