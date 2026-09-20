//! Remote heads tracking and notification.
//!
//! [`RemoteHeads`] carries a peer's current heads for a sedimentree,
//! alongside a monotonic counter for ordering in the face of
//! out-of-order delivery on non-TCP transports.
//!
//! Two traits define the notification pipeline:
//!
//! - [`RemoteHeadsObserver`] — application-facing callback for heads updates
//! - [`RemoteHeadsNotifier`] — handler-level entry point, filtered to changes

use alloc::{sync::Arc, vec::Vec};

use async_lock::Mutex;
use future_form::FutureForm;
use sedimentree_core::{
    collections::{Entry, Map},
    id::SedimentreeId,
    loose_commit::id::CommitId,
};

use crate::peer::id::PeerId;

pub mod watches;

/// A remote peer's heads for a sedimentree, with a monotonic counter
/// for ordering in the face of out-of-order delivery.
///
/// The counter is scoped per-peer and incremented on each message carrying
/// heads, so one sequence spans every sedimentree that peer talks about.
/// Receivers therefore track the high-water mark per `(peer, sedimentree)`;
/// a strictly greater counter orders an update, but does not on its own make
/// it worth reporting (see [`FilteredHeadsNotifier`]). A counter of `0` means
/// "no heads reported" — what [`RemoteHeads::default`] carries on `NotFound`
/// and `Unauthorized` responses — and is ignored.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RemoteHeads {
    /// Monotonic per-peer counter — higher means newer.
    pub counter: u64,

    /// The heads (tip commits) of the sedimentree.
    pub heads: Vec<CommitId>,
}

impl RemoteHeads {
    /// Returns `true` if there are no heads.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.heads.is_empty()
    }
}

/// Observer for remote heads notifications.
///
/// Called with `(sedimentree_id, peer_id, heads)` when a peer's heads for a
/// watched sedimentree change. Only sedimentrees passed to
/// [`Subduction::watch_heads`] are reported; syncing or being pushed to does
/// not imply a watch. Heads arrive on `HeadsUpdate`, `sender_heads`, and
/// `responder_heads`; repeats are suppressed so an observer that syncs in
/// response terminates.
///
/// [`Subduction::watch_heads`]: crate::subduction::Subduction::watch_heads
///
/// # Contract
///
/// `on_remote_heads` runs under the notifier's per-peer lock so deliveries
/// for one `(peer, sedimentree)` are ordered. The implementation must:
///
/// - return promptly: no blocking, sleeping, or I/O; hand off to a channel or
///   task.
/// - not call back into Subduction synchronously (`sync_with_peer`,
///   `disconnect_from_peer`, … take the same lock); schedule the call instead.
///
/// `heads.heads` arrives sorted and deduplicated.
pub trait RemoteHeadsObserver {
    /// Called when a remote peer's heads for a sedimentree change.
    fn on_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads);
}

/// A no-op [`RemoteHeadsObserver`] that discards all notifications.
///
/// This is the default observer used when remote heads notifications
/// are not needed.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoRemoteHeadsObserver;

impl RemoteHeadsObserver for NoRemoteHeadsObserver {
    fn on_remote_heads(&self, _id: SedimentreeId, _peer: PeerId, _heads: RemoteHeads) {}
}

/// Wraps a [`RemoteHeadsObserver`], forwarding only changed heads.
///
/// State is keyed per `(peer, sedimentree)` because a peer's counter is
/// shared across all its trees.
///
/// ```text
/// incoming RemoteHeads ──► FilteredHeadsNotifier::notify
///                              │
///                              ├─ counter <= last for (peer, tree)? → drop (stale)
///                              │
///                              ├─ heads equal to last reported?     → drop (no change)
///                              │
///                              └─ otherwise                         → observer.on_remote_heads(...)
/// ```
pub struct FilteredHeadsNotifier<R: RemoteHeadsObserver> {
    observer: R,
    peers: Arc<Mutex<Map<PeerId, Arc<Mutex<PeerFilter>>>>>,
}

/// Filter state for one peer.
#[derive(Debug, Default)]
struct PeerFilter {
    reported: Map<SedimentreeId, Reported>,

    /// Set once the cap warning has been logged for this session.
    cap_warned: bool,
}

/// What was last reported to the observer for one `(peer, sedimentree)` pair.
#[derive(Debug)]
struct Reported {
    counter: u64,
    heads: HeadsDigest,
}

/// BLAKE3 digest of a canonical (sorted, deduplicated) heads list.
///
/// Stored instead of the heads themselves so an entry has constant size and
/// [`MAX_TREES_PER_PEER`] bounds memory, not just entry count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeadsDigest([u8; 32]);

impl HeadsDigest {
    fn of(heads: &[CommitId]) -> Self {
        let mut hasher = blake3::Hasher::new();
        for head in heads {
            hasher.update(head.as_bytes());
        }
        Self(*hasher.finalize().as_bytes())
    }
}

/// How many `(peer, sedimentree)` entries one peer may occupy.
///
/// `HeadsUpdate` names an arbitrary sedimentree with no authorship to check,
/// so an unbounded map lets a peer grow memory by streaming fresh ids. Past
/// the cap, updates are still delivered but not recorded, so quiescence is
/// lost for that peer's excess trees. A memory bound, not a tuning knob.
pub const MAX_TREES_PER_PEER: usize = 4096;

impl<R: RemoteHeadsObserver> FilteredHeadsNotifier<R> {
    /// Create a new filtered notifier wrapping the given observer.
    pub fn new(observer: R) -> Self {
        Self {
            observer,
            peers: Arc::new(Mutex::new(Map::new())),
        }
    }

    /// Notify the observer if `heads` is newer and different from the last
    /// report for `(peer, id)`.
    ///
    /// Delivers under the peer's lock so recording order equals delivery
    /// order; otherwise a stale delivery could land last and never be
    /// corrected. See [`RemoteHeadsObserver`].
    pub async fn notify(&self, id: SedimentreeId, peer: PeerId, mut heads: RemoteHeads) {
        // Zero is the placeholder on responses that carry no heads
        // (`NotFound`, `Unauthorized`).
        if heads.counter == 0 {
            tracing::trace!(peer = %peer, tree = ?id, "heads update carries no counter; ignoring");
            return;
        }

        // Senders build `heads` from a `Set` with unspecified order;
        // canonicalize so equal sets compare equal.
        heads.heads.sort_unstable();
        heads.heads.dedup();
        let digest = HeadsDigest::of(&heads.heads);

        let filter = self.peers.lock().await.entry(peer).or_default().clone();
        let mut filter = filter.lock().await;
        let PeerFilter {
            reported,
            cap_warned,
        } = &mut *filter;
        let at_cap = reported.len() >= MAX_TREES_PER_PEER;

        match reported.entry(id) {
            Entry::Occupied(mut entry) => {
                let last = entry.get_mut();
                if heads.counter <= last.counter {
                    return;
                }

                last.counter = heads.counter;
                if last.heads == digest {
                    return;
                }

                last.heads = digest;
            }
            Entry::Vacant(slot) => {
                if at_cap {
                    // At the cap: deliver without recording. Warn once per session.
                    if *cap_warned {
                        tracing::trace!(peer = %peer, tree = ?id, "heads notifier at cap; delivering without recording");
                    } else {
                        *cap_warned = true;
                        tracing::warn!(
                            peer = %peer,
                            cap = MAX_TREES_PER_PEER,
                            "heads notifier at its per-peer cap; delivering without recording"
                        );
                    }
                } else {
                    slot.insert(Reported {
                        counter: heads.counter,
                        heads: digest,
                    });
                }
            }
        }

        self.observer.on_remote_heads(id, peer, heads);
    }

    /// Forget everything recorded for `peer`.
    ///
    /// Entries are per-session; call from [`Handler::on_peer_disconnect`] so
    /// a reconnecting peer's first report is delivered.
    ///
    /// [`Handler::on_peer_disconnect`]: crate::handler::Handler::on_peer_disconnect
    pub async fn remove_peer(&self, peer: PeerId) {
        self.peers.lock().await.remove(&peer);
    }
}

impl<R: RemoteHeadsObserver + core::fmt::Debug> core::fmt::Debug for FilteredHeadsNotifier<R> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FilteredHeadsNotifier")
            .field("observer", &self.observer)
            .finish_non_exhaustive()
    }
}

impl<R: RemoteHeadsObserver + Clone> Clone for FilteredHeadsNotifier<R> {
    fn clone(&self) -> Self {
        Self {
            observer: self.observer.clone(),
            peers: self.peers.clone(),
        }
    }
}

/// Trait for handlers that can notify the application of remote heads updates.
///
/// [`Subduction`] calls this when it receives `responder_heads` in a
/// [`BatchSyncResponse`] during sync. Handlers that also process
/// subscription pushes and `HeadsUpdate` messages (like [`SyncHandler`])
/// should call this from their own dispatch logic too.
///
/// Implementing this trait allows all remote-heads notifications to flow
/// through a single path regardless of which protocol step produced them.
///
/// [`Subduction`]: crate::subduction::Subduction
/// [`BatchSyncResponse`]: crate::connection::message::BatchSyncResponse
/// [`SyncHandler`]: crate::handler::sync::SyncHandler
pub trait RemoteHeadsNotifier<Async: FutureForm> {
    /// Notify the application of a remote peer's current heads for a sedimentree.
    fn notify_remote_heads(
        &self,
        id: SedimentreeId,
        peer: PeerId,
        heads: RemoteHeads,
    ) -> Async::Future<'_, ()>;
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct RecordingObserver(Arc<StdMutex<Vec<(SedimentreeId, PeerId, RemoteHeads)>>>);

    impl RecordingObserver {
        fn deliveries(&self) -> Vec<(SedimentreeId, PeerId, RemoteHeads)> {
            self.0.lock().expect("poisoned").clone()
        }

        fn count(&self) -> usize {
            self.0.lock().expect("poisoned").len()
        }
    }

    impl RemoteHeadsObserver for RecordingObserver {
        fn on_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
            self.0.lock().expect("poisoned").push((id, peer, heads));
        }
    }

    fn peer(n: u8) -> PeerId {
        PeerId::new([n; 32])
    }

    fn tree(n: u8) -> SedimentreeId {
        SedimentreeId::new([n; 32])
    }

    fn tree_from(n: usize) -> SedimentreeId {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&n.to_le_bytes());
        SedimentreeId::new(bytes)
    }

    fn heads(counter: u64, ids: &[u8]) -> RemoteHeads {
        RemoteHeads {
            counter,
            heads: ids.iter().map(|n| CommitId::new([*n; 32])).collect(),
        }
    }

    /// Peers stamp one counter sequence across every tree, so a message about
    /// tree Y must not mask a fresh, lower-counter message about tree X.
    #[tokio::test]
    async fn a_later_tree_does_not_mask_an_earlier_one() {
        let observer = RecordingObserver::default();
        let notifier = FilteredHeadsNotifier::new(observer.clone());

        notifier
            .notify(tree(b'y'), peer(1), heads(6, &[0xBB]))
            .await;
        notifier
            .notify(tree(b'x'), peer(1), heads(5, &[0xAA]))
            .await;

        assert_eq!(observer.count(), 2);
    }

    /// Reference model of the documented rule: delivered iff the counter is
    /// non-zero, advanced for `(peer, tree)`, and the canonical heads differ
    /// from the last delivery; `remove_peer` forgets that peer's entries.
    #[test]
    fn prop_observer_sees_exactly_the_changes() {
        use futures::executor::block_on;
        use sedimentree_core::collections::Map as ModelMap;

        const PEERS: u8 = 3;
        const TREES: u8 = 3;
        const HEADS: u8 = 4;

        #[derive(Debug, Clone, bolero::TypeGenerator)]
        enum Op {
            Notify {
                peer: u8,
                tree: u8,
                counter: u8,
                heads: Vec<u8>,
            },
            RemovePeer {
                peer: u8,
            },
        }

        let peers: Vec<PeerId> = (0..PEERS).map(peer).collect();
        let trees: Vec<SedimentreeId> = (0..TREES).map(tree).collect();
        let commits: Vec<CommitId> = (0..HEADS).map(|h| CommitId::new([h; 32])).collect();

        bolero::check!()
            .with_type::<Vec<Op>>()
            .for_each(|ops: &Vec<Op>| {
                let observer = RecordingObserver::default();
                let notifier = FilteredHeadsNotifier::new(observer.clone());

                let mut model: ModelMap<(usize, usize), (u64, Vec<CommitId>)> = ModelMap::new();
                let mut expected: Vec<(SedimentreeId, PeerId, Vec<CommitId>)> = Vec::new();

                for op in ops.iter().take(32) {
                    match op {
                        Op::RemovePeer { peer } => {
                            let pi = usize::from(*peer % PEERS);
                            model.retain(|(p, _), _| *p != pi);
                            block_on(notifier.remove_peer(peers[pi]));
                        }
                        Op::Notify {
                            peer,
                            tree,
                            counter,
                            heads,
                        } => {
                            let (pi, ti) = (usize::from(*peer % PEERS), usize::from(*tree % TREES));
                            let counter = u64::from(*counter);

                            let raw: Vec<CommitId> = heads
                                .iter()
                                .take(6)
                                .map(|h| commits[usize::from(*h % HEADS)])
                                .collect();
                            let mut canonical = raw.clone();
                            canonical.sort_unstable();
                            canonical.dedup();

                            if counter != 0 {
                                match model.entry((pi, ti)) {
                                    Entry::Occupied(mut e) => {
                                        let (last_counter, last_heads) = e.get_mut();
                                        if counter > *last_counter {
                                            *last_counter = counter;
                                            if *last_heads != canonical {
                                                last_heads.clone_from(&canonical);
                                                expected.push((trees[ti], peers[pi], canonical));
                                            }
                                        }
                                    }
                                    Entry::Vacant(v) => {
                                        v.insert((counter, canonical.clone()));
                                        expected.push((trees[ti], peers[pi], canonical));
                                    }
                                }
                            }

                            block_on(notifier.notify(
                                trees[ti],
                                peers[pi],
                                RemoteHeads {
                                    counter,
                                    heads: raw,
                                },
                            ));
                        }
                    }
                }

                let actual: Vec<(SedimentreeId, PeerId, Vec<CommitId>)> = observer
                    .deliveries()
                    .into_iter()
                    .map(|(id, peer, heads)| (id, peer, heads.heads))
                    .collect();

                assert_eq!(actual, expected);
            });
    }

    /// Past the cap, updates are delivered but not recorded, the warning
    /// fires once, and `remove_peer` frees the budget.
    #[tokio::test]
    async fn cap_delivers_without_recording() {
        let observer = RecordingObserver::default();
        let notifier = FilteredHeadsNotifier::new(observer.clone());
        let p = peer(1);

        for t in 0..=MAX_TREES_PER_PEER {
            notifier.notify(tree_from(t), p, heads(1, &[1])).await;
        }
        assert_eq!(
            observer.count(),
            MAX_TREES_PER_PEER + 1,
            "every update delivered"
        );

        let recorded = |notifier: &FilteredHeadsNotifier<_>| {
            let peers = notifier.peers.try_lock().expect("uncontended");
            let filter = peers.get(&p).expect("peer present");
            let filter = filter.try_lock().expect("uncontended");
            (filter.reported.len(), filter.cap_warned)
        };
        assert_eq!(recorded(&notifier), (MAX_TREES_PER_PEER, true));

        // The excess tree is never remembered: identical heads are delivered again.
        notifier
            .notify(tree_from(MAX_TREES_PER_PEER), p, heads(2, &[1]))
            .await;
        assert_eq!(observer.count(), MAX_TREES_PER_PEER + 2);

        // A recorded tree still filters.
        notifier.notify(tree_from(0), p, heads(3, &[1])).await;
        assert_eq!(observer.count(), MAX_TREES_PER_PEER + 2);

        notifier.remove_peer(p).await;
        notifier
            .notify(tree_from(MAX_TREES_PER_PEER), p, heads(4, &[1]))
            .await;
        assert_eq!(
            recorded(&notifier),
            (1, false),
            "budget reset with the session"
        );
    }

    /// Concurrent notifications for one `(peer, tree)` must be delivered in
    /// the order they were recorded; a stale last delivery is permanent.
    ///
    /// Needs worker threads: on `current_thread` an uncontended
    /// `async_lock::Mutex` never yields, every `notify` runs to completion,
    /// and a deliver-after-unlock implementation would pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_updates_deliver_in_recorded_order() {
        const ROUNDS: usize = 200;
        const UPDATES: u8 = 64;

        for round in 0..ROUNDS {
            let observer = RecordingObserver::default();
            let notifier = Arc::new(FilteredHeadsNotifier::new(observer.clone()));

            let tasks: Vec<_> = (1..=UPDATES)
                .map(|counter| {
                    let notifier = Arc::clone(&notifier);
                    tokio::spawn(async move {
                        notifier
                            .notify(tree(9), peer(9), heads(u64::from(counter), &[counter]))
                            .await;
                    })
                })
                .collect();
            for task in tasks {
                task.await.expect("notify task panicked");
            }

            let counters: Vec<u64> = observer
                .deliveries()
                .iter()
                .map(|(_, _, h)| h.counter)
                .collect();
            assert!(
                counters.windows(2).all(|w| w[0] < w[1]),
                "round {round}: deliveries out of order: {counters:?}"
            );
            assert_eq!(
                counters.last().copied(),
                Some(u64::from(UPDATES)),
                "round {round}: final delivery is not the newest: {counters:?}"
            );
        }
    }
}
