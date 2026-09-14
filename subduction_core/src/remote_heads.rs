//! Remote heads tracking and notification.
//!
//! [`RemoteHeads`] carries a peer's current heads for a sedimentree,
//! alongside a monotonic counter for ordering in the face of
//! out-of-order delivery on non-TCP transports.
//!
//! Two traits define the notification pipeline:
//!
//! - [`RemoteHeadsObserver`] — application-facing callback for heads updates
//! - [`RemoteHeadsNotifier`] — handler-level entry point, filtered to genuine changes

use alloc::{sync::Arc, vec::Vec};

use async_lock::Mutex;
use future_form::FutureForm;
use sedimentree_core::{
    collections::{Entry, Map},
    id::SedimentreeId,
    loose_commit::id::CommitId,
};

use crate::peer::id::PeerId;

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
/// sedimentree _change_ — not every time a peer mentions them, which is
/// constantly: on `HeadsUpdate`, on `sender_heads` of subscription pushes, and
/// on `responder_heads` of every sync response. An observer that reacts by
/// syncing would otherwise never stop, because its own sync's response repeats
/// the heads and re-fires the callback.
///
/// Within Subduction the observer is reached only through
/// [`FilteredHeadsNotifier`], so the rule cannot be bypassed.
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

/// What was last reported to the observer for one `(peer, sedimentree)` pair.
#[derive(Debug)]
struct Reported {
    /// Highest counter seen, for ordering on transports that reorder.
    counter: u64,

    /// Sorted and deduplicated, so a reordering of the same heads is not a
    /// change.
    heads: Vec<CommitId>,
}

/// Wraps a [`RemoteHeadsObserver`], forwarding only genuine changes.
///
/// The observer is reachable only through [`notify`](Self::notify), so neither
/// filter can be bypassed. State is keyed per `(peer, sedimentree)`: peers
/// stamp one counter sequence across *all* trees, so a per-peer key would let
/// a message about one tree mask a fresh update about another.
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
    reported: Arc<Mutex<Map<(PeerId, SedimentreeId), Reported>>>,
}

impl<R: RemoteHeadsObserver> FilteredHeadsNotifier<R> {
    /// Create a new filtered notifier wrapping the given observer.
    pub fn new(observer: R) -> Self {
        Self {
            observer,
            reported: Arc::new(Mutex::new(Map::new())),
        }
    }

    /// Notify the observer if these heads are newer than, and different from,
    /// the last heads reported for this peer and sedimentree.
    ///
    /// The observer runs while the filter's lock is held, so that the order
    /// updates are recorded in is the order they are delivered in. Two
    /// concurrent notifications for one `(peer, sedimentree)` would otherwise
    /// be free to deliver newest-first and leave the observer permanently
    /// holding the older heads, since the newer ones are already recorded and
    /// will never be reported again. `on_remote_heads` must therefore not
    /// block; hand work to a channel or a task.
    ///
    pub async fn notify(&self, id: SedimentreeId, peer: PeerId, mut heads: RemoteHeads) {
        // Counter zero is the placeholder on responses that carry no heads at
        // all (`NotFound`, `Unauthorized`). `PeerCounter::next` only produces
        // it on `u64` wraparound, which needs 2^64 stamps or a seed of
        // `u64::MAX`.
        if heads.counter == 0 {
            tracing::trace!(peer = %peer, tree = ?id, "heads update carries no counter; ignoring");
            return;
        }

        // Canonicalized before the lock: senders build `heads` from a `Set`,
        // whose iteration order is unspecified and varies between processes,
        // so raw `Vec` comparison would report changes that are not changes.
        heads.heads.sort_unstable();
        heads.heads.dedup();

        let mut reported = self.reported.lock().await;

        match reported.entry((peer, id)) {
            Entry::Occupied(mut entry) => {
                let last = entry.get_mut();
                if heads.counter <= last.counter {
                    return;
                }

                last.counter = heads.counter;
                if last.heads == heads.heads {
                    return;
                }

                last.heads.clone_from(&heads.heads);
            }
            Entry::Vacant(slot) => {
                slot.insert(Reported {
                    counter: heads.counter,
                    heads: heads.heads.clone(),
                });
            }
        }

        self.observer.on_remote_heads(id, peer, heads);
    }

    /// Forget everything recorded for `peer`.
    ///
    /// Call this from [`Handler::on_peer_disconnect`], as [`SyncHandler`]
    /// does: the entries are per-session, so dropping them keeps the map to
    /// live peers and lets a reconnecting peer report its heads once for the
    /// new session.
    ///
    /// [`Handler::on_peer_disconnect`]: crate::handler::Handler::on_peer_disconnect
    /// [`SyncHandler`]: crate::handler::sync::SyncHandler
    pub async fn remove_peer(&self, peer: PeerId) {
        // Awaits the lock rather than `try_lock`ing it: nothing will call this
        // again for a peer that has gone, so giving up would leak the entries
        // for the life of the process.
        self.reported
            .lock()
            .await
            .retain(|(recorded, _), _| *recorded != peer);
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
            reported: self.reported.clone(),
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
