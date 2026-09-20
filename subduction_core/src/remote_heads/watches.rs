//! Heads watches: standing requests for a peer's heads.
//!
//! A watch is the heads-only counterpart of a push subscription. Where a
//! subscription rides on a batch sync and delivers full payloads, a watch is
//! established by [`WatchHeads`] and delivers only [`HeadsUpdate`]s. Both
//! directions live here:
//!
//! ```text
//! watched  : Set<Tree>            what the application asked for; the delivery gate
//! watchers : Map<Tree, Set<Peer>> peers we owe HeadsUpdates to
//! ```
//!
//! [`WatchHeads`]: crate::connection::message::WatchHeads
//! [`HeadsUpdate`]: crate::connection::message::SyncMessage::HeadsUpdate

use alloc::vec::Vec;

use async_lock::Mutex;
use sedimentree_core::{
    collections::{Map, Set},
    id::SedimentreeId,
};

use crate::peer::id::PeerId;

/// How many sedimentrees one peer may watch on this node.
///
/// A watch costs an entry for as long as the peer stays connected, and a
/// peer may name any id, so the table needs a bound the peer cannot lift.
pub const MAX_WATCHERS_PER_PEER: usize = 4096;

/// Both directions of heads-watch state, shared between [`Subduction`] and
/// [`SyncHandler`].
///
/// [`Subduction`]: crate::subduction::Subduction
/// [`SyncHandler`]: crate::handler::sync::SyncHandler
#[derive(Debug, Default)]
pub struct HeadsWatches {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    watched: Set<SedimentreeId>,
    watchers: Map<SedimentreeId, Set<PeerId>>,
    watcher_counts: Map<PeerId, usize>,
}

/// Why a peer's watch was not recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchRefused {
    /// The peer already holds [`MAX_WATCHERS_PER_PEER`] watches here.
    AtCapacity,
}

impl HeadsWatches {
    /// Create empty watch state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    // ── application intent ──────────────────────────────────────────────

    /// Record that the application wants heads for `id`. Returns `true` if
    /// this is new.
    pub async fn watch(&self, id: SedimentreeId) -> bool {
        self.state.lock().await.watched.insert(id)
    }

    /// Drop the application's interest in `id`. Returns `true` if it was watched.
    pub async fn unwatch(&self, id: SedimentreeId) -> bool {
        self.state.lock().await.watched.remove(&id)
    }

    /// Every sedimentree the application is watching.
    pub async fn watched(&self) -> Vec<SedimentreeId> {
        self.state.lock().await.watched.iter().copied().collect()
    }

    /// Whether the application is watching `id`. This is the delivery gate:
    /// heads about `id` reach the observer only if true.
    pub async fn is_watched(&self, id: SedimentreeId) -> bool {
        self.state.lock().await.watched.contains(&id)
    }

    // ── watches peers hold on us ────────────────────────────────────────

    /// Record `peer` as a watcher of `id`, subject to the per-peer cap.
    /// Idempotent: re-watching an already-watched id is `Ok` and costs
    /// nothing.
    ///
    /// # Errors
    ///
    /// [`WatchRefused::AtCapacity`] if `peer` already holds
    /// [`MAX_WATCHERS_PER_PEER`] watches.
    pub async fn add_watcher(&self, peer: PeerId, id: SedimentreeId) -> Result<(), WatchRefused> {
        let mut state = self.state.lock().await;
        let State {
            watchers,
            watcher_counts,
            ..
        } = &mut *state;

        let count = watcher_counts.entry(peer).or_default();
        let ids = watchers.entry(id).or_default();
        if ids.contains(&peer) {
            return Ok(());
        }
        if *count >= MAX_WATCHERS_PER_PEER {
            if ids.is_empty() {
                watchers.remove(&id);
            }
            return Err(WatchRefused::AtCapacity);
        }
        ids.insert(peer);
        *count += 1;
        Ok(())
    }

    /// Stop sending `peer` heads for `ids`.
    pub async fn remove_watcher(&self, peer: PeerId, ids: &[SedimentreeId]) {
        let mut state = self.state.lock().await;
        let State {
            watchers,
            watcher_counts,
            ..
        } = &mut *state;

        let mut removed = 0;
        for id in ids {
            if let Some(peers) = watchers.get_mut(id) {
                if peers.remove(&peer) {
                    removed += 1;
                }
                if peers.is_empty() {
                    watchers.remove(id);
                }
            }
        }
        if let Some(count) = watcher_counts.get_mut(&peer) {
            *count = count.saturating_sub(removed);
            if *count == 0 {
                watcher_counts.remove(&peer);
            }
        }
    }

    /// Peers watching `id`.
    pub async fn watchers_of(&self, id: SedimentreeId) -> Vec<PeerId> {
        self.state
            .lock()
            .await
            .watchers
            .get(&id)
            .map(|peers| peers.iter().copied().collect())
            .unwrap_or_default()
    }

    // ── session lifecycle ───────────────────────────────────────────────

    /// Forget `peer`'s watches on us. Application intent survives, so our
    /// watches are re-sent when the peer reconnects.
    pub async fn remove_peer(&self, peer: PeerId) {
        let mut state = self.state.lock().await;
        state.watcher_counts.remove(&peer);
        state.watchers.retain(|_, peers| {
            peers.remove(&peer);
            !peers.is_empty()
        });
    }

    /// [`remove_peer`](Self::remove_peer) for every peer at once.
    pub async fn remove_all_peers(&self) {
        let mut state = self.state.lock().await;
        state.watcher_counts.clear();
        state.watchers.clear();
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const fn peer(n: u8) -> PeerId {
        PeerId::new([n; 32])
    }

    const fn tree(n: u8) -> SedimentreeId {
        SedimentreeId::new([n; 32])
    }

    #[tokio::test]
    async fn remove_peer_keeps_intent() {
        let watches = HeadsWatches::new();
        watches.watch(tree(1)).await;
        watches
            .add_watcher(peer(1), tree(2))
            .await
            .expect("below cap");

        watches.remove_peer(peer(1)).await;
        assert!(watches.watchers_of(tree(2)).await.is_empty());
        assert_eq!(watches.watched().await, alloc::vec![tree(1)]);
    }

    #[tokio::test]
    async fn watcher_cap_is_per_peer_and_released_on_removal() {
        let watches = HeadsWatches::new();
        for n in 0..MAX_WATCHERS_PER_PEER {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&n.to_le_bytes());
            watches
                .add_watcher(peer(1), SedimentreeId::new(bytes))
                .await
                .expect("below cap");
        }
        assert_eq!(
            watches.add_watcher(peer(1), tree(0xFF)).await,
            Err(WatchRefused::AtCapacity)
        );
        assert!(
            watches.watchers_of(tree(0xFF)).await.is_empty(),
            "no entry left behind"
        );
        assert_eq!(
            watches.add_watcher(peer(2), tree(0xFF)).await,
            Ok(()),
            "other peers unaffected"
        );

        watches
            .remove_watcher(peer(1), &[SedimentreeId::new([0u8; 32])])
            .await;
        assert_eq!(watches.add_watcher(peer(1), tree(0xFF)).await, Ok(()));
    }

    /// Reference model over an arbitrary sequence of operations: the count
    /// used for the cap always equals the number of trees the peer watches.
    #[test]
    fn prop_watcher_count_matches_table() {
        use futures::executor::block_on;

        #[derive(Debug, Clone, bolero::TypeGenerator)]
        enum Op {
            Add { peer: u8, tree: u8 },
            Remove { peer: u8, trees: Vec<u8> },
            RemovePeer { peer: u8 },
        }

        bolero::check!().with_type::<Vec<Op>>().for_each(|ops| {
            let watches = HeadsWatches::new();
            for op in ops.iter().take(64) {
                match op {
                    Op::Add { peer: p, tree: t } => {
                        let _ = block_on(watches.add_watcher(peer(*p % 4), tree(*t % 8)));
                    }
                    Op::Remove { peer: p, trees } => {
                        let ids: Vec<_> = trees.iter().map(|t| tree(*t % 8)).collect();
                        block_on(watches.remove_watcher(peer(*p % 4), &ids));
                    }
                    Op::RemovePeer { peer: p } => block_on(watches.remove_peer(peer(*p % 4))),
                }
            }

            let state = watches.state.try_lock().expect("uncontended");
            for p in 0..4u8 {
                let actual = state
                    .watchers
                    .values()
                    .filter(|peers| peers.contains(&peer(p)))
                    .count();
                assert_eq!(
                    state.watcher_counts.get(&peer(p)).copied().unwrap_or(0),
                    actual
                );
            }
            assert!(state.watchers.values().all(|peers| !peers.is_empty()));
        });
    }
}
