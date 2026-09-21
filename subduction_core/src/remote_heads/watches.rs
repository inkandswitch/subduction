//! Heads watches: standing requests for a peer's heads.
//!
//! A watch is the heads-only counterpart of a push subscription. Where a
//! subscription rides on a batch sync and delivers full payloads, a watch is
//! established by [`WatchHeads`] and delivers only [`HeadsUpdate`]s. Both
//! directions live here:
//!
//! ```text
//! watched  : Set<Tree>            what the application asked for; the delivery gate
//! watchers : Map<Tree, Set<Peer>> peers to send HeadsUpdates to
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

/// How many watches one peer may hold on this node.
///
/// A watch costs an entry for as long as the peer stays connected, and a
/// peer may name any id, so the table is bounded per peer.
pub const MAX_WATCHES_PER_PEER: usize = 4096;

/// Both directions of heads-watch state, shared between [`Subduction`] and
/// [`SyncHandler`].
///
/// [`Subduction`]: crate::subduction::Subduction
/// [`SyncHandler`]: crate::handler::sync::SyncHandler
#[derive(Debug)]
pub struct HeadsWatches {
    state: Mutex<State>,
    cap: usize,
}

impl Default for HeadsWatches {
    fn default() -> Self {
        Self::with_cap(MAX_WATCHES_PER_PEER)
    }
}

#[derive(Debug, Default)]
struct State {
    watched: Set<SedimentreeId>,
    watchers: Map<SedimentreeId, Set<PeerId>>,
    watcher_counts: Map<PeerId, usize>,
}

/// Whether recording a watch created a new entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Watched {
    /// The watch is new.
    Added,
    /// The peer already watched this id.
    Already,
}

/// Why a peer's watch was not recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum WatchRefused {
    /// The peer already holds as many watches here as the cap allows.
    #[error("peer is at its heads-watch cap")]
    AtCapacity,
}

impl HeadsWatches {
    /// Create empty watch state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create empty watch state with a custom per-peer watch cap.
    #[must_use]
    pub fn with_cap(cap: usize) -> Self {
        Self {
            state: Mutex::new(State::default()),
            cap,
        }
    }

    /// The per-peer watch cap.
    #[must_use]
    pub const fn cap(&self) -> usize {
        self.cap
    }

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

    /// Whether the application is watching `id`.
    pub async fn is_watched(&self, id: SedimentreeId) -> bool {
        self.state.lock().await.watched.contains(&id)
    }

    /// Record `peer` as a watcher of `id`, subject to the per-peer cap.
    /// Idempotent: an id the peer already watches is [`Watched::Already`]
    /// regardless of the cap.
    ///
    /// # Errors
    ///
    /// [`WatchRefused::AtCapacity`] if the id is new and `peer` already holds
    /// as many watches as the cap allows.
    pub(crate) async fn add_watcher(
        &self,
        peer: PeerId,
        id: SedimentreeId,
    ) -> Result<Watched, WatchRefused> {
        let mut state = self.state.lock().await;
        let State {
            watchers,
            watcher_counts,
            ..
        } = &mut *state;

        if watchers.get(&id).is_some_and(|ids| ids.contains(&peer)) {
            return Ok(Watched::Already);
        }
        if watcher_counts.get(&peer).copied().unwrap_or(0) >= self.cap {
            return Err(WatchRefused::AtCapacity);
        }
        watchers.entry(id).or_default().insert(peer);
        *watcher_counts.entry(peer).or_default() += 1;
        Ok(Watched::Added)
    }

    /// Stop sending `peer` heads for `ids`.
    pub(crate) async fn remove_watcher(&self, peer: PeerId, ids: &[SedimentreeId]) {
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

    /// Whether `peer` already watches `id`.
    pub(crate) async fn is_watcher(&self, peer: PeerId, id: SedimentreeId) -> bool {
        self.state
            .lock()
            .await
            .watchers
            .get(&id)
            .is_some_and(|peers| peers.contains(&peer))
    }

    /// Peers watching `id`.
    pub(crate) async fn watchers_of(&self, id: SedimentreeId) -> Vec<PeerId> {
        self.state
            .lock()
            .await
            .watchers
            .get(&id)
            .map(|peers| peers.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Drop `peer`'s watches on this node. The application's own watches are
    /// kept and replayed on reconnect.
    pub(crate) async fn remove_peer(&self, peer: PeerId) {
        let mut state = self.state.lock().await;
        state.watcher_counts.remove(&peer);
        state.watchers.retain(|_, peers| {
            peers.remove(&peer);
            !peers.is_empty()
        });
    }

    /// [`remove_peer`](Self::remove_peer) for every peer at once.
    pub(crate) async fn remove_all_peers(&self) {
        let mut state = self.state.lock().await;
        state.watcher_counts.clear();
        state.watchers.clear();
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use futures::executor::block_on;

    use super::*;

    const fn peer(n: u8) -> PeerId {
        PeerId::new([n; 32])
    }

    const fn tree(n: u8) -> SedimentreeId {
        SedimentreeId::new([n; 32])
    }

    /// Reference model over arbitrary operation sequences and a small cap.
    /// After every step: `watched()` is the application's intent; the
    /// watcher table matches a set of `(peer, tree)` pairs; `add_watcher`
    /// refuses exactly when the pair is new and the peer is at the cap; no
    /// empty sets or zero counts linger.
    #[test]
    fn prop_heads_watches_match_model() {
        #[derive(Debug, Clone, bolero::TypeGenerator)]
        enum Op {
            Watch { tree: u8 },
            Unwatch { tree: u8 },
            Add { peer: u8, tree: u8 },
            Remove { peer: u8, trees: Vec<u8> },
            RemovePeer { peer: u8 },
            RemoveAllPeers,
        }

        bolero::check!()
            .with_type::<(u8, Vec<Op>)>()
            .for_each(|(cap, ops)| {
                let cap = usize::from(*cap % 5);
                let watches = HeadsWatches::with_cap(cap);
                let mut intent: Set<SedimentreeId> = Set::new();
                let mut table: Set<(PeerId, SedimentreeId)> = Set::new();

                for op in ops.iter().take(64) {
                    match op {
                        Op::Watch { tree: t } => {
                            let t = tree(*t % 8);
                            assert_eq!(block_on(watches.watch(t)), intent.insert(t));
                        }
                        Op::Unwatch { tree: t } => {
                            let t = tree(*t % 8);
                            assert_eq!(block_on(watches.unwatch(t)), intent.remove(&t));
                        }
                        Op::Add { peer: p, tree: t } => {
                            let (p, t) = (peer(*p % 4), tree(*t % 8));
                            let held = table.iter().filter(|(q, _)| *q == p).count();
                            let expected = if table.contains(&(p, t)) {
                                Ok(Watched::Already)
                            } else if held < cap {
                                table.insert((p, t));
                                Ok(Watched::Added)
                            } else {
                                Err(WatchRefused::AtCapacity)
                            };
                            assert_eq!(block_on(watches.add_watcher(p, t)), expected);
                        }
                        Op::Remove { peer: p, trees } => {
                            let p = peer(*p % 4);
                            let ids: Vec<_> = trees.iter().map(|t| tree(*t % 8)).collect();
                            for t in &ids {
                                table.remove(&(p, *t));
                            }
                            block_on(watches.remove_watcher(p, &ids));
                        }
                        Op::RemovePeer { peer: p } => {
                            let p = peer(*p % 4);
                            table.retain(|(q, _)| *q != p);
                            block_on(watches.remove_peer(p));
                        }
                        Op::RemoveAllPeers => {
                            table.clear();
                            block_on(watches.remove_all_peers());
                        }
                    }

                    let intent_now: Set<SedimentreeId> =
                        block_on(watches.watched()).into_iter().collect();
                    assert_eq!(intent_now, intent);
                    for t in (0..8).map(tree) {
                        assert_eq!(block_on(watches.is_watched(t)), intent.contains(&t));
                        let got: Set<PeerId> =
                            block_on(watches.watchers_of(t)).into_iter().collect();
                        let want: Set<PeerId> = table
                            .iter()
                            .filter(|(_, u)| *u == t)
                            .map(|(q, _)| *q)
                            .collect();
                        assert_eq!(got, want, "watchers of {t:?}");
                    }

                    let state = watches.state.try_lock().expect("uncontended");
                    assert!(state.watchers.values().all(|peers| !peers.is_empty()));
                    for p in (0..4).map(peer) {
                        let count = table.iter().filter(|(q, _)| *q == p).count();
                        assert_eq!(
                            state.watcher_counts.get(&p).copied(),
                            (count > 0).then_some(count),
                            "count for {p}"
                        );
                    }
                }
            });
    }
}
