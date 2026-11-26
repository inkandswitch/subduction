//! Periodic event cache.
//!
//! - Per-agent hash sets: what each agent can see, expressed as
//!   event hashes. Rebuilt on each refresh and atomically swapped.
//! - Global byte stores: keyed by hash, deduplicated across all
//!   agents. Events are immutable so these grow monotonically.
//!
//! [`events_for_peer_pair`](PeriodicEventCache::events_for_peer_pair)
//! joins the per-agent sets with the global byte stores at request time.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use dupe::Dupe;

use crate::{
    KeyhivePeerId, KeyhiveProtocol,
    access::KeyhiveAccess,
    connection::KeyhiveConnection,
    message::{AgentHashMap, EventHash},
    protocol::KeyhiveProtocolError,
    storage::KeyhiveStorage,
};

/// Periodic event cache.
#[derive(Debug, Default)]
pub(crate) struct PeriodicEventCache {
    /// Per-agent hash sets.
    ///
    /// For any peer pair `(a, b)`, the relevant event set is
    /// `agent_hashes[a] ∩ agent_hashes[b]`, computed at query time.
    agent_hashes: BTreeMap<KeyhivePeerId, BTreeSet<EventHash>>,

    /// Hash -> `Arc`-shared event bytes. Monotonic growth across
    /// refreshes. Events are immutable, so entries are never invalidated.
    /// Served responses clone the `Arc`s, not the bytes, so concurrent
    /// responses share this one copy.
    event_data: BTreeMap<EventHash, Arc<[u8]>>,

    /// Last keyhive `total_ops` seen, so refresh can skip rebuilds
    /// when nothing has changed.
    last_total_ops: Option<u64>,
}

impl PeriodicEventCache {
    /// Create an empty cache.
    pub(crate) const fn new() -> Self {
        Self {
            agent_hashes: BTreeMap::new(),
            event_data: BTreeMap::new(),
            last_total_ops: None,
        }
    }

    /// Whether the cache has no agent entries.
    #[cfg(all(test, feature = "test-utils"))]
    pub(crate) fn is_empty(&self) -> bool {
        self.agent_hashes.is_empty()
    }

    /// Total number of tracked agents.
    #[cfg(all(test, feature = "test-utils"))]
    pub(crate) fn agent_count(&self) -> usize {
        self.agent_hashes.len()
    }

    /// Last observed keyhive total-ops count.
    #[cfg(all(test, feature = "test-utils"))]
    pub(crate) const fn last_total_ops(&self) -> Option<u64> {
        self.last_total_ops
    }

    /// Events both `a` and `b` can see (`A ∩ B`) that are in the byte store.
    ///
    /// Returns an `Arc`-shared map whose values are `Arc`-shared bytes, so
    /// callers share the cache's single copy rather than deep-copying. A peer
    /// the cache does not know sees nothing.
    pub(crate) fn events_for_peer_pair(
        &self,
        a: &KeyhivePeerId,
        b: &KeyhivePeerId,
    ) -> Arc<AgentHashMap> {
        let (Some(ha), Some(hb)) = (self.agent_hashes.get(a), self.agent_hashes.get(b)) else {
            return Arc::default();
        };
        Arc::new(
            ha.intersection(hb)
                .filter_map(|h| self.event_data.get(h).map(|bytes| (*h, bytes.dupe())))
                .collect(),
        )
    }

    /// Refresh the cache from the protocol's keyhive view.
    ///
    /// Returns `Ok(true)` if the cache was rebuilt, `Ok(false)` if the
    /// total op count was unchanged since the last refresh.
    ///
    /// # Errors
    ///
    /// Returns [`KeyhiveProtocolError`] if keyhive cannot count or list its events.
    pub(crate) async fn refresh<K, Conn, Store, Async>(
        &mut self,
        protocol: &KeyhiveProtocol<K, Conn, Store, Async>,
    ) -> Result<bool, KeyhiveProtocolError<K, Conn, Async>>
    where
        K: KeyhiveAccess<Async> + Dupe,
        Conn: KeyhiveConnection<Async>,
        Conn::SendError: 'static,
        Store: KeyhiveStorage<Async>,
        Async: future_form::FutureForm,
    {
        let total = protocol.total_ops().await?;
        if self.last_total_ops == Some(total) {
            return Ok(false);
        }

        let known: alloc::collections::BTreeSet<_> = self.event_data.keys().copied().collect();
        let all_agent_events = protocol.all_agent_events(&known).await?;

        for (h, data) in all_agent_events.event_data {
            self.event_data.entry(h).or_insert(data);
        }

        self.agent_hashes = all_agent_events.agent_hashes;
        self.last_total_ops = Some(total);
        Ok(true)
    }
}

/// Heap-profiling membenches, gated behind `dhat-heap` (which implies
/// `test-utils`).
#[cfg(all(test, feature = "dhat-heap"))]
mod membench;

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use alloc::vec;

    use super::*;

    fn peer(seed: u8) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes([seed; 32])
    }
    #[test]
    fn events_for_peer_pair_is_empty_for_unknown_peers() {
        let mut cache = PeriodicEventCache::new();
        let h: EventHash = [9; 32];
        cache.event_data.insert(h, vec![1, 2, 3].into());
        cache.agent_hashes.insert(peer(1), BTreeSet::from([h]));

        assert!(cache.events_for_peer_pair(&peer(1), &peer(2)).is_empty());
        assert!(cache.events_for_peer_pair(&peer(3), &peer(4)).is_empty());
    }

    #[test]
    fn events_for_peer_pair_intersects_known_peers() {
        let mut cache = PeriodicEventCache::new();
        let shared_h: EventHash = [1; 32];
        let alice_only: EventHash = [2; 32];
        let bob_only: EventHash = [3; 32];
        for h in [shared_h, alice_only, bob_only] {
            cache.event_data.insert(h, vec![h[0]].into());
        }
        cache
            .agent_hashes
            .insert(peer(1), BTreeSet::from([shared_h, alice_only]));
        cache
            .agent_hashes
            .insert(peer(2), BTreeSet::from([shared_h, bob_only]));

        let result = cache.events_for_peer_pair(&peer(1), &peer(2));
        assert!(result.contains_key(&shared_h), "shared hash present");
        assert!(!result.contains_key(&alice_only), "alice-only excluded");
        assert!(!result.contains_key(&bob_only), "bob-only excluded");
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn events_for_peer_pair_excludes_intersection_hash_absent_from_store() {
        // A hash reachable to both peers but never serialized into event_data
        // must not appear.
        let mut cache = PeriodicEventCache::new();
        let ghost_h: EventHash = [4; 32];
        cache
            .agent_hashes
            .insert(peer(1), BTreeSet::from([ghost_h]));
        cache
            .agent_hashes
            .insert(peer(2), BTreeSet::from([ghost_h]));

        assert!(cache.events_for_peer_pair(&peer(1), &peer(2)).is_empty());
    }
}
