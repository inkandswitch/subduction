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
use keyhive_core::{
    listener::membership::MembershipListener,
    principal::public::Public,
    store::ciphertext::{CiphertextStore, CiphertextStoreExt},
};
use keyhive_crypto::{content::reference::ContentRef, signer::async_signer::AsyncSigner};

use crate::{
    KeyhivePeerId, KeyhiveProtocol, ProtocolError,
    connection::KeyhiveConnection,
    message::{AgentHashMap, EventHash},
    storage::KeyhiveStorage,
};

/// Periodic event cache.
#[derive(Debug, Default)]
pub(crate) struct PeriodicEventCache {
    /// Per-agent hash sets.
    ///
    /// For any peer pair `(a, b)`, the relevant event set is
    /// `public_hashes ∪ (agent_hashes[a] ∩ agent_hashes[b])`. The
    /// intersection is computed at query time.
    agent_hashes: BTreeMap<KeyhivePeerId, BTreeSet<EventHash>>,

    /// Hashes of events accessible to the well-known public agent.
    ///
    /// Unioned into every peer pair's hash set without intersection.
    public_hashes: BTreeSet<EventHash>,

    /// The public set materialized as an `AgentHashMap`.
    public_map: Arc<AgentHashMap>,

    /// Hash -> `Arc`-shared bincode event bytes. Monotonic growth across
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
    pub(crate) fn new() -> Self {
        Self {
            agent_hashes: BTreeMap::new(),
            public_hashes: BTreeSet::new(),
            public_map: Arc::new(AgentHashMap::new()),
            event_data: BTreeMap::new(),
            last_total_ops: None,
        }
    }

    /// Whether the cache has no agent entries and no public hashes.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.agent_hashes.is_empty() && self.public_hashes.is_empty()
    }

    /// Total number of tracked agents (excluding the public agent,
    /// which lives in its own field).
    #[cfg(test)]
    pub(crate) fn agent_count(&self) -> usize {
        self.agent_hashes.len()
    }

    /// Cached hashes accessible to the public agent.
    #[cfg(test)]
    pub(crate) const fn public_hashes(&self) -> &BTreeSet<EventHash> {
        &self.public_hashes
    }

    /// Last observed keyhive total-ops count.
    #[cfg(test)]
    pub(crate) const fn last_total_ops(&self) -> Option<u64> {
        self.last_total_ops
    }

    /// Public hashes ∪ (A ∩ B).
    ///
    /// Every event the public agent can see is unconditionally included,
    /// plus events reachable to both `a` and `b`. Returns an `Arc`-shared
    /// map whose values are `Arc`-shared bytes, so callers share the cache's
    /// single copy rather than deep-copying.
    pub(crate) fn events_for_peer_pair(
        &self,
        a: &KeyhivePeerId,
        b: &KeyhivePeerId,
    ) -> Arc<AgentHashMap> {
        let (Some(ha), Some(hb)) = (self.agent_hashes.get(a), self.agent_hashes.get(b)) else {
            // Unknown peer: served set is the public set.
            return self.public_map.dupe();
        };

        // Private events reachable to both a and b, beyond the public set and
        // present in the byte store.
        let mut private = ha
            .intersection(hb)
            .filter(|h| !self.public_map.contains_key(*h))
            .filter_map(|h| self.event_data.get(h).map(|bytes| (*h, bytes.dupe())))
            .peekable();

        if private.peek().is_none() {
            return self.public_map.dupe();
        }

        let mut out = (*self.public_map).clone();
        out.extend(private);
        Arc::new(out)
    }

    /// Classify a batch of changed event hashes against the connected peer
    /// set.
    ///
    /// A peer is selected when any changed hash is public (visible to every
    /// peer) or is in that peer's cached visible set. Hashes absent from the
    /// visible projection — not public and not visible to the local agent —
    /// are pending (awaiting dependencies) or lagged; they are returned for
    /// conservative handling by the caller.
    pub(crate) fn notification_targets(
        &self,
        local: &KeyhivePeerId,
        connected: &BTreeSet<KeyhivePeerId>,
        changed: &BTreeSet<EventHash>,
    ) -> (BTreeSet<KeyhivePeerId>, BTreeSet<EventHash>) {
        let public_hit = self.public_hashes.intersection(changed).next().is_some();
        let mut peers = BTreeSet::new();
        let mut unclassified = BTreeSet::new();
        let local_visible = self.agent_hashes.get(local);
        if public_hit {
            // Public events select every connected peer.
            peers.extend(connected.iter().cloned());
        } else {
            for peer in connected {
                if self
                    .agent_hashes
                    .get(peer)
                    .is_some_and(|visible| visible.intersection(changed).next().is_some())
                {
                    peers.insert(peer.clone());
                }
            }
        }
        // Hashes absent from the visible projection — not public and not visible
        // to the local agent — are unclassified (pending or lagged).
        for hash in changed {
            let is_public = self.public_hashes.contains(hash);
            let is_local = local_visible.is_some_and(|visible| visible.contains(hash));
            if !is_public && !is_local {
                unclassified.insert(*hash);
            }
        }
        (peers, unclassified)
    }

    /// Build the public set as an `AgentHashMap`.
    fn build_public_map(&self) -> AgentHashMap {
        self.public_hashes
            .iter()
            .filter_map(|h| self.event_data.get(h).map(|bytes| (*h, bytes.dupe())))
            .collect()
    }

    /// Refresh the cache from the protocol's keyhive view.
    ///
    /// Always performs a full walk of the keyhive's per-agent visible event
    /// sets. Callers gate on cache-generation currency
    /// ([`crate::protocol`]'s `ensure_cache_current`), which only requests a
    /// rebuild after an explicit change signal; a rebuild must never be
    /// skipped here based on indirect heuristics. `total_ops` is not a sound
    /// change detector: pending-to-resolved incorporations and offsetting
    /// membership changes alter the projection without changing the count,
    /// and skipping their walk republishes a stale snapshot as current.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if event serialization fails.
    pub(crate) async fn refresh<
        Signer,
        CRef,
        Plaintext,
        CipherStore,
        Listener,
        Rng,
        Conn,
        Store,
        Async,
    >(
        &mut self,
        protocol: &KeyhiveProtocol<
            Signer,
            CRef,
            Plaintext,
            CipherStore,
            Listener,
            Rng,
            Conn,
            Store,
            Async,
        >,
    ) -> Result<(), ProtocolError<Conn::SendError>>
    where
        Signer: AsyncSigner<Async> + Clone,
        CRef: ContentRef + serde::de::DeserializeOwned,
        Plaintext: for<'de> serde::Deserialize<'de>,
        CipherStore: CiphertextStore<Async, CRef, Plaintext>
            + CiphertextStoreExt<Async, CRef, Plaintext>
            + Clone,
        Listener: MembershipListener<Async, Signer, CRef>,
        Rng: rand::CryptoRng + rand::RngCore,
        Conn: KeyhiveConnection<Async>,
        Conn::SendError: 'static,
        Conn::DisconnectError: 'static,
        Store: KeyhiveStorage<Async>,
        Async: future_form::FutureForm,
    {
        let known: alloc::collections::BTreeSet<_> = self.event_data.keys().copied().collect();
        let all_agent_events = protocol.all_agent_events(&known).await?;

        for (h, data) in all_agent_events.event_data {
            self.event_data.entry(h).or_insert(data);
        }

        // The public set is unioned unconditionally into every pair
        // query, so it lives in its own field.
        let mut new_agent_hashes = all_agent_events.agent_hashes;
        let public_peer = KeyhivePeerId::from_identifier(&Public.id());
        let new_public = new_agent_hashes.remove(&public_peer).unwrap_or_default();

        self.agent_hashes = new_agent_hashes;
        self.public_hashes = new_public;
        // Materialize the public set once per refresh so per-request serving
        // can share it by `Arc` instead of rebuilding it.
        self.public_map = Arc::new(self.build_public_map());
        self.last_total_ops = Some(protocol.total_ops().await);
        Ok(())
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
    fn local(seed: u8) -> KeyhivePeerId {
        peer(seed)
    }

    fn connected(seeds: &[u8]) -> BTreeSet<KeyhivePeerId> {
        seeds.iter().copied().map(peer).collect()
    }

    fn changed(seeds: &[u8]) -> BTreeSet<EventHash> {
        seeds.iter().copied().map(|s| [s; 32]).collect()
    }

    #[test]
    fn notification_targets_public_event_selects_every_connected_peer() {
        let mut cache = PeriodicEventCache::new();
        let public_h: EventHash = [9; 32];
        cache.public_hashes.insert(public_h);
        let conn = connected(&[1, 2, 3]);
        let (peers, unclassified) =
            cache.notification_targets(&local(0), &conn, &changed(&[9]));
        assert_eq!(peers, conn);
        assert!(unclassified.is_empty());
    }

    #[test]
    fn notification_targets_private_event_selects_only_visible_peers() {
        let mut cache = PeriodicEventCache::new();
        cache.agent_hashes.insert(peer(0), changed(&[1])); // local
        cache.agent_hashes.insert(peer(1), changed(&[1])); // alice sees it
        cache.agent_hashes.insert(peer(2), changed(&[2])); // bob does not
        let (peers, unclassified) =
            cache.notification_targets(&local(0), &connected(&[1, 2]), &changed(&[1]));
        assert_eq!(peers, connected(&[1]));
        assert!(unclassified.is_empty());
    }

    #[test]
    fn notification_targets_grant_then_revoke_changes_selection() {
        let mut cache = PeriodicEventCache::new();
        cache.agent_hashes.insert(peer(0), changed(&[1])); // local
        // Grant: alice gains visibility.
        cache.agent_hashes.insert(peer(1), changed(&[1]));
        let (peers, _) = cache.notification_targets(&local(0), &connected(&[1]), &changed(&[1]));
        assert_eq!(peers, connected(&[1]));
        // Revoke: alice loses visibility.
        cache.agent_hashes.remove(&peer(1));
        let (peers, _) = cache.notification_targets(&local(0), &connected(&[1]), &changed(&[1]));
        assert!(peers.is_empty());
    }

    #[test]
    fn notification_targets_unknown_peer_is_not_selected() {
        let mut cache = PeriodicEventCache::new();
        cache.agent_hashes.insert(peer(0), changed(&[1])); // local
        cache.agent_hashes.insert(peer(1), changed(&[1]));
        // Carol is connected but unknown to the cache.
        let (peers, _) =
            cache.notification_targets(&local(0), &connected(&[1, 3]), &changed(&[1]));
        assert_eq!(peers, connected(&[1]));
    }

    #[test]
    fn notification_targets_pending_hash_is_unclassified() {
        let cache = PeriodicEventCache::new();
        // The pending hash is in nobody's projection (not even local).
        let (peers, unclassified) =
            cache.notification_targets(&local(0), &connected(&[1]), &changed(&[5]));
        assert!(peers.is_empty());
        assert_eq!(unclassified, changed(&[5]));
    }

    #[test]
    fn notification_targets_local_only_event_is_classified_not_unclassified() {
        let mut cache = PeriodicEventCache::new();
        cache.agent_hashes.insert(peer(0), changed(&[6])); // local only
        let (peers, unclassified) =
            cache.notification_targets(&local(0), &connected(&[1]), &changed(&[6]));
        assert!(peers.is_empty());
        assert!(unclassified.is_empty(), "incorporated local-only event is classified");
    }

    #[test]
    fn notification_targets_mixed_public_and_private_events() {
        let mut cache = PeriodicEventCache::new();
        let public_h: EventHash = [9; 32];
        cache.public_hashes.insert(public_h);
        cache.agent_hashes.insert(peer(0), changed(&[6])); // local only private
        let conn = connected(&[1, 2]);
        let mut batch = changed(&[6]);
        batch.insert(public_h);
        let (peers, unclassified) = cache.notification_targets(&local(0), &conn, &batch);
        assert_eq!(peers, conn);
        assert!(unclassified.is_empty(), "known local private hash must not be marked unclassified");
    }

    #[test]
    fn events_for_peer_pair_returns_only_public_when_peers_unknown() {
        let mut cache = PeriodicEventCache::new();
        let h: EventHash = [9; 32];
        cache.event_data.insert(h, vec![1, 2, 3].into());
        cache.public_hashes.insert(h);
        cache.public_map = Arc::new(cache.build_public_map());

        let result = cache.events_for_peer_pair(&peer(1), &peer(2));
        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&h));
    }

    #[test]
    fn events_for_peer_pair_intersects_known_peers() {
        let mut cache = PeriodicEventCache::new();
        let public_h: EventHash = [9; 32];
        let shared_h: EventHash = [1; 32];
        let alice_only: EventHash = [2; 32];
        let bob_only: EventHash = [3; 32];

        for h in [public_h, shared_h, alice_only, bob_only] {
            cache.event_data.insert(h, vec![h[0]].into());
        }

        cache.public_hashes.insert(public_h);
        cache.public_map = Arc::new(cache.build_public_map());

        let mut alice = BTreeSet::new();
        alice.insert(shared_h);
        alice.insert(alice_only);
        cache.agent_hashes.insert(peer(1), alice);

        let mut bob = BTreeSet::new();
        bob.insert(shared_h);
        bob.insert(bob_only);
        cache.agent_hashes.insert(peer(2), bob);

        let result = cache.events_for_peer_pair(&peer(1), &peer(2));
        assert!(result.contains_key(&public_h), "public hash present");
        assert!(result.contains_key(&shared_h), "shared hash present");
        assert!(!result.contains_key(&alice_only), "alice-only excluded");
        assert!(!result.contains_key(&bob_only), "bob-only excluded");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn events_for_peer_pair_excludes_intersection_hash_absent_from_store() {
        // A hash reachable to both peers but never serialized into event_data
        // must not appear, and the served map must equal the public set.
        let mut cache = PeriodicEventCache::new();
        let public_h: EventHash = [9; 32];
        let ghost_h: EventHash = [4; 32];
        cache.event_data.insert(public_h, vec![1].into());
        cache.public_hashes.insert(public_h);
        cache.public_map = Arc::new(cache.build_public_map());

        cache
            .agent_hashes
            .insert(peer(1), BTreeSet::from([ghost_h]));
        cache
            .agent_hashes
            .insert(peer(2), BTreeSet::from([ghost_h]));

        let result = cache.events_for_peer_pair(&peer(1), &peer(2));
        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&public_h));
        assert!(!result.contains_key(&ghost_h), "unserialized hash excluded");
    }

    #[test]
    fn events_for_peer_pair_shares_public_arc_when_no_private_events() {
        let mut cache = PeriodicEventCache::new();
        let public_h: EventHash = [9; 32];
        cache.event_data.insert(public_h, vec![1].into());
        cache.public_hashes.insert(public_h);
        cache.public_map = Arc::new(cache.build_public_map());

        // Unknown peers -> public-only -> shares the cached Arc (no allocation).
        let r1 = cache.events_for_peer_pair(&peer(1), &peer(2));
        assert!(Arc::ptr_eq(&r1, &cache.public_map));

        // Known peers whose intersection is only the public hash -> still shared.
        cache
            .agent_hashes
            .insert(peer(1), BTreeSet::from([public_h]));
        cache
            .agent_hashes
            .insert(peer(2), BTreeSet::from([public_h]));
        let r2 = cache.events_for_peer_pair(&peer(1), &peer(2));
        assert!(Arc::ptr_eq(&r2, &cache.public_map));
    }

    #[test]
    fn events_for_peer_pair_returns_fresh_arc_with_private_events() {
        let mut cache = PeriodicEventCache::new();
        let public_h: EventHash = [9; 32];
        let private_h: EventHash = [5; 32];
        cache.event_data.insert(public_h, vec![1].into());
        cache.event_data.insert(private_h, vec![2].into());
        cache.public_hashes.insert(public_h);
        cache.public_map = Arc::new(cache.build_public_map());

        // Both peers reach the private hash beyond the public set, so the rare
        // clone-and-extend path runs.
        cache
            .agent_hashes
            .insert(peer(1), BTreeSet::from([public_h, private_h]));
        cache
            .agent_hashes
            .insert(peer(2), BTreeSet::from([public_h, private_h]));

        let result = cache.events_for_peer_pair(&peer(1), &peer(2));
        // A fresh map, not an alias of the shared public set.
        assert!(!Arc::ptr_eq(&result, &cache.public_map));
        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&public_h));
        assert!(result.contains_key(&private_h));
        // The shared public map is left untouched (still public-only).
        assert_eq!(cache.public_map.len(), 1);
    }
}
