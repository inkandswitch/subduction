//! Periodic event cache.
//!
//! - Per-agent hash sets: what each agent can see, expressed as
//!   event hashes. Rebuilt on each refresh and atomically swapped.
//! - Global byte stores: keyed by hash, deduplicated across all
//!   agents. Events are immutable so these grow monotonically.
//!
//! [`hashes_for_peer_pair`](PeriodicEventCache::hashes_for_peer_pair)
//! joins the per-agent sets with the global byte stores at request time.

use alloc::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use keyhive_core::{
    listener::membership::MembershipListener,
    principal::public::Public,
    store::ciphertext::{CiphertextStore, CiphertextStoreExt},
};
use keyhive_crypto::{content::reference::ContentRef, signer::async_signer::AsyncSigner};

use crate::{
    KeyhivePeerId, KeyhiveProtocol, ProtocolError,
    connection::KeyhiveConnection,
    message::{AgentHashMap, CborBytes, EventBytes, EventHash},
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

    /// Hash -> raw event bytes paired with pre-encoded CBOR byte-string
    /// framing. Monotonic growth across refreshes. Events are immutable,
    /// so entries are never invalidated.
    event_data: BTreeMap<EventHash, (EventBytes, CborBytes)>,

    /// Last keyhive `total_ops` seen, so refresh can skip rebuilds
    /// when nothing has changed.
    last_total_ops: Option<u64>,
}

impl PeriodicEventCache {
    /// Create an empty cache.
    pub(crate) const fn new() -> Self {
        Self {
            agent_hashes: BTreeMap::new(),
            public_hashes: BTreeSet::new(),
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
    /// plus events reachable to *both* `a` and `b`. Returns a map keyed
    /// by hash with `(EventBytes, CborBytes)` cloned from the byte stores.
    pub(crate) fn events_for_peer_pair(
        &self,
        a: &KeyhivePeerId,
        b: &KeyhivePeerId,
    ) -> AgentHashMap {
        let mut out = AgentHashMap::new();
        for h in &self.public_hashes {
            self.insert_pair_entry(&mut out, *h);
        }
        if let (Some(ha), Some(hb)) = (self.agent_hashes.get(a), self.agent_hashes.get(b)) {
            for h in ha.intersection(hb) {
                if let Entry::Vacant(e) = out.entry(*h)
                    && let Some((eb, cb)) = self.event_data.get(h)
                {
                    e.insert((eb.clone(), cb.clone()));
                }
            }
        }
        out
    }

    /// Insert `hash`'s deduplicated bytes into `out`.
    fn insert_pair_entry(&self, out: &mut AgentHashMap, hash: EventHash) {
        if let Some((eb, cb)) = self.event_data.get(&hash) {
            out.insert(hash, (eb.clone(), cb.clone()));
        }
    }

    /// Refresh the cache from the protocol's keyhive view.
    ///
    /// Returns `Ok(true)` if the cache was rebuilt, `Ok(false)` if the
    /// total op count was unchanged since the last refresh.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if event serialization fails.
    pub(crate) async fn refresh<Signer, T, P, C, L, R, Conn, Store, K>(
        &mut self,
        protocol: &KeyhiveProtocol<Signer, T, P, C, L, R, Conn, Store, K>,
    ) -> Result<bool, ProtocolError<Conn::SendError>>
    where
        Signer: AsyncSigner<K> + Clone,
        T: ContentRef + serde::de::DeserializeOwned,
        P: for<'de> serde::Deserialize<'de>,
        C: CiphertextStore<K, T, P> + CiphertextStoreExt<K, T, P> + Clone,
        L: MembershipListener<K, Signer, T>,
        R: rand::CryptoRng + rand::RngCore,
        Conn: KeyhiveConnection<K>,
        Conn::SendError: 'static,
        Conn::DisconnectError: 'static,
        Store: KeyhiveStorage<K>,
        K: future_form::FutureForm,
    {
        let total = protocol.total_ops().await;
        if self.last_total_ops == Some(total) {
            return Ok(false);
        }

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
        self.last_total_ops = Some(total);
        Ok(true)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use alloc::vec;

    use super::*;

    fn peer(seed: u8) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes([seed; 32])
    }

    #[test]
    fn events_for_peer_pair_returns_only_public_when_peers_unknown() {
        let mut cache = PeriodicEventCache::new();
        let h: EventHash = [9; 32];
        cache
            .event_data
            .insert(h, (vec![1, 2, 3], vec![0x43, 1, 2, 3]));
        cache.public_hashes.insert(h);

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
            cache.event_data.insert(h, (vec![h[0]], vec![0x41, h[0]]));
        }

        cache.public_hashes.insert(public_h);

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
}
