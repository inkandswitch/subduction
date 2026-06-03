//! A capacity-bounded LRU cache built on top of [`ShardedMap`].
//!
//! [`BoundedShardedMap`] is a thin layer *around* a plain [`ShardedMap`]
//! that adds a per-shard size bound with least-recently-used eviction. The
//! map itself stays a pure generic concurrent map; all recency and
//! eviction bookkeeping lives here, keeping the two concerns separate.
//!
//! # Design
//!
//! Recency is **fused into the map value**: the wrapped map stores
//! `(V, Tick)` rather than `V`, so a value and its last-access tick live in
//! the same slot under the same shard [`Mutex`]. Because every access
//! already takes the shard lock, the value and its tick are always updated
//! together — there is no separate recency structure that could drift out
//! of sync with the data.
//!
//! The public API is expressed in terms of plain `V`; the tick is an
//! internal detail callers never see.
//!
//! # Eviction
//!
//! The cap is distributed evenly across shards (`ceil(max / N)` each).
//! Inserting a *new* key into a full shard first evicts that shard's
//! least-recently-used entry (smallest tick), under the shard lock, so the
//! evict-and-insert is atomic. Per-shard caps avoid a global lock and stay
//! balanced because keys are SipHash-distributed across shards.
//!
//! Eviction only ever removes from this in-memory map; callers must be able
//! to reconstruct an evicted entry from a durable source (this is a cache).
//!
//! [`ShardedMap`]: crate::collections::sharded_map::ShardedMap
//! [`Mutex`]: async_lock::Mutex

use alloc::vec::Vec;
use core::{
    hash::Hash,
    sync::atomic::{AtomicU64, Ordering},
};

use sedimentree_core::collections::Map;

use crate::collections::sharded_map::ShardedMap;

/// A monotonic per-access stamp. Larger == more recently used.
///
/// Sourced from a process-wide counter (not a wall clock), so it is
/// runtime-agnostic (`no_std` / Wasm safe). Only relative ordering matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Tick(u64);

/// A capacity-bounded LRU cache over a
/// [`ShardedMap`](super::sharded_map::ShardedMap).
///
/// See the module docs for the design. The wrapped map stores `(V, Tick)`;
/// this type's API is in terms of `V`.
#[derive(Debug)]
pub struct BoundedShardedMap<K: Hash + Ord, V, const N: usize = 256> {
    inner: ShardedMap<K, (V, Tick), N>,

    /// Monotonic logical clock; `fetch_add(1)` stamps each access.
    tick: AtomicU64,

    /// Per-shard resident capacity. `None` ⇒ unbounded (no eviction and no
    /// recency bookkeeping — identical cost to a plain `ShardedMap`).
    per_shard_capacity: Option<usize>,
}

impl<K: Hash + Ord, V, const N: usize> BoundedShardedMap<K, V, N> {
    /// Create a new, unbounded map with a randomly generated shard key.
    ///
    /// Unbounded means no eviction; use [`with_capacity`](Self::with_capacity)
    /// to bound the resident set.
    ///
    /// # Panics
    ///
    /// Panics if the system random number generator fails.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: ShardedMap::new(),
            tick: AtomicU64::new(0),
            per_shard_capacity: None,
        }
    }

    /// Create a new, unbounded map with explicit `SipHash` keys
    /// (deterministic; for testing).
    #[must_use]
    pub fn with_key(key0: u64, key1: u64) -> Self {
        Self {
            inner: ShardedMap::with_key(key0, key1),
            tick: AtomicU64::new(0),
            per_shard_capacity: None,
        }
    }

    /// Bound the resident entries with an **approximate** capacity.
    ///
    /// The cap is enforced **per shard**, not globally: each shard holds at
    /// most `ceil(max_entries / N)` (floored at 1). Inserting a new key into
    /// a full shard first evicts that shard's least-recently-used entry.
    ///
    /// Consequently the effective global ceiling is
    /// `ceil(max_entries / N) * N`, which can exceed `max_entries` and is
    /// **at least `N`**. A `max_entries` smaller than the shard count `N`
    /// still permits up to one entry per shard. Per-shard caps avoid a
    /// global lock and stay balanced because keys are SipHash-distributed,
    /// at the cost of this slack; treat `max_entries` as an order-of-
    /// magnitude target rather than an exact limit.
    ///
    /// Eviction only removes from this in-memory map; an evicted entry must
    /// be reconstructable from a durable source (this is a cache).
    #[must_use]
    pub fn with_capacity(mut self, max_entries: usize) -> Self {
        let per_shard = max_entries.div_ceil(N).max(1);
        self.per_shard_capacity = Some(per_shard);
        self
    }

    /// The per-shard resident capacity, if bounded.
    #[must_use]
    pub const fn per_shard_capacity(&self) -> Option<usize> {
        self.per_shard_capacity
    }

    /// Next monotonic access tick.
    ///
    /// When unbounded (`per_shard_capacity == None`) recency is never
    /// consulted, so this skips the atomic increment entirely and returns a
    /// constant — keeping the uncapped (default) path free of recency
    /// bookkeeping.
    #[inline]
    fn next_tick(&self) -> Tick {
        if self.per_shard_capacity.is_none() {
            return Tick(0);
        }
        Tick(self.tick.fetch_add(1, Ordering::Relaxed))
    }

    /// Evict the least-recently-used entry from `shard` if it is at or over
    /// the per-shard capacity and `incoming` is a *new* key (so updating an
    /// existing key never triggers eviction).
    ///
    /// Must be called while holding `shard`'s lock so the evict-and-insert
    /// is atomic. No-op when unbounded.
    fn evict_if_full_locked(&self, shard: &mut Map<K, (V, Tick)>, incoming: &K)
    where
        K: Clone,
    {
        let Some(cap) = self.per_shard_capacity else {
            return;
        };
        if shard.len() < cap || shard.contains_key(incoming) {
            return;
        }

        // Smallest tick == least recently used.
        let victim = shard
            .iter()
            .min_by_key(|(_, (_, tick))| *tick)
            .map(|(k, _)| k.clone());

        if let Some(victim) = victim {
            shard.remove(&victim);
        }
    }

    /// Get a clone of the value for `key`, recording an access (LRU touch)
    /// on a hit so frequently-read keys stay resident.
    pub async fn get_cloned(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let tick = self.next_tick();
        let mut shard = self.inner.get_shard_containing(key).lock().await;
        let entry = shard.get_mut(key)?;
        if self.per_shard_capacity.is_some() {
            entry.1 = tick;
        }
        Some(entry.0.clone())
    }

    /// Apply `f` to the value for `key` if present, recording an access on a
    /// hit. Returns `None` if the key is absent.
    pub async fn with_entry<F: FnOnce(&mut V) -> R, R>(&self, key: &K, f: F) -> Option<R> {
        let tick = self.next_tick();
        let mut shard = self.inner.get_shard_containing(key).lock().await;
        let entry = shard.get_mut(key)?;
        if self.per_shard_capacity.is_some() {
            entry.1 = tick;
        }
        Some(f(&mut entry.0))
    }

    /// Get-or-insert-default for `key`, then apply `f` to the value.
    ///
    /// Enforces the per-shard capacity for a newly-inserted key (evicting
    /// the LRU entry first) and records an access.
    pub async fn with_entry_or_default<F: FnOnce(&mut V) -> R, R>(&self, key: K, f: F) -> R
    where
        V: Default,
        K: Clone,
    {
        let tick = self.next_tick();
        let mut shard = self.inner.get_shard_containing(&key).lock().await;
        self.evict_if_full_locked(&mut shard, &key);
        let entry = shard.entry(key).or_insert_with(|| (V::default(), tick));
        if self.per_shard_capacity.is_some() {
            entry.1 = tick;
        }
        f(&mut entry.0)
    }

    /// Return a clone of the value for `key`, inserting `make()`'s result
    /// first if the key is absent.
    ///
    /// `make` is invoked **while the shard lock is held**, so it must be
    /// cheap and non-blocking — callers should compute any expensive value
    /// (e.g. an async load from durable storage) *before* this call and pass
    /// a closure that merely returns it. If a concurrent caller already
    /// inserted a value, that one wins and `make()`'s result is dropped
    /// (idempotent for a cache over a single durable source). Enforces the
    /// per-shard capacity for a new key and records an access.
    ///
    /// For the load-on-miss pattern that must `.await` a fetch, use
    /// [`with_entry_hydrated`](Self::with_entry_hydrated), which releases the
    /// lock across the load.
    pub async fn get_or_insert_with<F: FnOnce() -> V>(&self, key: K, make: F) -> V
    where
        V: Clone,
        K: Clone,
    {
        let tick = self.next_tick();
        let mut shard = self.inner.get_shard_containing(&key).lock().await;
        if let Some(entry) = shard.get_mut(&key) {
            if self.per_shard_capacity.is_some() {
                entry.1 = tick;
            }
            return entry.0.clone();
        }
        self.evict_if_full_locked(&mut shard, &key);
        let value = make();
        shard.insert(key, (value.clone(), tick));
        value
    }

    /// Hydrate-on-miss, then run a read-only `inspect` against the resident
    /// value.
    ///
    /// Ensures the value for `key` is resident (loading it from the backing
    /// store with **no lock held** on a miss, like
    /// [`with_entry_hydrated`](Self::with_entry_hydrated)), then applies
    /// `inspect` under the shard lock and returns its result. Leaves the
    /// value resident so a subsequent mutation is a cheap hit.
    ///
    /// Returns `Ok(None)` if the key does not exist in the backing store
    /// (`load` returned `Ok(None)`); nothing is installed in that case.
    ///
    /// Used by write paths to read pre-mutation state (e.g. "is this commit
    /// new?") without a separate storage scan and without cloning the value.
    ///
    /// # Errors
    ///
    /// Returns `load`'s error if hydration fails.
    pub async fn with_hydrated_ref<Load, Fut, Inspect, R, E>(
        &self,
        key: K,
        load: Load,
        inspect: Inspect,
    ) -> Result<Option<R>, E>
    where
        K: Clone,
        Load: FnOnce() -> Fut,
        Fut: core::future::Future<Output = Result<Option<V>, E>>,
        Inspect: FnOnce(&V) -> R,
    {
        // Fast path: resident hit.
        {
            let tick = self.next_tick();
            let mut shard = self.inner.get_shard_containing(&key).lock().await;
            if let Some(entry) = shard.get_mut(&key) {
                if self.per_shard_capacity.is_some() {
                    entry.1 = tick;
                }
                return Ok(Some(inspect(&entry.0)));
            }
        }

        // Miss: load with NO lock held.
        let Some(loaded) = load().await? else {
            return Ok(None);
        };

        // Install (or adopt a concurrently-installed value) and inspect.
        let tick = self.next_tick();
        let mut shard = self.inner.get_shard_containing(&key).lock().await;
        if let Some(entry) = shard.get_mut(&key) {
            if self.per_shard_capacity.is_some() {
                entry.1 = tick;
            }
            return Ok(Some(inspect(&entry.0)));
        }
        self.evict_if_full_locked(&mut shard, &key);
        let result = inspect(&loaded);
        shard.insert(key, (loaded, tick));
        Ok(Some(result))
    }

    /// Hydrate-on-miss, then mutate.
    ///
    /// This is the **write-path** entry point: it guarantees the mutation is
    /// applied to the *complete* value, not a fresh default, even after the
    /// key was evicted. On a resident hit it mutates in place. On a miss it
    /// calls `load` (an async fetch from the durable backing store, run with
    /// **no shard lock held**), installs the loaded value — or `V::default()`
    /// if `load` returns `Ok(None)`, meaning the key does not exist in the
    /// backing store yet — then applies `mutate`. Enforces the per-shard
    /// capacity for a newly-installed key and records an access.
    ///
    /// Without this, a plain [`with_entry_or_default`](Self::with_entry_or_default)
    /// on an evicted key would start from an empty default and silently drop
    /// the value's prior (durably-stored) state.
    ///
    /// # Errors
    ///
    /// Returns `load`'s error if hydration fails (the mutation is not applied).
    pub async fn with_entry_hydrated<Load, Fut, Mutate, R, E>(
        &self,
        key: K,
        load: Load,
        mutate: Mutate,
    ) -> Result<R, E>
    where
        K: Clone,
        V: Default,
        Load: FnOnce() -> Fut,
        Fut: core::future::Future<Output = Result<Option<V>, E>>,
        Mutate: FnOnce(&mut V) -> R,
    {
        // Fast path: resident hit — mutate in place.
        {
            let tick = self.next_tick();
            let mut shard = self.inner.get_shard_containing(&key).lock().await;
            if let Some(entry) = shard.get_mut(&key) {
                if self.per_shard_capacity.is_some() {
                    entry.1 = tick;
                }
                return Ok(mutate(&mut entry.0));
            }
        }

        // Miss: load the full value from the backing store with NO lock held.
        let loaded = load().await?;

        // Re-lock and install. If a concurrent caller installed a value in
        // the meantime, mutate that one (it is the same durable state) and
        // drop our load.
        let tick = self.next_tick();
        let mut shard = self.inner.get_shard_containing(&key).lock().await;
        if let Some(entry) = shard.get_mut(&key) {
            if self.per_shard_capacity.is_some() {
                entry.1 = tick;
            }
            return Ok(mutate(&mut entry.0));
        }
        self.evict_if_full_locked(&mut shard, &key);
        let mut value = loaded.unwrap_or_default();
        let result = mutate(&mut value);
        shard.insert(key, (value, tick));
        Ok(result)
    }

    /// Remove a key, returning its value if present.
    pub async fn remove(&self, key: &K) -> Option<V> {
        self.inner
            .get_shard_containing(key)
            .lock()
            .await
            .remove(key)
            .map(|(value, _)| value)
    }

    /// Collect all keys from all shards.
    pub async fn into_keys(&self) -> Vec<K>
    where
        K: Clone,
    {
        self.inner.into_keys().await
    }

    /// Total number of resident entries across all shards.
    pub async fn len(&self) -> usize {
        self.inner.len().await
    }

    /// Whether the map has no resident entries.
    pub async fn is_empty(&self) -> bool {
        self.inner.is_empty().await
    }
}

impl<K: Hash + Ord, V, const N: usize> Default for BoundedShardedMap<K, V, N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use sedimentree_core::id::SedimentreeId;

    /// Single-shard map (N=1) makes the per-shard cap a global cap, so
    /// eviction order is deterministic and easy to assert.
    fn single_shard_capped(max: usize) -> BoundedShardedMap<SedimentreeId, u32, 1> {
        BoundedShardedMap::<SedimentreeId, u32, 1>::with_key(0, 0).with_capacity(max)
    }

    fn sid(n: u8) -> SedimentreeId {
        SedimentreeId::new([n; 32])
    }

    /// Insert via the get-or-insert path (the cache's real insert entry
    /// point), enforcing capacity.
    async fn put<const N: usize>(map: &BoundedShardedMap<SedimentreeId, u32, N>, n: u8, v: u32) {
        map.get_or_insert_with(sid(n), || v).await;
    }

    #[tokio::test]
    async fn unbounded_by_default_never_evicts() {
        let map: BoundedShardedMap<SedimentreeId, u32, 1> = BoundedShardedMap::with_key(0, 0);
        assert_eq!(map.per_shard_capacity(), None);
        for n in 0..100 {
            put(&map, n, u32::from(n)).await;
        }
        assert_eq!(map.len().await, 100, "no cap ⇒ nothing evicted");
    }

    #[tokio::test]
    async fn capacity_caps_resident_entries() {
        let map = single_shard_capped(3);
        assert_eq!(map.per_shard_capacity(), Some(3));
        for n in 0..10 {
            put(&map, n, u32::from(n)).await;
        }
        assert_eq!(map.len().await, 3, "shard must never exceed its cap");

        // Not just the count: the survivors must be the three most recently
        // inserted (7, 8, 9). A buggy eviction that kept the *oldest* entries
        // would also satisfy `len == 3`, so assert the actual identities.
        for survivor in [7u8, 8, 9] {
            assert_eq!(
                map.get_cloned(&sid(survivor)).await,
                Some(u32::from(survivor)),
                "the three most-recent inserts must survive"
            );
        }
        for evicted in [0u8, 1, 2, 3, 4, 5, 6] {
            assert!(
                map.get_cloned(&sid(evicted)).await.is_none(),
                "older insert {evicted} must have been evicted"
            );
        }
    }

    #[tokio::test]
    async fn evicts_least_recently_used() {
        let map = single_shard_capped(3);
        put(&map, 1, 1).await;
        put(&map, 2, 2).await;
        put(&map, 3, 3).await;

        // Touch 1 so it's most-recently-used; 2 becomes the LRU.
        assert_eq!(map.get_cloned(&sid(1)).await, Some(1));

        // Insert 4 → evicts the LRU, which is now 2 (not 1).
        put(&map, 4, 4).await;

        assert!(
            map.get_cloned(&sid(2)).await.is_none(),
            "2 was LRU, evicted"
        );
        assert_eq!(map.get_cloned(&sid(1)).await, Some(1), "1 touched, kept");
        assert_eq!(map.get_cloned(&sid(3)).await, Some(3));
        assert_eq!(map.get_cloned(&sid(4)).await, Some(4));
        assert_eq!(map.len().await, 3);
    }

    #[tokio::test]
    async fn updating_existing_key_does_not_evict() {
        let map = single_shard_capped(2);
        put(&map, 1, 1).await;
        put(&map, 2, 2).await;
        // Touch an existing key via with_entry: not a new key, no eviction.
        let r = map.with_entry(&sid(1), |v| *v += 10).await;
        assert_eq!(r, Some(()));
        assert_eq!(map.len().await, 2, "updating existing key must not evict");
        assert_eq!(map.get_cloned(&sid(1)).await, Some(11));
        assert_eq!(map.get_cloned(&sid(2)).await, Some(2));
    }

    #[tokio::test]
    async fn with_entry_or_default_respects_capacity() {
        let map = single_shard_capped(2);
        for n in 0..5 {
            map.with_entry_or_default(sid(n), |v| *v = u32::from(n))
                .await;
        }
        assert_eq!(map.len().await, 2);

        // The survivors must be the two most-recently inserted (3, 4) with
        // their written values, not merely *some* two entries.
        assert_eq!(map.get_cloned(&sid(3)).await, Some(3));
        assert_eq!(map.get_cloned(&sid(4)).await, Some(4));
        for evicted in [0u8, 1, 2] {
            assert!(
                map.get_cloned(&sid(evicted)).await.is_none(),
                "older entry {evicted} must have been evicted"
            );
        }
    }

    #[tokio::test]
    async fn zero_capacity_is_floored_to_one() {
        let map = single_shard_capped(0);
        assert_eq!(map.per_shard_capacity(), Some(1));
        put(&map, 1, 1).await;
        put(&map, 2, 2).await;
        assert_eq!(map.len().await, 1, "cap floored at 1 per shard");
        assert_eq!(map.get_cloned(&sid(2)).await, Some(2), "newest kept");
    }

    /// On the unbounded path no tick bookkeeping happens — `get_cloned` still
    /// works, and nothing is ever evicted regardless of access pattern.
    #[tokio::test]
    async fn unbounded_path_does_not_evict_or_require_touch() {
        let map: BoundedShardedMap<SedimentreeId, u32, 4> = BoundedShardedMap::with_key(0, 0);
        for n in 0..50 {
            put(&map, n, u32::from(n)).await;
        }
        let _ = map.get_cloned(&sid(0)).await;
        map.with_entry_or_default(sid(1), |v| *v += 1).await;
        let _ = map.with_entry(&sid(3), |v| *v).await;
        let _ = map.remove(&sid(5)).await;
        assert_eq!(map.len().await, 49, "unbounded ⇒ only the explicit remove");
    }

    /// Tests for the hydrate-on-miss methods (`with_hydrated_ref` and
    /// `with_entry_hydrated`). These are the load-bearing write/read paths
    /// for the LRU cache over durable storage: on a miss they must reload the
    /// *full* value from the backing store, never start from a stale or empty
    /// default, and must converge correctly when two callers race the same
    /// evicted key.
    mod hydration {
        use super::*;
        use core::convert::Infallible;

        /// A ready future yielding a present value, for the loader closures.
        fn load_some(value: u32) -> core::future::Ready<Result<Option<u32>, Infallible>> {
            core::future::ready(Ok(Some(value)))
        }

        /// `with_entry_hydrated` reloads the full value on a miss and applies
        /// the mutation to it — not to a fresh default. This is the
        /// write-after-eviction guarantee at the unit level.
        #[tokio::test]
        async fn with_entry_hydrated_loads_full_value_on_miss() {
            let map = single_shard_capped(2);
            // Key is absent; loader supplies the durable prior value `100`.
            let out = map
                .with_entry_hydrated(
                    sid(7),
                    || load_some(100),
                    |v: &mut u32| {
                        *v += 1;
                        *v
                    },
                )
                .await
                .expect("infallible load");
            assert_eq!(
                out, 101,
                "mutation must apply to the loaded value, not a default"
            );
            assert_eq!(
                map.get_cloned(&sid(7)).await,
                Some(101),
                "hydrated value must be installed and resident"
            );
        }

        /// On a resident hit, `with_entry_hydrated` mutates in place and never
        /// calls the loader (which would clobber unsaved in-RAM state).
        #[tokio::test]
        async fn with_entry_hydrated_uses_resident_value_without_loading() {
            let map = single_shard_capped(2);
            put(&map, 7, 42).await;
            let loader_called = core::cell::Cell::new(false);
            let out = map
                .with_entry_hydrated(
                    sid(7),
                    || {
                        loader_called.set(true);
                        load_some(999) // wrong value; must NOT be used
                    },
                    |v: &mut u32| {
                        *v += 1;
                        *v
                    },
                )
                .await
                .expect("infallible");
            assert_eq!(out, 43, "must mutate the resident 42, not the loaded 999");
            assert!(
                !loader_called.get(),
                "loader must not run on a resident hit"
            );
        }

        /// When `load` returns `Ok(None)` (key absent in durable storage),
        /// `with_entry_hydrated` mutates `V::default()` and installs it.
        #[tokio::test]
        async fn with_entry_hydrated_defaults_when_storage_has_nothing() {
            let map = single_shard_capped(2);
            let out = map
                .with_entry_hydrated(
                    sid(7),
                    || async { Ok::<_, Infallible>(None) },
                    |v: &mut u32| {
                        *v += 5;
                        *v
                    },
                )
                .await
                .expect("infallible");
            assert_eq!(
                out, 5,
                "absent-in-storage must start from default (0) then mutate"
            );
            assert_eq!(map.get_cloned(&sid(7)).await, Some(5));
        }

        /// A failing `load` propagates the error and leaves the map unchanged
        /// — the mutation is not applied and nothing is installed.
        #[tokio::test]
        async fn with_entry_hydrated_load_error_does_not_mutate_or_install() {
            #[derive(Debug, PartialEq)]
            struct LoadFailed;
            let map = single_shard_capped(2);
            let res = map
                .with_entry_hydrated(
                    sid(7),
                    || async { Err::<Option<u32>, _>(LoadFailed) },
                    |v: &mut u32| {
                        *v += 1;
                    },
                )
                .await;
            assert_eq!(res, Err(LoadFailed), "load error must propagate");
            assert!(
                map.get_cloned(&sid(7)).await.is_none(),
                "nothing must be installed when the load fails"
            );
        }

        /// `with_hydrated_ref` returns `Ok(None)` and installs nothing when
        /// the key is absent in durable storage.
        #[tokio::test]
        async fn with_hydrated_ref_none_when_absent_installs_nothing() {
            let map = single_shard_capped(2);
            let res: Option<u32> = map
                .with_hydrated_ref(sid(7), || async { Ok::<_, Infallible>(None) }, |v: &u32| *v)
                .await
                .expect("infallible");
            assert_eq!(res, None, "absent key must inspect to None");
            assert!(
                map.get_cloned(&sid(7)).await.is_none(),
                "nothing installed for an absent key"
            );
        }

        /// `with_hydrated_ref` reads (does not mutate) the hydrated value and
        /// leaves it resident for a subsequent cheap hit.
        #[tokio::test]
        async fn with_hydrated_ref_reads_and_installs() {
            let map = single_shard_capped(2);
            let seen = map
                .with_hydrated_ref(sid(7), || load_some(77), |v: &u32| *v)
                .await
                .expect("infallible");
            assert_eq!(seen, Some(77), "must inspect the loaded value");
            assert_eq!(
                map.get_cloned(&sid(7)).await,
                Some(77),
                "hydrated value left resident"
            );
        }

        /// Concurrent hydration of the same evicted key must not lose a write.
        /// Two tasks each append a distinct element via `with_entry_hydrated`;
        /// whichever installs first, the second must adopt the resident value
        /// and append to *it* (not to its own dropped load). Both elements
        /// must survive regardless of interleaving.
        ///
        /// This is the lost-update guard for the adoption branch
        /// (`with_entry_hydrated` lines that mutate an entry installed by a
        /// concurrent caller). The integration suite only drives one task per
        /// key, so this path was otherwise untested.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn concurrent_hydration_does_not_lose_writes() {
            use alloc::vec::Vec;
            use std::sync::Arc;

            // Run many rounds to shake out interleavings.
            for round in 0..200u32 {
                let map: Arc<BoundedShardedMap<SedimentreeId, Vec<u32>, 1>> =
                    Arc::new(BoundedShardedMap::with_key(0, 0).with_capacity(8));
                let key = sid(1);

                // Durable prior state the loader reconstructs on a miss: empty.
                let m1 = Arc::clone(&map);
                let m2 = Arc::clone(&map);
                let k1 = key;
                let k2 = key;

                let t1 = tokio::spawn(async move {
                    m1.with_entry_hydrated(
                        k1,
                        || async { Ok::<_, Infallible>(Some(Vec::new())) },
                        |v: &mut Vec<u32>| v.push(1),
                    )
                    .await
                    .expect("infallible");
                });
                let t2 = tokio::spawn(async move {
                    m2.with_entry_hydrated(
                        k2,
                        || async { Ok::<_, Infallible>(Some(Vec::new())) },
                        |v: &mut Vec<u32>| v.push(2),
                    )
                    .await
                    .expect("infallible");
                });
                t1.await.expect("task 1");
                t2.await.expect("task 2");

                let mut final_vec = map.get_cloned(&key).await.expect("key present");
                final_vec.sort_unstable();
                assert_eq!(
                    final_vec,
                    [1, 2],
                    "round {round}: both concurrent appends must survive \
                     (no lost update via the adoption branch)"
                );
            }
        }
    }
}
