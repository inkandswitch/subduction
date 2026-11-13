//! The stock in-process store: a sharded, optionally bounded LRU map.

use alloc::{sync::Arc, vec::Vec};
use core::convert::Infallible;

use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    id::SedimentreeId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};

use super::{Install, Lookup, MinimizedTreeStore};
use crate::collections::bounded_sharded_map::{BoundedShardedMap, Checkout};

/// Keeps minimized trees in this process's memory.
///
/// Trees are spread over `SHARDS` independently locked shards, so reads of
/// different trees rarely contend. The store is unbounded by default; bound it
/// with [`with_capacity`](Self::with_capacity) to evict least-recently-used
/// trees.
///
/// Only this node sees the store, so its generation is `()`: the node's own
/// coordination orders installs against writes. Clones share the same map.
#[derive(Debug)]
pub struct MemoryMinimizedTreeStore<const SHARDS: usize = 256> {
    trees: Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>>,
}

impl<const SHARDS: usize> MemoryMinimizedTreeStore<SHARDS> {
    /// An empty, unbounded store.
    ///
    /// # Panics
    ///
    /// Panics if the system random number generator fails.
    #[must_use]
    pub fn new() -> Self {
        Self::from_map(BoundedShardedMap::new())
    }

    /// An empty store holding about `max_trees` trees, evicting the
    /// least-recently-used tree beyond that.
    ///
    /// The bound is enforced per shard, so treat it as approximate; see
    /// [`BoundedShardedMap::with_capacity`].
    ///
    /// # Panics
    ///
    /// Panics if the system random number generator fails.
    #[must_use]
    pub fn with_capacity(max_trees: usize) -> Self {
        Self::from_map(BoundedShardedMap::new().with_capacity(max_trees))
    }

    /// Wrap an existing map, for example one built with a fixed shard key.
    #[must_use]
    pub fn from_map(map: BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>) -> Self {
        Self {
            trees: Arc::new(map),
        }
    }

    /// A copy of the resident tree for `id`, if any.
    pub async fn get_cloned(&self, id: SedimentreeId) -> Option<MinimizedSedimentree> {
        self.trees.get_cloned(&id).await
    }

    /// Insert `tree` for `id` unless one is already resident, returning
    /// whether it was inserted.
    ///
    /// Use this to seed a store before handing it to a node. The tree must
    /// match storage; once the node owns the store, let it manage entries.
    pub async fn insert_if_absent(&self, id: SedimentreeId, tree: MinimizedSedimentree) -> bool {
        self.trees.insert_if_absent(id, tree).await
    }

    /// Drop the resident tree for `id` directly, returning it if present.
    ///
    /// This bypasses the node's coordination. It is safe only because a miss
    /// rebuilds the tree from storage; prefer letting the node manage entries.
    pub async fn evict(&self, id: SedimentreeId) -> Option<MinimizedSedimentree> {
        self.trees.remove(&id).await
    }

    /// The ids of the resident trees.
    pub async fn resident_ids(&self) -> Vec<SedimentreeId> {
        self.trees.into_keys().await
    }

    /// The number of resident trees.
    pub async fn len(&self) -> usize {
        self.trees.len().await
    }

    /// Whether no tree is resident.
    pub async fn is_empty(&self) -> bool {
        self.trees.is_empty().await
    }
}

impl<const SHARDS: usize> Clone for MemoryMinimizedTreeStore<SHARDS> {
    fn clone(&self) -> Self {
        Self {
            trees: self.trees.clone(),
        }
    }
}

impl<const SHARDS: usize> Default for MemoryMinimizedTreeStore<SHARDS> {
    fn default() -> Self {
        Self::new()
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm, const SHARDS: usize> MinimizedTreeStore<Async>
    for MemoryMinimizedTreeStore<SHARDS>
{
    type Error = Infallible;
    type Generation = ();
    type Entry = Checkout<SedimentreeId, MinimizedSedimentree>;

    fn get(
        &self,
        id: SedimentreeId,
    ) -> Async::Future<'_, Result<Lookup<Self::Entry, ()>, Infallible>> {
        Async::from_future(async move {
            Ok(match self.trees.checkout(&id).await {
                Some(entry) => Lookup::Hit(entry),
                None => Lookup::Miss(()),
            })
        })
    }

    fn install(
        &self,
        id: SedimentreeId,
        tree: MinimizedSedimentree,
        (): (),
    ) -> Async::Future<'_, Result<Install, Infallible>> {
        Async::from_future(async move {
            // A resident tree came from an equally current read; keep it.
            self.trees.insert_if_absent(id, tree).await;
            Ok(Install::Current)
        })
    }

    fn merge(
        &self,
        id: SedimentreeId,
        update: Sedimentree,
    ) -> Async::Future<'_, Result<(), Infallible>> {
        Async::from_future(async move {
            self.trees.with_entry(&id, |tree| tree.merge(update)).await;
            Ok(())
        })
    }

    fn remove(&self, id: SedimentreeId) -> Async::Future<'_, Result<(), Infallible>> {
        Async::from_future(async move {
            self.trees.remove(&id).await;
            Ok(())
        })
    }
}
