//! The boundary between storage coordination and the derived-tree cache.
//!
//! Callers cannot check a revision and install later, or settle a mutation before
//! its cache maintenance: the raw operations are private to the parent module.
//! Both paths here acquire the cache shard before briefly locking tree state.
//! Storage I/O runs before acquiring the cache, and mutation notifications run
//! after releasing it. Cache ownership remains with the node.

use super::{ReadAttempt, TrackedMutation, TreeCoordination};
use crate::{collections::bounded_sharded_map::BoundedShardedMap, stored::StoredItem};
use alloc::vec::Vec;
use core::future::Future;
use sedimentree_core::{
    id::SedimentreeId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};

type Cache<const SHARDS: usize> = BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>;

/// A prepared read bound to its tree and destination cache. Consuming it through
/// `install` keeps revision validation and installation in one critical section.
#[must_use = "install the loaded value through this handle, or discard the read"]
pub(crate) struct CacheRead<'a, const SHARDS: usize> {
    cache: &'a Cache<SHARDS>,
    attempt: ReadAttempt,
}

impl TreeCoordination {
    /// Wait for pending mutations before reading storage for this cache.
    /// Does not hold a lock during the caller's read or prevent later mutations.
    pub(crate) async fn prepare_cache_read<'a, const SHARDS: usize>(
        &self,
        cache: &'a Cache<SHARDS>,
    ) -> CacheRead<'a, SHARDS> {
        CacheRead {
            cache,
            attempt: self.prepare_read().await,
        }
    }
}

impl<const SHARDS: usize> CacheRead<'_, SHARDS> {
    /// Adopt a resident entry or install a still-current storage read.
    /// The outer `None` means retry; `Some(None)` means a valid absent tree.
    /// `inspect` runs only on the accepted value, under the cache shard lock.
    pub(crate) async fn install<Inspect, R>(
        self,
        loaded: Option<MinimizedSedimentree>,
        inspect: Inspect,
    ) -> Option<Option<R>>
    where
        Inspect: FnOnce(&mut MinimizedSedimentree) -> R,
    {
        self.cache
            .install_checked(
                self.attempt.coordination.state.id,
                loaded,
                || self.attempt.is_current(),
                inspect,
            )
            .await
    }
}

impl TrackedMutation {
    /// Run admitted storage work, maintain the resident cache, then notify.
    /// Successful writes merge only into resident entries; deletion or failure
    /// invalidates. Completion never reads storage or populates a cold cache.
    /// Dropping this future unfinished closes coordination, just like losing
    /// the admitted mutation before its first poll.
    pub(crate) async fn persist<Work, R, E, const SHARDS: usize>(
        mut self,
        cache: &Cache<SHARDS>,
        work: Work,
        update: Option<Sedimentree>,
        items: Option<Vec<StoredItem>>,
    ) -> Result<R, E>
    where
        Work: Future<Output = Result<R, E>>,
    {
        let result = work.await;
        let id = self.coordination.state.id;
        // Errors can follow partial storage effects; never merge their update.
        let update = if result.is_ok() { update } else { None };
        if let Some(update) = update {
            cache
                .with_optional_entry(&id, |tree| {
                    if let Some(tree) = tree {
                        tree.merge(update);
                    }
                    // Settle even on a cache miss, and publish independently of
                    // residency. The shard stays locked through settlement.
                    self.complete(items.as_deref());
                })
                .await;
        } else {
            cache.remove_with(&id, || self.complete(None)).await;
        }
        // The shard guard has been released. Drop wakes readers and watchers;
        // even a waker that immediately accesses the cache cannot deadlock.
        drop(self);
        result
    }
}

#[cfg(test)]
mod tests;
