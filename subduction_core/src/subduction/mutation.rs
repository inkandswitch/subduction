//! Finish storage mutations before maintaining the resident tree cache.
//!
//! Local publication and received data share this completion path. Verification
//! and capability authorization happen before it; network propagation happens
//! afterward. Storage remains the source of truth when a tree is not resident.
//! Mutation revisions prevent an overlapping scan from reinstalling a stale
//! cache entry after partial persistence or deletion invalidates it.

use core::future::Future;
use future_form::FutureForm;
use sedimentree_core::{
    id::SedimentreeId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};

#[cfg(test)]
mod tests;
use crate::{
    collections::bounded_sharded_map::BoundedShardedMap,
    storage::{fetcher::Fetcher, traits::Storage},
};

/// Apply one authorized storage operation and maintain its cached contents.
/// `Some(update)` merges persisted items into an existing cache entry;
/// `None` invalidates after deletion. Missing entries stay uncached: a later
/// read will load them from storage. Completion never reads storage or retries.
/// The node runner owns this whole future; API callers only wait for its result.
/// Losing the driver closes observation rather than claiming rollback.
pub(crate) async fn complete<Async, Store, Work, R, const SHARDS: usize>(
    cache: &BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>,
    fetcher: &Fetcher<Async, Store>,
    work: Work,
    update: Option<Sedimentree>,
    mut mutation: crate::storage::state::TrackedMutation,
) -> Result<R, Store::Error>
where
    Async: FutureForm,
    Store: Storage<Async>,
    Work: Future<Output = Result<R, Store::Error>>,
{
    let id = fetcher.sedimentree_id();
    let result = match work.await {
        Ok(result) => result,
        Err(error) => {
            // An error may follow a partial write or delete. Clear the cache
            // and mark the mutation complete under the same lock.
            cache.remove_with(&id, || mutation.complete()).await;
            return Err(error);
        }
    };
    let Some(update) = update else {
        cache.remove_with(&id, || mutation.complete()).await;
        return Ok(result);
    };
    cache
        .with_optional_entry(&id, |tree| {
            if let Some(tree) = tree {
                tree.merge(update);
            }
            // Finish under the cache lock even when the entry is absent, so
            // waiting readers can safely start loading the stored tree.
            mutation.complete();
        })
        .await;
    Ok(result)
}
