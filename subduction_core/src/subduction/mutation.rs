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

use super::ingest;

#[cfg(test)]
mod tests;
use crate::{
    collections::bounded_sharded_map::BoundedShardedMap,
    storage::{fetcher::Fetcher, traits::Storage},
};

/// Apply one authorized storage operation and maintain its cached contents.
/// `Some(update)` adds persisted items; `None` invalidates after deletion.
/// This does not claim rollback or cancellation-independent execution.
#[allow(
    clippy::expect_used,
    reason = "completion is consumed only after an entry is adopted or validated"
)]
pub(crate) async fn complete<Async, Store, Work, R, const SHARDS: usize>(
    cache: &BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>,
    fetcher: &Fetcher<Async, Store>,
    work: Work,
    update: Option<Sedimentree>,
) -> Result<R, Store::Error>
where
    Async: FutureForm,
    Store: Storage<Async>,
    Work: Future<Output = Result<R, Store::Error>>,
{
    let mut guard = fetcher.state.begin();
    let id = fetcher.sedimentree_id();
    let result = match work.await {
        Ok(result) => result,
        Err(error) => {
            // Failed writes and deletes may have changed storage. Release the
            // mutation revision with removal, before a stale scan can install.
            cache.remove_with(&id, || guard.finish()).await;
            return Err(error);
        }
    };
    let Some(update) = update else {
        cache.remove_with(&id, || guard.finish()).await;
        return Ok(result);
    };
    let mut update = Some(update);
    loop {
        // Resident completion performs no storage scan. A concurrently installed
        // value also wins over a loaded copy below, preserving other updates.
        if cache
            .with_entry(&id, |tree| {
                tree.merge(update.take().expect("unconsumed update"));
                guard.finish();
            })
            .await
            .is_some()
        {
            return Ok(result);
        }
        let revision = fetcher.state.revision();
        let loaded = match ingest::load_tree(fetcher).await {
            Ok(tree) => tree.unwrap_or_default(),
            Err(error) => {
                cache.remove_with(&id, || guard.finish()).await;
                return Err(error);
            }
        };
        if cache
            .install_checked(
                id,
                Some(loaded),
                || fetcher.state.unchanged(revision),
                |tree| {
                    tree.merge(update.take().expect("unconsumed update"));
                    guard.finish();
                },
            )
            .await
            .is_some()
        {
            return Ok(result);
        }
        crate::spawn::yield_once().await;
    }
}
