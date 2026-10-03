//! Finish storage mutations before maintaining the resident tree cache.
//!
//! Local publication and received data share this completion path. Verification
//! and capability authorization happen before it; network propagation happens
//! afterward. Storage remains the source of truth when a tree is not resident.

use core::future::Future;
use future_form::FutureForm;
use sedimentree_core::{
    id::SedimentreeId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};

use super::ingest;
use crate::{
    collections::bounded_sharded_map::BoundedShardedMap,
    storage::{fetcher::Fetcher, traits::Storage},
};

/// Apply one authorized storage operation and maintain its cached contents.
/// `Some(update)` adds persisted items; `None` invalidates after deletion.
/// This does not claim rollback or cancellation-independent execution.
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
    let result = work.await?;
    let id = fetcher.sedimentree_id();
    if let Some(update) = update {
        cache
            .with_entry_hydrated(id, || ingest::load_tree(fetcher), |tree| tree.merge(update))
            .await?;
    } else {
        cache.remove(&id).await;
    }
    Ok(result)
}
