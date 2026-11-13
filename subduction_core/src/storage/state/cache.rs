//! The boundary between storage coordination and the minimized tree store.
//!
//! Callers cannot check a revision and populate later, or settle a mutation before
//! its store maintenance: the raw operations are private to the parent module.
//! Both paths here hold the tree's store lock across the store call and the
//! brief tree-state update. Storage I/O runs before acquiring it, and mutation
//! notifications run after releasing it. Store hits never take the lock.

use super::{ReadAttempt, StorageState, TrackedMutation, TreeCoordination};
use crate::{
    minimized_tree_store::{Lookup, MinimizedTreeStore, Populate},
    stored::StoredItem,
};
use alloc::vec::Vec;
use core::{future::Future, marker::PhantomData};
use future_form::FutureForm;
use sedimentree_core::{
    id::SedimentreeId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};

/// A tree's store entry, as seen by the node.
pub(crate) enum Resident<Entry, Generation> {
    /// The tree is resident.
    Hit(Entry),

    /// The tree is not resident. Populate a rebuilt copy against `Some`
    /// generation; `None` means the store is unusable for this tree right now
    /// (it failed, or the tree is bypassed), so read storage without populating.
    Miss(Option<Generation>),
}

impl StorageState {
    fn is_bypassed(&self, id: SedimentreeId) -> bool {
        self.bypassed.lock().contains(&id)
    }

    fn set_bypassed(&self, id: SedimentreeId, bypassed: bool) {
        let mut set = self.bypassed.lock();
        if bypassed {
            set.insert(id);
        } else {
            set.remove(&id);
        }
    }
}

impl TreeCoordination {
    /// Look this tree up in the store.
    ///
    /// A store error is a miss without a generation. A bypassed tree first
    /// retries the `remove` that failed; until that succeeds, it misses too.
    pub(crate) async fn lookup<Async, Trees>(
        &self,
        store: &Trees,
    ) -> Resident<Trees::Entry, Trees::Generation>
    where
        Async: FutureForm,
        Trees: MinimizedTreeStore<Async>,
    {
        let id = self.state.id;
        let owner = &self.state.owner;
        if owner.is_bypassed(id) {
            // Never queue behind another task's store maintenance: while the
            // store is failing, a busy lock means read storage directly.
            let Some(_store) = self.state.store.try_lock() else {
                return Resident::Miss(None);
            };
            if let Err(error) = store.remove(id).await {
                tracing::warn!(tree = ?id, %error, "minimized tree store still failing; reading storage directly");
                return Resident::Miss(None);
            }
            owner.set_bypassed(id, false);
        }
        match store.get(id).await {
            Ok(Lookup::Hit(entry)) => Resident::Hit(entry),
            Ok(Lookup::Miss(generation)) => Resident::Miss(Some(generation)),
            Err(error) => {
                tracing::warn!(tree = ?id, %error, "minimized tree store lookup failed; reading storage");
                Resident::Miss(None)
            }
        }
    }

    /// Wait for pending mutations before reading storage for the store.
    /// Does not hold a lock during the caller's read or prevent later mutations.
    pub(crate) async fn prepare_cache_read<'a, Async, Trees>(
        &self,
        store: &'a Trees,
        seen: Option<Trees::Generation>,
    ) -> CacheRead<'a, Async, Trees>
    where
        Async: FutureForm,
        Trees: MinimizedTreeStore<Async>,
    {
        CacheRead {
            store,
            attempt: self.prepare_read().await,
            seen,
            _form: PhantomData,
        }
    }
}

/// A prepared read bound to its tree and destination store. Consuming it
/// through `populate` keeps revision validation and populating in one
/// critical section.
#[must_use = "populate the store through this handle, or discard the read"]
pub(crate) struct CacheRead<'a, Async: FutureForm, Trees: MinimizedTreeStore<Async>> {
    store: &'a Trees,
    attempt: ReadAttempt,
    seen: Option<Trees::Generation>,
    _form: PhantomData<fn() -> Async>,
}

impl<Async: FutureForm, Trees: MinimizedTreeStore<Async>> CacheRead<'_, Async, Trees> {
    /// Populate a still-current storage read, after running `inspect` on it.
    ///
    /// The outer `None` means retry: a local mutation overlapped the read, or
    /// the store reported a newer generation. This never adopts a tree another
    /// operation made resident meanwhile; the caller's next lookup hits it.
    /// `Some(None)` means a valid absent tree. `inspect` runs on the loaded
    /// tree before it moves into the store, so it may run again on a retry.
    pub(crate) async fn populate<Inspect, R>(
        self,
        loaded: Option<MinimizedSedimentree>,
        inspect: &mut Inspect,
    ) -> Option<Option<R>>
    where
        Inspect: FnMut(&mut MinimizedSedimentree) -> R,
    {
        // Nothing will be stored, so there is nothing for an overlapping
        // write to make stale. The read still began after pending writes
        // settled; skip the store lock and revision check.
        let Some(seen) = self.seen else {
            return Some(loaded.map(|mut tree| inspect(&mut tree)));
        };
        let state = &self.attempt.coordination.state;
        let id = state.id;
        let _store = state.store.lock().await;
        if !self.attempt.is_current() {
            return None;
        }
        let Some(mut tree) = loaded else {
            return Some(None);
        };
        let result = inspect(&mut tree);
        match self.store.populate(id, tree, seen).await {
            Ok(Populate::Populated) => Some(Some(result)),
            Ok(Populate::Stale) => None,
            Err(error) => {
                tracing::warn!(tree = ?id, %error, "minimized tree store populate failed");
                Some(Some(result))
            }
        }
    }
}

impl TrackedMutation {
    /// Run admitted storage work, maintain the store, then notify.
    /// Successful writes merge into the store; deletion or failure removes the
    /// tree. Completion never reads storage or populates a cold store.
    /// Dropping this future unfinished closes coordination, just like losing
    /// the admitted mutation before its first poll.
    pub(crate) async fn persist<Async, Trees, Work, R, E>(
        mut self,
        store: &Trees,
        work: Work,
        update: Option<Sedimentree>,
        items: Option<Vec<StoredItem>>,
    ) -> Result<R, E>
    where
        Async: FutureForm,
        Trees: MinimizedTreeStore<Async>,
        Work: Future<Output = Result<R, E>>,
    {
        let result = work.await;
        let state = self.coordination.state.clone();
        let id = state.id;
        // Errors can follow partial storage effects; never merge their update.
        let update = if result.is_ok() { update } else { None };
        let succeeded = update.is_some();
        {
            let _store = state.store.lock().await;
            let bypassed = state.owner.is_bypassed(id);
            // A bypassed tree may hold a stale entry: replace a merge with a
            // remove, so a success clears it.
            let outcome = match update {
                Some(update) if !bypassed => store.merge(id, update).await,
                _ => store
                    .remove(id)
                    .await
                    .inspect(|()| state.owner.set_bypassed(id, false)),
            };
            if let Err(error) = outcome {
                tracing::warn!(tree = ?id, %error, "minimized tree store maintenance failed; bypassing it for this tree");
                state.owner.set_bypassed(id, true);
            }
            // Settle even on a store miss or failure, and publish independently
            // of residency. The store lock stays held through settlement.
            self.complete(if succeeded { items.as_deref() } else { None });
        }
        // The store lock has been released. Drop wakes readers and watchers;
        // even a waker that immediately reads the store cannot deadlock.
        drop(self);
        result
    }
}

#[cfg(test)]
mod tests;
