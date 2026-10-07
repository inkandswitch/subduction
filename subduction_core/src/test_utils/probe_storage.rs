//! A [`MemoryStorage`] wrapper that can fail reads for chosen trees and
//! counts blob-carrying loads.
//!
//! Lets tests reach the storage-error paths and check that a read that only
//! needs metadata does not pull blob bytes.

use alloc::{collections::BTreeSet, sync::Arc, vec::Vec};
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    collections::Set,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_crypto::verified_meta::VerifiedMeta;
use thiserror::Error;

use crate::storage::{
    memory::{MemoryStorage, MemoryStorageError},
    traits::Storage,
};

/// Clones share the inner storage, the failing set, and the counter.
#[derive(Debug, Clone, Default)]
pub struct ProbeStorage {
    inner: MemoryStorage,
    failing: Arc<Mutex<BTreeSet<SedimentreeId>>>,
    blob_loads: Arc<AtomicUsize>,
}

impl ProbeStorage {
    /// Wrap `inner`. Writes made through either handle are visible to both.
    #[must_use]
    pub fn new(inner: MemoryStorage) -> Self {
        Self {
            inner,
            failing: Arc::default(),
            blob_loads: Arc::default(),
        }
    }

    /// Make every per-tree read of `id` fail with [`ProbeError::Injected`].
    pub fn fail(&self, id: SedimentreeId) {
        self.failing().insert(id);
    }

    /// Undo [`fail`](Self::fail).
    pub fn recover(&self, id: SedimentreeId) {
        self.failing().remove(&id);
    }

    /// Number of loads so far that returned blob bytes.
    #[must_use]
    pub fn blob_loads(&self) -> usize {
        self.blob_loads.load(Ordering::Relaxed)
    }

    /// The set is only ever inserted into or removed from, so a panic while
    /// holding the lock cannot leave it inconsistent.
    fn failing(&self) -> MutexGuard<'_, BTreeSet<SedimentreeId>> {
        self.failing.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn check(&self, id: SedimentreeId) -> Result<(), ProbeError> {
        if self.failing().contains(&id) {
            Err(ProbeError::Injected(id))
        } else {
            Ok(())
        }
    }

    fn count_blob_load(&self) {
        self.blob_loads.fetch_add(1, Ordering::Relaxed);
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> Storage<Async> for ProbeStorage {
    type Error = ProbeError;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            Ok(Storage::<Async>::save_sedimentree_id(&self.inner, sedimentree_id).await?)
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            Ok(Storage::<Async>::delete_sedimentree_id(&self.inner, sedimentree_id).await?)
        })
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> Async::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Async::from_future(async move {
            Ok(Storage::<Async>::load_all_sedimentree_ids(&self.inner).await?)
        })
    }

    fn contains_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<bool, Self::Error>> {
        Async::from_future(async move {
            self.check(sedimentree_id)?;
            Ok(Storage::<Async>::contains_sedimentree_id(&self.inner, sedimentree_id).await?)
        })
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            Ok(Storage::<Async>::save_loose_commit(&self.inner, sedimentree_id, verified).await?)
        })
    }

    fn list_commit_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Async::from_future(async move {
            self.check(sedimentree_id)?;
            Ok(Storage::<Async>::list_commit_ids(&self.inner, sedimentree_id).await?)
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Async::from_future(async move {
            self.check(sedimentree_id)?;
            self.count_blob_load();
            Ok(Storage::<Async>::load_loose_commits(&self.inner, sedimentree_id).await?)
        })
    }

    fn load_loose_commit_metas(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<LooseCommit>, Self::Error>> {
        Async::from_future(async move {
            self.check(sedimentree_id)?;
            Ok(Storage::<Async>::load_loose_commit_metas(&self.inner, sedimentree_id).await?)
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Async::from_future(async move {
            self.check(sedimentree_id)?;
            self.count_blob_load();
            Ok(Storage::<Async>::load_loose_commit(&self.inner, sedimentree_id, commit_id).await?)
        })
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            Ok(
                Storage::<Async>::delete_loose_commit(&self.inner, sedimentree_id, commit_id)
                    .await?,
            )
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            Ok(Storage::<Async>::delete_loose_commits(&self.inner, sedimentree_id).await?)
        })
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            Ok(Storage::<Async>::save_fragment(&self.inner, sedimentree_id, verified).await?)
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Async::from_future(async move {
            self.check(sedimentree_id)?;
            self.count_blob_load();
            Ok(Storage::<Async>::load_fragment(&self.inner, sedimentree_id, fragment_head).await?)
        })
    }

    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Async::from_future(async move {
            self.check(sedimentree_id)?;
            Ok(Storage::<Async>::list_fragment_ids(&self.inner, sedimentree_id).await?)
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Async::from_future(async move {
            self.check(sedimentree_id)?;
            self.count_blob_load();
            Ok(Storage::<Async>::load_fragments(&self.inner, sedimentree_id).await?)
        })
    }

    fn load_fragment_metas(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<Fragment>, Self::Error>> {
        Async::from_future(async move {
            self.check(sedimentree_id)?;
            Ok(Storage::<Async>::load_fragment_metas(&self.inner, sedimentree_id).await?)
        })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            Ok(
                Storage::<Async>::delete_fragment(&self.inner, sedimentree_id, fragment_head)
                    .await?,
            )
        })
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            Ok(Storage::<Async>::delete_fragments(&self.inner, sedimentree_id).await?)
        })
    }

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> Async::Future<'_, Result<usize, Self::Error>> {
        Async::from_future(async move {
            Ok(
                Storage::<Async>::save_batch(&self.inner, sedimentree_id, commits, fragments)
                    .await?,
            )
        })
    }
}

/// A [`ProbeStorage`] read failure.
#[derive(Debug, Clone, Copy, Error)]
pub enum ProbeError {
    /// The tree was marked with [`ProbeStorage::fail`].
    #[error("injected read failure for {0:?}")]
    Injected(SedimentreeId),

    /// The inner [`MemoryStorage`] failed.
    #[error(transparent)]
    Inner(#[from] MemoryStorageError),
}
