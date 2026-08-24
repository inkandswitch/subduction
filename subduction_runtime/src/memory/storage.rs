//! Map-backed in-memory item storage.

use core::cell::RefCell;

use future_form::{FutureForm as _, Local};
use futures::future::LocalBoxFuture;
use sedimentree_core::{
    collections::Map,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};
use subduction_crypto::signed::Signed;
use subduction_protocol::storage::StorageFailure;

use crate::storage::{FetchedItems, Storage};

/// Map-backed item storage.
///
/// Interior mutability is a lock-free [`RefCell`]: the driver task is the
/// only accessor. That makes this store [`Local`]-only; a `Sendable`
/// backend needs real shared storage underneath.
#[derive(Debug, Default)]
pub struct MemoryStorage {
    inner: RefCell<Trees>,
}

impl MemoryStorage {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The stored commit ids for `tree`, sorted (test introspection).
    #[must_use]
    pub fn commit_ids(&self, tree: SedimentreeId) -> Vec<CommitId> {
        let mut ids: Vec<CommitId> = self
            .inner
            .borrow()
            .commits
            .get(&tree)
            .map(|items| items.keys().copied().collect())
            .unwrap_or_default();
        ids.sort_unstable();
        ids
    }
}

impl Storage<Local> for MemoryStorage {
    fn persist_items(
        &self,
        tree: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Vec<u8>)>,
        fragments: Vec<(Signed<Fragment>, Vec<u8>)>,
    ) -> LocalBoxFuture<'_, Result<u32, StorageFailure>> {
        Local::from_future(async move {
            let mut trees = self.inner.borrow_mut();
            let mut stored = 0u32;
            for (signed, blob) in commits {
                let Ok(verified) = signed.try_decode_trusted_payload() else {
                    return Err(StorageFailure::Permanent);
                };
                let _previous = trees
                    .commits
                    .entry(tree)
                    .or_default()
                    .insert(verified.head(), (signed, blob));
                stored += 1;
            }
            for (signed, blob) in fragments {
                let Ok(verified) = signed.try_decode_trusted_payload() else {
                    return Err(StorageFailure::Permanent);
                };
                let _previous = trees
                    .fragments
                    .entry(tree)
                    .or_default()
                    .insert(verified.head(), (signed, blob));
                stored += 1;
            }
            Ok(stored)
        })
    }

    fn fetch_items(
        &self,
        tree: SedimentreeId,
        commit_ids: Vec<CommitId>,
        fragment_heads: Vec<CommitId>,
    ) -> LocalBoxFuture<'_, Result<Option<FetchedItems>, StorageFailure>> {
        Local::from_future(async move {
            let trees = self.inner.borrow();
            let commits = trees.commits.get(&tree);
            let fragments = trees.fragments.get(&tree);
            if commits.is_none() && fragments.is_none() {
                return Ok(None);
            }
            let mut items = FetchedItems::default();
            for id in commit_ids {
                if let Some(found) = commits.and_then(|c| c.get(&id)) {
                    items.commits.push(found.clone());
                }
            }
            for head in fragment_heads {
                if let Some(found) = fragments.and_then(|f| f.get(&head)) {
                    items.fragments.push(found.clone());
                }
            }
            Ok(Some(items))
        })
    }

    fn delete_tree(&self, tree: SedimentreeId) -> LocalBoxFuture<'_, Result<(), StorageFailure>> {
        Local::from_future(async move {
            let mut trees = self.inner.borrow_mut();
            let _commits = trees.commits.remove(&tree);
            let _fragments = trees.fragments.remove(&tree);
            Ok(())
        })
    }
}

/// A stored item with its blob bytes.
type Stored<T> = Map<CommitId, (Signed<T>, Vec<u8>)>;

#[derive(Debug, Default)]
struct Trees {
    commits: Map<SedimentreeId, Stored<LooseCommit>>,
    fragments: Map<SedimentreeId, Stored<Fragment>>,
}
