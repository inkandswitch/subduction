//! The mutation vocabulary shared by managed put and delete capabilities.
//!
//! Backends only persist data. A node installs a typed submission function in
//! its powerbox; capabilities use it to submit persistence and cache completion
//! together. It captures the existing spawner, not another executor or registry.
//! Standalone powerboxes execute inline and provide no cancellation ownership.

use super::{fetcher::Fetcher, traits::Storage};
use alloc::{sync::Arc, vec::Vec};
use future_form::FutureForm;
use sedimentree_core::{
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};
use subduction_crypto::verified_meta::VerifiedMeta;

/// A backend failure or loss of the node-owned mutation driver.
#[derive(Debug, thiserror::Error)]
pub enum MutationError<E> {
    /// Persistence or cache reconstruction failed; effects may be partial.
    #[error(transparent)]
    Storage(E),
    /// Admission closed or the executor dropped the mutation driver.
    #[error("storage mutation interrupted")]
    Closed,
}
impl<E> From<E> for MutationError<E> {
    fn from(error: E) -> Self {
        Self::Storage(error)
    }
}

pub(crate) enum Operation {
    Commit(VerifiedMeta<LooseCommit>),
    Fragment(VerifiedMeta<Fragment>),
    Batch(Vec<VerifiedMeta<LooseCommit>>, Vec<VerifiedMeta<Fragment>>),
    Register,
    DeleteCommit(CommitId),
    DeleteCommits,
    DeleteFragment(CommitId),
    DeleteFragments,
    DeleteId,
    DeleteTree,
}
impl Operation {
    pub(crate) fn items(&self) -> Option<Vec<crate::stored::StoredItem>> {
        use crate::{
            storage::state::{MAX_PENDING_BYTES, MAX_PENDING_ITEMS},
            stored::{StoredItem, item_size},
        };
        let (count, bytes) = match self {
            Self::Commit(v) => (1, item_size(v)),
            Self::Fragment(v) => (1, item_size(v)),
            Self::Batch(commits, fragments) => (
                commits.len().saturating_add(fragments.len()),
                commits
                    .iter()
                    .map(item_size)
                    .chain(fragments.iter().map(item_size))
                    .fold(0usize, usize::saturating_add),
            ),
            Self::Register => return Some(Vec::new()),
            Self::DeleteCommit(_)
            | Self::DeleteCommits
            | Self::DeleteFragment(_)
            | Self::DeleteFragments
            | Self::DeleteId
            | Self::DeleteTree => return None,
        };
        // Do not clone a batch just to discover it exceeds the queue budget.
        if count > MAX_PENDING_ITEMS || bytes > MAX_PENDING_BYTES {
            return None;
        }
        match self {
            Self::Commit(v) => Some(alloc::vec![StoredItem::Commit(Arc::new(v.clone()))]),
            Self::Fragment(v) => Some(alloc::vec![StoredItem::Fragment(Arc::new(v.clone()))]),
            Self::Batch(commits, fragments) => Some(
                commits
                    .iter()
                    .cloned()
                    .map(|v| StoredItem::Commit(Arc::new(v)))
                    .chain(
                        fragments
                            .iter()
                            .cloned()
                            .map(|v| StoredItem::Fragment(Arc::new(v))),
                    )
                    .collect(),
            ),
            Self::Register
            | Self::DeleteCommit(_)
            | Self::DeleteCommits
            | Self::DeleteFragment(_)
            | Self::DeleteFragments
            | Self::DeleteId
            | Self::DeleteTree => None,
        }
    }

    pub(crate) fn update(&self) -> Option<Sedimentree> {
        match self {
            Self::Commit(v) => Some(Sedimentree::new(
                Vec::new(),
                alloc::vec![v.payload().clone()],
            )),
            Self::Fragment(v) => Some(Sedimentree::new(
                alloc::vec![v.payload().clone()],
                Vec::new(),
            )),
            Self::Batch(commits, fragments) => Some(Sedimentree::new(
                fragments.iter().map(|v| v.payload().clone()).collect(),
                commits.iter().map(|v| v.payload().clone()).collect(),
            )),
            Self::Register => Some(Sedimentree::default()),
            Self::DeleteCommit(_)
            | Self::DeleteCommits
            | Self::DeleteFragment(_)
            | Self::DeleteFragments
            | Self::DeleteId
            | Self::DeleteTree => None,
        }
    }
    pub(crate) async fn persist<Async: FutureForm, Store: Storage<Async>>(
        self,
        storage: &Store,
        id: SedimentreeId,
    ) -> Result<usize, Store::Error> {
        match self {
            Self::Commit(v) => storage.save_loose_commit(id, v).await?,
            Self::Fragment(v) => storage.save_fragment(id, v).await?,
            Self::Batch(commits, fragments) => {
                return storage.save_batch(id, commits, fragments).await;
            }
            Self::Register => storage.save_sedimentree_id(id).await?,
            Self::DeleteCommit(head) => storage.delete_loose_commit(id, head).await?,
            Self::DeleteCommits => storage.delete_loose_commits(id).await?,
            Self::DeleteFragment(head) => storage.delete_fragment(id, head).await?,
            Self::DeleteFragments => storage.delete_fragments(id).await?,
            Self::DeleteId => storage.delete_sedimentree_id(id).await?,
            Self::DeleteTree => {
                storage.delete_loose_commits(id).await?;
                storage.delete_fragments(id).await?;
                storage.delete_sedimentree_id(id).await?;
            }
        }
        Ok(1)
    }
}

pub(crate) type Runner<Async, Store> = dyn Fn(
        Fetcher<Async, Store>,
        Operation,
    )
        -> async_channel::Receiver<Result<usize, MutationError<<Store as Storage<Async>>::Error>>>
    + Send
    + Sync;
pub(crate) type Submission<Async, Store> = Arc<spin::Mutex<Option<Arc<Runner<Async, Store>>>>>;

pub(crate) async fn apply<Async: FutureForm, Store: Storage<Async>>(
    submission: &Submission<Async, Store>,
    fetcher: Fetcher<Async, Store>,
    operation: Operation,
) -> Result<usize, MutationError<Store::Error>> {
    // Never call a spawner under the submission lock: rejection may drop work
    // synchronously, including its completion and observation guards.
    let runner = submission.lock().clone();
    if let Some(runner) = runner {
        if fetcher.state.closed() {
            return Err(MutationError::Closed);
        }
        if let Ok(result) = runner(fetcher.clone(), operation).recv().await {
            result
        } else {
            fetcher.state.close();
            Err(MutationError::Closed)
        }
    } else {
        operation
            .persist::<Async, Store>(fetcher.storage(), fetcher.sedimentree_id())
            .await
            .map_err(MutationError::Storage)
    }
}
