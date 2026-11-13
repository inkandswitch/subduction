//! The mutation vocabulary shared by managed put and delete capabilities.
//!
//! Backends only persist data. A node gives its powerbox a `Submission`: a
//! handle to the node's mutation worker, which capabilities use to submit
//! persistence and cache completion together. The worker runs on the existing
//! spawner, not another executor or registry, and spawns one task per mutation.
//! Every powerbox is constructed with its worker; mutations never execute inline.

use super::{fetcher::Fetcher, state::TrackedMutation, traits::Storage};
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
    /// Persistence failed; effects may be partial.
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

/// A request to the node's mutation worker.
pub(crate) struct Request<Async: FutureForm, Store: Storage<Async>> {
    pub(crate) fetcher: Fetcher<Async, Store>,
    pub(crate) operation: Operation,
    /// Taken at submission, so admission covers the request while it waits.
    pub(crate) tracked: TrackedMutation,
    pub(crate) reply: async_channel::Sender<Result<usize, MutationError<Store::Error>>>,
}

/// Starts a not-yet-running worker on the node's spawner.
pub(crate) type SpawnWorker<Async> =
    dyn Fn(<Async as FutureForm>::Future<'static, ()>) + Send + Sync;

/// The capabilities' handle to the node's mutation worker.
///
/// The worker owns the minimized tree store and the spawner, so this handle
/// is `Send` exactly when its requests and the pending worker future are:
/// always on `Sendable`, and on `Local` without requiring a thread-safe store.
/// The worker starts on the first submission, so building a node does not
/// need a running executor.
pub(crate) struct Submission<Async: FutureForm, Store: Storage<Async>> {
    requests: async_channel::Sender<Request<Async, Store>>,
    start: Arc<Start<Async>>,
}

struct Start<Async: FutureForm> {
    worker: spin::Mutex<Option<Async::Future<'static, ()>>>,
    spawn: Arc<SpawnWorker<Async>>,
}

impl<Async: FutureForm, Store: Storage<Async>> Submission<Async, Store> {
    /// A handle to `worker`, which must serve the receiving end of
    /// `requests`. `spawn` starts it on the first submission.
    pub(crate) fn new(
        requests: async_channel::Sender<Request<Async, Store>>,
        worker: Async::Future<'static, ()>,
        spawn: Arc<SpawnWorker<Async>>,
    ) -> Self {
        Self {
            requests,
            start: Arc::new(Start {
                worker: spin::Mutex::new(Some(worker)),
                spawn,
            }),
        }
    }

    fn start_worker(&self) {
        let worker = self.start.worker.lock().take();
        if let Some(worker) = worker {
            (self.start.spawn)(worker);
        }
    }
}

impl<Async: FutureForm, Store: Storage<Async>> Clone for Submission<Async, Store> {
    fn clone(&self) -> Self {
        Self {
            requests: self.requests.clone(),
            start: self.start.clone(),
        }
    }
}

pub(crate) async fn apply<Async: FutureForm, Store: Storage<Async>>(
    submission: &Submission<Async, Store>,
    fetcher: Fetcher<Async, Store>,
    operation: Operation,
) -> Result<usize, MutationError<Store::Error>> {
    if fetcher.coordination().closed() {
        return Err(MutationError::Closed);
    }
    let coordination = fetcher.coordination().clone();
    let (reply, receiver) = async_channel::bounded(1);
    // Admission covers submission too. A rejected, never-run request must
    // close validity even if its waiter disappears immediately: dropping
    // `tracked` unsettled does that.
    let tracked = coordination.track_mutation();
    submission.start_worker();
    let request = Request {
        fetcher,
        operation,
        tracked,
        reply,
    };
    if submission.requests.try_send(request).is_ok()
        && let Ok(result) = receiver.recv().await
    {
        return result;
    }
    coordination.close();
    Err(MutationError::Closed)
}
