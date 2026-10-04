//! Local deletion uses the same managed completion boundary as put capabilities.
use super::{
    fetcher::Fetcher,
    mutation::{self, MutationError, Operation, Submission},
    traits::Storage,
};
use future_form::FutureForm;
use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};

/// Local-only deletion capability. Never hand this to peers.
pub struct Destroyer<Async: FutureForm, Store: Storage<Async>> {
    fetcher: Fetcher<Async, Store>,
    submission: Submission<Async, Store>,
}
impl<Async: FutureForm, Store: Storage<Async>> Destroyer<Async, Store> {
    pub(super) const fn new(
        fetcher: Fetcher<Async, Store>,
        submission: Submission<Async, Store>,
    ) -> Self {
        Self {
            fetcher,
            submission,
        }
    }
    /// Tree to which this capability applies.
    #[must_use]
    pub const fn sedimentree_id(&self) -> SedimentreeId {
        self.fetcher.sedimentree_id()
    }
    async fn apply(&self, operation: Operation) -> Result<(), MutationError<Store::Error>> {
        mutation::apply(&self.submission, self.fetcher.clone(), operation)
            .await
            .map(|_| ())
    }
    /// Delete a commit and blob.
    /// # Errors
    /// Backend failure or interrupted driver; effects may be partial.
    pub async fn delete_loose_commit(
        &self,
        head: CommitId,
    ) -> Result<(), MutationError<Store::Error>> {
        self.apply(Operation::DeleteCommit(head)).await
    }
    /// Delete all commits and blobs.
    /// # Errors
    /// Backend failure or interrupted driver; effects may be partial.
    pub async fn delete_loose_commits(&self) -> Result<(), MutationError<Store::Error>> {
        self.apply(Operation::DeleteCommits).await
    }
    /// Delete a fragment and blob.
    /// # Errors
    /// Backend failure or interrupted driver; effects may be partial.
    pub async fn delete_fragment(&self, head: CommitId) -> Result<(), MutationError<Store::Error>> {
        self.apply(Operation::DeleteFragment(head)).await
    }
    /// Delete all fragments and blobs.
    /// # Errors
    /// Backend failure or interrupted driver; effects may be partial.
    pub async fn delete_fragments(&self) -> Result<(), MutationError<Store::Error>> {
        self.apply(Operation::DeleteFragments).await
    }
    /// Remove the durable ID index entry.
    /// # Errors
    /// Backend failure or interrupted driver; effects may be partial.
    pub async fn delete_sedimentree_id(&self) -> Result<(), MutationError<Store::Error>> {
        self.apply(Operation::DeleteId).await
    }
    /// Delete contents and ID in one admitted mutation.
    pub(crate) async fn delete_tree(&self) -> Result<(), MutationError<Store::Error>> {
        self.apply(Operation::DeleteTree).await
    }
}
impl<Async: FutureForm, Store: Storage<Async>> Clone for Destroyer<Async, Store> {
    fn clone(&self) -> Self {
        Self {
            fetcher: self.fetcher.clone(),
            submission: self.submission.clone(),
        }
    }
}
impl<Async: FutureForm, Store: Storage<Async>> core::fmt::Debug for Destroyer<Async, Store> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Destroyer")
            .field("sedimentree_id", &self.sedimentree_id())
            .finish_non_exhaustive()
    }
}
