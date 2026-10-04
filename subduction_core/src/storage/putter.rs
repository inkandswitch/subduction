//! Authorized writes use the powerbox's shared mutation submission boundary.
use super::{
    fetcher::Fetcher,
    mutation::{self, MutationError, Operation, Submission},
    traits::Storage,
};
use alloc::vec::Vec;
use future_form::FutureForm;
use sedimentree_core::{fragment::Fragment, id::SedimentreeId, loose_commit::LooseCommit};
use subduction_crypto::verified_meta::VerifiedMeta;

/// Verified-author write capability for one tree; put also grants fetch access.
/// Node-managed writes own persistence and cache completion after submission.
/// Standalone capabilities run inline. Cancellation is never rollback.
pub struct Putter<Async: FutureForm, Store: Storage<Async>> {
    fetcher: Fetcher<Async, Store>,
    submission: Submission<Async, Store>,
}
impl<Async: FutureForm, Store: Storage<Async>> Putter<Async, Store> {
    pub(super) const fn new(
        fetcher: Fetcher<Async, Store>,
        submission: Submission<Async, Store>,
    ) -> Self {
        Self {
            fetcher,
            submission,
        }
    }
    /// Tree to which authorization applies.
    #[must_use]
    pub const fn sedimentree_id(&self) -> SedimentreeId {
        self.fetcher.sedimentree_id()
    }
    /// Downgrade to read access.
    #[must_use]
    pub fn as_fetcher(&self) -> Fetcher<Async, Store> {
        self.fetcher.clone()
    }
    /// Save verified metadata and its blob.
    /// # Errors
    /// Returns a backend failure or interrupted mutation driver.
    pub async fn save_commit(
        &self,
        verified: VerifiedMeta<LooseCommit>,
    ) -> Result<(), MutationError<Store::Error>> {
        mutation::apply(
            &self.submission,
            self.as_fetcher(),
            Operation::Commit(verified),
        )
        .await
        .map(|_| ())
    }
    /// Save a verified fragment and blob.
    /// # Errors
    /// Returns a backend failure or interrupted mutation driver.
    pub async fn save_fragment(
        &self,
        verified: VerifiedMeta<Fragment>,
    ) -> Result<(), MutationError<Store::Error>> {
        mutation::apply(
            &self.submission,
            self.as_fetcher(),
            Operation::Fragment(verified),
        )
        .await
        .map(|_| ())
    }
    /// Save one backend batch, returning its stored item count.
    /// # Errors
    /// Returns a backend failure or interrupted mutation driver; effects may be partial.
    pub async fn save_batch(
        &self,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> Result<usize, MutationError<Store::Error>> {
        mutation::apply(
            &self.submission,
            self.as_fetcher(),
            Operation::Batch(commits, fragments),
        )
        .await
    }
    /// Register an empty tree in the ID index.
    /// # Errors
    /// Returns a backend failure or interrupted mutation driver.
    pub async fn save_sedimentree_id(&self) -> Result<(), MutationError<Store::Error>> {
        mutation::apply(&self.submission, self.as_fetcher(), Operation::Register)
            .await
            .map(|_| ())
    }
}
impl<Async: FutureForm, Store: Storage<Async>> Clone for Putter<Async, Store> {
    fn clone(&self) -> Self {
        Self {
            fetcher: self.fetcher.clone(),
            submission: self.submission.clone(),
        }
    }
}
impl<Async: FutureForm, Store: Storage<Async>> core::fmt::Debug for Putter<Async, Store> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Putter")
            .field("sedimentree_id", &self.sedimentree_id())
            .finish_non_exhaustive()
    }
}
