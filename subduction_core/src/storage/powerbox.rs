//! Mint authorized storage capabilities and share their node execution context.
//!
//! Trusted tree reads use `local_fetcher`; global ID enumeration remains separate.
//! Node construction supplies an immutable mutation runner before any capability
//! can be minted. All writes use that runner, owning persistence and cache
//! completion independently of the API waiter.

use super::{
    destroyer::Destroyer, fetcher::Fetcher, local_access::LocalStorageAccess, mutation::Submission,
    putter::Putter, traits::Storage,
};
use crate::{peer::id::PeerId, policy::storage::StoragePolicy};
use alloc::sync::Arc;
use future_form::{FutureForm, Sendable};
use sedimentree_core::id::SedimentreeId;
use subduction_crypto::verified_author::VerifiedAuthor;

/// The authorization and execution boundary for storage capabilities.
///
/// Every powerbox has a mutation runner from construction onward. Use the node
/// builder, or [`StorageTasks::storage_powerbox`](crate::subduction::StorageTasks::storage_powerbox)
/// when assembling a node and handler manually. There is no inline write mode.
///
/// A backend and policy alone cannot construct a powerbox:
///
/// ```compile_fail
/// use std::sync::Arc;
/// use subduction_core::{
///     policy::open::OpenPolicy,
///     storage::{memory::MemoryStorage, powerbox::StoragePowerbox},
/// };
/// let powerbox = StoragePowerbox::<_, _, future_form::Sendable>::new(
///     MemoryStorage::new(), Arc::new(OpenPolicy),
/// );
/// ```
pub struct StoragePowerbox<Store: Storage<Async>, Auth, Async: FutureForm = Sendable> {
    storage: Arc<Store>,
    policy: Arc<Auth>,
    pub(crate) state: Arc<super::state::StorageState>,
    submission: Submission<Async, Store>,
}
impl<Store: Storage<Async>, Auth, Async: FutureForm> StoragePowerbox<Store, Auth, Async> {
    /// Construct a powerbox with the node's immutable mutation runner.
    pub(crate) fn new(
        storage: Store,
        policy: Arc<Auth>,
        submission: Submission<Async, Store>,
    ) -> Self {
        Self {
            storage: Arc::new(storage),
            policy,
            state: Arc::default(),
            submission,
        }
    }
    /// Reference to the policy.
    #[must_use]
    pub fn policy(&self) -> &Auth {
        &self.policy
    }
    /// Shared ownership of the policy.
    #[must_use]
    pub fn policy_arc(&self) -> Arc<Auth> {
        self.policy.clone()
    }
    /// Mint a local-only deletion capability. Never hand this to peers.
    #[must_use]
    pub fn local_destroyer(&self, id: SedimentreeId) -> Destroyer<Async, Store> {
        Destroyer::new(self.local_fetcher(id), self.submission.clone())
    }
    /// Authorize a peer before minting its read capability.
    ///
    /// # Errors
    /// Returns the policy's fetch denial.
    pub async fn get_fetcher(
        &self,
        peer: PeerId,
        id: SedimentreeId,
    ) -> Result<Fetcher<Async, Store>, Auth::FetchDisallowed>
    where
        Auth: StoragePolicy<Async>,
    {
        self.policy.authorize_fetch(peer, id).await?;
        Ok(self.local_fetcher(id))
    }
    /// Authorize verified authorship before minting a remote write capability.
    ///
    /// # Errors
    /// Returns the policy's put denial.
    pub async fn get_putter(
        &self,
        requestor: PeerId,
        author: VerifiedAuthor,
        id: SedimentreeId,
    ) -> Result<Putter<Async, Store>, Auth::PutDisallowed>
    where
        Auth: StoragePolicy<Async>,
    {
        self.policy.authorize_put(requestor, author, id).await?;
        Ok(self.local_putter(id))
    }
    /// The node trusts its own writes; remote writes must use `get_putter`.
    pub(crate) fn local_putter(&self, id: SedimentreeId) -> Putter<Async, Store> {
        Putter::new(self.local_fetcher(id), self.submission.clone())
    }
    /// Trusted tree-scoped local reads, never a replacement for remote policy.
    pub(crate) fn local_fetcher(&self, id: SedimentreeId) -> Fetcher<Async, Store> {
        Fetcher::new(self.storage.clone(), id, self.state.tree(id))
    }
    /// The global ID index is not scoped to one tree.
    pub(crate) fn local_access(&self) -> LocalStorageAccess<Store> {
        LocalStorageAccess::new(self.storage.clone())
    }
}
impl<Store: Storage<Async>, Auth, Async: FutureForm> Clone for StoragePowerbox<Store, Auth, Async> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            policy: self.policy.clone(),
            state: self.state.clone(),
            submission: self.submission.clone(),
        }
    }
}
impl<Store: Storage<Async>, Auth, Async: FutureForm> core::fmt::Debug
    for StoragePowerbox<Store, Auth, Async>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StoragePowerbox").finish_non_exhaustive()
    }
}
