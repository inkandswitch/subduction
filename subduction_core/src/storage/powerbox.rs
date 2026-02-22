//! Storage powerbox for capability-gated access.
//!
//! The [`StoragePowerbox`] wraps a storage backend and policy, enforcing that all
//! operations go through capabilities ([`Fetcher`]/[`Putter`]/[`Destroyer`]).
//!
//! For startup hydration only, use [`hydration_access`][StoragePowerbox::hydration_access].

use alloc::sync::Arc;

use future_form::FutureForm;
use sedimentree_core::id::SedimentreeId;

use super::{
    destroyer::Destroyer, fetcher::Fetcher, local_access::LocalStorageAccess, putter::Putter,
    traits::Storage,
};
use crate::{peer::id::PeerId, policy::storage::StoragePolicy};

/// A powerbox that wraps storage and policy, only allowing access through capabilities.
///
/// This struct enforces the capability pattern at compile time: the underlying
/// storage is not directly accessible. Peer-facing operations must go through:
/// - [`get_fetcher`][Self::get_fetcher] for authorized reads
/// - [`get_putter`][Self::get_putter] for authorized writes
/// - [`local_destroyer`][Self::local_destroyer] for local cleanup
///
/// For startup hydration, use [`hydration_access`][Self::hydration_access] (crate-internal).
///
/// The powerbox holds both the storage backend and the authorization policy,
/// making it the single trust boundary for capability minting.
#[derive(Debug)]
pub struct StoragePowerbox<S, P> {
    storage: Arc<S>,
    policy: Arc<P>,
}

impl<S, P> StoragePowerbox<S, P> {
    /// Create a new powerbox wrapping the given storage and policy.
    pub fn new(storage: S, policy: Arc<P>) -> Self {
        Self {
            storage: Arc::new(storage),
            policy,
        }
    }

    /// Get a reference to the policy.
    ///
    /// This is useful for connection policy decisions outside of storage access.
    #[must_use]
    pub fn policy(&self) -> &P {
        &self.policy
    }

    /// Get a clone of the policy Arc.
    ///
    /// This is useful when you need shared ownership of the policy.
    #[must_use]
    pub fn policy_arc(&self) -> Arc<P> {
        self.policy.clone()
    }

    /// Create a destroyer for local cleanup operations.
    ///
    /// Use this for compaction, garbage collection, and other local-only
    /// delete operations. Never hand this capability to peers.
    #[must_use]
    pub fn local_destroyer<K: FutureForm>(&self, sedimentree_id: SedimentreeId) -> Destroyer<K, S>
    where
        S: Storage<K>,
    {
        Destroyer::new(self.storage.clone(), sedimentree_id)
    }

    /// Create a fetch capability for a peer to access a sedimentree.
    ///
    /// Checks authorization via the policy before minting the capability.
    ///
    /// # Errors
    ///
    /// Returns the policy's `FetchDisallowed` error if authorization fails.
    pub async fn get_fetcher<K: FutureForm>(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> Result<Fetcher<K, S>, P::FetchDisallowed>
    where
        S: Storage<K>,
        P: StoragePolicy<K>,
    {
        self.policy.authorize_fetch(peer, sedimentree_id).await?;
        Ok(Fetcher::new(self.storage.clone(), sedimentree_id))
    }

    /// Create a put capability for a peer to write to a sedimentree.
    ///
    /// Checks authorization via the policy before minting the capability.
    ///
    /// # Errors
    ///
    /// Returns the policy's `PutDisallowed` error if authorization fails.
    pub async fn get_putter<K: FutureForm>(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> Result<Putter<K, S>, P::PutDisallowed>
    where
        S: Storage<K>,
        P: StoragePolicy<K>,
    {
        self.policy
            .authorize_put(requestor, author, sedimentree_id)
            .await?;
        Ok(Putter::new(self.storage.clone(), sedimentree_id))
    }

    /// Get direct storage access for hydration (startup data loading).
    ///
    /// This bypasses the capability model and should only be used from
    /// [`hydrate`][crate::subduction::Subduction::hydrate] to rebuild
    /// in-memory state from storage.
    ///
    /// All other access should go through [`get_fetcher`](Self::get_fetcher)
    /// or [`get_putter`](Self::get_putter).
    #[must_use]
    pub(crate) fn hydration_access(&self) -> LocalStorageAccess<S> {
        LocalStorageAccess::new(self.storage.clone())
    }
}

impl<S, P> Clone for StoragePowerbox<S, P> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            policy: self.policy.clone(),
        }
    }
}
