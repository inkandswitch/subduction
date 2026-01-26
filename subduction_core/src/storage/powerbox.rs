//! Storage powerbox for capability-gated access.
//!
//! The [`StoragePowerbox`] wraps a storage backend and policy, preventing direct access
//! and forcing all operations to go through capabilities ([`Fetcher`]/[`Putter`]/[`Destroyer`]).

use alloc::sync::Arc;

use future_form::FutureForm;
use sedimentree_core::{
    blob::{Blob, Digest},
    collections::Set,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};

use super::traits::Storage;
use super::{destroyer::Destroyer, fetcher::Fetcher, putter::Putter};
use crate::{crypto::signed::Signed, peer::id::PeerId, policy::StoragePolicy};

/// A powerbox that wraps storage and policy, only allowing access through capabilities.
///
/// This struct enforces the capability pattern at compile time: the underlying
/// storage is not directly accessible. All operations must go through:
/// - [`get_fetcher`][Self::get_fetcher] for authorized reads
/// - [`get_putter`][Self::get_putter] for authorized writes
/// - [`load_blob`][Self::load_blob] for local content-addressed reads
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

    /// Load a blob by its digest (content-addressed, local access).
    ///
    /// This is for local blob access, not peer requests. Blobs are content-addressed
    /// and shared across sedimentrees, so they don't fit the sedimentree-scoped
    /// capability model.
    #[must_use]
    pub fn load_blob<K: FutureForm>(
        &self,
        digest: Digest,
    ) -> K::Future<'_, Result<Option<Blob>, S::Error>>
    where
        S: Storage<K>,
    {
        self.storage.load_blob(digest)
    }

    /// Save a blob (content-addressed, local access).
    ///
    /// This is for saving blobs received from peers or created locally.
    /// Blobs are content-addressed and shared across sedimentrees.
    #[must_use]
    pub fn save_blob<K: FutureForm>(&self, blob: Blob) -> K::Future<'_, Result<Digest, S::Error>>
    where
        S: Storage<K>,
    {
        self.storage.save_blob(blob)
    }

    /// Load all sedimentree IDs from storage (for hydration).
    ///
    /// This is for local initialization, loading our own data.
    #[must_use]
    pub fn load_all_sedimentree_ids<K: FutureForm>(
        &self,
    ) -> K::Future<'_, Result<Set<SedimentreeId>, S::Error>>
    where
        S: Storage<K>,
    {
        self.storage.load_all_sedimentree_ids()
    }

    /// Load loose commits for a sedimentree (for hydration).
    ///
    /// This is for local initialization, loading our own data.
    /// Returns digests alongside signed data for efficient indexing.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn load_loose_commits<K: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<alloc::vec::Vec<(Digest, Signed<LooseCommit>)>, S::Error>>
    where
        S: Storage<K>,
    {
        self.storage.load_loose_commits(sedimentree_id)
    }

    /// Load fragments for a sedimentree (for hydration).
    ///
    /// This is for local initialization, loading our own data.
    /// Returns digests alongside signed data for efficient indexing.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn load_fragments<K: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<alloc::vec::Vec<(Digest, Signed<Fragment>)>, S::Error>>
    where
        S: Storage<K>,
    {
        self.storage.load_fragments(sedimentree_id)
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
