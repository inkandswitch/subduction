//! Put capability for authorized write access to sedimentrees.

use core::marker::PhantomData;

use alloc::{sync::Arc, vec::Vec};

use future_form::FutureForm;
use sedimentree_core::{fragment::Fragment, id::SedimentreeId, loose_commit::LooseCommit};
use subduction_crypto::verified_meta::VerifiedMeta;

use super::{fetcher::Fetcher, traits::Storage};

/// A capability granting put access to a specific sedimentree's data.
///
/// This type bundles:
/// - Proof that put access was authorized
/// - The storage backend to write to
///
/// Created via [`Subduction::authorize_put`][crate::subduction::Subduction].
///
/// A `Putter` also grants fetch access (put implies fetch).
pub struct Putter<Async: FutureForm, Store: Storage<Async>> {
    storage: Arc<Store>,
    sedimentree_id: SedimentreeId,
    _marker: PhantomData<Async>,
}

impl<Async: FutureForm, Store: Storage<Async>> Putter<Async, Store> {
    /// Create a new putter capability.
    ///
    /// This should only be called after authorization has been verified.
    pub(super) const fn new(storage: Arc<Store>, sedimentree_id: SedimentreeId) -> Self {
        Self {
            storage,
            sedimentree_id,
            _marker: PhantomData,
        }
    }

    /// Get the sedimentree ID this capability grants access to.
    #[must_use]
    pub const fn sedimentree_id(&self) -> SedimentreeId {
        self.sedimentree_id
    }

    /// Downgrade to a fetch-only capability.
    ///
    /// This is useful when you have put access but only need to fetch.
    #[must_use]
    pub fn as_fetcher(&self) -> Fetcher<Async, Store> {
        Fetcher::new(self.storage.clone(), self.sedimentree_id)
    }

    // ==================== Commits ====================

    /// Save a commit with verified blob metadata.
    ///
    /// Takes [`VerifiedMeta<LooseCommit>`] to enforce at compile time that:
    /// 1. The signature has been verified
    /// 2. The blob content matches the claimed metadata
    ///
    /// The commit and blob are stored atomically.
    #[must_use]
    pub fn save_commit(
        &self,
        verified: VerifiedMeta<LooseCommit>,
    ) -> Async::Future<'_, Result<(), Store::Error>> {
        self.storage
            .save_loose_commit(self.sedimentree_id, verified)
    }

    // ==================== Fragments ====================

    /// Save a fragment with verified blob metadata.
    ///
    /// Takes [`VerifiedMeta<Fragment>`] to enforce at compile time that:
    /// 1. The signature has been verified
    /// 2. The blob content matches the claimed metadata
    ///
    /// The fragment and blob are stored atomically.
    #[must_use]
    pub fn save_fragment(
        &self,
        verified: VerifiedMeta<Fragment>,
    ) -> Async::Future<'_, Result<(), Store::Error>> {
        self.storage.save_fragment(self.sedimentree_id, verified)
    }

    // ==================== Batch Operations ====================

    /// Save a batch of commits and fragments with their blobs.
    ///
    /// Returns the count of items saved.
    #[must_use]
    pub fn save_batch(
        &self,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> Async::Future<'_, Result<usize, Store::Error>> {
        self.storage
            .save_batch(self.sedimentree_id, commits, fragments)
    }

    // ==================== Bookkeeping ====================

    /// Register this sedimentree ID as having data stored.
    ///
    /// This is bookkeeping to track which sedimentrees exist.
    #[must_use]
    pub fn save_sedimentree_id(&self) -> Async::Future<'_, Result<(), Store::Error>> {
        self.storage.save_sedimentree_id(self.sedimentree_id)
    }
}

impl<Async: FutureForm, Store: Storage<Async>> Clone for Putter<Async, Store> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sedimentree_id: self.sedimentree_id,
            _marker: PhantomData,
        }
    }
}

impl<Async: FutureForm, Store: Storage<Async>> core::fmt::Debug for Putter<Async, Store> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Putter")
            .field("sedimentree_id", &self.sedimentree_id)
            .finish_non_exhaustive()
    }
}
