//! Put capability for authorized write access to sedimentrees.

use core::marker::PhantomData;

use alloc::{sync::Arc, vec::Vec};

use future_form::FutureForm;
use sedimentree_core::{
    blob::Blob, collections::Set, crypto::digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};

use super::{fetcher::Fetcher, traits::Storage};
use subduction_crypto::{
    signed::Signed, verified_meta::VerifiedMeta, verified_signature::VerifiedSignature,
};

/// A capability granting put access to a specific sedimentree's data.
///
/// This type bundles:
/// - Proof that put access was authorized
/// - The storage backend to write to
///
/// Created via [`Subduction::authorize_put`][crate::subduction::Subduction].
///
/// A `Putter` also grants fetch access (put implies fetch).
pub struct Putter<K: FutureForm, S: Storage<K>> {
    storage: Arc<S>,
    sedimentree_id: SedimentreeId,
    _marker: PhantomData<K>,
}

impl<K: FutureForm, S: Storage<K>> Putter<K, S> {
    /// Create a new putter capability.
    ///
    /// This should only be called after authorization has been verified.
    pub(super) const fn new(storage: Arc<S>, sedimentree_id: SedimentreeId) -> Self {
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
    pub fn as_fetcher(&self) -> Fetcher<K, S> {
        Fetcher::new(self.storage.clone(), self.sedimentree_id)
    }

    // ==================== Commits ====================

    /// Save a commit with verified blob metadata.
    ///
    /// Takes [`VerifiedMeta<LooseCommit>`] to enforce at compile time that:
    /// 1. The signature has been verified
    /// 2. The blob content matches the claimed metadata
    ///
    /// Blob is saved first, then the commit metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if storage fails.
    pub async fn save_commit(
        &self,
        verified_meta: VerifiedMeta<LooseCommit>,
    ) -> Result<Digest<LooseCommit>, S::Error> {
        let (verified_sig, blob) = verified_meta.into_parts();
        self.storage.save_blob(self.sedimentree_id, blob).await?;
        self.storage
            .save_loose_commit(self.sedimentree_id, verified_sig.into_signed())
            .await
    }

    /// Save a verified loose commit (signature only), returning its digest.
    ///
    /// This is a lower-level method that only verifies the signature, not the blob.
    /// Prefer [`save_commit`](Self::save_commit) which takes `VerifiedMeta` and
    /// enforces blob verification at compile time.
    ///
    /// Use this only when you've already saved the blob separately.
    #[must_use]
    pub fn save_loose_commit(
        &self,
        verified: VerifiedSignature<LooseCommit>,
    ) -> K::Future<'_, Result<Digest<LooseCommit>, S::Error>> {
        self.storage
            .save_loose_commit(self.sedimentree_id, verified.into_signed())
    }

    /// Load a loose commit by its digest.
    #[must_use]
    pub fn load_loose_commit(
        &self,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<Option<Signed<LooseCommit>>, S::Error>> {
        self.storage.load_loose_commit(self.sedimentree_id, digest)
    }

    /// List all commit digests for this sedimentree.
    #[must_use]
    pub fn list_commit_digests(&self) -> K::Future<'_, Result<Set<Digest<LooseCommit>>, S::Error>> {
        self.storage.list_commit_digests(self.sedimentree_id)
    }

    /// Load all loose commits for this sedimentree.
    ///
    /// Returns digests alongside signed data for efficient indexing.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn load_loose_commits(
        &self,
    ) -> K::Future<'_, Result<Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>, S::Error>> {
        self.storage.load_loose_commits(self.sedimentree_id)
    }

    // ==================== Fragments ====================

    /// Save a fragment with verified blob metadata.
    ///
    /// Takes [`VerifiedMeta<Fragment>`] to enforce at compile time that:
    /// 1. The signature has been verified
    /// 2. The blob content matches the claimed metadata
    ///
    /// Blob is saved first, then the fragment metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if storage fails.
    pub async fn save_fragment_with_blob(
        &self,
        verified_meta: VerifiedMeta<Fragment>,
    ) -> Result<Digest<Fragment>, S::Error> {
        let (verified_sig, blob) = verified_meta.into_parts();
        self.storage.save_blob(self.sedimentree_id, blob).await?;
        self.storage
            .save_fragment(self.sedimentree_id, verified_sig.into_signed())
            .await
    }

    /// Save a verified fragment (signature only), returning its digest.
    ///
    /// This is a lower-level method that only verifies the signature, not the blob.
    /// Prefer [`save_fragment_with_blob`](Self::save_fragment_with_blob) which takes
    /// `VerifiedMeta` and enforces blob verification at compile time.
    ///
    /// Use this only when you've already saved the blob separately.
    #[must_use]
    pub fn save_fragment(
        &self,
        verified: VerifiedSignature<Fragment>,
    ) -> K::Future<'_, Result<Digest<Fragment>, S::Error>> {
        self.storage
            .save_fragment(self.sedimentree_id, verified.into_signed())
    }

    /// Load a fragment by its digest.
    #[must_use]
    pub fn load_fragment(
        &self,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<Option<Signed<Fragment>>, S::Error>> {
        self.storage.load_fragment(self.sedimentree_id, digest)
    }

    /// List all fragment digests for this sedimentree.
    #[must_use]
    pub fn list_fragment_digests(&self) -> K::Future<'_, Result<Set<Digest<Fragment>>, S::Error>> {
        self.storage.list_fragment_digests(self.sedimentree_id)
    }

    /// Load all fragments for this sedimentree.
    ///
    /// Returns digests alongside signed data for efficient indexing.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn load_fragments(
        &self,
    ) -> K::Future<'_, Result<Vec<(Digest<Fragment>, Signed<Fragment>)>, S::Error>> {
        self.storage.load_fragments(self.sedimentree_id)
    }

    // ==================== Blobs ====================

    /// Save a blob under this sedimentree and return its digest.
    #[must_use]
    pub fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest<Blob>, S::Error>> {
        self.storage.save_blob(self.sedimentree_id, blob)
    }

    /// Load a blob by its digest within this sedimentree.
    #[must_use]
    pub fn load_blob(&self, digest: Digest<Blob>) -> K::Future<'_, Result<Option<Blob>, S::Error>> {
        self.storage.load_blob(self.sedimentree_id, digest)
    }

    // ==================== Bookkeeping ====================

    /// Register this sedimentree ID as having data stored.
    ///
    /// This is bookkeeping to track which sedimentrees exist.
    #[must_use]
    pub fn save_sedimentree_id(&self) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.save_sedimentree_id(self.sedimentree_id)
    }
}

impl<K: FutureForm, S: Storage<K>> Clone for Putter<K, S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sedimentree_id: self.sedimentree_id,
            _marker: PhantomData,
        }
    }
}

impl<K: FutureForm, S: Storage<K>> core::fmt::Debug for Putter<K, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Putter")
            .field("sedimentree_id", &self.sedimentree_id)
            .finish_non_exhaustive()
    }
}
