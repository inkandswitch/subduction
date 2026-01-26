//! Storage trait definition.
//!
//! The storage trait uses [`Signed`] types to ensure that only verified data
//! can be stored. This provides cryptographic proof of authorship for all
//! commits and fragments.

use alloc::vec::Vec;

use future_form::FutureForm;
use sedimentree_core::{
    blob::{Blob, Digest},
    collections::Set,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};

use crate::crypto::signed::Signed;

/// Abstraction over storage for `Sedimentree` data.
///
/// All commits and fragments are stored as [`Signed`] values, ensuring that
/// only cryptographically verified data enters storage. This provides:
///
/// - **Provenance**: Every stored commit/fragment has a verified author
/// - **Integrity**: Signatures prevent tampering with stored data
/// - **Trust on load**: Data loaded from storage is trusted (was verified before store)
pub trait Storage<K: FutureForm + ?Sized> {
    /// The error type for storage operations.
    type Error: core::error::Error;

    /// Insert a sedimentree ID to know which sedimentrees have data stored.
    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Delete a sedimentree ID.
    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Get all sedimentree IDs that have data stored.
    fn load_all_sedimentree_ids(&self) -> K::Future<'_, Result<Set<SedimentreeId>, Self::Error>>;

    /// Save a signed loose commit to storage.
    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: Signed<LooseCommit>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Load all signed loose commits from storage.
    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<Signed<LooseCommit>>, Self::Error>>;

    /// Delete all loose commits from storage.
    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Save a signed fragment to storage.
    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Load all signed fragments from storage.
    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<Signed<Fragment>>, Self::Error>>;

    /// Delete all fragments from storage.
    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Save a blob to storage.
    fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest, Self::Error>>;

    /// Load a blob from storage.
    fn load_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<Option<Blob>, Self::Error>>;

    /// Delete a blob from storage.
    fn delete_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<(), Self::Error>>;

    /// Save a commit with its blob.
    fn save_commit_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        commit: Signed<LooseCommit>,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest, Self::Error>>;

    /// Save a fragment with its blob.
    fn save_fragment_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest, Self::Error>>;

    /// Save a batch of commits and fragments.
    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Blob)>,
        fragments: Vec<(Signed<Fragment>, Blob)>,
    ) -> K::Future<'_, Result<BatchResult, Self::Error>>;
}

/// Result of a batch save operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchResult {
    /// Digests of saved blobs (commits first, then fragments).
    pub blob_digests: Vec<Digest>,
}
