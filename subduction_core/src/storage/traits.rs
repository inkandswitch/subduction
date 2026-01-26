//! Storage trait definition.
//!
//! The storage trait uses [`Signed`] types to ensure that only verified data
//! can be stored. This provides cryptographic proof of authorship for all
//! commits and fragments.
//!
//! # Content-Addressed Storage
//!
//! Commits and fragments follow content-addressed storage (CAS) patterns,
//! similar to blobs:
//!
//! ```text
//! Blobs:     save(blob) → digest,  load(digest) → blob
//! Commits:   save(id, commit) → digest,  load(id, digest) → commit
//! Fragments: save(id, fragment) → digest,  load(id, digest) → fragment
//! ```
//!
//! The `SedimentreeId` acts as a namespace — commits and fragments are
//! content-addressed within their sedimentree.

use alloc::vec::Vec;

use future_form::FutureForm;
use sedimentree_core::{
    blob::Blob, collections::Set, digest::Digest, fragment::Fragment, id::SedimentreeId,
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
///
/// # Content-Addressed Pattern
///
/// Commits and fragments are keyed by the digest of their payload (not the
/// signature). This enables:
///
/// - O(1) lookup by digest for sync protocols
/// - Efficient "what do I have vs. what do you have" comparisons
/// - Forwarding signed data without decoding
#[allow(clippy::type_complexity)]
pub trait Storage<K: FutureForm + ?Sized> {
    /// The error type for storage operations.
    type Error: core::error::Error;

    // ==================== Sedimentree IDs ====================

    /// Register a sedimentree ID to track which sedimentrees have data stored.
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

    // ==================== Loose Commits (CAS) ====================

    /// Save a signed loose commit, returning its digest.
    ///
    /// The digest is computed from the commit payload and used as the key.
    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: Signed<LooseCommit>,
    ) -> K::Future<'_, Result<Digest<LooseCommit>, Self::Error>>;

    /// Load a loose commit by its digest.
    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<Option<Signed<LooseCommit>>, Self::Error>>;

    /// List all commit digests for a sedimentree.
    fn list_commit_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Set<Digest<LooseCommit>>, Self::Error>>;

    /// Load all loose commits for a sedimentree.
    ///
    /// Returns digests alongside signed data for efficient indexing.
    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>, Self::Error>>;

    /// Delete a loose commit by its digest.
    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Delete all loose commits for a sedimentree.
    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    // ==================== Fragments (CAS) ====================

    /// Save a signed fragment, returning its digest.
    ///
    /// The digest is computed from the fragment payload and used as the key.
    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
    ) -> K::Future<'_, Result<Digest<Fragment>, Self::Error>>;

    /// Load a fragment by its digest.
    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<Option<Signed<Fragment>>, Self::Error>>;

    /// List all fragment digests for a sedimentree.
    fn list_fragment_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Set<Digest<Fragment>>, Self::Error>>;

    /// Load all fragments for a sedimentree.
    ///
    /// Returns digests alongside signed data for efficient indexing.
    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<(Digest<Fragment>, Signed<Fragment>)>, Self::Error>>;

    /// Delete a fragment by its digest.
    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Delete all fragments for a sedimentree.
    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    // ==================== Blobs (CAS) ====================

    /// Save a blob, returning its digest.
    fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest<Blob>, Self::Error>>;

    /// Load a blob by its digest.
    fn load_blob(
        &self,
        blob_digest: Digest<Blob>,
    ) -> K::Future<'_, Result<Option<Blob>, Self::Error>>;

    /// Delete a blob by its digest.
    fn delete_blob(&self, blob_digest: Digest<Blob>) -> K::Future<'_, Result<(), Self::Error>>;

    // ==================== Convenience Methods ====================

    /// Save a commit with its blob.
    fn save_commit_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        commit: Signed<LooseCommit>,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>>;

    /// Save a fragment with its blob.
    fn save_fragment_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>>;

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
    /// Digests of saved commits.
    pub commit_digests: Vec<Digest<LooseCommit>>,

    /// Digests of saved fragments.
    pub fragment_digests: Vec<Digest<Fragment>>,
}
