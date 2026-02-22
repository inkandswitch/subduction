//! Storage trait definition.
//!
//! The storage trait uses [`VerifiedMeta`] types to ensure that only verified data
//! can be stored. This provides cryptographic proof of authorship and blob integrity
//! for all commits and fragments.
//!
//! # Compound Storage
//!
//! Commits and fragments are stored _together with their blobs_ as a single
//! atomic unit via [`VerifiedMeta<T>`]. This ensures:
//!
//! - Blob is always available when loading a commit/fragment
//! - No orphaned blobs or commits/fragments without blobs
//! - Signature and blob integrity verified before storage
//!
//! ```text
//! Save: VerifiedMeta<LooseCommit> → storage (signed bytes + blob)
//! Load: digest → VerifiedMeta<LooseCommit> (reconstructed from trusted storage)
//! ```
//!
//! # Content-Addressed Storage
//!
//! Commits and fragments are keyed by the digest of their payload:
//!
//! - O(1) lookup by digest for sync protocols
//! - Efficient "what do I have vs. what do you have" comparisons

use alloc::vec::Vec;

use future_form::FutureForm;
use sedimentree_core::{
    collections::Set, crypto::digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_crypto::verified_meta::VerifiedMeta;

/// Abstraction over storage for `Sedimentree` data.
///
/// All commits and fragments are stored as [`VerifiedMeta`] values, which bundle:
/// - The signed payload (cryptographic proof of authorship)
/// - The blob content (verified to match claimed metadata)
///
/// This provides:
/// - **Provenance**: Every stored commit/fragment has a verified author
/// - **Integrity**: Signatures and blob hashes prevent tampering
/// - **Atomicity**: Commit/fragment and blob are always stored together
/// - **Trust on load**: Data loaded from storage is trusted (was verified before store)
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

    // ==================== Loose Commits (compound with blob) ====================

    /// Save a verified loose commit with its blob.
    ///
    /// The commit and blob are stored atomically. The digest is computed from
    /// the commit payload and used as the key.
    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Load a loose commit with its blob by digest.
    ///
    /// Returns `None` if no commit exists with the given digest.
    /// The returned `VerifiedMeta` is reconstructed from trusted storage
    /// without re-verifying the signature or blob.
    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>>;

    /// List all commit digests for a sedimentree.
    fn list_commit_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Set<Digest<LooseCommit>>, Self::Error>>;

    /// Load all loose commits with their blobs for a sedimentree.
    ///
    /// Used for hydration at startup. All returned `VerifiedMeta` values are
    /// reconstructed from trusted storage without re-verification.
    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>>;

    /// Delete a loose commit and its blob by digest.
    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Delete all loose commits and their blobs for a sedimentree.
    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    // ==================== Fragments (compound with blob) ====================

    /// Save a verified fragment with its blob.
    ///
    /// The fragment and blob are stored atomically. The digest is computed from
    /// the fragment payload and used as the key.
    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Load a fragment with its blob by digest.
    ///
    /// Returns `None` if no fragment exists with the given digest.
    /// The returned `VerifiedMeta` is reconstructed from trusted storage
    /// without re-verifying the signature or blob.
    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>>;

    /// List all fragment digests for a sedimentree.
    fn list_fragment_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Set<Digest<Fragment>>, Self::Error>>;

    /// Load all fragments with their blobs for a sedimentree.
    ///
    /// Used for hydration at startup. All returned `VerifiedMeta` values are
    /// reconstructed from trusted storage without re-verification.
    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>>;

    /// Delete a fragment and its blob by digest.
    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Delete all fragments and their blobs for a sedimentree.
    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    // ==================== Batch Operations ====================

    /// Save a batch of commits and fragments with their blobs.
    ///
    /// Returns the total count of items saved (commits + fragments).
    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> K::Future<'_, Result<usize, Self::Error>>;
}
