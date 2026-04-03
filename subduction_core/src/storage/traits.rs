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
//! Load: sedimentree_id → Vec<VerifiedMeta<LooseCommit>>
//! ```
//!
//! # Content-Addressed Storage
//!
//! Both commits and fragments are stored using content-addressed keys
//! (`Digest<T>`) derived from their payload. This provides:
//!
//! - Idempotent writes (same content → same key → no-op)
//! - No interleaving or TOCTOU concerns
//! - Byzantine-safe: conflicting payloads for the same [`CommitId`] produce
//!   different digests and coexist on disk
//!
//! The user-supplied [`CommitId`] is the _in-memory_ identity for DAG
//! traversal and sync. The content hash is purely a storage concern.
//! On hydration, all commits are bulk-loaded, decoded, and re-keyed by
//! [`CommitId`] (first-loaded-wins on duplicate identities).

use alloc::vec::Vec;

use future_form::FutureForm;
use sedimentree_core::{
    collections::Set,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
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
///
/// # Commit & Fragment Storage
///
/// Both commits and fragments are content-addressed by their respective
/// [`Digest`] internally (CAS), but the trait surface uses causal identity
/// keys: [`CommitId`] for commits and [`CommitId`] (fragment head) for fragments.
///
/// Bulk operations ([`load_loose_commits`](Storage::load_loose_commits),
/// [`load_fragments`](Storage::load_fragments)) are the primary hydration
/// path. Single-item operations ([`load_loose_commit`](Storage::load_loose_commit),
/// [`load_fragment`](Storage::load_fragment)) are available for targeted
/// lookups (e.g., fetching a specific blob).
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
    /// The commit and blob are stored atomically. The content hash
    /// (`Digest<LooseCommit>`) is computed internally and used as the
    /// storage key (CAS). Saving the same content twice is a no-op.
    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// List all [`CommitId`] values for a sedimentree.
    ///
    /// This is derived from the stored payloads — each commit is decoded
    /// and its `head()` extracted. Duplicate [`CommitId`] values (from
    /// Byzantine equivocation) are deduplicated.
    fn list_commit_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Set<CommitId>, Self::Error>>;

    /// Load all loose commits with their blobs for a sedimentree.
    ///
    /// Used for hydration at startup and sync. All returned [`VerifiedMeta`]
    /// values are reconstructed from trusted storage without re-verification.
    ///
    /// If multiple payloads share the same [`CommitId`] (Byzantine
    /// equivocation), all are returned. The caller is responsible for
    /// deduplication (typically first-loaded-wins via `entry().or_insert()`).
    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>>;

    /// Load a single loose commit by [`CommitId`].
    ///
    /// Returns `None` if no commit exists with the given identity.
    /// The returned `VerifiedMeta` is reconstructed from trusted storage
    /// without re-verifying the signature or blob.
    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> K::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>>;

    /// Delete a single loose commit and its blob by [`CommitId`].
    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
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

    /// Load a fragment with its blob by fragment head [`CommitId`].
    ///
    /// Returns `None` if no fragment exists with the given identity.
    /// The returned `VerifiedMeta` is reconstructed from trusted storage
    /// without re-verifying the signature or blob.
    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> K::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>>;

    /// List all fragment head [`CommitId`] values for a sedimentree.
    ///
    /// This is derived from the stored payloads — each fragment is decoded
    /// and its `head()` extracted. Duplicate values are deduplicated.
    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Set<CommitId>, Self::Error>>;

    /// Load all fragments with their blobs for a sedimentree.
    ///
    /// Used for hydration at startup. All returned `VerifiedMeta` values are
    /// reconstructed from trusted storage without re-verification.
    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>>;

    /// Delete a fragment and its blob by fragment head [`CommitId`].
    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
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
