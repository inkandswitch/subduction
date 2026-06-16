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
pub trait Storage<Async: FutureForm + ?Sized> {
    /// The error type for storage operations.
    type Error: core::error::Error;

    // ==================== Sedimentree IDs ====================

    /// Register a sedimentree ID to track which sedimentrees have data stored.
    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>>;

    /// Delete a sedimentree ID.
    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>>;

    /// Get all sedimentree IDs that have data stored.
    fn load_all_sedimentree_ids(
        &self,
    ) -> Async::Future<'_, Result<Set<SedimentreeId>, Self::Error>>;

    /// Whether a sedimentree ID is registered (a single-key existence check).
    ///
    /// Existence is tracked by the id index independently of whether the tree
    /// has any commits/fragments, so a registered-but-empty tree returns
    /// `true`. Backends must implement this as a single-key lookup, *not* by
    /// enumerating [`load_all_sedimentree_ids`](Self::load_all_sedimentree_ids):
    /// it is on the hydration hot path (cache-miss existence checks), where an
    /// `O(total trees)` scan would be a latency and `DoS` hazard.
    fn contains_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<bool, Self::Error>>;

    // ==================== Loose Commits (compound with blob) ====================

    /// Save a verified loose commit with its blob.
    ///
    /// The commit and blob are stored atomically. The content hash
    /// (`Digest<LooseCommit>`) is computed internally and used as the
    /// storage key (CAS). Saving the same content twice is a no-op.
    ///
    /// # Contract
    ///
    /// Implementations **must** register `sedimentree_id` (the moral
    /// equivalent of [`save_sedimentree_id`](Self::save_sedimentree_id)) on
    /// a successful save: afterwards,
    /// [`contains_sedimentree_id`](Self::contains_sedimentree_id) and
    /// [`load_all_sedimentree_ids`](Self::load_all_sedimentree_ids) must
    /// reflect the tree, including after a reopen. Conversely, a **failed**
    /// save must **not** register the id in the backend's *observable* view
    /// (the in-memory / same-transaction state queried immediately
    /// afterwards) — no registered-but-empty trees.
    ///
    /// One caveat for backends that derive registration from durable layout
    /// rather than an explicit record (e.g. `FsStorage` rediscovers tree
    /// ids by scanning directories on reopen): a save that fails *after*
    /// creating the tree's directory but *before* writing any item can
    /// leave an empty tree directory that reopen rediscovers. This is
    /// benign — the sync handler never caches empty trees, so the effect is
    /// limited to id enumeration — but it means the "failed save does not
    /// register" guarantee is exact only for the observable view, not
    /// necessarily across an interrupted-first-write then restart.
    /// Transactional backends (e.g. `RedbStorage` registers in the same
    /// write transaction as the item) have no such window.
    ///
    /// Backends can verify the observable contract with the conformance
    /// helpers in `storage::conformance` (behind the `test_utils` feature).
    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> Async::Future<'_, Result<(), Self::Error>>;

    /// List all [`CommitId`] values for a sedimentree.
    ///
    /// This is derived from the stored payloads — each commit is decoded
    /// and its `head()` extracted. Duplicate [`CommitId`] values (from
    /// Byzantine equivocation) are deduplicated.
    fn list_commit_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Set<CommitId>, Self::Error>>;

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
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>>;

    /// Load all loose-commit *payloads* for a sedimentree, **without** their
    /// blobs.
    ///
    /// Hydration rebuilds the in-memory tree from [`LooseCommit`] metadata
    /// alone — the resident `MinimizedSedimentree` holds no blob bytes — so
    /// loading the blobs only to discard them is wasted I/O (on backends
    /// that store large blobs externally, an entire file read per item).
    ///
    /// Backends with a cheaper metadata-only read path implement it directly;
    /// those without can delegate to
    /// [`load_loose_commit_metas_via_full`] (load full, drop blobs). Same set
    /// semantics as [`load_loose_commits`](Self::load_loose_commits) (all
    /// equivocating payloads returned; caller dedups).
    fn load_loose_commit_metas(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<LooseCommit>, Self::Error>>;

    /// Load a single loose commit by [`CommitId`].
    ///
    /// Returns `None` if no commit exists with the given identity.
    /// The returned `VerifiedMeta` is reconstructed from trusted storage
    /// without re-verifying the signature or blob.
    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>>;

    /// Delete a single loose commit and its blob by [`CommitId`].
    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<(), Self::Error>>;

    /// Delete all loose commits and their blobs for a sedimentree.
    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>>;

    // ==================== Fragments (compound with blob) ====================

    /// Save a verified fragment with its blob.
    ///
    /// The fragment and blob are stored atomically. The digest is computed from
    /// the fragment payload and used as the key.
    ///
    /// # Contract
    ///
    /// Implementations **must** register `sedimentree_id` as part of the
    /// save — see [`save_loose_commit`](Self::save_loose_commit).
    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> Async::Future<'_, Result<(), Self::Error>>;

    /// Load a fragment with its blob by fragment head [`CommitId`].
    ///
    /// Returns `None` if no fragment exists with the given identity.
    /// The returned `VerifiedMeta` is reconstructed from trusted storage
    /// without re-verifying the signature or blob.
    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>>;

    /// List all fragment head [`CommitId`] values for a sedimentree.
    ///
    /// This is derived from the stored payloads — each fragment is decoded
    /// and its `head()` extracted. Duplicate values are deduplicated.
    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Set<CommitId>, Self::Error>>;

    /// Load all fragments with their blobs for a sedimentree.
    ///
    /// Used for hydration at startup. All returned `VerifiedMeta` values are
    /// reconstructed from trusted storage without re-verification.
    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>>;

    /// Load all fragment *payloads* for a sedimentree, **without** their
    /// blobs — the fragment-side twin of
    /// [`load_loose_commit_metas`](Self::load_loose_commit_metas).
    ///
    /// Backends without a cheaper metadata-only read path can delegate to
    /// [`load_fragment_metas_via_full`].
    fn load_fragment_metas(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<Fragment>, Self::Error>>;

    /// Delete a fragment and its blob by fragment head [`CommitId`].
    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<(), Self::Error>>;

    /// Delete all fragments and their blobs for a sedimentree.
    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>>;

    // ==================== Batch Operations ====================

    /// Save a batch of commits and fragments with their blobs.
    ///
    /// Returns the total count of items saved (commits + fragments).
    ///
    /// # Contract
    ///
    /// Implementations **must** register `sedimentree_id` for the writes
    /// (the moral equivalent of [`save_sedimentree_id`](Self::save_sedimentree_id))
    /// before, as part of, or immediately after a successful persist of the
    /// commits or fragments — but **never on a failed write** (a failed
    /// batch must not leave a registered-but-empty tree behind). Callers
    /// rely on this so that
    /// [`load_all_sedimentree_ids`](Self::load_all_sedimentree_ids) sees
    /// sedimentrees created exclusively via batch insert. Both an empty
    /// `commits` and empty `fragments` is a degenerate input but the ID
    /// registration is still expected to happen.
    ///
    /// # Atomicity
    ///
    /// Atomicity across the commit and fragment writes is **not** guaranteed
    /// by this trait. Backends with native transactional support (e.g.
    /// `IndexedDB`) commit all-or-nothing; backends without it (e.g. plain
    /// filesystem) may persist some items before erroring. Callers that
    /// require all-or-nothing semantics across the batch must layer their
    /// own reconciliation on top — for example by treating any error as a
    /// signal to discard in-memory state and rehydrate from storage, which
    /// recovers the partial state without divergence.
    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> Async::Future<'_, Result<usize, Self::Error>>;
}

/// Fallback for [`Storage::load_loose_commit_metas`]: load the full commits
/// and drop their blobs.
///
/// Backends without a cheaper metadata-only read path (e.g. `JsStorage`, or
/// delegating wrappers) implement the trait method as a one-line call to
/// this. There is no I/O win — the blobs are still read — but it keeps the
/// fallback in one place and matches the optimized backends' set semantics.
///
/// # Errors
///
/// Propagates the backend's load error.
pub async fn load_loose_commit_metas_via_full<Async, S>(
    storage: &S,
    sedimentree_id: SedimentreeId,
) -> Result<Vec<LooseCommit>, S::Error>
where
    Async: FutureForm,
    S: Storage<Async>,
{
    Ok(storage
        .load_loose_commits(sedimentree_id)
        .await?
        .into_iter()
        .map(|vm| vm.into_full_parts().1)
        .collect())
}

/// Fallback for [`Storage::load_fragment_metas`] — the fragment-side twin of
/// [`load_loose_commit_metas_via_full`].
///
/// # Errors
///
/// Propagates the backend's load error.
pub async fn load_fragment_metas_via_full<Async, S>(
    storage: &S,
    sedimentree_id: SedimentreeId,
) -> Result<Vec<Fragment>, S::Error>
where
    Async: FutureForm,
    S: Storage<Async>,
{
    Ok(storage
        .load_fragments(sedimentree_id)
        .await?
        .into_iter()
        .map(|vm| vm.into_full_parts().1)
        .collect())
}
