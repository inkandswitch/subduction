//! Storage effect vocabulary: what the machine asks of the store.
//!
//! The machine holds sedimentree *metadata* in memory (hydrated by the
//! driver at startup) and makes all sync decisions against it. Storage
//! effects exist for the two things the machine cannot do itself:
//! durable writes and blob reads. Blob bytes only ever *transit* the
//! machine (wire message ↔ effect); they are never resident state.
//!
//! # The powerbox pattern (fused authorization)
//!
//! Legacy gated storage behind async [`StoragePolicy`] checks and wrapped
//! access in capability objects (`StoragePowerbox` → `Fetcher`/`Putter`).
//! Policies can do IO (e.g. keyhive lookups), so they cannot be pure
//! machine verdicts. Instead, every op carries its [`Provenance`] and the
//! driver enforces policy + signature verification + blob-digest checks +
//! persistence as **one unit**, answering with a single result:
//!
//! ```text
//! machine ─ Ingest { provenance, items… } ──▶ driver:
//!                                               1. authorize (policy)
//!                                               2. verify signatures
//!                                               3. check blob digests
//!                                               4. persist atomically-ish
//! machine ◀─ StorageDone { Ingested / Unauthorized / Failed } ──┘
//! ```
//!
//! One round-trip per ingest (ADR-006a), and [`StorageResult::Unauthorized`]
//! maps directly onto the wire's `SyncResult::Unauthorized`.
//!
//! # FFI note
//!
//! Ops carry concrete `Signed<…>`/`Blob` values (not raw bytes) for Rust
//! ergonomics; each is mechanically byte-encodable (`as_bytes`), so FFI
//! bindings serialize at the boundary without any generics.

use alloc::vec::Vec;

use sedimentree_core::{
    blob::Blob,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};
use subduction_crypto::signed::Signed;

use crate::peer_id::PeerId;

/// Where data (or a request for it) came from — determines which policy
/// check the driver applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Provenance {
    /// A local application operation (no peer policy applies).
    Local,

    /// Data or a request from an authenticated remote peer.
    Remote(PeerId),
}

/// A storage operation for the driver, always paired with a
/// [`StorageTicket`](crate::ticket::StorageTicket) on the emitting effect.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum StorageOp {
    /// Authorize, verify (signatures + blob digests), and persist a batch
    /// of commits and fragments.
    ///
    /// Item-level verification failures reject the *item* (reported in
    /// [`StorageResult::Ingested::rejected`]); a policy denial rejects the
    /// *whole op* ([`StorageResult::Unauthorized`]).
    Ingest {
        /// The sedimentree being written to.
        tree: SedimentreeId,
        /// Who supplied the data (policy input).
        provenance: Provenance,
        /// Signed commits with their blobs.
        commits: Vec<(Signed<LooseCommit>, Blob)>,
        /// Signed fragments with their blobs.
        fragments: Vec<(Signed<Fragment>, Blob)>,
    },

    /// Authorize and load specific items *with their blobs* (for building
    /// sync responses; the machine already knows the metadata).
    FetchItems {
        /// The sedimentree being read.
        tree: SedimentreeId,
        /// Who is asking (policy input).
        provenance: Provenance,
        /// Commits to load, by causal identity.
        commit_ids: Vec<CommitId>,
        /// Fragments to load, by head identity.
        fragment_heads: Vec<CommitId>,
    },

    /// Delete a sedimentree and all its data.
    DeleteTree {
        /// The sedimentree to remove.
        tree: SedimentreeId,
        /// Who is asking (policy input).
        provenance: Provenance,
    },

    /// Persist already-verified items whose blobs live in the driver's
    /// buffer table (Design D: verification happened inside the
    /// connection machine — the driver's duty here is authorize +
    /// persist ONLY; ADR-015 shrinks the driver's security surface to
    /// custody and durability).
    PersistItems {
        /// The tree being written to.
        tree: SedimentreeId,
        /// The authenticated peer the data came from (policy input).
        provenance: Provenance,
        /// Verified signed commits with blob refs.
        commits: Vec<(Signed<LooseCommit>, crate::blob_ref::BlobRef)>,
        /// Verified signed fragments with blob refs.
        fragments: Vec<(Signed<Fragment>, crate::blob_ref::BlobRef)>,
    },

    /// Authorize and load specific items, returning blobs as refs into
    /// the driver's buffer table (the storage executor retains what it
    /// reads and mints refs — the ref-world twin of
    /// [`FetchItems`](StorageOp::FetchItems)).
    FetchItemRefs {
        /// The sedimentree being read.
        tree: SedimentreeId,
        /// Who is asking (policy input).
        provenance: Provenance,
        /// Commits to load, by causal identity.
        commit_ids: Vec<CommitId>,
        /// Fragments to load, by head identity.
        fragment_heads: Vec<CommitId>,
    },

    /// Seal and persist locally-authored commits in one round trip: for
    /// each item the driver hashes the blob, builds the [`LooseCommit`],
    /// signs it with the machine's identity key (which the driver holds),
    /// and persists commit + blob. Answers with
    /// [`StorageResult::LocallyIngested`] carrying the sealed commits so
    /// the machine can update its resident tree — resident state never
    /// gets ahead of durability.
    IngestLocal {
        /// The tree to append to.
        tree: SedimentreeId,
        /// New commits as raw parts.
        commits: Vec<crate::command::NewCommit>,
        /// New fragments as raw parts.
        fragments: Vec<crate::command::NewFragment>,
    },
}

/// Why one item within an [`Ingest`](StorageOp::Ingest) was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
pub enum IngestRejection {
    /// The signature did not verify against the claimed issuer.
    BadSignature,

    /// The blob bytes did not match the signed metadata's digest/size.
    BlobMismatch,

    /// The item's author is not allowed to write to this tree (per-author
    /// policy, distinct from the whole-op requestor check).
    AuthorDenied,
}

/// Which kind of item a rejection refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
pub enum ItemKind {
    /// A loose commit.
    Commit,

    /// A fragment.
    Fragment,
}

/// The result of a [`StorageOp`], echoed back via
/// [`Event::StorageDone`](crate::event::Event::StorageDone).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum StorageResult {
    /// An [`Ingest`](StorageOp::Ingest) finished. `stored` counts durably
    /// persisted items; `rejected` lists per-item verification failures
    /// (indexes into the op's `commits`/`fragments` vectors).
    Ingested {
        /// Items durably persisted (commits + fragments).
        stored: u32,
        /// Items dropped, with reasons (tier-3 telemetry feeds on this).
        rejected: Vec<(ItemKind, u32, IngestRejection)>,
    },

    /// A [`FetchItems`](StorageOp::FetchItems) finished. Missing items are
    /// simply absent (the store may have pruned them).
    Fetched {
        /// Requested commits that were found, with blobs.
        commits: Vec<(Signed<LooseCommit>, Blob)>,
        /// Requested fragments that were found, with blobs.
        fragments: Vec<(Signed<Fragment>, Blob)>,
    },

    /// A [`DeleteTree`](StorageOp::DeleteTree) finished.
    TreeDeleted,

    /// A [`PersistItems`](StorageOp::PersistItems) finished. No
    /// rejection list: items were verified before the op was issued;
    /// only policy (whole-op `Unauthorized`) or backend failure apply.
    Persisted {
        /// Items durably persisted.
        stored: u32,
    },

    /// A [`FetchItemRefs`](StorageOp::FetchItemRefs) finished. Missing
    /// items are simply absent.
    FetchedRefs {
        /// Requested commits that were found, blobs as refs.
        commits: Vec<(Signed<LooseCommit>, crate::blob_ref::BlobRef)>,
        /// Requested fragments that were found, blobs as refs.
        fragments: Vec<(Signed<Fragment>, crate::blob_ref::BlobRef)>,
    },

    /// An [`IngestLocal`](StorageOp::IngestLocal) finished: the sealed,
    /// durably-persisted commits, in request order.
    LocallyIngested {
        /// The signed commits (blobs stayed in storage).
        commits: Vec<Signed<LooseCommit>>,
        /// The signed fragments (blobs stayed in storage).
        fragments: Vec<Signed<Fragment>>,
    },

    /// The whole op was denied by policy (requestor-level).
    Unauthorized,

    /// The sedimentree does not exist in storage.
    UnknownTree,

    /// The backend failed (IO error, corruption, …). The machine surfaces
    /// this to the application; retry policy is a driver/app concern.
    Failed(StorageFailure),
}

/// A backend failure, kept coarse: the machine cannot meaningfully
/// distinguish backend error causes — but the driver's tier-1 telemetry
/// can, before it ever reaches the machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
pub enum StorageFailure {
    /// The op may succeed if retried (transient IO, lock contention).
    Retryable,

    /// The op will not succeed (corruption, permanent backend error).
    Permanent,
}
