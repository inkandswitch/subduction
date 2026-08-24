//! Storage effect vocabulary: what the machine asks of the store.
//!
//! The machine holds sedimentree _metadata_ in memory (hydrated by the
//! driver at startup) and makes all sync decisions against it. Storage
//! effects exist for the two things the machine cannot do itself:
//! durable writes and blob reads. Blob bytes only ever _transit_ the
//! machine (wire message ↔ effect); they are never resident state.
//!
//! # Fused authorization
//!
//! Storage policies may perform IO (e.g. keyhive lookups), so they cannot
//! be pure machine verdicts. Every op therefore carries its
//! [`Provenance`], and the driver enforces policy and persistence as one
//! unit, answering with a single result. Signature and blob-digest
//! verification is not a driver duty for remote data: it happens inline
//! in the connection machine before items reach the core, leaving the
//! driver responsible for custody and durability only:
//!
//! ```text
//! machine ─ PersistItems { provenance, verified items… } ──▶ driver:
//!                                               1. authorize (policy)
//!                                               2. persist atomically-ish
//! machine ◀─ StorageDone { Persisted / Unauthorized / Failed } ──┘
//! ```
//!
//! The exception is [`IngestLocal`](StorageOp::IngestLocal): for
//! locally-authored writes the driver does hash blobs and sign commits
//! (it holds the identity key), fused with the persist in one round trip.
//! [`StorageResult::Unauthorized`] maps directly onto the wire's
//! `SyncResult::Unauthorized`.
//!
//! # FFI note
//!
//! Ops carry concrete `Signed<…>`/`Blob` values (not raw bytes) for Rust
//! ergonomics; each is mechanically byte-encodable (`as_bytes`), so FFI
//! bindings serialize at the boundary without any generics.

use alloc::vec::Vec;

use sedimentree_core::{
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_crypto::signed::Signed;

use crate::peer_id::PeerId;

/// A storage operation for the driver, always paired with a
/// [`StorageTicket`](crate::ticket::StorageTicket) on the emitting effect.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum StorageOp {
    /// Delete a sedimentree and all its data.
    DeleteTree {
        /// The sedimentree to remove.
        tree: SedimentreeId,
        /// Who is asking (policy input).
        provenance: Provenance,
    },

    /// Persist already-verified items whose blobs live in the driver's
    /// buffer table. Verification happened inside the connection
    /// machine; the driver's duty here is authorize + persist only.
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
    /// reads and mints refs, so fetched blobs fan out by reference like
    /// any other frame data).
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

/// The result of a [`StorageOp`], echoed back via
/// [`NodeEvent::StorageDone`](crate::node::NodeEvent::StorageDone).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum StorageResult {
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
    /// items are absent from the result.
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
