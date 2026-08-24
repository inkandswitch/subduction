//! Local commands: what the application asks the machine to do.
//!
//! Commands arrive as [`NodeEvent::Command`](crate::node::NodeEvent::Command)
//! — same funnel as everything else, so ordering with network events is
//! total and there is no separate API surface to synchronize.
//!
//! # Local writes are fused driver ops
//!
//! [`AddCommits`](Command::AddCommits) hands _raw parts_ (head, parents,
//! blob) to the machine, which forwards them as one
//! [`IngestLocal`](crate::storage::StorageOp::IngestLocal) storage op. The
//! driver — which holds the signing key — hashes each blob, builds and
//! signs the [`LooseCommit`]s, and persists, all in a single round trip
//!. The machine updates its resident tree only from the
//! completion, so resident state never gets ahead of durability.
//!
//! [`LooseCommit`]: sedimentree_core::loose_commit::LooseCommit

use alloc::{collections::BTreeSet, vec::Vec};

use sedimentree_core::{
    blob::Blob,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};

use crate::id::ConnId;

/// An application request to the machine.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Command {
    /// Install a sedimentree's _metadata_ loaded from storage by the
    /// driver at startup. Merging is idempotent and monotone; no effects
    /// are produced (the data came _from_ storage).
    HydrateTree {
        /// The tree being hydrated.
        tree: SedimentreeId,
        /// Commit metadata (no blobs — they stay in storage).
        commits: Vec<LooseCommit>,
        /// Fragment metadata (no blobs).
        fragments: Vec<Fragment>,
    },

    /// Author new commits locally. Emits one fused
    /// [`IngestLocal`](crate::storage::StorageOp::IngestLocal) op; the
    /// resident tree updates when the completion confirms durability, and
    /// [`CommitsStored`](crate::effect::AppEvent::CommitsStored) reports
    /// back to the application.
    AddCommits {
        /// The tree to append to (created if absent).
        tree: SedimentreeId,
        /// The commits, as raw parts.
        commits: Vec<NewCommit>,
    },

    /// Author new fragments locally — the fragment-side twin of
    /// [`AddCommits`](Command::AddCommits).
    AddFragments {
        /// The tree to append to (created if absent).
        tree: SedimentreeId,
        /// The fragments, as raw parts.
        fragments: Vec<NewFragment>,
    },

    /// Stop receiving pushes for these trees from the peer on `conn`
    /// (sends a `RemoveSubscriptions` message; the peer prunes us from
    /// its subscriber sets).
    Unsubscribe {
        /// The connection to unsubscribe on (must be authenticated).
        conn: ConnId,
        /// The trees to unsubscribe from.
        trees: Vec<SedimentreeId>,
    },

    /// Remove a sedimentree locally: resident state immediately, storage
    /// via [`DeleteTree`](crate::storage::StorageOp::DeleteTree), and
    /// [`TreeRemoved`](crate::effect::AppEvent::TreeRemoved) on completion.
    RemoveTree {
        /// The tree to remove.
        tree: SedimentreeId,
    },

    /// Start a batch sync of one tree over an authenticated connection.
    /// Concludes with
    /// [`SyncFinished`](crate::effect::AppEvent::SyncFinished); incoming
    /// data additionally surfaces as
    /// [`TreeUpdated`](crate::effect::AppEvent::TreeUpdated).
    SyncTree {
        /// The connection to sync over (must be authenticated).
        conn: ConnId,
        /// The tree to sync. A tree we do not hold syncs as empty (the
        /// peer sends us everything).
        tree: SedimentreeId,
        /// Also subscribe to the peer's future updates for this tree.
        subscribe: bool,
    },

    /// Send an extension-protocol message on an authenticated connection
    ///. Ignored (with a reason) if the
    /// connection is not authenticated.
    SendExtension {
        /// The connection to send on.
        conn: ConnId,
        /// One complete extension message, schema prefix included.
        bytes: Vec<u8>,
    },
}

/// A new, locally-authored commit as raw parts. The driver seals it with
/// the machine's identity key.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct NewCommit {
    /// The commit's causal identity.
    pub head: CommitId,

    /// Parent commit ids.
    pub parents: BTreeSet<CommitId>,

    /// The commit's payload bytes.
    pub blob: Blob,
}

/// A new, locally-authored fragment as raw parts. The driver seals it
/// with the machine's identity key.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct NewFragment {
    /// The fragment's head commit.
    pub head: CommitId,

    /// Boundary commit ids.
    pub boundary: BTreeSet<CommitId>,

    /// Checkpoint commit ids.
    pub checkpoints: Vec<CommitId>,

    /// The fragment's payload bytes.
    pub blob: Blob,
}
