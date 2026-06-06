//! A single fragment item for the batch-store / batch-add APIs.

use alloc::{collections::BTreeSet, vec::Vec};

use sedimentree_core::{blob::Blob, loose_commit::id::CommitId};

/// A single fragment for [`Subduction::store_fragments_batch`].
///
/// [`Subduction::store_fragments_batch`]: super::Subduction::store_fragments_batch
#[derive(Debug, Clone)]
pub struct FragmentBatchItem {
    /// The head commit of the fragment.
    pub head: CommitId,
    /// The boundary commits (fragment edges).
    pub boundary: BTreeSet<CommitId>,
    /// Checkpoint digests within the fragment.
    pub checkpoints: Vec<CommitId>,
    /// The blob containing the fragment's data.
    pub blob: Blob,
}
