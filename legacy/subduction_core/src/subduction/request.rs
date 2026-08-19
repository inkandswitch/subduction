//! Messages handed back to the caller.

use sedimentree_core::{depth::Depth, loose_commit::id::CommitId};

/// A request for a fragment at a certain depth, starting from a given head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct FragmentRequested {
    /// The head commit identifier from which the fragment is requested.
    head: CommitId,

    /// The depth of the requested fragment.
    depth: Depth,
}

impl FragmentRequested {
    /// Create a new fragment request from the given head and depth.
    ///
    /// # Parameters
    ///
    /// - `head`: The head commit identifier from which the fragment is requested.
    /// - `depth`: The depth of the requested fragment.
    #[must_use]
    pub const fn new(head: CommitId, depth: Depth) -> Self {
        Self { head, depth }
    }

    /// Get the head commit identifier of the [`FragmentRequested`].
    #[must_use]
    pub const fn head(&self) -> CommitId {
        self.head
    }

    /// Get the depth of the [`FragmentRequested`].
    #[must_use]
    pub const fn depth(&self) -> &Depth {
        &self.depth
    }
}
