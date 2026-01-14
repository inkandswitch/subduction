//! Messages handed back to the caller.

use sedimentree_core::{blob::Digest, depth::Depth};

/// A request for a fragment at a certain depth, starting from a given head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FragmentRequested {
    /// The head digest from which the fragment is requested.
    head: Digest,

    /// The depth of the requested fragment.
    depth: Depth,
}

impl FragmentRequested {
    /// Create a new fragment request from the given head and depth.
    ///
    /// # Parameters
    ///
    /// - `head`: The head digest from which the fragment is requested.
    /// - `depth`: The depth of the requested fragment.
    #[must_use]
    pub const fn new(head: Digest, depth: Depth) -> Self {
        Self { head, depth }
    }

    /// Get the head digest of the [`FragmentRequested`].
    #[must_use]
    pub const fn head(&self) -> &Digest {
        &self.head
    }

    /// Get the depth of the [`FragmentRequested`].
    #[must_use]
    pub const fn depth(&self) -> &Depth {
        &self.depth
    }
}
