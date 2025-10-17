//! Messages handed back to the caller.

use sedimentree_core::{Depth, Digest};

/// A request for a fragment at a certain depth, starting from a given head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FragmentRequested {
    /// The head digest from which the fragment is requested.
    pub head: Digest,

    /// The depth of the requested fragment.
    pub depth: Depth,
}
