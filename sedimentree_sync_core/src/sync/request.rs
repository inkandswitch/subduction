//! Messages handed back to the caller.

use sedimentree_core::{Depth, Digest};

/// A request for a chunk at a certain depth, starting from a given head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkRequested {
    /// The head digest from which the chunk is requested.
    pub head: Digest,

    /// The depth of the requested chunk.
    pub depth: Depth,
}
