//! Messages handed back to the caller.

use sedimentree_core::{blob::Digest, depth::Depth};

/// A request for a fragment at a certain depth, starting from a given head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

#[cfg(test)]
mod tests {
    use super::*;

    mod constructor {
        use super::*;

        #[test]
        fn test_new() {
            let head = Digest::from([1u8; 32]);
            let depth = Depth(5);
            let req = FragmentRequested::new(head, depth);

            assert_eq!(req.head(), &head);
            assert_eq!(req.depth(), &depth);
        }

        #[test]
        fn test_new_zero_depth() {
            let head = Digest::from([2u8; 32]);
            let depth = Depth(0);
            let req = FragmentRequested::new(head, depth);

            assert_eq!(req.head(), &head);
            assert_eq!(req.depth(), &depth);
        }
    }

    mod accessors {
        use super::*;

        #[test]
        fn test_head_accessor() {
            let head = Digest::from([3u8; 32]);
            let req = FragmentRequested::new(head, Depth(1));

            assert_eq!(req.head(), &head);
        }

        #[test]
        fn test_depth_accessor() {
            let depth = Depth(10);
            let req = FragmentRequested::new(Digest::from([4u8; 32]), depth);

            assert_eq!(req.depth(), &depth);
        }
    }
}
