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

    mod ordering {
        use super::*;

        #[test]
        fn test_ordering_by_head_first() {
            let head1 = Digest::from([0u8; 32]);
            let head2 = Digest::from([1u8; 32]);
            let req1 = FragmentRequested::new(head1, Depth(5));
            let req2 = FragmentRequested::new(head2, Depth(5));

            assert!(req1 < req2);
        }

        #[test]
        fn test_ordering_by_depth_when_head_equal() {
            let head = Digest::from([5u8; 32]);
            let req1 = FragmentRequested::new(head, Depth(1));
            let req2 = FragmentRequested::new(head, Depth(2));

            assert!(req1 < req2);
        }

        #[test]
        fn test_equality() {
            let head = Digest::from([6u8; 32]);
            let depth = Depth(3);
            let req1 = FragmentRequested::new(head, depth);
            let req2 = FragmentRequested::new(head, depth);

            assert_eq!(req1, req2);
        }

        #[test]
        fn test_inequality() {
            let req1 = FragmentRequested::new(Digest::from([7u8; 32]), Depth(1));
            let req2 = FragmentRequested::new(Digest::from([7u8; 32]), Depth(2));

            assert_ne!(req1, req2);
        }
    }

    mod copy_clone {
        use super::*;

        #[test]
        fn test_copy() {
            let req1 = FragmentRequested::new(Digest::from([8u8; 32]), Depth(7));
            let req2 = req1;

            assert_eq!(req1, req2);
        }

        #[test]
        fn test_clone() {
            let req1 = FragmentRequested::new(Digest::from([9u8; 32]), Depth(8));
            let req2 = req1.clone();

            assert_eq!(req1, req2);
        }
    }

    #[cfg(all(test, feature = "std", feature = "bolero"))]
    mod proptests {
        use super::*;

        #[test]
        fn prop_accessors_return_constructor_values() {
            bolero::check!()
                .with_arbitrary::<(Digest, Depth)>()
                .for_each(|(head, depth)| {
                    let req = FragmentRequested::new(*head, *depth);
                    assert_eq!(req.head(), head);
                    assert_eq!(req.depth(), depth);
                });
        }

        #[test]
        fn prop_equality_is_reflexive() {
            bolero::check!()
                .with_arbitrary::<FragmentRequested>()
                .for_each(|req| {
                    assert_eq!(req, req);
                });
        }

        #[test]
        fn prop_clone_equals_original() {
            bolero::check!()
                .with_arbitrary::<FragmentRequested>()
                .for_each(|req| {
                    assert_eq!(req.clone(), *req);
                });
        }

        #[test]
        fn prop_copy_equals_original() {
            bolero::check!()
                .with_arbitrary::<FragmentRequested>()
                .for_each(|req| {
                    let copied = *req;
                    assert_eq!(copied, *req);
                });
        }

        #[test]
        fn prop_ordering_is_transitive() {
            bolero::check!()
                .with_arbitrary::<(FragmentRequested, FragmentRequested, FragmentRequested)>()
                .for_each(|(r1, r2, r3)| {
                    if r1 < r2 && r2 < r3 {
                        assert!(r1 < r3);
                    }
                });
        }
    }
}
