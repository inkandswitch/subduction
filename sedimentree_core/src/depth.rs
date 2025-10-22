//! Stratum depth.

use crate::blob::Digest;

/// The maximum depth of strata that a [`Sedimentree`] can go to.
pub const MAX_STRATA_DEPTH: Depth = Depth(2);

/// How deep in the Sedimentree a stratum is.
///
/// The greater the depth, the more leading zeros, the (probabilistically) larger,
/// and thus "lower" the stratum. They become larger due to the fragmenting strategy.
/// This means that the same data can appear in multiple strata, but may be fragmented
/// into smaller or larger sections based on a hash hardness metric.
///
/// The depth is determined by the number of leading zeros in each hash in base 10.
/// If there's zero-or-more leading zeros, it may only live in the topmost (0th) layer.
/// If there is one leading zero (or more), it can only live in the 0th or 1st layer.
/// If there are two leading zeros (or more), it can only live in the 0th, 1st, or 2nd layer
/// (and so on).
///
/// ```diagram
///         ┌───┐ ┌───┐ ┌───┐ ┌─────────┐ ┌───┐ ┌───┐
/// Depth 0 │ 1 │ │ 1 │ │ 1 │ │    2    │ │ 1 │ │ 1 │
///         └───┘ └───┘ └───┘ └─────────┘ └───┘ └───┘
///         ┌───────────────┐ ┌─────────────────────┐
/// Depth 1 │   3 commits   │ │      4 commits      │
///         └───────────────┘ └─────────────────────┘
///         ┌───────────────────────────────────────┐
/// Depth 2 │               7 commits               │
///         └───────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Depth(pub u32);

impl std::fmt::Display for Depth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Depth({})", self.0)
    }
}

/// A strategy for determining the depth of a commit based on its digest.
pub trait DepthMetric {
    /// Calculates the depth of a digest using this strategy.
    fn to_depth(&self, digest: Digest) -> Depth;
}

impl<Digestish: From<Digest>, Depthish: Into<Depth>> DepthMetric for fn(Digestish) -> Depthish {
    fn to_depth(&self, digest: Digest) -> Depth {
        self(Digestish::from(digest)).into()
    }
}

impl<T: DepthMetric> DepthMetric for Box<T> {
    fn to_depth(&self, digest: Digest) -> Depth {
        T::to_depth(self, digest)
    }
}
