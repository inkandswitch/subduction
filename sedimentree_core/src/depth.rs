//! Stratum depth.

use alloc::boxed::Box;

use crate::loose_commit::id::CommitId;

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

impl core::fmt::Display for Depth {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Depth({})", self.0)
    }
}

/// A strategy for determining the depth of a commit based on its identifier.
pub trait DepthMetric {
    /// Calculates the depth of a commit identifier using this strategy.
    fn to_depth(&self, id: CommitId) -> Depth;
}

impl<Idish: From<CommitId>, Depthish: Into<Depth>> DepthMetric for fn(Idish) -> Depthish {
    fn to_depth(&self, id: CommitId) -> Depth {
        self(Idish::from(id)).into()
    }
}

impl<T: DepthMetric> DepthMetric for Box<T> {
    fn to_depth(&self, id: CommitId) -> Depth {
        T::to_depth(self, id)
    }
}

/// A depth strategy that counts leading zero bytes in the commit identifier.
///
/// For example, the identifier `0x00012345...` has a depth of 3,
/// the identifier `0x00abcdef...` has a depth of 2,
/// and the identifier `0x12345678...` has a depth of 0.
#[derive(Debug, Clone, Copy)]
pub struct CountLeadingZeroBytes;

impl DepthMetric for CountLeadingZeroBytes {
    fn to_depth(&self, id: CommitId) -> Depth {
        let mut acc = 0;
        for &byte in id.as_bytes() {
            if byte == 0 {
                acc += 1;
            } else {
                break;
            }
        }
        Depth(acc)
    }
}

/// A depth strategy that counts trailing zeros in the commit identifier in a given base.
#[derive(Debug, Clone, Copy)]
pub struct CountTrailingZerosInBase(NonZero<u8>);

impl CountTrailingZerosInBase {
    /// Creates a new `CountTrailingZerosInBase` strategy for the given base.
    ///
    /// # Panics
    ///
    /// Panics if `base` is less than 2.
    #[must_use]
    pub const fn new(base: NonZero<u8>) -> Self {
        Self(base)
    }
}

impl From<NonZero<u8>> for CountTrailingZerosInBase {
    fn from(base: NonZero<u8>) -> Self {
        Self::new(base)
    }
}

impl From<CountTrailingZerosInBase> for NonZero<u8> {
    fn from(strategy: CountTrailingZerosInBase) -> Self {
        strategy.0
    }
}

impl From<CountTrailingZerosInBase> for u8 {
    fn from(strategy: CountTrailingZerosInBase) -> Self {
        strategy.0.into()
    }
}

use core::num::NonZero;

impl DepthMetric for CountTrailingZerosInBase {
    fn to_depth(&self, id: CommitId) -> Depth {
        let arr = id.as_bytes();
        let inner_depth: u8 = self.0.into();
        let (_, bytes) = num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, arr)
            .to_radix_be(inner_depth.into());

        #[allow(clippy::expect_used)]
        let int = u32::try_from(bytes.into_iter().rev().take_while(|&i| i == 0).count())
            .expect("u32 should be big enough, but isn't");

        Depth(int)
    }
}
