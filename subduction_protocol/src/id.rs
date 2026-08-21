//! Identifiers used across the machine boundary.
//!
//! All ids are plain integers so they cross FFI unchanged. [`ConnId`] is
//! allocated by the _driver_ (it owns the sockets); [`Generation`] and
//! [`Seq`] are allocated by the _machine_ (they witness machine state).

/// A driver-allocated identifier for one transport connection.
///
/// The driver must never reuse a `ConnId` within the lifetime of a machine,
/// even after the connection closes. (A `u64` counter cannot realistically
/// wrap.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ConnId(u64);

impl ConnId {
    /// Create a connection id from a raw driver-allocated value.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The raw id value.
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl core::fmt::Display for ConnId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "conn-{}", self.0)
    }
}

/// A machine-allocated teardown counter for an entity (connection, session).
///
/// Bumped whenever the entity is torn down or restarted. Completions carry
/// the generation they were issued under; a completion whose generation is
/// not current is _stale_ and is dropped — this is how the machine stays
/// safe under interleaved completions without locks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Generation(u64);

impl Generation {
    /// The first generation of a fresh entity.
    pub const FIRST: Self = Self(0);

    /// The generation after this one (saturating; `u64` cannot
    /// realistically be exhausted by teardowns).
    #[must_use]
    pub const fn next(&self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// The raw generation counter.
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

/// A machine-allocated sequence number distinguishing in-flight operations
/// issued under the same generation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Seq(u64);

impl Seq {
    /// The first sequence number.
    pub const FIRST: Self = Self(0);

    /// The next sequence number (saturating).
    #[must_use]
    pub const fn next(&self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// The raw sequence value.
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

#[cfg(all(test, feature = "std", feature = "bolero"))]
mod proptests {
    use super::*;

    #[test]
    fn prop_generation_next_is_strictly_greater_below_max() {
        bolero::check!().with_type::<Generation>().for_each(|g| {
            if g.as_u64() < u64::MAX {
                assert!(g.next() > *g);
            } else {
                assert_eq!(g.next(), *g);
            }
        });
    }
}
