//! Types for identifying connections.

/// A unique identifier for a particular connection.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    minicbor::Encode,
    minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cbor(transparent)]
pub struct ConnectionId(#[n(0)] usize);

impl ConnectionId {
    /// Create a new [`ConnectionId`] from a `usize`.
    #[must_use]
    pub const fn new(id: usize) -> Self {
        ConnectionId(id)
    }

    /// Get the inner `usize` representation of the [`ConnectionId`].
    #[must_use]
    pub const fn as_usize(&self) -> usize {
        self.0
    }
}

impl From<usize> for ConnectionId {
    fn from(value: usize) -> Self {
        ConnectionId(value)
    }
}

impl From<ConnectionId> for usize {
    fn from(value: ConnectionId) -> Self {
        value.0
    }
}

impl core::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "conn-{}", self.as_usize())
    }
}

#[cfg(all(test, feature = "std", feature = "bolero"))]
mod tests {
    use super::*;

    #[test]
    fn prop_roundtrip_usize() {
        bolero::check!().with_type::<usize>().for_each(|value| {
            let id = ConnectionId::from(*value);
            let back: usize = id.into();
            assert_eq!(value, &back);
        });
    }
}
