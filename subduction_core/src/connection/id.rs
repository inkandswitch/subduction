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

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;
    use sedimentree_core::collections::Map;

    mod display {
        use super::*;

        #[test]
        fn test_display_zero() {
            let id = ConnectionId::new(0);
            assert_eq!(format!("{id}"), "conn-0");
        }

        #[test]
        fn test_display_small_numbers() {
            let id = ConnectionId::new(42);
            assert_eq!(format!("{id}"), "conn-42");
        }

        #[test]
        fn test_display_large_numbers() {
            let id = ConnectionId::new(usize::MAX);
            let expected = format!("conn-{}", usize::MAX);
            assert_eq!(format!("{id}"), expected);
        }

        #[test]
        fn test_debug_shows_inner() {
            let id = ConnectionId::new(123);
            let debug = format!("{id:?}");
            assert!(debug.contains("123"));
        }
    }

    mod conversions {
        use super::*;

        #[test]
        fn test_from_usize() {
            let id = ConnectionId::from(42usize);
            assert_eq!(id.as_usize(), 42);
        }

        #[test]
        fn test_into_usize() {
            let id = ConnectionId::new(99);
            let value: usize = id.into();
            assert_eq!(value, 99);
        }

        #[test]
        fn test_roundtrip() {
            let original = 12345usize;
            let id = ConnectionId::from(original);
            let back: usize = id.into();
            assert_eq!(original, back);
        }
    }

    mod ordering {
        use super::*;

        #[test]
        fn test_ordering() {
            let id1 = ConnectionId::new(1);
            let id2 = ConnectionId::new(2);

            assert!(id1 < id2);
            assert!(id2 > id1);
        }

        #[test]
        fn test_equality() {
            let id1 = ConnectionId::new(42);
            let id2 = ConnectionId::new(42);
            let id3 = ConnectionId::new(43);

            assert_eq!(id1, id2);
            assert_ne!(id1, id3);
        }
    }

    mod hashing {
        use super::*;

        #[test]
        fn test_as_map_key() {
            let id1 = ConnectionId::new(1);
            let id2 = ConnectionId::new(1);

            let mut map = Map::new();
            map.insert(id1, "value");

            assert_eq!(map.get(&id2), Some(&"value"));
        }
    }

    mod default {
        use super::*;

        #[test]
        fn test_default_is_zero() {
            let id = ConnectionId::default();
            assert_eq!(id.as_usize(), 0);
        }
    }

    #[cfg(all(test, feature = "std", feature = "bolero"))]
    mod proptests {
        use super::*;

        #[test]
        fn prop_display_starts_with_conn() {
            bolero::check!().with_type::<ConnectionId>().for_each(|id| {
                let display = format!("{id}");
                assert!(display.starts_with("conn-"));
            });
        }

        #[test]
        fn prop_roundtrip_usize() {
            bolero::check!().with_type::<usize>().for_each(|value| {
                let id = ConnectionId::from(*value);
                let back: usize = id.into();
                assert_eq!(value, &back);
            });
        }

        #[test]
        fn prop_ordering_matches_usize() {
            bolero::check!()
                .with_type::<(ConnectionId, ConnectionId)>()
                .for_each(|(id1, id2)| {
                    assert_eq!(id1.cmp(id2), id1.as_usize().cmp(&id2.as_usize()));
                });
        }

        #[test]
        fn prop_equality_is_reflexive() {
            bolero::check!().with_type::<ConnectionId>().for_each(|id| {
                assert_eq!(id, id);
            });
        }
    }
}
