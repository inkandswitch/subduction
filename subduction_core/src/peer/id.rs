//! A simple wrapper around a String to represent a Peer ID.

use alloc::string::String;
use core::fmt::Write;

/// A Peer ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PeerId([u8; 32]);

impl PeerId {
    /// Create a new [`PeerId`].
    #[must_use]
    pub const fn new(id: [u8; 32]) -> Self {
        Self(id)
    }

    /// Get the byte array representation of the [`PeerId`].
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Get the slice representation of the [`PeerId`].
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl core::fmt::Display for PeerId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        to_hex(self.as_slice()).fmt(f)
    }
}

impl core::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(self, f)
    }
}
fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        #[allow(clippy::expect_used)]
        write!(&mut s, "{b:02x}").expect("preallocated length should be sufficient");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    mod hex_formatting {
        use super::*;

        #[test]
        fn test_hex_formatting_all_zeros() {
            let bytes = [0u8; 32];
            let peer_id = PeerId::new(bytes);
            let hex = format!("{}", peer_id);

            assert_eq!(hex.len(), 64);
            assert_eq!(hex, "0".repeat(64));
        }

        #[test]
        fn test_hex_formatting_all_ones() {
            let bytes = [0xffu8; 32];
            let peer_id = PeerId::new(bytes);
            let hex = format!("{}", peer_id);

            assert_eq!(hex.len(), 64);
            assert_eq!(hex, "f".repeat(64));
        }

        #[test]
        fn test_hex_formatting_known_values() {
            let mut bytes = [0u8; 32];
            bytes[0] = 0xab;
            bytes[1] = 0xcd;
            bytes[2] = 0xef;
            bytes[31] = 0x12;

            let peer_id = PeerId::new(bytes);
            let hex = format!("{}", peer_id);

            assert!(hex.starts_with("abcdef"));
            assert!(hex.ends_with("12"));
            assert_eq!(hex.len(), 64);
        }

        #[test]
        fn test_hex_lowercase() {
            let mut bytes = [0u8; 32];
            bytes[0] = 0xAB; // uppercase in source, should be lowercase in output

            let peer_id = PeerId::new(bytes);
            let hex = format!("{}", peer_id);

            assert!(hex.starts_with("ab")); // lowercase
            assert!(hex
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        }

        #[test]
        fn test_debug_equals_display() {
            let bytes = [0x42u8; 32];
            let peer_id = PeerId::new(bytes);

            let display = format!("{}", peer_id);
            let debug = format!("{:?}", peer_id);

            assert_eq!(display, debug);
        }
    }

    mod ordering {
        use super::*;

        #[test]
        fn test_ordering_lexicographic() {
            let bytes1 = [0u8; 32];
            let mut bytes2 = [0u8; 32];
            bytes2[0] = 1;

            let peer_id1 = PeerId::new(bytes1);
            let peer_id2 = PeerId::new(bytes2);

            assert!(peer_id1 < peer_id2);
            assert!(peer_id2 > peer_id1);
        }

        #[test]
        fn test_ordering_later_byte() {
            let bytes1 = [0u8; 32];
            let mut bytes2 = [0u8; 32];
            bytes2[31] = 1; // Only last byte differs

            let peer_id1 = PeerId::new(bytes1);
            let peer_id2 = PeerId::new(bytes2);

            assert!(peer_id1 < peer_id2);
        }

        #[test]
        fn test_equality_reflexive() {
            let bytes = [42u8; 32];
            let peer_id = PeerId::new(bytes);

            assert_eq!(peer_id, peer_id);
        }

        #[test]
        fn test_equality_symmetric() {
            let bytes = [42u8; 32];
            let peer_id1 = PeerId::new(bytes);
            let peer_id2 = PeerId::new(bytes);

            assert_eq!(peer_id1, peer_id2);
            assert_eq!(peer_id2, peer_id1); // symmetric
        }
    }

    #[cfg(all(test, feature = "std", feature = "arbitrary"))]
    mod proptests {
        use super::*;

        #[test]
        fn prop_hex_format_always_64_chars() {
            #[derive(Debug)]
            struct Scenario {
                peer_id: PeerId,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    Ok(Self {
                        peer_id: PeerId::arbitrary(u)?,
                    })
                }
            }
            bolero::check!()
                .with_arbitrary::<Scenario>()
                .for_each(|Scenario { peer_id }| {
                    let hex = format!("{}", peer_id);
                    assert_eq!(hex.len(), 64);
                });
        }

        #[test]
        fn prop_hex_format_is_valid_lowercase_hex() {
            #[derive(Debug)]
            struct Scenario {
                peer_id: PeerId,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    Ok(Self {
                        peer_id: PeerId::arbitrary(u)?,
                    })
                }
            }
            bolero::check!()
                .with_arbitrary::<Scenario>()
                .for_each(|Scenario { peer_id }| {
                    let hex = format!("{}", peer_id);
                    assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
                    assert!(hex.chars().all(|c| !c.is_ascii_uppercase()));
                });
        }

        #[test]
        fn prop_equality_is_reflexive() {
            #[derive(Debug)]
            struct Scenario {
                peer_id: PeerId,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    Ok(Self {
                        peer_id: PeerId::arbitrary(u)?,
                    })
                }
            }
            bolero::check!()
                .with_arbitrary::<Scenario>()
                .for_each(|Scenario { peer_id }| {
                    assert_eq!(peer_id, peer_id);
                });
        }

        #[test]
        fn prop_equality_is_symmetric() {
            #[derive(Debug)]
            struct Scenario {
                peer_id: PeerId,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    Ok(Self {
                        peer_id: PeerId::arbitrary(u)?,
                    })
                }
            }
            bolero::check!()
                .with_arbitrary::<Scenario>()
                .for_each(|Scenario { peer_id }| {
                    // peer_id is &PeerId here
                    assert_eq!(peer_id, peer_id);
                });
        }

        #[test]
        fn prop_ordering_is_transitive() {
            #[derive(Debug)]
            struct Scenario {
                p1: PeerId,
                p2: PeerId,
                p3: PeerId,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    Ok(Self {
                        p1: PeerId::arbitrary(u)?,
                        p2: PeerId::arbitrary(u)?,
                        p3: PeerId::arbitrary(u)?,
                    })
                }
            }
            bolero::check!()
                .with_arbitrary::<Scenario>()
                .for_each(|Scenario { p1, p2, p3 }| {
                    if p1 < p2 && p2 < p3 {
                        assert!(p1 < p3);
                    }
                });
        }

        #[test]
        fn prop_ordering_matches_byte_ordering() {
            #[derive(Debug)]
            struct Scenario {
                p1: PeerId,
                p2: PeerId,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    Ok(Self {
                        p1: PeerId::arbitrary(u)?,
                        p2: PeerId::arbitrary(u)?,
                    })
                }
            }
            bolero::check!()
                .with_arbitrary::<Scenario>()
                .for_each(|Scenario { p1, p2 }| {
                    assert_eq!(p1.cmp(p2), p1.as_bytes().cmp(p2.as_bytes()));
                });
        }

        #[test]
        fn prop_debug_and_display_are_equal() {
            #[derive(Debug)]
            struct Scenario {
                peer_id: PeerId,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    Ok(Self {
                        peer_id: PeerId::arbitrary(u)?,
                    })
                }
            }
            bolero::check!()
                .with_arbitrary::<Scenario>()
                .for_each(|Scenario { peer_id }| {
                    let display = format!("{}", peer_id);
                    let debug = format!("{:?}", peer_id);
                    assert_eq!(display, debug);
                });
        }

        #[test]
        fn prop_as_bytes_roundtrip() {
            #[derive(Debug)]
            struct Scenario {
                peer_id: PeerId,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    Ok(Self {
                        peer_id: PeerId::arbitrary(u)?,
                    })
                }
            }
            bolero::check!()
                .with_arbitrary::<Scenario>()
                .for_each(|Scenario { peer_id }| {
                    let bytes = peer_id.as_bytes();
                    let reconstructed = PeerId::new(*bytes);
                    assert_eq!(peer_id, &reconstructed);
                });
        }

        #[test]
        fn prop_as_slice_matches_as_bytes() {
            #[derive(Debug)]
            struct Scenario {
                peer_id: PeerId,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    Ok(Self {
                        peer_id: PeerId::arbitrary(u)?,
                    })
                }
            }
            bolero::check!()
                .with_arbitrary::<Scenario>()
                .for_each(|Scenario { peer_id }| {
                    assert_eq!(peer_id.as_slice(), peer_id.as_bytes() as &[u8]);
                });
        }

        #[test]
        fn prop_equal_ids_are_not_less_or_greater() {
            #[derive(Debug)]
            struct Scenario {
                peer_id: PeerId,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    Ok(Self {
                        peer_id: PeerId::arbitrary(u)?,
                    })
                }
            }
            bolero::check!()
                .with_arbitrary::<Scenario>()
                .for_each(|Scenario { peer_id }| {
                    // peer_id is &PeerId here, compare to itself
                    assert!(!(peer_id < peer_id));
                    assert!(!(peer_id > peer_id));
                    assert!(peer_id <= peer_id);
                    assert!(peer_id >= peer_id);
                });
        }
    }
}
