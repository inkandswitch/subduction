//! Truncated hash values for compact membership checks.
//!
//! [`Truncated<V, N>`] stores the first `N` bytes of a value, providing compact
//! set membership testing at the cost of a small (negligible for realistic
//! workloads) false positive probability.
//!
//! The default truncation size is 12 bytes (96 bits). Security properties:
//!
//! - **Random collision** (birthday): ~N²/2⁹⁶ where N is set size.
//!   For 1 million items: ~10⁻¹⁷ — effectively zero.
//! - **Adversarial collision**: ~2⁴⁸ work (birthday attack on 96 bits).
//!   Feasible for well-resourced attackers, but checkpoints are non-security-critical.
//! - **Preimage resistance**: ~2⁹⁶ work — infeasible.
//!
//! For cryptographic collision resistance, use the full 32-byte digest.
//!
//! Truncation is one-way: a `Truncated<V, N>` cannot be converted back to `V`.

use core::marker::PhantomData;

use super::digest::Digest;

/// Default truncation size in bytes (96 bits).
pub const DEFAULT_TRUNCATION_BYTES: usize = 12;

/// The first `N` bytes of a value, used for compact membership checks.
///
/// Construction is explicit via [`Truncated::new`] to prevent accidental
/// lossy conversion. A `Truncated<V, N>` cannot be converted back to `V`.
///
/// # Collision Probability
///
/// The collision probability is ~n²/2^(8N) where n is the set size:
///
/// | N (bytes) | Bits | Collision at 1M items |
/// |-----------|------|------------------------|
/// | 8         | 64   | ~10⁻⁸ (1 in 100M)      |
/// | 12        | 96   | ~10⁻¹⁷ (effectively 0) |
/// | 16        | 128  | ~10⁻²⁷ (effectively 0) |
/// | 32        | 256  | ~10⁻⁶⁶ (cryptographic) |
///
/// The default is 12 bytes (96 bits), which is safe for all realistic workloads
/// while saving 25% on wire size compared to 16 bytes.
pub struct Truncated<V, const N: usize = DEFAULT_TRUNCATION_BYTES> {
    bytes: [u8; N],
    _phantom: PhantomData<V>,
}

// Manual derives to avoid requiring bounds on V

impl<V, const N: usize> Clone for Truncated<V, N> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V, const N: usize> Copy for Truncated<V, N> {}

impl<V, const N: usize> PartialEq for Truncated<V, N> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl<V, const N: usize> Eq for Truncated<V, N> {}

impl<V, const N: usize> core::hash::Hash for Truncated<V, N> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl<V, const N: usize> PartialOrd for Truncated<V, N> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<V, const N: usize> Ord for Truncated<V, N> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl<V, const N: usize> core::fmt::Debug for Truncated<V, N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Truncated(")?;
        let preview_len = N.min(4);
        for byte in &self.bytes[..preview_len] {
            write!(f, "{byte:02x}")?;
        }
        if N > 4 {
            write!(f, "…")?;
        }
        write!(f, ")")
    }
}

impl<V, const N: usize> core::fmt::Display for Truncated<V, N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for byte in &self.bytes {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl<V, const N: usize> Truncated<V, N> {
    /// Create a truncated value from raw bytes.
    ///
    /// This is the inverse of [`as_bytes`](Self::as_bytes).
    #[must_use]
    pub const fn from_bytes(bytes: [u8; N]) -> Self {
        Self {
            bytes,
            _phantom: PhantomData,
        }
    }

    /// The raw bytes of the truncated value.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; N] {
        &self.bytes
    }
}

impl<V, const N: usize> From<[u8; N]> for Truncated<V, N> {
    fn from(bytes: [u8; N]) -> Self {
        Self::from_bytes(bytes)
    }
}

impl<T: 'static, const N: usize> Truncated<Digest<T>, N> {
    /// Truncate a [`Digest`] to its first `N` bytes.
    ///
    /// # Panics
    ///
    /// Panics if `N > 32` (the size of a Digest).
    #[must_use]
    pub fn new(digest: Digest<T>) -> Self {
        assert!(
            N <= 32,
            "truncation size cannot exceed digest size (32 bytes)"
        );
        let mut bytes = [0u8; N];
        // SAFETY: We assert N <= 32 above, and digest is always 32 bytes
        #[allow(clippy::indexing_slicing)]
        bytes.copy_from_slice(&digest.as_bytes()[..N]);
        Self {
            bytes,
            _phantom: PhantomData,
        }
    }
}

impl<Ctx, V: 'static, const N: usize> minicbor::Encode<Ctx> for Truncated<V, N> {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _ctx: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        e.bytes(&self.bytes)?;
        Ok(())
    }
}

impl<'b, Ctx, V: 'static, const N: usize> minicbor::Decode<'b, Ctx> for Truncated<V, N> {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        _ctx: &mut Ctx,
    ) -> Result<Self, minicbor::decode::Error> {
        let bytes = d.bytes()?;
        if bytes.len() != N {
            return Err(minicbor::decode::Error::message(
                "truncated digest has wrong length",
            ));
        }
        let mut arr = [0u8; N];
        arr.copy_from_slice(bytes);
        Ok(Self {
            bytes: arr,
            _phantom: PhantomData,
        })
    }
}

#[cfg(feature = "arbitrary")]
impl<'a, V: 'static, const N: usize> arbitrary::Arbitrary<'a> for Truncated<V, N> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let mut bytes = [0u8; N];
        u.fill_buffer(&mut bytes)?;
        Ok(Self {
            bytes,
            _phantom: PhantomData,
        })
    }
}

#[cfg(feature = "serde")]
impl<V, const N: usize> serde::Serialize for Truncated<V, N> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.bytes)
    }
}

#[cfg(feature = "serde")]
impl<'de, V: 'static, const N: usize> serde::Deserialize<'de> for Truncated<V, N> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TruncatedVisitor<V, const N: usize>(PhantomData<V>);

        impl<V: 'static, const N: usize> serde::de::Visitor<'_> for TruncatedVisitor<V, N> {
            type Value = Truncated<V, N>;

            fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                write!(formatter, "a byte array of length {N}")
            }

            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                if v.len() != N {
                    return Err(E::invalid_length(v.len(), &self));
                }
                let mut bytes = [0u8; N];
                bytes.copy_from_slice(v);
                Ok(Truncated {
                    bytes,
                    _phantom: PhantomData,
                })
            }
        }

        deserializer.deserialize_bytes(TruncatedVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loose_commit::LooseCommit;

    #[test]
    fn different_truncation_sizes_are_distinct_types() {
        let digest = Digest::<LooseCommit>::from_bytes([42u8; 32]);

        let t8: Truncated<Digest<LooseCommit>, 8> = Truncated::new(digest);
        let t12: Truncated<Digest<LooseCommit>, 12> = Truncated::new(digest);
        let t16: Truncated<Digest<LooseCommit>, 16> = Truncated::new(digest);

        // These are different types, so this is a compile-time check.
        // At runtime, we verify the byte lengths differ.
        assert_eq!(t8.as_bytes().len(), 8);
        assert_eq!(t12.as_bytes().len(), 12);
        assert_eq!(t16.as_bytes().len(), 16);
    }

    #[cfg(feature = "bolero")]
    mod proptests {
        use super::*;

        #[test]
        fn truncation_preserves_first_n_bytes() {
            bolero::check!()
                .with_arbitrary::<[u8; 32]>()
                .for_each(|bytes| {
                    let digest = Digest::<LooseCommit>::from_bytes(*bytes);
                    let truncated: Truncated<Digest<LooseCommit>> = Truncated::new(digest);
                    assert_eq!(truncated.as_bytes(), &bytes[..12]);
                });
        }

        #[test]
        fn truncation_ignores_suffix() {
            bolero::check!()
                .with_arbitrary::<([u8; 12], [u8; 20], [u8; 20])>()
                .for_each(|(prefix, suffix_a, suffix_b)| {
                    let mut bytes_a = [0u8; 32];
                    let mut bytes_b = [0u8; 32];

                    bytes_a[..12].copy_from_slice(prefix);
                    bytes_a[12..].copy_from_slice(suffix_a);

                    bytes_b[..12].copy_from_slice(prefix);
                    bytes_b[12..].copy_from_slice(suffix_b);

                    let a: Truncated<Digest<LooseCommit>> =
                        Truncated::new(Digest::from_bytes(bytes_a));
                    let b: Truncated<Digest<LooseCommit>> =
                        Truncated::new(Digest::from_bytes(bytes_b));

                    assert_eq!(a, b, "same prefix should produce equal truncations");
                });
        }

        #[test]
        fn different_prefixes_produce_different_truncations() {
            bolero::check!()
                .with_arbitrary::<([u8; 32], [u8; 32])>()
                .for_each(|(bytes_a, bytes_b)| {
                    // Only test when prefixes actually differ
                    if bytes_a[..12] == bytes_b[..12] {
                        return;
                    }

                    let a: Truncated<Digest<LooseCommit>> =
                        Truncated::new(Digest::from_bytes(*bytes_a));
                    let b: Truncated<Digest<LooseCommit>> =
                        Truncated::new(Digest::from_bytes(*bytes_b));

                    assert_ne!(
                        a, b,
                        "different prefixes should produce different truncations"
                    );
                });
        }
    }
}
