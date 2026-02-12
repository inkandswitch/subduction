//! Truncated hash values for compact membership checks.
//!
//! [`Truncated<V>`] stores the first 8 bytes of a value, providing compact
//! set membership testing at the cost of a small (negligible for realistic
//! workloads) false positive probability.
//!
//! Truncation is one-way: a `Truncated<V>` cannot be converted back to `V`.

use core::marker::PhantomData;

use super::digest::Digest;

/// The first 8 bytes of a value, used for compact membership checks.
///
/// Construction is explicit via [`Truncated::new`] to prevent accidental
/// lossy conversion. A `Truncated<V>` cannot be converted back to `V`.
///
/// For 64-bit truncation, the collision probability is ~N²/2⁶⁴ where N is
/// the set size. For 400 items this is ~10⁻¹³ — negligible.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Truncated<V> {
    bytes: [u8; 8],
    _phantom: PhantomData<V>,
}

// Manual derives to avoid requiring bounds on V

impl<V> Clone for Truncated<V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> Copy for Truncated<V> {}

impl<V> PartialEq for Truncated<V> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl<V> Eq for Truncated<V> {}

impl<V> core::hash::Hash for Truncated<V> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl<V> PartialOrd for Truncated<V> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<V> Ord for Truncated<V> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl<V> core::fmt::Debug for Truncated<V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Truncated(")?;
        for byte in &self.bytes[..4] {
            write!(f, "{byte:02x}")?;
        }
        write!(f, "…)")
    }
}

impl<V> core::fmt::Display for Truncated<V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for byte in &self.bytes {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl<V> Truncated<V> {
    /// The raw bytes of the truncated value.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 8] {
        &self.bytes
    }
}

impl<T: 'static> Truncated<Digest<T>> {
    /// Truncate a [`Digest`] to its first 8 bytes.
    #[must_use]
    pub fn new(digest: Digest<T>) -> Self {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&digest.as_bytes()[..8]);
        Self {
            bytes,
            _phantom: PhantomData,
        }
    }
}

impl<Ctx, V: 'static> minicbor::Encode<Ctx> for Truncated<V> {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _ctx: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        e.bytes(&self.bytes)?;
        Ok(())
    }
}

impl<'b, Ctx, V: 'static> minicbor::Decode<'b, Ctx> for Truncated<V> {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        _ctx: &mut Ctx,
    ) -> Result<Self, minicbor::decode::Error> {
        let bytes = d.bytes()?;
        if bytes.len() != 8 {
            return Err(minicbor::decode::Error::message(
                "truncated digest must be exactly 8 bytes",
            ));
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(bytes);
        Ok(Self {
            bytes: arr,
            _phantom: PhantomData,
        })
    }
}

#[cfg(feature = "arbitrary")]
impl<'a, V: 'static> arbitrary::Arbitrary<'a> for Truncated<V> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let bytes: [u8; 8] = u.arbitrary()?;
        Ok(Self {
            bytes,
            _phantom: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loose_commit::LooseCommit;

    #[test]
    fn truncation_preserves_first_8_bytes() {
        let digest = Digest::<LooseCommit>::from_bytes([42u8; 32]);
        let truncated = Truncated::new(digest);
        assert_eq!(truncated.as_bytes(), &[42u8; 8]);
    }

    #[test]
    fn same_digest_produces_same_truncation() {
        let digest = Digest::<LooseCommit>::from_bytes([1u8; 32]);
        let a = Truncated::new(digest);
        let b = Truncated::new(digest);
        assert_eq!(a, b);
    }

    #[test]
    fn different_first_8_bytes_produce_different_truncation() {
        let mut bytes_a = [0u8; 32];
        bytes_a[0] = 1;
        let mut bytes_b = [0u8; 32];
        bytes_b[0] = 2;

        let a = Truncated::new(Digest::<LooseCommit>::from_bytes(bytes_a));
        let b = Truncated::new(Digest::<LooseCommit>::from_bytes(bytes_b));
        assert_ne!(a, b);
    }

    #[test]
    fn different_bytes_after_8_produce_same_truncation() {
        let mut bytes_a = [0u8; 32];
        bytes_a[31] = 1;
        let mut bytes_b = [0u8; 32];
        bytes_b[31] = 2;

        let a = Truncated::new(Digest::<LooseCommit>::from_bytes(bytes_a));
        let b = Truncated::new(Digest::<LooseCommit>::from_bytes(bytes_b));
        assert_eq!(a, b);
    }
}
