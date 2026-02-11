//! Short keyed hashing. Not a cryptographic fingerprint, but a compact hash.
//!
//! Uses SipHash-2-4 with a per-request random [`FingerprintSeed`] to produce
//! compact (`u64`) [`Fingerprint<T>`] values. This is especially
//! useful for bandwidth-efficient sync.

use core::{hash::Hasher, marker::PhantomData};

use siphasher::sip::SipHasher24;

/// A 128-bit seed for SipHash-2-4 fingerprinting.
///
/// Generated randomly per sync request to prevent an attacker from
/// precomputing collisions. The two `u64` keys are `SipHash`'s native
/// key format.
///
/// # Examples
///
/// ```
/// use sedimentree_core::crypto::fingerprint::FingerprintSeed;
///
/// let seed = FingerprintSeed::new(42, 99);
/// assert_eq!(seed.key0(), 42);
/// assert_eq!(seed.key1(), 99);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, minicbor::Encode, minicbor::Decode)]
pub struct FingerprintSeed {
    #[n(0)]
    key0: u64,

    #[n(1)]
    key1: u64,
}

impl FingerprintSeed {
    /// Create a new seed from two u64 keys.
    #[must_use]
    pub const fn new(key0: u64, key1: u64) -> Self {
        Self { key0, key1 }
    }

    /// The first key component.
    #[must_use]
    pub const fn key0(&self) -> u64 {
        self.key0
    }

    /// The second key component.
    #[must_use]
    pub const fn key1(&self) -> u64 {
        self.key1
    }

    /// Create a new SipHash-2-4 hasher seeded with this value.
    #[must_use]
    pub fn hasher(&self) -> SipHasher24 {
        SipHasher24::new_with_keys(self.key0, self.key1)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for FingerprintSeed {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.key0.to_le_bytes());
        bytes[8..].copy_from_slice(&self.key1.to_le_bytes());
        serializer.serialize_bytes(&bytes)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for FingerprintSeed {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SeedVisitor;

        impl serde::de::Visitor<'_> for SeedVisitor {
            type Value = FingerprintSeed;

            fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter.write_str("16 bytes")
            }

            #[allow(clippy::expect_used)]
            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                if v.len() != 16 {
                    return Err(E::invalid_length(v.len(), &"16 bytes"));
                }
                let key0 = u64::from_le_bytes(
                    v.get(..8)
                        .expect("checked length")
                        .try_into()
                        .expect("checked length"),
                );
                let key1 = u64::from_le_bytes(
                    v.get(8..)
                        .expect("checked length")
                        .try_into()
                        .expect("checked length"),
                );
                Ok(FingerprintSeed::new(key0, key1))
            }
        }

        deserializer.deserialize_bytes(SeedVisitor)
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for FingerprintSeed {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::new(u.arbitrary()?, u.arbitrary()?))
    }
}

/// A short keyed hash for set reconciliation.
///
/// Computed via SipHash-2-4 with a per-request [`FingerprintSeed`].
/// The phantom type `T` tracks what was fingerprinted:
/// - [`Fingerprint<CommitId>`][crate::loose_commit::id::CommitId] — fingerprint of a commit's content digest
/// - [`Fingerprint<FragmentId>`][crate::fragment::id::FragmentId] — fingerprint of a fragment's causal range
///
/// # Collision Probability
///
/// | Items  | Probability     |
/// |--------|-----------------|
/// | 1,000  | ~2.7 × 10⁻¹¹   |
/// | 10,000 | ~2.7 × 10⁻⁹    |
///
/// Per-request random seeds prevent precomputation. Worst case: a
/// collision means one item doesn't sync this round, caught on next sync.
pub struct Fingerprint<T> {
    hash: u64,
    _marker: PhantomData<T>,
}

impl<T: core::hash::Hash> Fingerprint<T> {
    /// Compute a fingerprint of a hashable value using the given seed.
    ///
    /// Feeds the value into SipHash-2-4 via its [`Hash`] implementation.
    ///
    /// ```
    /// use sedimentree_core::crypto::fingerprint::{Fingerprint, FingerprintSeed};
    ///
    /// let seed = FingerprintSeed::new(42, 99);
    /// let fp: Fingerprint<u64> = Fingerprint::new(&seed, &12345u64);
    /// assert_eq!(fp, Fingerprint::new(&seed, &12345u64));
    /// ```
    #[must_use]
    pub fn new(seed: &FingerprintSeed, value: &T) -> Self {
        let mut hasher = seed.hasher();
        value.hash(&mut hasher);
        Self::from_u64(hasher.finish())
    }
}

impl<T> Fingerprint<T> {
    /// Create a fingerprint from a raw u64 hash value.
    #[must_use]
    pub const fn from_u64(hash: u64) -> Self {
        Self {
            hash,
            _marker: PhantomData,
        }
    }

    /// The raw u64 hash value.
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.hash
    }
}

impl<T> Clone for Fingerprint<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Fingerprint<T> {}

impl<T> PartialEq for Fingerprint<T> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl<T> Eq for Fingerprint<T> {}

impl<T> PartialOrd for Fingerprint<T> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Fingerprint<T> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.hash.cmp(&other.hash)
    }
}

impl<T> core::hash::Hash for Fingerprint<T> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl<T> core::fmt::Debug for Fingerprint<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "Fingerprint<{}>({})",
            core::any::type_name::<T>(),
            self.hash
        )
    }
}

impl<T> core::fmt::Display for Fingerprint<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:016x}", self.hash)
    }
}

impl<T, Ctx> minicbor::Encode<Ctx> for Fingerprint<T> {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _ctx: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        e.u64(self.hash)?;
        Ok(())
    }
}

impl<'b, T, Ctx> minicbor::Decode<'b, Ctx> for Fingerprint<T> {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        _ctx: &mut Ctx,
    ) -> Result<Self, minicbor::decode::Error> {
        Ok(Self::from_u64(d.u64()?))
    }
}

#[cfg(feature = "serde")]
impl<T> serde::Serialize for Fingerprint<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.hash)
    }
}

#[cfg(feature = "serde")]
impl<'de, T> serde::Deserialize<'de> for Fingerprint<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let hash = u64::deserialize(deserializer)?;
        Ok(Self::from_u64(hash))
    }
}

#[cfg(feature = "arbitrary")]
impl<'a, T: 'static> arbitrary::Arbitrary<'a> for Fingerprint<T> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::from_u64(u.arbitrary()?))
    }
}
