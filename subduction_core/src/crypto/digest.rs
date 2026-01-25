//! Typed digest for phantom-type safety.

use core::marker::PhantomData;

use sedimentree_core::blob::Digest as UntypedDigest;

/// A 32-byte digest with phantom type tracking what was digested.
///
/// This provides type safety to ensure digests of different types
/// are not accidentally mixed up.
pub struct Digest<T> {
    bytes: [u8; 32],
    _marker: PhantomData<T>,
}

impl<T, Ctx> minicbor::Encode<Ctx> for Digest<T> {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _ctx: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        e.bytes(&self.bytes)?;
        Ok(())
    }
}

impl<'b, T, Ctx> minicbor::Decode<'b, Ctx> for Digest<T> {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        _ctx: &mut Ctx,
    ) -> Result<Self, minicbor::decode::Error> {
        let bytes = d.bytes()?;
        if bytes.len() != 32 {
            return Err(minicbor::decode::Error::message(
                "expected 32 bytes for digest",
            ));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(bytes);
        Ok(Self::from_bytes(arr))
    }
}

impl<T> Digest<T> {
    /// Create a digest from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self {
            bytes,
            _marker: PhantomData,
        }
    }

    /// Get the raw bytes of the digest.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }

    /// Hash raw bytes and create a digest.
    #[must_use]
    pub fn hash_bytes(data: &[u8]) -> Self {
        let untyped = UntypedDigest::hash(data);
        Self::from_bytes(*untyped.as_bytes())
    }
}

impl<T: minicbor::Encode<()>> Digest<T> {
    /// Encode and hash a value to create a digest.
    ///
    /// # Panics
    ///
    /// Panics if CBOR encoding fails (should never happen for well-formed types).
    #[allow(clippy::expect_used)]
    #[must_use]
    pub fn hash(value: &T) -> Self {
        let encoded = minicbor::to_vec(value).expect("encoding should not fail");
        Self::hash_bytes(&encoded)
    }
}

impl<T> Clone for Digest<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Digest<T> {}

impl<T> PartialEq for Digest<T> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl<T> Eq for Digest<T> {}

impl<T> PartialOrd for Digest<T> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Digest<T> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl<T> core::hash::Hash for Digest<T> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl<T> core::fmt::Debug for Digest<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Digest<{}>(", core::any::type_name::<T>())?;
        for byte in self.bytes {
            write!(f, "{byte:02x}")?;
        }
        write!(f, ")")
    }
}

#[cfg(feature = "serde")]
impl<T> serde::Serialize for Digest<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.bytes)
    }
}

#[cfg(feature = "serde")]
impl<'de, T> serde::Deserialize<'de> for Digest<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DigestVisitor<T>(PhantomData<T>);

        impl<T> serde::de::Visitor<'_> for DigestVisitor<T> {
            type Value = Digest<T>;

            fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter.write_str("32 bytes")
            }

            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                if v.len() != 32 {
                    return Err(E::invalid_length(v.len(), &"32 bytes"));
                }
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(v);
                Ok(Digest::from_bytes(bytes))
            }
        }

        deserializer.deserialize_bytes(DigestVisitor(PhantomData))
    }
}

#[cfg(feature = "arbitrary")]
impl<'a, T: 'static> arbitrary::Arbitrary<'a> for Digest<T> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let bytes: [u8; 32] = u.arbitrary()?;
        Ok(Self::from_bytes(bytes))
    }
}

#[cfg(feature = "bolero")]
impl<T: 'static> bolero::generator::TypeGenerator for Digest<T> {
    fn generate<D: bolero::Driver>(driver: &mut D) -> Option<Self> {
        let bytes: [u8; 32] = bolero::generator::TypeGenerator::generate(driver)?;
        Some(Self::from_bytes(bytes))
    }
}
