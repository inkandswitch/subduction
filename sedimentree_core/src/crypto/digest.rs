//! Typed digest for phantom-type safety.

use core::marker::PhantomData;

use crate::{
    codec::{Decode, Encode},
    hex::decode_hex,
};

/// A 32-byte digest with phantom type tracking what was digested.
///
/// This provides type safety to ensure digests of different types
/// are not accidentally mixed up.
pub struct Digest<T> {
    bytes: [u8; 32],
    _marker: PhantomData<T>,
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

    /// Get the raw bytes of the digest by reference.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }

    /// Consume the digest and return the raw bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.bytes
    }

    /// Hash raw bytes and create a digest.
    #[must_use]
    pub fn hash_bytes(data: &[u8]) -> Self {
        let hash = blake3::hash(data);
        Self::from_bytes(*hash.as_bytes())
    }

    /// Cast to a different phantom type.
    ///
    /// This is useful when you know two digest types are compatible
    /// (e.g., converting from a generic digest to a specific type).
    #[must_use]
    pub const fn cast<U>(self) -> Digest<U> {
        Digest {
            bytes: self.bytes,
            _marker: PhantomData,
        }
    }

    /// Erase the type parameter to get an untyped digest.
    #[must_use]
    pub const fn erase(self) -> Digest<()> {
        self.cast()
    }
}

impl<T: Encode + Decode<Context = ()>> Digest<T> {
    /// Encode and hash a value to create a digest.
    ///
    /// The value is encoded using its [`Encode`] implementation
    /// and then hashed with BLAKE3.
    #[must_use]
    pub fn hash(value: &T) -> Self {
        let mut buf = alloc::vec::Vec::with_capacity(T::MIN_SIZE);
        buf.extend_from_slice(&T::SCHEMA);
        value.encode_fields(&(), &mut buf);
        Self::hash_bytes(&buf)
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

impl<T> core::fmt::Display for Digest<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for byte in self.bytes {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl<T> core::str::FromStr for Digest<T> {
    type Err = InvalidDigest;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = decode_hex(s).ok_or(InvalidDigest::InvalidHex)?;
        let arr: [u8; 32] = bytes.try_into().map_err(|_| InvalidDigest::WrongLength)?;
        Ok(Self::from_bytes(arr))
    }
}

/// Error parsing a digest from a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidDigest {
    /// The hex string was invalid.
    InvalidHex,
    /// The digest was not 32 bytes.
    WrongLength,
}

impl core::fmt::Display for InvalidDigest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidHex => write!(f, "invalid hex string"),
            Self::WrongLength => write!(f, "digest must be exactly 32 bytes"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for InvalidDigest {}

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
