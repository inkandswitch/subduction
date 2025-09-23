//! Binary objects.

use std::str::FromStr;

/// A binary object.
///
/// Just a wrapper around a `Vec<u8>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Blob(Vec<u8>);

impl Blob {
    /// Create a new blob from the given contents.
    #[must_use]
    pub const fn new(contents: Vec<u8>) -> Self {
        Blob(contents)
    }

    /// The contents of the blob.
    #[must_use]
    pub const fn contents(&self) -> &Vec<u8> {
        &self.0
    }

    /// Consume the blob and return its contents.
    #[must_use]
    pub fn into_contents(self) -> Vec<u8> {
        self.0
    }

    /// Get the contents of the blob as a slice.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Get metadata for the blob.
    #[must_use]
    pub fn meta(&self) -> BlobMeta {
        BlobMeta::new(&self.0)
    }
}

/// Metadata for the underlying payload data itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BlobMeta {
    digest: Digest,
    size_bytes: u64,
}

impl BlobMeta {
    /// Generate metadata for the given contents.
    #[must_use]
    pub fn new(contents: &[u8]) -> Self {
        let digest = Digest::hash(contents);
        let size_bytes = contents.len() as u64;
        Self { digest, size_bytes }
    }

    /// Manually create metadata from a digest and size.
    ///
    /// Since this is manual, it may be incorrect.
    #[must_use]
    pub const fn from_digest_size(digest: Digest, size_bytes: u64) -> Self {
        BlobMeta { digest, size_bytes }
    }

    /// The digest of the blob.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// The size of the blob in bytes.
    #[must_use]
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

/// A 32-byte digest.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Digest([u8; 32]);

impl std::fmt::Debug for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Digest({})", hex::encode(self.0))
    }
}

impl Digest {
    /// Create a new digest for the given data.
    #[must_use]
    pub fn hash(data: &[u8]) -> Self {
        let b3_digest = blake3::hash(data);
        let mut bytes = [0; 32];
        bytes.copy_from_slice(b3_digest.as_bytes());
        Self(bytes)
    }

    /// Get the raw bytes of the digest.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for Digest {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl std::fmt::Display for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        hex::encode(self.0).fmt(f)
    }
}

impl FromStr for Digest {
    type Err = error::InvalidDigest;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s).map_err(error::InvalidDigest::InvalidHex)?;
        if bytes.len() != 32 {
            return Err(error::InvalidDigest::InvalidLength);
        }
        let mut hash = [0; 32];
        hash.copy_from_slice(&bytes);
        Ok(Digest(hash))
    }
}

mod error {
    use thiserror::Error;

    /// An error parsing a digest.
    #[derive(Clone, Copy, PartialEq, Error)]
    pub enum InvalidDigest {
        /// Not enough input.
        #[error("Not enough input")]
        NotEnoughInput,

        /// Invalid hex encoding.
        #[error("Invalid hex: {0}")]
        InvalidHex(hex::FromHexError),

        /// Invalid length.
        #[error("Invalid length")]
        InvalidLength,
    }

    impl std::fmt::Debug for InvalidDigest {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(self, f)
        }
    }
}
