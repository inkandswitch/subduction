use std::str::FromStr;

/// Metadata for the underlying payload data itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct BlobMeta {
    digest: Digest,
    size_bytes: u64,
}

impl BlobMeta {
    /// Generate metadata for the given contents.
    pub fn new(contents: &[u8]) -> Self {
        let digest = Digest::new(contents);
        let size_bytes = contents.len() as u64;
        Self { digest, size_bytes }
    }

    /// Manually create metadata from a digest and size.
    ///
    /// Since this is manual, it may be incorrect.
    pub fn from_digest_size(digest: Digest, size_bytes: u64) -> Self {
        BlobMeta { digest, size_bytes }
    }

    /// The digest of the blob.
    pub fn digest(&self) -> Digest {
        self.digest
    }

    /// The size of the blob in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

/// A 32-byte digest.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Digest([u8; 32]);

impl std::fmt::Debug for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Digest({})", hex::encode(self.0))
    }
}

impl Digest {
    /// Create a new digest for the given data.
    pub fn new(data: &[u8]) -> Self {
        let hash = blake3::hash(data);
        let mut bytes = [0; 32];
        bytes.copy_from_slice(hash.as_bytes());
        Self(bytes)
    }

    /// Wrap a raw 32-byte array as a digest.
    pub fn from_raw_bytes(bytes: [u8; 32]) -> Self {
        Digest(bytes)
    }

    /// Get the raw bytes of the digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
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
    #[derive(Clone, PartialEq, Error)]
    pub enum InvalidDigest {
        #[error("Not enough input")]
        NotEnoughInput,

        #[error("Invalid hex: {0}")]
        InvalidHex(hex::FromHexError),

        #[error("Invalid length")]
        InvalidLength,
    }

    impl std::fmt::Debug for InvalidDigest {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(self, f)
        }
    }
}
