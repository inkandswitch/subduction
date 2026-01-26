//! Binary objects.

use core::cmp::min;

use alloc::vec::Vec;

use crate::digest::Digest;

/// A binary object.
///
/// Just a wrapper around a `Vec<u8>`.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cbor(transparent)]
pub struct Blob(#[n(0)] Vec<u8>);

impl core::fmt::Debug for Blob {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let preview_byte_count = {
            const MAX_PREVIEW_BYTES: usize = 32;
            min(MAX_PREVIEW_BYTES, self.0.len())
        };

        write!(f, "Blob({} bytes, ", self.0.len())?;

        if self.0.len() <= preview_byte_count {
            for byte in &self.0 {
                write!(f, "{byte:02x}")?;
            }
        } else {
            #[allow(clippy::expect_used)]
            let preview_bytes = self
                .0
                .get(..preview_byte_count)
                .expect("sliced out of bounds");

            for byte in preview_bytes {
                write!(f, "{byte:02x}")?;
            }

            write!(f, "...")?;
        }

        write!(f, " ({} bytes total)", self.0.len())?;
        Ok(())
    }
}

impl core::fmt::Display for Blob {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let preview_byte_count = {
            const MAX_PREVIEW_BYTES: usize = 32;
            min(MAX_PREVIEW_BYTES, self.0.len())
        };

        if self.0.len() <= preview_byte_count {
            for byte in &self.0 {
                write!(f, "{byte:02x}")?;
            }
        } else {
            #[allow(clippy::expect_used)]
            let preview_bytes = self
                .0
                .get(..preview_byte_count)
                .expect("sliced out of bounds");

            for byte in preview_bytes {
                write!(f, "{byte:02x}")?;
            }

            write!(f, "..({} bytes total)", self.0.len())?;
        }
        Ok(())
    }
}

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

impl From<Vec<u8>> for Blob {
    fn from(contents: Vec<u8>) -> Self {
        Blob::new(contents)
    }
}

impl From<&[u8]> for Blob {
    fn from(contents: &[u8]) -> Self {
        Blob::new(contents.to_vec())
    }
}

impl From<Blob> for Vec<u8> {
    fn from(blob: Blob) -> Self {
        blob.into_contents()
    }
}

/// Metadata for the underlying payload data itself.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BlobMeta {
    #[n(0)]
    digest: Digest<Blob>,

    #[n(1)]
    size_bytes: u64,
}

impl BlobMeta {
    /// Generate metadata for the given contents.
    #[must_use]
    pub fn new(contents: &[u8]) -> Self {
        let digest = Digest::hash_bytes(contents);
        let size_bytes = contents.len() as u64;
        Self { digest, size_bytes }
    }

    /// Manually create metadata from a digest and size.
    ///
    /// Since this is manual, it may be incorrect.
    #[must_use]
    pub const fn from_digest_size(digest: Digest<Blob>, size_bytes: u64) -> Self {
        BlobMeta { digest, size_bytes }
    }

    /// The digest of the blob.
    #[must_use]
    pub const fn digest(&self) -> Digest<Blob> {
        self.digest
    }

    /// The size of the blob in bytes.
    #[must_use]
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}
