//! Loose commit metadata for Sedimentree.

pub mod id;

use alloc::{collections::BTreeSet, vec::Vec};

use id::CommitId;

use crate::{
    blob::{Blob, BlobMeta, has_meta::HasBlobMeta},
    codec::{
        decode::{self, Decode},
        encode::{self, Encode},
        error::{BufferTooShort, DecodeError, ReadingType},
        schema::{self, Schema},
    },
    crypto::digest::Digest,
    id::SedimentreeId,
};

/// The smallest unit of metadata in a Sedimentree.
///
/// It includes the digest of the data, plus pointers to any (causal) parents.
/// The `sedimentree_id` field cryptographically binds the commit to a specific
/// document, preventing replay attacks across documents.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LooseCommit {
    sedimentree_id: SedimentreeId,
    digest: Digest<LooseCommit>,
    parents: BTreeSet<Digest<LooseCommit>>,
    blob_meta: BlobMeta,
}

impl LooseCommit {
    /// Extract the causal identity of this commit.
    #[must_use]
    pub const fn commit_id(&self) -> CommitId {
        CommitId::new(self.digest)
    }

    /// Constructor for a [`LooseCommit`].
    #[must_use]
    pub const fn new(
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
        parents: BTreeSet<Digest<LooseCommit>>,
        blob_meta: BlobMeta,
    ) -> Self {
        Self {
            sedimentree_id,
            digest,
            parents,
            blob_meta,
        }
    }

    /// The [`SedimentreeId`] this commit belongs to.
    #[must_use]
    pub const fn sedimentree_id(&self) -> SedimentreeId {
        self.sedimentree_id
    }

    /// The unique [`Digest`] of this [`LooseCommit`], derived from its content.
    #[must_use]
    pub const fn digest(&self) -> Digest<LooseCommit> {
        self.digest
    }

    /// The (possibly empty) set of parent commits.
    #[must_use]
    pub const fn parents(&self) -> &BTreeSet<Digest<LooseCommit>> {
        &self.parents
    }

    /// Metadata about the payload blob.
    #[must_use]
    pub const fn blob_meta(&self) -> &BlobMeta {
        &self.blob_meta
    }
}

impl HasBlobMeta for LooseCommit {
    type Args = (
        SedimentreeId,
        Digest<LooseCommit>,
        BTreeSet<Digest<LooseCommit>>,
    );

    fn blob_meta(&self) -> BlobMeta {
        self.blob_meta
    }

    fn from_args((sedimentree_id, digest, parents): Self::Args, blob_meta: BlobMeta) -> Self {
        Self::new(sedimentree_id, digest, parents, blob_meta)
    }
}

// ============================================================================
// Local Storage Encoding
// ============================================================================

/// Fixed size for local storage: SedimentreeId(32) + Digest<Commit>(32) + Digest<Blob>(32) + |Parents|(1) + BlobSize(4).
const LOCAL_FIXED_SIZE: usize = 32 + 32 + 32 + 1 + 4;

impl LooseCommit {
    /// Encode to bytes for local storage.
    ///
    /// Format: `SedimentreeId(32) ++ Digest(32) ++ BlobDigest(32) ++ ParentCount(1) ++ BlobSize(4) ++ Parents(32 each)`
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let size = LOCAL_FIXED_SIZE + self.parents.len() * 32;
        let mut buf = Vec::with_capacity(size);

        encode::array(self.sedimentree_id.as_bytes(), &mut buf);
        encode::array(self.digest.as_bytes(), &mut buf);
        encode::array(self.blob_meta.digest().as_bytes(), &mut buf);

        #[allow(clippy::cast_possible_truncation)]
        encode::u8(self.parents.len() as u8, &mut buf);

        #[allow(clippy::cast_possible_truncation)]
        encode::u32(self.blob_meta.size_bytes() as u32, &mut buf);

        for parent in &self.parents {
            encode::array(parent.as_bytes(), &mut buf);
        }

        buf
    }

    /// Decode from bytes stored locally.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] if the buffer is malformed.
    pub fn try_from_bytes(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < LOCAL_FIXED_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: "LooseCommit (local)",
                need: LOCAL_FIXED_SIZE,
                have: buf.len(),
            });
        }

        let mut offset = 0;

        let sedimentree_id_bytes: [u8; 32] = decode::array(buf, offset)?;
        let sedimentree_id = SedimentreeId::new(sedimentree_id_bytes);
        offset += 32;

        let digest_bytes: [u8; 32] = decode::array(buf, offset)?;
        let digest = Digest::from_bytes(digest_bytes);
        offset += 32;

        let blob_digest_bytes: [u8; 32] = decode::array(buf, offset)?;
        let blob_digest = Digest::<Blob>::from_bytes(blob_digest_bytes);
        offset += 32;

        let parent_count = decode::u8(buf, offset)? as usize;
        offset += 1;

        let blob_size = decode::u32(buf, offset)? as u64;
        offset += 4;

        let parents_size = parent_count * 32;
        if buf.len() < offset + parents_size {
            return Err(BufferTooShort {
                reading: ReadingType::Slice { len: parents_size },
                offset,
                need: parents_size,
                have: buf.len().saturating_sub(offset),
            }
            .into());
        }

        let mut parent_arrays: Vec<[u8; 32]> = Vec::with_capacity(parent_count);
        for _ in 0..parent_count {
            let parent_bytes: [u8; 32] = decode::array(buf, offset)?;
            parent_arrays.push(parent_bytes);
            offset += 32;
        }

        decode::verify_sorted(&parent_arrays)?;

        let parents: BTreeSet<Digest<LooseCommit>> =
            parent_arrays.into_iter().map(Digest::from_bytes).collect();

        let blob_meta = BlobMeta::from_digest_size(blob_digest, blob_size);

        Ok(LooseCommit::new(sedimentree_id, digest, parents, blob_meta))
    }
}

// ============================================================================
// Codec Implementation
// ============================================================================

/// Fixed fields size: SedimentreeId(32) + Digest<Commit>(32) + Digest<Blob>(32) + |Parents|(1) + BlobSize(4).
const CODEC_FIXED_FIELDS_SIZE: usize = 32 + 32 + 32 + 1 + 4;

/// Minimum signed message size: Schema(4) + IssuerVK(32) + Fields(101) + Signature(64).
const CODEC_MIN_SIZE: usize = 4 + 32 + CODEC_FIXED_FIELDS_SIZE + 64;

impl Schema for LooseCommit {
    const PREFIX: [u8; 2] = schema::SEDIMENTREE_PREFIX;
    const TYPE_BYTE: u8 = b'C';
    const VERSION: u8 = 0;
}

impl Encode for LooseCommit {
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        encode::array(self.sedimentree_id.as_bytes(), buf);
        encode::array(self.digest().as_bytes(), buf);
        encode::array(self.blob_meta().digest().as_bytes(), buf);

        #[allow(clippy::cast_possible_truncation)]
        encode::u8(self.parents().len() as u8, buf);

        #[allow(clippy::cast_possible_truncation)]
        encode::u32(self.blob_meta().size_bytes() as u32, buf);

        for parent in self.parents() {
            encode::array(parent.as_bytes(), buf);
        }
    }

    fn fields_size(&self) -> usize {
        CODEC_FIXED_FIELDS_SIZE + (self.parents().len() * 32)
    }
}

impl Decode for LooseCommit {
    const MIN_SIZE: usize = CODEC_MIN_SIZE;

    fn try_decode_fields(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < CODEC_FIXED_FIELDS_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: "LooseCommit",
                need: CODEC_FIXED_FIELDS_SIZE,
                have: buf.len(),
            });
        }

        let mut offset = 0;

        let sedimentree_id_bytes: [u8; 32] = decode::array(buf, offset)?;
        let sedimentree_id = SedimentreeId::new(sedimentree_id_bytes);
        offset += 32;

        let digest_bytes: [u8; 32] = decode::array(buf, offset)?;
        let digest = Digest::<LooseCommit>::from_bytes(digest_bytes);
        offset += 32;

        let blob_digest_bytes: [u8; 32] = decode::array(buf, offset)?;
        let blob_digest = Digest::<Blob>::from_bytes(blob_digest_bytes);
        offset += 32;

        let parent_count = decode::u8(buf, offset)? as usize;
        offset += 1;

        let blob_size = decode::u32(buf, offset)? as u64;
        offset += 4;

        let parents_size = parent_count * 32;
        if buf.len() < offset + parents_size {
            return Err(BufferTooShort {
                reading: ReadingType::Slice { len: parents_size },
                offset,
                need: parents_size,
                have: buf.len().saturating_sub(offset),
            }
            .into());
        }

        let mut parent_arrays: Vec<[u8; 32]> = Vec::with_capacity(parent_count);
        for _ in 0..parent_count {
            let parent_bytes: [u8; 32] = decode::array(buf, offset)?;
            parent_arrays.push(parent_bytes);
            offset += 32;
        }

        decode::verify_sorted(&parent_arrays)?;

        let parents: BTreeSet<Digest<LooseCommit>> =
            parent_arrays.into_iter().map(Digest::from_bytes).collect();

        let blob_meta = BlobMeta::from_digest_size(blob_digest, blob_size);

        Ok(LooseCommit::new(sedimentree_id, digest, parents, blob_meta))
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    use alloc::vec;

    fn make_digest<T: 'static>(byte: u8) -> Digest<T> {
        Digest::from_bytes([byte; 32])
    }

    fn make_sedimentree_id(byte: u8) -> SedimentreeId {
        SedimentreeId::new([byte; 32])
    }

    #[test]
    fn codec_round_trip_no_parents() {
        let id = make_sedimentree_id(0x01);
        let commit = LooseCommit::new(
            id,
            make_digest(0x10),
            BTreeSet::new(),
            BlobMeta::from_digest_size(make_digest(0x20), 1024),
        );

        let mut buf = Vec::new();
        commit.encode_fields(&mut buf);
        assert_eq!(buf.len(), CODEC_FIXED_FIELDS_SIZE);

        let decoded = LooseCommit::try_decode_fields(&buf).expect("decode should succeed");
        assert_eq!(decoded, commit);
    }

    #[test]
    fn codec_round_trip_with_parents() {
        let id = make_sedimentree_id(0x01);
        let parents = BTreeSet::from([make_digest(0x30), make_digest(0x40), make_digest(0x50)]);
        let commit = LooseCommit::new(
            id,
            make_digest(0x10),
            parents,
            BlobMeta::from_digest_size(make_digest(0x20), 2048),
        );

        let mut buf = Vec::new();
        commit.encode_fields(&mut buf);
        assert_eq!(buf.len(), CODEC_FIXED_FIELDS_SIZE + 3 * 32);

        let decoded = LooseCommit::try_decode_fields(&buf).expect("decode should succeed");
        assert_eq!(decoded, commit);
    }

    #[test]
    fn codec_unsorted_parents_rejected() {
        let id = make_sedimentree_id(0x01);

        let mut buf = Vec::new();
        encode::array(id.as_bytes(), &mut buf);
        encode::array(&[0x10; 32], &mut buf);
        encode::array(&[0x20; 32], &mut buf);
        encode::u8(2, &mut buf);
        encode::u32(1024, &mut buf);
        encode::array(&[0x50; 32], &mut buf);
        encode::array(&[0x30; 32], &mut buf);

        let result = LooseCommit::try_decode_fields(&buf);
        assert!(matches!(result, Err(DecodeError::UnsortedArray(_))));
    }

    #[test]
    fn codec_buffer_too_short_rejected() {
        let buf = vec![0u8; 50];

        let result = LooseCommit::try_decode_fields(&buf);
        assert!(matches!(result, Err(DecodeError::MessageTooShort { .. })));
    }

    #[test]
    fn codec_schema_is_correct() {
        assert_eq!(LooseCommit::SCHEMA, *b"STC\x00");
    }

    #[test]
    fn codec_min_size_is_correct() {
        assert_eq!(LooseCommit::MIN_SIZE, 201);
    }
}
