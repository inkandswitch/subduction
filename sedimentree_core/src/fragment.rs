//! Fragment types for Sedimentree data partitioning.

pub mod checkpoint;
pub mod id;

use alloc::{collections::BTreeSet, vec::Vec};

use checkpoint::Checkpoint;
use id::FragmentId;

use crate::{
    blob::{has_meta::HasBlobMeta, Blob, BlobMeta},
    codec::{
        decode::{self, Decode},
        encode::{self, Encode},
        error::{BufferTooShort, DecodeError, ReadingType},
        schema::{self, Schema},
    },
    crypto::{
        digest::Digest,
        fingerprint::{Fingerprint, FingerprintSeed},
    },
    depth::{Depth, DepthMetric},
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};

/// A portion of a Sedimentree that includes a set of checkpoints.
///
/// This is created by breaking up (fragmenting) a larger document or log
/// into smaller pieces (a "fragment"). Since Sedimentree is not able to
/// read the content in a particular fragment (e.g. because it's in
/// an arbitrary format or is encrypted), it maintains some basic
/// metadata about the the content to aid in deduplication and synchronization.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Fragment {
    sedimentree_id: SedimentreeId,
    summary: FragmentSummary,
    checkpoints: BTreeSet<Checkpoint>,
    digest: Digest<Fragment>,
}

impl Fragment {
    /// Constructor for a [`Fragment`].
    ///
    /// The `checkpoints` are raw commit digests that fall within the fragment's
    /// range. They are truncated to 12 bytes internally for compact storage.
    #[must_use]
    pub fn new(
        sedimentree_id: SedimentreeId,
        head: Digest<LooseCommit>,
        boundary: BTreeSet<Digest<LooseCommit>>,
        checkpoints: &[Digest<LooseCommit>],
        blob_meta: BlobMeta,
    ) -> Self {
        let truncated_checkpoints: BTreeSet<Checkpoint> =
            checkpoints.iter().map(|d| Checkpoint::new(*d)).collect();

        Self::from_parts(
            sedimentree_id,
            head,
            boundary,
            truncated_checkpoints,
            blob_meta,
        )
    }

    /// Constructor from pre-truncated checkpoints.
    ///
    /// Used by codec decoding where checkpoints are already in truncated form.
    #[must_use]
    pub fn from_parts(
        sedimentree_id: SedimentreeId,
        head: Digest<LooseCommit>,
        boundary: BTreeSet<Digest<LooseCommit>>,
        checkpoints: BTreeSet<Checkpoint>,
        blob_meta: BlobMeta,
    ) -> Self {
        let digest = {
            let mut hasher = blake3::Hasher::new();
            hasher.update(sedimentree_id.as_bytes());
            hasher.update(head.as_bytes());

            for end in &boundary {
                hasher.update(end.as_bytes());
            }
            hasher.update(blob_meta.digest().as_bytes());

            for checkpoint in &checkpoints {
                hasher.update(checkpoint.as_bytes());
            }

            Digest::from_bytes(*hasher.finalize().as_bytes())
        };

        Self {
            sedimentree_id,
            summary: FragmentSummary {
                head,
                boundary,
                blob_meta,
            },
            checkpoints,
            digest,
        }
    }

    /// Returns true if this fragment supports the given fragment summary.
    ///
    /// A fragment supports another if:
    /// 1. They are identical, OR
    /// 2. This fragment is deeper (or equal depth) AND the other's entire
    ///    range falls within this fragment's range.
    ///
    /// Range containment is checked by verifying the other's head and boundary
    /// are all within this fragment (via `supports_block`), or the other's
    /// boundary is a subset of this fragment's boundary.
    #[must_use]
    pub fn supports<M: DepthMetric>(&self, other: &FragmentSummary, hash_metric: &M) -> bool {
        // Identical fragments
        if &self.summary == other {
            return true;
        }

        // Shallower can never support deeper
        if self.depth(hash_metric) < other.depth(hash_metric) {
            return false;
        }

        // Other's head must be within our range
        if !self.supports_block(other.head) {
            return false;
        }

        // Other's boundary must be within our range OR a subset of our boundary
        other.boundary.iter().all(|b| self.supports_block(*b))
            || self.summary.boundary.is_superset(&other.boundary)
    }

    /// Returns true if this [`Fragment`] supports the given commit digest.
    ///
    /// A commit is supported if it falls within the fragment's range:
    /// - The head (top of range)
    /// - Any checkpoint (interior, truncated comparison)
    /// - Any boundary commit (bottom of range)
    #[must_use]
    pub fn supports_block(&self, digest: Digest<LooseCommit>) -> bool {
        self.summary.head == digest
            || self.checkpoints.contains(&Checkpoint::new(digest))
            || self.summary.boundary.contains(&digest)
    }

    /// Convert to a [`FragmentSummary`].
    #[must_use]
    pub const fn summary(&self) -> &FragmentSummary {
        &self.summary
    }

    /// The depth of this stratum, determined by the number of leading zeros.
    #[must_use]
    pub fn depth<M: DepthMetric>(&self, hash_metric: &M) -> Depth {
        self.summary.depth(hash_metric)
    }

    /// The head of the fragment.
    #[must_use]
    pub const fn head(&self) -> Digest<LooseCommit> {
        self.summary.head
    }

    /// The (possibly ragged) end(s) of the fragment.
    #[must_use]
    pub const fn boundary(&self) -> &BTreeSet<Digest<LooseCommit>> {
        &self.summary.boundary
    }

    /// The checkpoint set for compact covering checks.
    #[must_use]
    pub const fn checkpoints(&self) -> &BTreeSet<Checkpoint> {
        &self.checkpoints
    }

    /// The unique [`Digest`] of this [`Fragment`], derived from its content.
    #[must_use]
    pub const fn digest(&self) -> Digest<Fragment> {
        self.digest
    }
    /// The causal identity of this fragment (its head digest).
    #[must_use]
    pub const fn fragment_id(&self) -> FragmentId {
        FragmentId::new(self.summary.head)
    }

    /// The [`SedimentreeId`] this fragment belongs to.
    #[must_use]
    pub const fn sedimentree_id(&self) -> SedimentreeId {
        self.sedimentree_id
    }
}

impl HasBlobMeta for Fragment {
    type Args = (
        SedimentreeId,
        Digest<LooseCommit>,
        BTreeSet<Digest<LooseCommit>>,
        Vec<Digest<LooseCommit>>,
    );

    fn blob_meta(&self) -> BlobMeta {
        self.summary().blob_meta()
    }

    fn from_args(
        (sedimentree_id, head, boundary, checkpoints): Self::Args,
        blob_meta: BlobMeta,
    ) -> Self {
        Self::new(sedimentree_id, head, boundary, &checkpoints, blob_meta)
    }
}

/// The minimal data for a [`Fragment`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FragmentSummary {
    head: Digest<LooseCommit>,
    boundary: BTreeSet<Digest<LooseCommit>>,
    blob_meta: BlobMeta,
}

impl FragmentSummary {
    /// Constructor for a [`FragmentSummary`].
    #[must_use]
    pub const fn new(
        head: Digest<LooseCommit>,
        boundary: BTreeSet<Digest<LooseCommit>>,
        blob_meta: BlobMeta,
    ) -> Self {
        Self {
            head,
            boundary,
            blob_meta,
        }
    }

    /// The head of the fragment.
    #[must_use]
    pub const fn head(&self) -> Digest<LooseCommit> {
        self.head
    }

    /// The (possibly ragged) end(s) of the fragment.
    #[must_use]
    pub const fn boundary(&self) -> &BTreeSet<Digest<LooseCommit>> {
        &self.boundary
    }

    /// Basic information about the payload blob.
    #[must_use]
    pub const fn blob_meta(&self) -> BlobMeta {
        self.blob_meta
    }

    /// The depth of this stratum, determined by the number of leading zeros.
    #[must_use]
    pub fn depth<M: DepthMetric>(&self, hash_metric: &M) -> Depth {
        hash_metric.to_depth(self.head)
    }
}

/// The barest information needed to identify a fragment.
#[derive(Debug, Clone)]
pub struct FragmentSpec {
    id: SedimentreeId,
    head: Digest<LooseCommit>,
    seed: FingerprintSeed,
    checkpoints: BTreeSet<Fingerprint<CommitId>>,
    boundary: BTreeSet<Digest<LooseCommit>>,
}

impl FragmentSpec {
    /// Constructor for a [`FragmentSpec`].
    #[must_use]
    pub const fn new(
        id: SedimentreeId,
        head: Digest<LooseCommit>,
        seed: FingerprintSeed,
        checkpoints: BTreeSet<Fingerprint<CommitId>>,
        boundary: BTreeSet<Digest<LooseCommit>>,
    ) -> Self {
        Self {
            id,
            head,
            seed,
            checkpoints,
            boundary,
        }
    }

    /// The sedimentree ID.
    #[must_use]
    pub const fn id(&self) -> SedimentreeId {
        self.id
    }

    /// The head of the fragment.
    #[must_use]
    pub const fn head(&self) -> Digest<LooseCommit> {
        self.head
    }

    /// The (possibly ragged) end(s) of the fragment.
    #[must_use]
    pub const fn boundary(&self) -> &BTreeSet<Digest<LooseCommit>> {
        &self.boundary
    }

    /// The fingerprint seed used for checkpoint fingerprints.
    #[must_use]
    pub const fn seed(&self) -> &FingerprintSeed {
        &self.seed
    }

    /// The inner checkpoint fingerprints of the fragment.
    #[must_use]
    pub const fn checkpoints(&self) -> &BTreeSet<Fingerprint<CommitId>> {
        &self.checkpoints
    }
}

/// Fixed fields size: SedimentreeId(32) + Head(32) + Digest<Blob>(32) + |Boundary|(1) + |Checkpoints|(2) + BlobSize(4).
const CODEC_FIXED_FIELDS_SIZE: usize = 32 + 32 + 32 + 1 + 2 + 4;

/// Minimum signed message size: Schema(4) + IssuerVK(32) + Fields(103) + Signature(64).
const CODEC_MIN_SIZE: usize = 4 + 32 + CODEC_FIXED_FIELDS_SIZE + 64;

/// Checkpoint truncation size in bytes.
const CHECKPOINT_BYTES: usize = 12;

impl Schema for Fragment {
    const PREFIX: [u8; 2] = schema::SEDIMENTREE_PREFIX;
    const TYPE_BYTE: u8 = b'F';
    const VERSION: u8 = 0;
}

impl Encode for Fragment {
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        encode::array(self.sedimentree_id.as_bytes(), buf);
        encode::array(self.head().as_bytes(), buf);
        encode::array(self.summary().blob_meta().digest().as_bytes(), buf);

        #[allow(clippy::cast_possible_truncation)]
        encode::u8(self.boundary().len() as u8, buf);

        #[allow(clippy::cast_possible_truncation)]
        encode::u16(self.checkpoints().len() as u16, buf);

        #[allow(clippy::cast_possible_truncation)]
        encode::u32(self.summary().blob_meta().size_bytes() as u32, buf);

        for boundary in self.boundary() {
            encode::array(boundary.as_bytes(), buf);
        }

        for checkpoint in self.checkpoints() {
            encode::array(checkpoint.as_bytes(), buf);
        }
    }

    fn fields_size(&self) -> usize {
        CODEC_FIXED_FIELDS_SIZE
            + (self.boundary().len() * 32)
            + (self.checkpoints().len() * CHECKPOINT_BYTES)
    }
}

impl Decode for Fragment {
    const MIN_SIZE: usize = CODEC_MIN_SIZE;

    fn try_decode_fields(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < CODEC_FIXED_FIELDS_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: "Fragment",
                need: CODEC_FIXED_FIELDS_SIZE,
                have: buf.len(),
            });
        }

        let mut offset = 0;

        let sedimentree_id_bytes: [u8; 32] = decode::array(buf, offset)?;
        let sedimentree_id = SedimentreeId::new(sedimentree_id_bytes);
        offset += 32;

        let head_bytes: [u8; 32] = decode::array(buf, offset)?;
        let head = Digest::<LooseCommit>::from_bytes(head_bytes);
        offset += 32;

        let blob_digest_bytes: [u8; 32] = decode::array(buf, offset)?;
        let blob_digest = Digest::<Blob>::from_bytes(blob_digest_bytes);
        offset += 32;

        let boundary_count = decode::u8(buf, offset)? as usize;
        offset += 1;

        let checkpoint_count = decode::u16(buf, offset)? as usize;
        offset += 2;

        let blob_size = u64::from(decode::u32(buf, offset)?);
        offset += 4;

        let boundary_size = boundary_count * 32;
        let checkpoints_size = checkpoint_count * CHECKPOINT_BYTES;
        let total_variable_size = boundary_size + checkpoints_size;

        if buf.len() < offset + total_variable_size {
            return Err(BufferTooShort {
                reading: ReadingType::Slice {
                    len: total_variable_size,
                },
                offset,
                need: total_variable_size,
                have: buf.len().saturating_sub(offset),
            }
            .into());
        }

        let mut boundary_arrays: Vec<[u8; 32]> = Vec::with_capacity(boundary_count);
        for _ in 0..boundary_count {
            let boundary_bytes: [u8; 32] = decode::array(buf, offset)?;
            boundary_arrays.push(boundary_bytes);
            offset += 32;
        }

        decode::verify_sorted(&boundary_arrays)?;

        let boundary: BTreeSet<Digest<LooseCommit>> = boundary_arrays
            .into_iter()
            .map(Digest::from_bytes)
            .collect();

        let mut checkpoint_arrays: Vec<[u8; CHECKPOINT_BYTES]> =
            Vec::with_capacity(checkpoint_count);
        for _ in 0..checkpoint_count {
            let checkpoint_bytes: [u8; CHECKPOINT_BYTES] = decode::array(buf, offset)?;
            checkpoint_arrays.push(checkpoint_bytes);
            offset += CHECKPOINT_BYTES;
        }

        decode::verify_sorted(&checkpoint_arrays)?;

        let checkpoints: BTreeSet<Checkpoint> = checkpoint_arrays
            .into_iter()
            .map(Checkpoint::from_bytes)
            .collect();

        let blob_meta = BlobMeta::from_digest_size(blob_digest, blob_size);

        Ok(Fragment::from_parts(
            sedimentree_id,
            head,
            boundary,
            checkpoints,
            blob_meta,
        ))
    }
}

// ============================================================================
// Local Storage Encoding
// ============================================================================

#[cfg(test)]
mod codec_tests {
    use super::*;
    use alloc::vec;
    use testresult::TestResult;

    fn make_digest<T: 'static>(byte: u8) -> Digest<T> {
        Digest::from_bytes([byte; 32])
    }

    fn make_sedimentree_id(byte: u8) -> SedimentreeId {
        SedimentreeId::new([byte; 32])
    }

    fn make_checkpoint(byte: u8) -> Checkpoint {
        Checkpoint::new(make_digest(byte))
    }

    #[test]
    fn codec_round_trip_empty() -> TestResult {
        let sedimentree_id = make_sedimentree_id(0x01);
        let fragment = Fragment::from_parts(
            sedimentree_id,
            make_digest(0x10),
            BTreeSet::new(),
            BTreeSet::new(),
            BlobMeta::from_digest_size(make_digest(0x20), 1024),
        );

        let mut buf = Vec::new();
        fragment.encode_fields(&mut buf);
        assert_eq!(buf.len(), CODEC_FIXED_FIELDS_SIZE);

        let decoded = Fragment::try_decode_fields(&buf)?;
        assert_eq!(decoded.sedimentree_id(), fragment.sedimentree_id());
        assert_eq!(decoded.head(), fragment.head());
        assert_eq!(decoded.boundary(), fragment.boundary());
        assert_eq!(decoded.checkpoints(), fragment.checkpoints());
        Ok(())
    }

    #[test]
    fn codec_round_trip_with_data() -> TestResult {
        let sedimentree_id = make_sedimentree_id(0x01);
        let boundary = BTreeSet::from([make_digest(0x30), make_digest(0x40)]);
        let checkpoints = BTreeSet::from([make_checkpoint(0x50), make_checkpoint(0x60)]);

        let fragment = Fragment::from_parts(
            sedimentree_id,
            make_digest(0x10),
            boundary,
            checkpoints,
            BlobMeta::from_digest_size(make_digest(0x20), 2048),
        );

        let mut buf = Vec::new();
        fragment.encode_fields(&mut buf);

        let decoded = Fragment::try_decode_fields(&buf)?;
        assert_eq!(decoded.sedimentree_id(), fragment.sedimentree_id());
        assert_eq!(decoded.head(), fragment.head());
        assert_eq!(decoded.boundary(), fragment.boundary());
        assert_eq!(decoded.checkpoints(), fragment.checkpoints());
        Ok(())
    }

    #[test]
    fn codec_unsorted_boundary_rejected() {
        let sedimentree_id = make_sedimentree_id(0x01);

        let mut buf = Vec::new();
        encode::array(sedimentree_id.as_bytes(), &mut buf);
        encode::array(&[0x10; 32], &mut buf);
        encode::array(&[0x20; 32], &mut buf);
        encode::u8(2, &mut buf);
        encode::u16(0, &mut buf);
        encode::u32(1024, &mut buf);
        encode::array(&[0x50; 32], &mut buf);
        encode::array(&[0x30; 32], &mut buf);

        let result = Fragment::try_decode_fields(&buf);
        assert!(matches!(result, Err(DecodeError::UnsortedArray(_))));
    }

    #[test]
    fn codec_buffer_too_short_rejected() {
        let buf = vec![0u8; 50];

        let result = Fragment::try_decode_fields(&buf);
        assert!(matches!(result, Err(DecodeError::MessageTooShort { .. })));
    }

    #[test]
    fn codec_schema_is_correct() {
        assert_eq!(Fragment::SCHEMA, *b"STF\x00");
    }

    #[test]
    fn codec_min_size_is_correct() {
        assert_eq!(Fragment::MIN_SIZE, 203);
    }
}

#[cfg(test)]
mod tests {
    use alloc::collections::BTreeSet;

    use crate::{
        blob::BlobMeta,
        commit::CountLeadingZeroBytes,
        crypto::digest::Digest,
        fragment::{Fragment, FragmentSummary},
        id::SedimentreeId,
        loose_commit::LooseCommit,
        test_utils::digest_with_depth,
    };

    fn make_fragment(
        head: Digest<LooseCommit>,
        boundary: BTreeSet<Digest<LooseCommit>>,
        checkpoints: &[Digest<LooseCommit>],
    ) -> Fragment {
        let sedimentree_id = SedimentreeId::new([0x42; 32]);
        let blob_meta = BlobMeta::new(&[1]);
        Fragment::new(sedimentree_id, head, boundary, checkpoints, blob_meta)
    }

    // ============================================================
    // supports_block() tests
    // ============================================================

    #[test]
    fn supports_block_head() {
        let head = digest_with_depth(2, 1);
        let boundary = digest_with_depth(1, 100);
        let fragment = make_fragment(head, BTreeSet::from([boundary]), &[]);

        // Head should be supported
        assert!(fragment.supports_block(head));
    }

    #[test]
    fn supports_block_boundary() {
        let head = digest_with_depth(2, 1);
        let boundary = digest_with_depth(1, 100);
        let fragment = make_fragment(head, BTreeSet::from([boundary]), &[]);

        // Boundary should be supported
        assert!(fragment.supports_block(boundary));
    }

    #[test]
    fn supports_block_checkpoint() {
        let head = digest_with_depth(2, 1);
        let boundary = digest_with_depth(1, 100);
        let checkpoint = digest_with_depth(1, 50);
        let fragment = make_fragment(head, BTreeSet::from([boundary]), &[checkpoint]);

        // Checkpoint should be supported (via truncated match)
        assert!(fragment.supports_block(checkpoint));
    }

    #[test]
    fn supports_block_unknown_digest() {
        let head = digest_with_depth(2, 1);
        let boundary = digest_with_depth(1, 100);
        let fragment = make_fragment(head, BTreeSet::from([boundary]), &[]);

        let unknown = digest_with_depth(1, 200);
        assert!(!fragment.supports_block(unknown));
    }

    // ============================================================
    // supports() tests
    // ============================================================

    #[test]
    fn supports_self() {
        let head = digest_with_depth(2, 1);
        let boundary = digest_with_depth(1, 100);
        let fragment = make_fragment(head, BTreeSet::from([boundary]), &[]);

        // Fragment should support its own summary
        assert!(fragment.supports(fragment.summary(), &CountLeadingZeroBytes));
    }

    #[test]
    fn deeper_supports_shallower_with_matching_range() {
        // Deep fragment (depth 3)
        let deep_head = digest_with_depth(3, 1);
        let deep_boundary = digest_with_depth(1, 100);
        let shallow_head = digest_with_depth(2, 1);
        let shallow_boundary = digest_with_depth(1, 101);

        // Deep fragment has shallow's head and boundary in checkpoints
        let deep = make_fragment(
            deep_head,
            BTreeSet::from([deep_boundary]),
            &[shallow_head, shallow_boundary],
        );

        // Shallow fragment (depth 2)
        let shallow_summary = FragmentSummary::new(
            shallow_head,
            BTreeSet::from([shallow_boundary]),
            BlobMeta::new(&[2]),
        );

        assert!(deep.supports(&shallow_summary, &CountLeadingZeroBytes));
    }

    #[test]
    fn shallower_never_supports_deeper() {
        // Shallow fragment (depth 2)
        let shallow_head = digest_with_depth(2, 1);
        let shallow_boundary = digest_with_depth(1, 100);
        let shallow = make_fragment(shallow_head, BTreeSet::from([shallow_boundary]), &[]);

        // Deep fragment summary (depth 3)
        let deep_head = digest_with_depth(3, 1);
        let deep_boundary = digest_with_depth(1, 101);
        let deep_summary = FragmentSummary::new(
            deep_head,
            BTreeSet::from([deep_boundary]),
            BlobMeta::new(&[2]),
        );

        // Shallow should never support deeper, regardless of range
        assert!(!shallow.supports(&deep_summary, &CountLeadingZeroBytes));
    }

    #[test]
    fn same_depth_partial_overlap_no_support() {
        // Two fragments at depth 2 with different heads
        let head1 = digest_with_depth(2, 1);
        let boundary1 = digest_with_depth(1, 100);
        let fragment1 = make_fragment(head1, BTreeSet::from([boundary1]), &[]);

        let head2 = digest_with_depth(2, 2);
        let boundary2 = digest_with_depth(1, 101);
        let summary2 =
            FragmentSummary::new(head2, BTreeSet::from([boundary2]), BlobMeta::new(&[2]));

        // Neither should support the other
        assert!(!fragment1.supports(&summary2, &CountLeadingZeroBytes));
    }

    #[test]
    fn supports_boundary_subset() {
        // Fragment with boundary {A, B}
        let head = digest_with_depth(2, 1);
        let boundary_a = digest_with_depth(1, 100);
        let boundary_b = digest_with_depth(1, 101);
        let fragment = make_fragment(head, BTreeSet::from([boundary_a, boundary_b]), &[]);

        // Summary with same head but boundary subset {A}
        let summary = FragmentSummary::new(head, BTreeSet::from([boundary_a]), BlobMeta::new(&[2]));

        // Should support (boundary is subset)
        assert!(fragment.supports(&summary, &CountLeadingZeroBytes));
    }

    #[test]
    fn supports_head_in_checkpoints_boundary_in_checkpoints() {
        // Deep fragment
        let deep_head = digest_with_depth(3, 1);
        let deep_boundary = digest_with_depth(1, 100);
        let checkpoint1 = digest_with_depth(2, 50);
        let checkpoint2 = digest_with_depth(1, 51);

        let deep = make_fragment(
            deep_head,
            BTreeSet::from([deep_boundary]),
            &[checkpoint1, checkpoint2],
        );

        // Shallow summary where head and boundary are both in deep's checkpoints
        let shallow_summary = FragmentSummary::new(
            checkpoint1,
            BTreeSet::from([checkpoint2]),
            BlobMeta::new(&[2]),
        );

        assert!(deep.supports(&shallow_summary, &CountLeadingZeroBytes));
    }

    #[test]
    fn no_support_when_head_not_in_range() {
        let deep_head = digest_with_depth(3, 1);
        let deep_boundary = digest_with_depth(1, 100);
        let deep = make_fragment(deep_head, BTreeSet::from([deep_boundary]), &[]);

        // Shallow with head not in deep's range
        let other_head = digest_with_depth(2, 99);
        let shallow_summary = FragmentSummary::new(
            other_head,
            BTreeSet::from([deep_boundary]), // boundary matches, but head doesn't
            BlobMeta::new(&[2]),
        );

        assert!(!deep.supports(&shallow_summary, &CountLeadingZeroBytes));
    }

    #[test]
    fn no_support_when_boundary_not_in_range() {
        let deep_head = digest_with_depth(3, 1);
        let deep_boundary = digest_with_depth(1, 100);
        let shallow_head = digest_with_depth(2, 50);

        // Deep has shallow's head in checkpoints
        let deep = make_fragment(deep_head, BTreeSet::from([deep_boundary]), &[shallow_head]);

        // Shallow boundary is NOT in deep's range
        let other_boundary = digest_with_depth(1, 200);
        let shallow_summary = FragmentSummary::new(
            shallow_head,
            BTreeSet::from([other_boundary]),
            BlobMeta::new(&[2]),
        );

        assert!(!deep.supports(&shallow_summary, &CountLeadingZeroBytes));
    }

    #[cfg(feature = "bolero")]
    mod proptests {
        use crate::{commit::CountLeadingZeroBytes, fragment::Fragment};

        #[test]
        fn supports_self() {
            bolero::check!()
                .with_arbitrary::<Fragment>()
                .for_each(|fragment| {
                    assert!(
                        fragment.supports(fragment.summary(), &CountLeadingZeroBytes),
                        "every fragment should support its own summary"
                    );
                });
        }

        #[test]
        fn shallower_never_supports_deeper() {
            #[derive(Debug)]
            struct DepthPair {
                shallow: Fragment,
                deep: Fragment,
            }

            impl<'a> arbitrary::Arbitrary<'a> for DepthPair {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    let shallow: Fragment = u.arbitrary()?;
                    let deep: Fragment = u.arbitrary()?;
                    Ok(Self { shallow, deep })
                }
            }

            bolero::check!()
                .with_arbitrary::<DepthPair>()
                .for_each(|pair| {
                    let shallow_depth = pair.shallow.depth(&CountLeadingZeroBytes);
                    let deep_depth = pair.deep.depth(&CountLeadingZeroBytes);

                    if shallow_depth < deep_depth {
                        assert!(
                            !pair.shallow.supports(pair.deep.summary(), &CountLeadingZeroBytes),
                            "shallower fragment (depth {shallow_depth:?}) should never support deeper (depth {deep_depth:?})"
                        );
                    }
                });
        }

        #[test]
        fn supports_block_includes_head() {
            bolero::check!()
                .with_arbitrary::<Fragment>()
                .for_each(|fragment| {
                    assert!(
                        fragment.supports_block(fragment.head()),
                        "supports_block should always include the fragment's head"
                    );
                });
        }

        #[test]
        fn supports_block_includes_boundary() {
            bolero::check!()
                .with_arbitrary::<Fragment>()
                .for_each(|fragment| {
                    for boundary_commit in fragment.summary().boundary() {
                        assert!(
                            fragment.supports_block(*boundary_commit),
                            "supports_block should include all boundary commits"
                        );
                    }
                });
        }
    }
}
