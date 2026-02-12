//! Fragment types for Sedimentree data partitioning.

pub mod id;

use alloc::{collections::BTreeSet, vec::Vec};
use id::FragmentId;

use crate::{
    blob::BlobMeta,
    crypto::digest::Digest,
    depth::{Depth, DepthMetric},
    id::SedimentreeId,
    loose_commit::LooseCommit,
};

/// A portion of a Sedimentree that includes a set of checkpoints.
///
/// This is created by breaking up (fragmenting) a larger document or log
/// into smaller pieces (a "fragment"). Since Sedimentree is not able to
/// read the content in a particular fragment (e.g. because it's in
/// an arbitrary format or is encrypted), it maintains some basic
/// metadata about the the content to aid in deduplication and synchronization.
///
/// The [`FragmentId`] is precomputed at construction and cached. It is
/// _not_ serialized — on deserialization it is recomputed from `head`
/// and `boundary`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Fragment {
    summary: FragmentSummary,
    checkpoints: Vec<Digest<LooseCommit>>,
    digest: Digest<Fragment>,

    /// Precomputed causal identity. Not serialized — recomputed on decode.
    causal_id: FragmentId,
}

impl<Ctx> minicbor::Encode<Ctx> for Fragment {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        ctx: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        e.map(3)?;
        e.u32(0)?;
        self.summary.encode(e, ctx)?;
        e.u32(1)?;
        self.checkpoints.encode(e, ctx)?;
        e.u32(2)?;
        self.digest.encode(e, ctx)?;
        Ok(())
    }
}

impl<'b, Ctx> minicbor::Decode<'b, Ctx> for Fragment {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        ctx: &mut Ctx,
    ) -> Result<Self, minicbor::decode::Error> {
        let len = d.map()?;
        let mut summary = None;
        let mut checkpoints = None;
        let mut digest = None;

        let count = len.unwrap_or(0);
        for _ in 0..count {
            match d.u32()? {
                0 => summary = Some(FragmentSummary::decode(d, ctx)?),
                1 => checkpoints = Some(Vec::<Digest<LooseCommit>>::decode(d, ctx)?),
                2 => digest = Some(Digest::<Fragment>::decode(d, ctx)?),
                _ => {
                    d.skip()?;
                }
            }
        }

        let summary =
            summary.ok_or_else(|| minicbor::decode::Error::message("missing field: summary"))?;
        let checkpoints = checkpoints
            .ok_or_else(|| minicbor::decode::Error::message("missing field: checkpoints"))?;
        let digest =
            digest.ok_or_else(|| minicbor::decode::Error::message("missing field: digest"))?;

        let causal_id = FragmentId::new(summary.head(), &summary.boundary);

        Ok(Self {
            summary,
            checkpoints,
            digest,
            causal_id,
        })
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Fragment {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let summary = FragmentSummary::arbitrary(u)?;
        let checkpoints = Vec::<Digest<LooseCommit>>::arbitrary(u)?;
        let digest = Digest::<Fragment>::arbitrary(u)?;
        let causal_id = FragmentId::new(summary.head(), &summary.boundary);
        Ok(Self {
            summary,
            checkpoints,
            digest,
            causal_id,
        })
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Fragment {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("Fragment", 3)?;
        s.serialize_field("summary", &self.summary)?;
        s.serialize_field("checkpoints", &self.checkpoints)?;
        s.serialize_field("digest", &self.digest)?;
        s.end()
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Fragment {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct FragmentFields {
            summary: FragmentSummary,
            checkpoints: Vec<Digest<LooseCommit>>,
            digest: Digest<Fragment>,
        }

        let fields = FragmentFields::deserialize(deserializer)?;
        let causal_id = FragmentId::new(fields.summary.head(), &fields.summary.boundary);
        Ok(Self {
            summary: fields.summary,
            checkpoints: fields.checkpoints,
            digest: fields.digest,
            causal_id,
        })
    }
}

impl Fragment {
    /// Constructor for a [`Fragment`].
    #[must_use]
    pub fn new(
        head: Digest<LooseCommit>,
        boundary: BTreeSet<Digest<LooseCommit>>,
        checkpoints: Vec<Digest<LooseCommit>>,
        blob_meta: BlobMeta,
    ) -> Self {
        let digest = {
            let mut hasher = blake3::Hasher::new();
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

        let causal_id = FragmentId::new(head, &boundary);

        Self {
            summary: FragmentSummary {
                head,
                boundary,
                blob_meta,
            },
            checkpoints,
            digest,
            causal_id,
        }
    }

    /// Returns true if this fragment supports the given fragment summary.
    #[must_use]
    pub fn supports<M: DepthMetric>(&self, other: &FragmentSummary, hash_metric: &M) -> bool {
        if &self.summary == other {
            return true;
        }

        if self.depth(hash_metric) < other.depth(hash_metric) {
            return false;
        }

        if self.summary.head == other.head
            && other
                .boundary
                .iter()
                .all(|end| self.checkpoints.contains(end))
        {
            return true;
        }

        if self.checkpoints.contains(&other.head)
            && other
                .boundary
                .iter()
                .all(|end| self.checkpoints.contains(end))
        {
            return true;
        }

        if self.checkpoints.contains(&other.head)
            && self.summary.boundary.is_superset(&other.boundary)
        {
            return true;
        }

        false
    }

    /// Returns true if this [`Fragment`] covers the given [`Digest`].
    #[must_use]
    pub fn supports_block(&self, fragment_end: Digest<LooseCommit>) -> bool {
        self.checkpoints.contains(&fragment_end) || self.summary.boundary.contains(&fragment_end)
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

    /// The inner checkpoints of the fragment.
    #[must_use]
    pub const fn checkpoints(&self) -> &Vec<Digest<LooseCommit>> {
        &self.checkpoints
    }

    /// The unique [`Digest`] of this [`Fragment`], derived from its content.
    #[must_use]
    pub const fn digest(&self) -> Digest<Fragment> {
        self.digest
    }

    /// The precomputed causal identity of this fragment.
    #[must_use]
    pub const fn fragment_id(&self) -> FragmentId {
        self.causal_id
    }
}

/// The minimal data for a [`Fragment`].
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FragmentSummary {
    #[n(0)]
    head: Digest<LooseCommit>,
    #[n(1)]
    boundary: BTreeSet<Digest<LooseCommit>>,
    #[n(2)]
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
    checkpoints: Vec<Digest<LooseCommit>>,
    boundary: BTreeSet<Digest<LooseCommit>>,
}

impl FragmentSpec {
    /// Constructor for a [`FragmentSpec`].
    #[must_use]
    pub const fn new(
        id: SedimentreeId,
        head: Digest<LooseCommit>,
        checkpoints: Vec<Digest<LooseCommit>>,
        boundary: BTreeSet<Digest<LooseCommit>>,
    ) -> Self {
        Self {
            id,
            head,
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

    /// The inner checkpoints of the fragment.
    #[must_use]
    pub const fn checkpoints(&self) -> &Vec<Digest<LooseCommit>> {
        &self.checkpoints
    }
}
