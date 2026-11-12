//! Constructors enforce wire counts after transport-set normalization.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::collections::BTreeSet;

use sedimentree_core::{
    blob::{Blob, BlobMeta, has_meta::HasBlobMeta},
    codec::{decode::DecodeFields, encode::EncodeFields},
    fragment::{
        Fragment, InvalidFragment, MAX_BOUNDARIES, MAX_CHECKPOINTS, checkpoint::Checkpoint,
    },
    id::SedimentreeId,
    loose_commit::{InvalidLooseCommit, LooseCommit, MAX_PARENTS, id::CommitId},
};

const TREE: SedimentreeId = SedimentreeId::new([1; 32]);

fn id(n: usize) -> CommitId {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&(n as u64).to_le_bytes());
    CommitId::new(bytes)
}

fn meta() -> BlobMeta {
    BlobMeta::new(&Blob::new(vec![1, 2, 3]))
}

fn roundtrip<T: EncodeFields + DecodeFields + PartialEq + std::fmt::Debug>(value: &T) {
    let mut bytes = Vec::new();
    value.encode_fields(&mut bytes);
    let (decoded, consumed) = T::try_decode_fields(&bytes).expect("valid encoding");
    assert_eq!(&decoded, value);
    assert_eq!(consumed, bytes.len());
    assert_eq!(value.fields_size(), bytes.len());
}

#[test]
fn parent_limit_and_generic_construction() {
    let parents: BTreeSet<_> = (0..MAX_PARENTS).map(id).collect();
    let commit = LooseCommit::new(TREE, id(999), parents.clone(), meta()).expect("at limit");
    roundtrip(&commit);
    // More input entries than the limit, but still only MAX_PARENTS unique IDs.
    let repeated = parents.iter().chain(&parents).copied().collect();
    assert_eq!(
        LooseCommit::new(TREE, id(999), repeated, meta()),
        Ok(commit)
    );

    let oversized: BTreeSet<_> = (0..=MAX_PARENTS).map(id).collect();
    let error = InvalidLooseCommit::TooManyParents {
        actual: MAX_PARENTS + 1,
    };
    assert_eq!(
        LooseCommit::new(TREE, id(999), oversized.clone(), meta()),
        Err(error)
    );
    assert_eq!(
        LooseCommit::from_args((TREE, id(999), oversized), meta()),
        Err(error)
    );
}

#[test]
fn boundary_limit_in_both_fragment_constructors() {
    let boundary: BTreeSet<_> = (0..MAX_BOUNDARIES).map(id).collect();
    let fragment = Fragment::new(TREE, id(999), boundary.clone(), &[], meta()).expect("at limit");
    roundtrip(&fragment);
    assert_eq!(
        Fragment::from_parts(TREE, id(999), boundary, BTreeSet::new(), meta()),
        Ok(fragment)
    );

    let oversized: BTreeSet<_> = (0..=MAX_BOUNDARIES).map(id).collect();
    let error = InvalidFragment::TooManyBoundaries {
        actual: MAX_BOUNDARIES + 1,
    };
    assert_eq!(
        Fragment::new(TREE, id(999), oversized.clone(), &[], meta()),
        Err(error)
    );
    assert_eq!(
        Fragment::from_parts(TREE, id(999), oversized.clone(), BTreeSet::new(), meta()),
        Err(error)
    );
    assert_eq!(
        Fragment::from_args((TREE, id(999), oversized, vec![]), meta()),
        Err(error)
    );
}

#[test]
fn checkpoint_limit_after_prefix_truncation_and_deduplication() {
    let ids: Vec<_> = (0..MAX_CHECKPOINTS).map(id).collect();
    let prefixes = ids.iter().copied().map(Checkpoint::new).collect();
    let full = Fragment::new(TREE, id(999), BTreeSet::new(), &ids, meta()).expect("at limit");
    let from_prefixes =
        Fragment::from_parts(TREE, id(999), BTreeSet::new(), prefixes, meta()).expect("at limit");
    assert_eq!(full, from_prefixes);
    roundtrip(&full);

    let mut repeated = ids.clone();
    repeated.push(id(0));
    let mut collision = [0; 32];
    collision[31] = 1; // A different full ID with the same prefix as id(0).
    repeated.push(CommitId::new(collision));
    assert_eq!(
        Fragment::new(TREE, id(999), BTreeSet::new(), &repeated, meta()),
        Ok(full)
    );

    let oversized: Vec<_> = (0..=MAX_CHECKPOINTS).map(id).collect();
    let error = InvalidFragment::TooManyCheckpoints {
        actual: MAX_CHECKPOINTS + 1,
    };
    assert_eq!(
        Fragment::new(TREE, id(999), BTreeSet::new(), &oversized, meta()),
        Err(error)
    );
    assert_eq!(
        Fragment::from_parts(
            TREE,
            id(999),
            BTreeSet::new(),
            oversized.iter().copied().map(Checkpoint::new).collect(),
            meta()
        ),
        Err(error)
    );
    assert_eq!(
        Fragment::from_args((TREE, id(999), BTreeSet::new(), oversized), meta()),
        Err(error)
    );
}

#[cfg(feature = "serde")]
mod serde_validation {
    use super::*;
    use sedimentree_core::fragment::FragmentSummary;
    use serde::Serialize;

    // Match the existing serialized field layout, including sequence formats.
    #[derive(Serialize)]
    struct RawCommit {
        sedimentree_id: SedimentreeId,
        head: CommitId,
        parents: BTreeSet<CommitId>,
        blob_meta: BlobMeta,
    }

    #[derive(Serialize)]
    struct RawFragment {
        sedimentree_id: SedimentreeId,
        summary: FragmentSummary,
        checkpoints: BTreeSet<Checkpoint>,
    }

    fn assert_rejected<T: serde::de::DeserializeOwned>(raw: &impl Serialize) {
        let mut cbor = Vec::new();
        ciborium::into_writer(raw, &mut cbor).expect("serialize");
        assert!(ciborium::from_reader::<T, _>(cbor.as_slice()).is_err());
        assert!(bincode::deserialize::<T>(&bincode::serialize(raw).expect("serialize")).is_err());
    }

    #[test]
    fn deserialization_cannot_bypass_limits() {
        assert_rejected::<LooseCommit>(&RawCommit {
            sedimentree_id: TREE,
            head: id(999),
            parents: (0..=MAX_PARENTS).map(id).collect(),
            blob_meta: meta(),
        });
        assert_rejected::<Fragment>(&RawFragment {
            sedimentree_id: TREE,
            summary: FragmentSummary::new(id(999), (0..=MAX_BOUNDARIES).map(id).collect(), meta()),
            checkpoints: BTreeSet::new(),
        });
        assert_rejected::<Fragment>(&RawFragment {
            sedimentree_id: TREE,
            summary: FragmentSummary::new(id(999), BTreeSet::new(), meta()),
            checkpoints: (0..=MAX_CHECKPOINTS)
                .map(|n| Checkpoint::new(id(n)))
                .collect(),
        });
    }

    fn assert_roundtrip<
        T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    >(
        value: &T,
    ) {
        let mut cbor = Vec::new();
        ciborium::into_writer(value, &mut cbor).expect("serialize");
        assert_eq!(
            ciborium::from_reader::<T, _>(cbor.as_slice()).expect("deserialize"),
            *value
        );
        let binary = bincode::serialize(value).expect("serialize");
        assert_eq!(
            bincode::deserialize::<T>(&binary).expect("deserialize"),
            *value
        );
    }

    #[test]
    fn valid_serde_layout_is_unchanged() {
        let parents = (0..MAX_PARENTS).map(id).collect();
        let commit = LooseCommit::new(TREE, id(999), parents, meta()).expect("at limit");
        assert_roundtrip(&commit);
        let raw = RawCommit {
            sedimentree_id: TREE,
            head: commit.head(),
            parents: commit.parents().clone(),
            blob_meta: meta(),
        };
        assert_eq!(
            bincode::serialize(&raw).expect("serialize"),
            bincode::serialize(&commit).expect("serialize")
        );

        let boundary = (0..MAX_BOUNDARIES).map(id).collect();
        let checkpoints = (0..MAX_CHECKPOINTS)
            .map(|n| Checkpoint::new(id(n)))
            .collect();
        let fragment =
            Fragment::from_parts(TREE, id(999), boundary, checkpoints, meta()).expect("at limit");
        assert_roundtrip(&fragment);
        let raw = RawFragment {
            sedimentree_id: TREE,
            summary: fragment.summary().clone(),
            checkpoints: fragment.checkpoints().clone(),
        };
        assert_eq!(
            bincode::serialize(&raw).expect("serialize"),
            bincode::serialize(&fragment).expect("serialize")
        );
    }
}
