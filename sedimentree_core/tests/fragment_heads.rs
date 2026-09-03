//! Regression tests for <https://github.com/inkandswitch/subduction/issues/286>.

use std::collections::BTreeSet;

use sedimentree_core::{
    blob::{Blob, BlobMeta},
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};

const SEDIMENTREE_ID: SedimentreeId = SedimentreeId::new([42; 32]);

const fn fragment_id(seed: u8) -> CommitId {
    let mut bytes = [0; 32];
    bytes[1] = seed;
    CommitId::new(bytes)
}

const fn deep_fragment_id(seed: u8) -> CommitId {
    let mut bytes = [0; 32];
    bytes[2] = seed;
    CommitId::new(bytes)
}

const fn loose_id(seed: u8) -> CommitId {
    CommitId::new([seed; 32])
}

fn blob_meta() -> BlobMeta {
    BlobMeta::new(&Blob::new(Vec::new()))
}

fn fragment(head: CommitId, boundary: &[CommitId]) -> Fragment {
    Fragment::new(
        SEDIMENTREE_ID,
        head,
        boundary.iter().copied().collect(),
        &[],
        blob_meta(),
    )
}

#[test]
fn fragment_contributes_its_head_instead_of_its_boundary() {
    let head = fragment_id(1);
    let boundary = fragment_id(2);
    let tree = Sedimentree::new(vec![fragment(head, &[boundary])], vec![]);

    assert_eq!(tree.heads(&CountLeadingZeroBytes), vec![head]);
}

#[test]
fn only_the_tip_of_chained_fragments_and_loose_commits_is_a_head() {
    let first_fragment_head = fragment_id(1);
    let second_fragment_head = fragment_id(2);
    let loose_tip = loose_id(3);

    let tree = Sedimentree::new(
        vec![
            fragment(first_fragment_head, &[]),
            fragment(second_fragment_head, &[first_fragment_head]),
        ],
        vec![LooseCommit::new(
            SEDIMENTREE_ID,
            loose_tip,
            BTreeSet::from([second_fragment_head]),
            blob_meta(),
        )],
    );

    assert_eq!(tree.heads(&CountLeadingZeroBytes), vec![loose_tip]);
}

#[test]
fn disconnected_components_each_contribute_a_head() {
    let fragment_head = fragment_id(1);
    let loose_parent = loose_id(2);
    let loose_tip = loose_id(3);

    let tree = Sedimentree::new(
        vec![fragment(fragment_head, &[])],
        vec![
            LooseCommit::new(SEDIMENTREE_ID, loose_parent, BTreeSet::new(), blob_meta()),
            LooseCommit::new(
                SEDIMENTREE_ID,
                loose_tip,
                BTreeSet::from([loose_parent]),
                blob_meta(),
            ),
        ],
    );

    let heads: BTreeSet<_> = tree.heads(&CountLeadingZeroBytes).into_iter().collect();
    assert_eq!(heads, BTreeSet::from([fragment_head, loose_tip]));
}

#[test]
fn fragment_checkpoint_marks_a_stored_head_as_an_ancestor() {
    let checkpoint = fragment_id(1);
    let unrelated_boundary = fragment_id(2);
    let covering_head = deep_fragment_id(3);
    let covering_fragment = Fragment::new(
        SEDIMENTREE_ID,
        covering_head,
        BTreeSet::new(),
        &[checkpoint],
        blob_meta(),
    );
    let tree = Sedimentree::new(
        vec![
            covering_fragment,
            fragment(checkpoint, &[unrelated_boundary]),
        ],
        vec![],
    );

    // The unrelated boundary keeps both fragments in the minimal tree, so
    // head computation itself must recognize the checkpoint as an ancestor.
    let minimal = tree.minimize(&CountLeadingZeroBytes);
    assert_eq!(minimal.fragments().count(), 2);
    assert_eq!(minimal.heads_assuming_minimal(), vec![covering_head]);
}
