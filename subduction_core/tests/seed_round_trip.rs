//! Targeted seed/fingerprint sanity checks. Codec round-trip and diff
//! invariance are covered by `tests/sync_codec_props.rs` (bolero) and
//! `subduction_core::connection::message::proptests::prop_message_codec_roundtrip`.

#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::panic)]

use std::collections::BTreeSet;

use sedimentree_core::{
    blob::{Blob, BlobMeta},
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{FingerprintSummary, Sedimentree},
};

const SED_ID: SedimentreeId = SedimentreeId::new([42u8; 32]);

fn commit(seed: u8, parents: &[CommitId]) -> LooseCommit {
    let blob = Blob::new(vec![seed; 64]);
    let blob_meta = BlobMeta::new(&blob);
    let head = CommitId::new([seed; 32]);
    LooseCommit::new(
        SED_ID,
        head,
        parents.iter().copied().collect::<BTreeSet<_>>(),
        blob_meta,
    )
}

const fn head(seed: u8) -> CommitId {
    CommitId::new([seed; 32])
}

fn tree_of(commits: Vec<LooseCommit>) -> Sedimentree {
    Sedimentree::new(vec![], commits)
}

#[test]
fn different_seeds_produce_different_fingerprints() {
    let id = head(42);
    let seed_a = FingerprintSeed::new(1, 2);
    let seed_b = FingerprintSeed::new(3, 4);

    let fp_a: Fingerprint<CommitId> = Fingerprint::new(&seed_a, &id);
    let fp_b: Fingerprint<CommitId> = Fingerprint::new(&seed_b, &id);

    assert_ne!(fp_a, fp_b);
}

#[test]
fn different_values_produce_different_fingerprints() {
    let seed = FingerprintSeed::new(7, 11);
    let fp_a: Fingerprint<CommitId> = Fingerprint::new(&seed, &head(1));
    let fp_b: Fingerprint<CommitId> = Fingerprint::new(&seed, &head(2));

    assert_ne!(fp_a, fp_b);
}

/// `Fingerprint::new(seed, x)` depends only on `x`'s 32 raw bytes — it
/// must not be sensitive to any hidden state on `CommitId`.
#[test]
fn fingerprint_depends_only_on_commit_id_bytes() {
    let seed = FingerprintSeed::new(123, 456);
    let original = head(99);
    let reconstructed = CommitId::new(*original.as_bytes());

    let fp_original: Fingerprint<CommitId> = Fingerprint::new(&seed, &original);
    let fp_reconstructed: Fingerprint<CommitId> = Fingerprint::new(&seed, &reconstructed);
    assert_eq!(fp_original, fp_reconstructed);
}

/// A buggy peer sending a summary whose seed field doesn't match the
/// seed actually used to compute the fingerprints produces a fully
/// disjoint diff, isolating the seed as the failure dimension.
#[test]
fn summary_with_mismatched_seed_produces_full_disjoint_diff() {
    let real_seed = FingerprintSeed::new(0xAAAA_AAAA_AAAA_AAAA, 0xBBBB_BBBB_BBBB_BBBB);
    let claimed_seed = FingerprintSeed::new(0xCCCC_CCCC_CCCC_CCCC, 0xDDDD_DDDD_DDDD_DDDD);

    let requester_tree = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);
    let real_summary = requester_tree.fingerprint_summarize(&real_seed);

    // Same fingerprints, but the seed field lies.
    let corrupt_summary = FingerprintSummary::new(
        claimed_seed,
        real_summary.commit_fingerprints().clone(),
        real_summary.fragment_fingerprints().clone(),
    );

    let responder_tree = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);

    let diff = responder_tree.diff_remote_fingerprints(&corrupt_summary);

    assert_eq!(diff.local_only_commits.len(), 3);
    assert_eq!(diff.remote_only_commit_fingerprints.len(), 3);
}
