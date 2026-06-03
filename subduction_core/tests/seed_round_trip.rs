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

/// Fingerprinting is a pure, deterministic function of the
/// `(seed_bytes, commit_id_bytes)` tuple: the same inputs always yield the
/// same fingerprint, across arbitrary seeds and ids.
///
/// This is the *law* that the old hardcoded `assert_ne!` examples gestured
/// at but couldn't state. (We deliberately do NOT assert "different inputs ⇒
/// different fingerprints": the fingerprint is a 64-bit `SipHash`, so
/// collisions exist and that is not a universal property — see the
/// `seed_*`/`value_*` witness tests below for the non-colliding examples.)
#[test]
fn fingerprint_is_deterministic_over_arbitrary_inputs() {
    bolero::check!()
        .with_arbitrary::<(u64, u64, [u8; 32])>()
        .for_each(|&(s0, s1, id_bytes)| {
            let seed = FingerprintSeed::new(s0, s1);
            let id = CommitId::new(id_bytes);

            let fp1: Fingerprint<CommitId> = Fingerprint::new(&seed, &id);
            let fp2: Fingerprint<CommitId> = Fingerprint::new(&seed, &id);
            assert_eq!(fp1, fp2, "fingerprint must be deterministic");

            // Depends only on the raw bytes, not on `CommitId` identity.
            let id_reconstructed = CommitId::new(id_bytes);
            let fp3: Fingerprint<CommitId> = Fingerprint::new(&seed, &id_reconstructed);
            assert_eq!(fp1, fp3, "fingerprint must depend only on the id bytes");
        });
}

/// The fingerprint actually incorporates the seed: changing only the seed
/// changes the fingerprint for these specific (non-colliding) inputs.
///
/// Witness, not a law — a 64-bit hash can collide for *some* seed pair, so
/// this is a concrete demonstration that the seed participates, paired with
/// the `fingerprint_is_deterministic_over_arbitrary_inputs` property.
#[test]
fn seed_participates_in_fingerprint_witness() {
    let id = head(42);
    let fp_a: Fingerprint<CommitId> = Fingerprint::new(&FingerprintSeed::new(1, 2), &id);
    let fp_b: Fingerprint<CommitId> = Fingerprint::new(&FingerprintSeed::new(3, 4), &id);
    assert_ne!(fp_a, fp_b, "distinct seeds must change the fingerprint here");
}

/// The fingerprint actually incorporates the value: changing only the
/// commit id changes the fingerprint for these specific (non-colliding)
/// inputs. Witness, not a law (see `seed_participates_in_fingerprint_witness`).
#[test]
fn value_participates_in_fingerprint_witness() {
    let seed = FingerprintSeed::new(7, 11);
    let fp_a: Fingerprint<CommitId> = Fingerprint::new(&seed, &head(1));
    let fp_b: Fingerprint<CommitId> = Fingerprint::new(&seed, &head(2));
    assert_ne!(fp_a, fp_b, "distinct ids must change the fingerprint here");
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
