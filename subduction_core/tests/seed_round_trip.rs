//! **Wire-codec round-trip tests for `FingerprintSeed` and
//! `FingerprintSummary`** — answers the question "could the seed be
//! mis-encoded, or could the responder be using a different seed than
//! the one in the summary?"
//!
//! The diff layer documents this invariant: the responder must
//! fingerprint its own items using `remote.seed()` (the seed field of
//! the incoming `FingerprintSummary`), and the fingerprints inside that
//! summary were computed by the requester using the same seed. If
//! either side computes with a different seed, the sets are disjoint
//! and we see the "N missing, requesting N" symptom.
//!
//! These tests pin down that:
//!
//! 1. The wire codec encodes and decodes the seed byte-identically.
//! 2. The fingerprints inside a `FingerprintSummary` survive the
//!    round-trip byte-identically.
//! 3. After a round-trip, the responder's diff against the decoded
//!    summary produces the identical result it would have produced
//!    against the original summary.
//! 4. Two peers running the same code, given the same `CommitId`
//!    bytes and the same seed, always compute the same fingerprint.

#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::panic)]

use std::collections::BTreeSet;

use sedimentree_core::{
    blob::{Blob, BlobMeta},
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{FingerprintSummary, Sedimentree},
};
use subduction_core::{
    connection::message::{BatchSyncRequest, RequestId, SyncMessage},
    peer::id::PeerId,
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

fn head(seed: u8) -> CommitId {
    CommitId::new([seed; 32])
}

fn tree_of(commits: Vec<LooseCommit>) -> Sedimentree {
    Sedimentree::new(vec![], commits)
}

fn make_request(summary: FingerprintSummary) -> BatchSyncRequest {
    BatchSyncRequest {
        id: SED_ID,
        req_id: RequestId {
            requestor: PeerId::new([0xAB; 32]),
            nonce: 0xDEADBEEF_CAFEF00D,
        },
        fingerprint_summary: summary,
        subscribe: true,
    }
}

fn encode_decode_request(req: &BatchSyncRequest) -> BatchSyncRequest {
    let msg = SyncMessage::BatchSyncRequest(req.clone());
    let bytes = msg.encode();
    let decoded = SyncMessage::try_decode(bytes.as_slice()).expect("decode should succeed");
    match decoded {
        SyncMessage::BatchSyncRequest(req) => req,
        other => panic!("expected BatchSyncRequest, got {other:?}"),
    }
}

// ============================================================================
// Section 1: seed and fingerprint bytes survive the wire codec
// ============================================================================

/// The seed encoded into a `BatchSyncRequest` must decode back to the
/// same `FingerprintSeed`. Plain round-trip test.
#[test]
fn seed_round_trips_through_wire_codec() {
    let seed = FingerprintSeed::new(0x1234_5678_9ABC_DEF0, 0xFEDC_BA98_7654_3210);
    let summary = FingerprintSummary::new(seed, BTreeSet::new(), BTreeSet::new());
    let req = make_request(summary);

    let decoded = encode_decode_request(&req);
    let decoded_seed = decoded.fingerprint_summary.seed();

    assert_eq!(decoded_seed.key0(), 0x1234_5678_9ABC_DEF0);
    assert_eq!(decoded_seed.key1(), 0xFEDC_BA98_7654_3210);
    assert_eq!(decoded_seed, &seed, "seed must round-trip byte-identically");
}

/// Every fingerprint value in the summary must survive the wire codec
/// byte-identically. If even one bit flips, the set difference at the
/// receiver yields wrong results.
#[test]
fn fingerprint_values_round_trip_through_wire_codec() {
    let seed = FingerprintSeed::new(11, 22);
    let local = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);
    let original_summary = local.fingerprint_summarize(&seed);

    let req = make_request(original_summary.clone());
    let decoded = encode_decode_request(&req);
    let decoded_summary = decoded.fingerprint_summary;

    assert_eq!(
        decoded_summary.seed(),
        original_summary.seed(),
        "seed survives"
    );
    assert_eq!(
        decoded_summary.commit_fingerprints(),
        original_summary.commit_fingerprints(),
        "commit fingerprint set survives byte-identically"
    );
    assert_eq!(
        decoded_summary.fragment_fingerprints(),
        original_summary.fragment_fingerprints(),
        "fragment fingerprint set survives byte-identically"
    );
}

/// **The strongest version of the user's question.** Encode a summary
/// computed by the requester, decode it on the responder, then have the
/// responder fingerprint *its own commits* using the decoded seed and
/// check those match the requester's fingerprints for shared commits.
///
/// If this fails: the seed transport is broken, OR
/// `Fingerprint::new(seed, x)` is non-deterministic. Either way, the
/// "N missing, requesting N" symptom is explained.
#[test]
fn after_round_trip_responder_fingerprints_match_requesters_for_shared_commits() {
    let seed = FingerprintSeed::new(0xAA_BB_CC_DD_11_22_33_44, 0xCAFE_BABE_DEAD_BEEF);

    // Requester's tree
    let requester_tree = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);
    let requester_summary = requester_tree.fingerprint_summarize(&seed);

    // Responder's tree happens to have the same 3 commits (post-sync)
    let responder_tree = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);

    // Wire round-trip the summary
    let req = make_request(requester_summary.clone());
    let decoded = encode_decode_request(&req);
    let decoded_summary = decoded.fingerprint_summary;

    // Responder computes its fingerprints with the *decoded* seed
    let decoded_seed = decoded_summary.seed();
    let responder_commit_fps: BTreeSet<Fingerprint<CommitId>> = responder_tree
        .loose_commits()
        .map(|c| Fingerprint::new(decoded_seed, &c.head()))
        .collect();

    // The two sets must be identical: requester's fps decoded from the
    // wire == responder's fps computed locally with the same seed.
    assert_eq!(
        &responder_commit_fps,
        decoded_summary.commit_fingerprints(),
        "responder fps (using decoded seed) MUST match decoded fps from \
         requester. If this assertion fails, either the seed didn't \
         round-trip or Fingerprint::new is non-deterministic — both \
         would produce the 'N missing, requesting N' bug."
    );
}

/// Same as the above but with a `diff_remote_fingerprints` call — the
/// end-to-end path the protocol actually uses.
#[test]
fn diff_against_round_tripped_summary_finds_full_overlap_when_data_matches() {
    let seed = FingerprintSeed::new(123, 456);

    let shared_commits = vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ];
    let requester_tree = tree_of(shared_commits.clone());
    let responder_tree = tree_of(shared_commits);

    let req = make_request(requester_tree.fingerprint_summarize(&seed));
    let decoded = encode_decode_request(&req);
    let diff = responder_tree.diff_remote_fingerprints(&decoded.fingerprint_summary);

    assert!(
        diff.local_only_commits.is_empty(),
        "identical data + round-tripped summary: nothing should be local-only, \
         got {} commits — this would mean the seed broke transport",
        diff.local_only_commits.len(),
    );
    assert!(
        diff.remote_only_commit_fingerprints.is_empty(),
        "identical data + round-tripped summary: no fingerprints should be \
         remote-only, got {} — this would mean the seed broke transport",
        diff.remote_only_commit_fingerprints.len(),
    );
}

/// Same diff result before vs. after the codec round-trip. Catches
/// any subtle codec asymmetry that doesn't show up as a byte-identity
/// failure (e.g., if `BTreeSet` ordering or duplicate filtering
/// differs).
#[test]
fn diff_result_is_invariant_under_codec_round_trip() {
    let seed = FingerprintSeed::new(0xABCD, 0xEF01);

    // Requester has commits 1, 2, 3, 4. Responder has 1, 2, 5, 6.
    // Two are shared (1, 2), each has 2 unique.
    let requester_tree = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
        commit(4, &[head(3)]),
    ]);
    let responder_tree = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(5, &[head(2)]),
        commit(6, &[head(5)]),
    ]);

    let original_summary = requester_tree.fingerprint_summarize(&seed);

    // Diff against the ORIGINAL summary (no codec)
    let direct_diff = responder_tree.diff_remote_fingerprints(&original_summary);

    // Diff against the ROUND-TRIPPED summary
    let req = make_request(original_summary);
    let decoded = encode_decode_request(&req);
    let codec_diff = responder_tree.diff_remote_fingerprints(&decoded.fingerprint_summary);

    let direct_local_only_ids: BTreeSet<CommitId> = direct_diff
        .local_only_commits
        .iter()
        .map(|(id, _)| **id)
        .collect();
    let codec_local_only_ids: BTreeSet<CommitId> = codec_diff
        .local_only_commits
        .iter()
        .map(|(id, _)| **id)
        .collect();
    assert_eq!(
        direct_local_only_ids, codec_local_only_ids,
        "round-trip must not change which local commits are flagged as missing"
    );

    let direct_remote_only: BTreeSet<Fingerprint<CommitId>> =
        direct_diff.remote_only_commit_fingerprints.iter().copied().collect();
    let codec_remote_only: BTreeSet<Fingerprint<CommitId>> =
        codec_diff.remote_only_commit_fingerprints.iter().copied().collect();
    assert_eq!(
        direct_remote_only, codec_remote_only,
        "round-trip must not change the echoed-back fingerprint set"
    );
}

// ============================================================================
// Section 2: Fingerprint::new is deterministic given the same inputs
// ============================================================================

/// **The most fundamental invariant.** `Fingerprint::new(seed, value)`
/// must produce the same `u64` every time given the same inputs.
#[test]
fn fingerprint_new_is_deterministic_for_fixed_seed_and_value() {
    let seed = FingerprintSeed::new(7, 11);
    let id = head(42);

    let fp1: Fingerprint<CommitId> = Fingerprint::new(&seed, &id);
    let fp2: Fingerprint<CommitId> = Fingerprint::new(&seed, &id);
    let fp3: Fingerprint<CommitId> = Fingerprint::new(&seed, &id);

    assert_eq!(fp1, fp2);
    assert_eq!(fp2, fp3);
    assert_eq!(fp1.as_u64(), fp2.as_u64());
}

/// Different seeds produce different fingerprints (with extremely high
/// probability; collisions are ~2^-64).
#[test]
fn different_seeds_produce_different_fingerprints() {
    let id = head(42);
    let seed_a = FingerprintSeed::new(1, 2);
    let seed_b = FingerprintSeed::new(3, 4);

    let fp_a: Fingerprint<CommitId> = Fingerprint::new(&seed_a, &id);
    let fp_b: Fingerprint<CommitId> = Fingerprint::new(&seed_b, &id);

    assert_ne!(
        fp_a, fp_b,
        "different seeds should produce different fingerprints for the same value"
    );
}

/// Same seed, different values → different fingerprints.
#[test]
fn different_values_produce_different_fingerprints() {
    let seed = FingerprintSeed::new(7, 11);
    let id_a = head(1);
    let id_b = head(2);

    let fp_a: Fingerprint<CommitId> = Fingerprint::new(&seed, &id_a);
    let fp_b: Fingerprint<CommitId> = Fingerprint::new(&seed, &id_b);

    assert_ne!(fp_a, fp_b);
}

/// CommitId bytes round-trip through `as_bytes()` and `CommitId::new`
/// produce a CommitId with the same hash.
#[test]
fn commit_id_bytes_round_trip_preserve_fingerprint() {
    let original = head(99);
    let bytes = *original.as_bytes();
    let reconstructed = CommitId::new(bytes);

    assert_eq!(original, reconstructed);

    let seed = FingerprintSeed::new(123, 456);
    let fp_original: Fingerprint<CommitId> = Fingerprint::new(&seed, &original);
    let fp_reconstructed: Fingerprint<CommitId> = Fingerprint::new(&seed, &reconstructed);
    assert_eq!(fp_original, fp_reconstructed);
}

// ============================================================================
// Section 3: Adversarial / corrupt-seed scenarios
// ============================================================================

/// **What if a buggy peer sent a summary whose seed doesn't match its
/// fingerprints?** I.e. the summary's fingerprints were computed with
/// seed S1, but the summary's seed field claims S2. This is exactly
/// the "wrong seed" scenario the user is asking about.
///
/// We construct that situation manually and confirm: the responder's
/// diff produces the disjoint-sets pattern ("N missing, requesting N"),
/// exactly matching the bug shape.
#[test]
fn summary_with_mismatched_seed_produces_full_disjoint_diff() {
    let real_seed = FingerprintSeed::new(0xAAAA_AAAA_AAAA_AAAA, 0xBBBB_BBBB_BBBB_BBBB);
    let claimed_seed = FingerprintSeed::new(0xCCCC_CCCC_CCCC_CCCC, 0xDDDD_DDDD_DDDD_DDDD);

    // Requester computes fingerprints with the REAL seed
    let requester_tree = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);
    let real_summary = requester_tree.fingerprint_summarize(&real_seed);

    // Manually construct a corrupt summary: same fingerprints, but the
    // seed field claims to be `claimed_seed` instead of `real_seed`.
    let corrupt_summary = FingerprintSummary::new(
        claimed_seed,
        real_summary.commit_fingerprints().clone(),
        real_summary.fragment_fingerprints().clone(),
    );

    // Responder has the SAME tree as requester
    let responder_tree = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);

    let diff = responder_tree.diff_remote_fingerprints(&corrupt_summary);

    // Because the responder hashes its commits with `claimed_seed` (the
    // wrong seed) and compares to fingerprints computed with `real_seed`,
    // every local commit appears as local-only AND every remote
    // fingerprint appears as remote-only.
    assert_eq!(
        diff.local_only_commits.len(),
        3,
        "mismatched seed reproduces the 'N missing' symptom: \
         responder thinks all {} of its commits are unknown to requester",
        diff.local_only_commits.len(),
    );
    assert_eq!(
        diff.remote_only_commit_fingerprints.len(),
        3,
        "mismatched seed reproduces the 'requesting N' symptom: \
         responder thinks none of requester's {} fps match anything",
        diff.remote_only_commit_fingerprints.len(),
    );
}

/// Sanity check on the corrupt-seed scenario: when the seed field is
/// correct (same on both sides) AND the tree contents match, the diff
/// is empty. This confirms the corruption test above isolates the seed
/// as the variable.
#[test]
fn matching_seed_and_identical_data_produces_empty_diff() {
    let seed = FingerprintSeed::new(123, 456);

    let tree_a = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);
    let tree_b = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);

    let summary_a = tree_a.fingerprint_summarize(&seed);
    let diff = tree_b.diff_remote_fingerprints(&summary_a);

    assert!(diff.local_only_commits.is_empty());
    assert!(diff.remote_only_commit_fingerprints.is_empty());
}
