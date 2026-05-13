//! Wire-codec round-trip for `FingerprintSeed` and `FingerprintSummary`:
//! the seed and fingerprint bytes inside a `BatchSyncRequest` must
//! survive encode-then-decode byte-identically, and a responder that
//! computes its own fingerprints under the decoded seed must agree with
//! the requester's for shared commits.

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

#[test]
fn seed_round_trips_through_wire_codec() {
    let seed = FingerprintSeed::new(0x1234_5678_9ABC_DEF0, 0xFEDC_BA98_7654_3210);
    let summary = FingerprintSummary::new(seed, BTreeSet::new(), BTreeSet::new());
    let req = make_request(summary);

    let decoded = encode_decode_request(&req);
    let decoded_seed = decoded.fingerprint_summary.seed();

    assert_eq!(decoded_seed.key0(), 0x1234_5678_9ABC_DEF0);
    assert_eq!(decoded_seed.key1(), 0xFEDC_BA98_7654_3210);
    assert_eq!(decoded_seed, &seed);
}

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

    assert_eq!(decoded_summary.seed(), original_summary.seed());
    assert_eq!(
        decoded_summary.commit_fingerprints(),
        original_summary.commit_fingerprints(),
    );
    assert_eq!(
        decoded_summary.fragment_fingerprints(),
        original_summary.fragment_fingerprints(),
    );
}

/// Decoded summary's fingerprints (computed by requester) must match
/// fingerprints the responder computes locally using the decoded seed.
/// If this fails, either the seed didn't round-trip or `Fingerprint::new`
/// has hidden state — either way the "N missing, requesting N" symptom
/// would result.
#[test]
fn after_round_trip_responder_fingerprints_match_requesters_for_shared_commits() {
    let seed = FingerprintSeed::new(0xAA_BB_CC_DD_11_22_33_44, 0xCAFE_BABE_DEAD_BEEF);

    let requester_tree = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);
    let requester_summary = requester_tree.fingerprint_summarize(&seed);

    let responder_tree = tree_of(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);

    let req = make_request(requester_summary.clone());
    let decoded = encode_decode_request(&req);
    let decoded_summary = decoded.fingerprint_summary;

    let decoded_seed = decoded_summary.seed();
    let responder_commit_fps: BTreeSet<Fingerprint<CommitId>> = responder_tree
        .loose_commits()
        .map(|c| Fingerprint::new(decoded_seed, &c.head()))
        .collect();

    assert_eq!(&responder_commit_fps, decoded_summary.commit_fingerprints());
}

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

    assert!(diff.local_only_commits.is_empty());
    assert!(diff.remote_only_commit_fingerprints.is_empty());
}

/// The codec must not change the diff result. Catches asymmetries that
/// don't show up as byte-identity failures (e.g. ordering, duplicate
/// filtering).
#[test]
fn diff_result_is_invariant_under_codec_round_trip() {
    let seed = FingerprintSeed::new(0xABCD, 0xEF01);

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
    let direct_diff = responder_tree.diff_remote_fingerprints(&original_summary);

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
    assert_eq!(direct_local_only_ids, codec_local_only_ids);

    let direct_remote_only: BTreeSet<Fingerprint<CommitId>> = direct_diff
        .remote_only_commit_fingerprints
        .iter()
        .copied()
        .collect();
    let codec_remote_only: BTreeSet<Fingerprint<CommitId>> = codec_diff
        .remote_only_commit_fingerprints
        .iter()
        .copied()
        .collect();
    assert_eq!(direct_remote_only, codec_remote_only);
}

#[test]
fn fingerprint_new_is_deterministic_for_fixed_seed_and_value() {
    let seed = FingerprintSeed::new(7, 11);
    let id = head(42);

    let fp1: Fingerprint<CommitId> = Fingerprint::new(&seed, &id);
    let fp2: Fingerprint<CommitId> = Fingerprint::new(&seed, &id);
    let fp3: Fingerprint<CommitId> = Fingerprint::new(&seed, &id);

    assert_eq!(fp1, fp2);
    assert_eq!(fp2, fp3);
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

#[test]
fn commit_id_bytes_round_trip_preserve_fingerprint() {
    let original = head(99);
    let reconstructed = CommitId::new(*original.as_bytes());
    assert_eq!(original, reconstructed);

    let seed = FingerprintSeed::new(123, 456);
    let fp_original: Fingerprint<CommitId> = Fingerprint::new(&seed, &original);
    let fp_reconstructed: Fingerprint<CommitId> = Fingerprint::new(&seed, &reconstructed);
    assert_eq!(fp_original, fp_reconstructed);
}

/// A buggy peer sending a summary whose seed field doesn't match the
/// seed actually used to compute the fingerprints produces a fully
/// disjoint diff — exactly the user's bug shape, isolated to the seed.
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
