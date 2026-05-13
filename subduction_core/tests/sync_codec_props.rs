//! Property tests for the `SyncMessage` codec's effect on
//! `diff_remote_fingerprints`. Byte-identity round-trip is already
//! covered by `subduction_core::connection::message::proptests` and
//! `tests/codec_proptest.rs`; what's tested here is the stronger
//! claim that the codec preserves the *semantic* answer of
//! `diff_remote_fingerprints`.

#![cfg(feature = "bolero")]
#![allow(clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;

use sedimentree_core::{
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};
use subduction_core::connection::message::{BatchSyncRequest, SyncMessage};

fn close_ancestry(commits: &[LooseCommit]) -> Vec<LooseCommit> {
    let ids: BTreeSet<CommitId> = commits.iter().map(LooseCommit::head).collect();
    commits
        .iter()
        .map(|c| {
            let pruned_parents: BTreeSet<CommitId> = c
                .parents()
                .iter()
                .copied()
                .filter(|p| ids.contains(p))
                .collect();
            LooseCommit::new(c.sedimentree_id(), c.head(), pruned_parents, *c.blob_meta())
        })
        .collect()
}

fn tree_from(commits: &[LooseCommit]) -> Sedimentree {
    Sedimentree::new(vec![], close_ancestry(commits))
}

fn codec_roundtrip(req: BatchSyncRequest) -> BatchSyncRequest {
    let bytes = SyncMessage::BatchSyncRequest(req).encode();
    let decoded = SyncMessage::try_decode(&bytes).expect("decode should succeed");
    let SyncMessage::BatchSyncRequest(r) = decoded else {
        panic!("expected BatchSyncRequest, got {decoded:?}");
    };
    r
}

/// `diff_remote_fingerprints` against a decoded summary returns the same
/// answer as against the original summary. Byte-identity of the codec
/// is not enough — this pins the semantic invariant that callers care
/// about.
#[test]
fn prop_codec_roundtrip_preserves_diff_result() {
    bolero::check!()
        .with_arbitrary::<(Vec<LooseCommit>, BatchSyncRequest)>()
        .for_each(|(responder_commits, request)| {
            let responder = tree_from(responder_commits);

            let direct = responder.diff_remote_fingerprints(&request.fingerprint_summary);
            let after_codec_req = codec_roundtrip(request.clone());
            let codec = responder.diff_remote_fingerprints(&after_codec_req.fingerprint_summary);

            let direct_local: BTreeSet<CommitId> = direct
                .local_only_commits
                .iter()
                .map(|(id, _)| **id)
                .collect();
            let codec_local: BTreeSet<CommitId> = codec
                .local_only_commits
                .iter()
                .map(|(id, _)| **id)
                .collect();
            assert_eq!(direct_local, codec_local);

            let direct_remote: BTreeSet<Fingerprint<CommitId>> = direct
                .remote_only_commit_fingerprints
                .iter()
                .copied()
                .collect();
            let codec_remote: BTreeSet<Fingerprint<CommitId>> = codec
                .remote_only_commit_fingerprints
                .iter()
                .copied()
                .collect();
            assert_eq!(direct_remote, codec_remote);
        });
}

/// A responder that computes its fingerprints under the seed found in
/// a decoded `BatchSyncRequest` gets the same fingerprints the requester
/// included. Catches "the seed travels on the wire but isn't actually
/// being used end-to-end to compute fingerprints" class of bug.
#[test]
fn prop_decoded_seed_yields_matching_fingerprints_for_shared_commits() {
    bolero::check!()
        .with_arbitrary::<(Vec<LooseCommit>, FingerprintSeed)>()
        .for_each(|(commits, seed)| {
            let tree = tree_from(commits);

            // Encode + decode a summary built from `seed`.
            let summary = tree.fingerprint_summarize(seed);
            let dummy_req = BatchSyncRequest {
                id: sedimentree_core::id::SedimentreeId::new([0; 32]),
                req_id: subduction_core::connection::message::RequestId {
                    requestor: subduction_core::peer::id::PeerId::new([0; 32]),
                    nonce: 0,
                },
                fingerprint_summary: summary.clone(),
                subscribe: false,
            };
            let decoded = codec_roundtrip(dummy_req);
            let decoded_summary = decoded.fingerprint_summary;

            // For every commit the responder has, the fp it computes
            // under the decoded seed must be in the decoded summary.
            for commit in tree.loose_commits() {
                let fp = Fingerprint::new(decoded_summary.seed(), &commit.head());
                assert!(
                    decoded_summary.commit_fingerprints().contains(&fp),
                    "fp computed under decoded seed missing from decoded summary",
                );
            }
        });
}
