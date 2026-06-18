//! Regression test: decoding a `BatchSyncResponse` carrying N missing
//! commits must use memory **linear** in N, not quadratic.
//!
//! Background: `decode_sync_diff` decodes each commit via
//! `Signed::try_decode(payload.get(*offset..)?.to_vec())` — it copies the
//! *entire remaining payload* per commit, and `Signed::try_decode` finalizes
//! its buffer with `Vec::truncate` (which shrinks length but **not**
//! capacity). So each of the N decoded `Signed` values retains a `Vec` whose
//! capacity is the whole remaining payload at its decode point. Summed over
//! N commits that is `Σ (N-k) ≈ ½·N²` bytes held at once — ~12 GiB for an
//! 8.7k-commit document, which overflows wasm's 4 GiB linear-memory heap and
//! crashes sync with `RuntimeError: unreachable`.
//!
//! This test fails (red) on the quadratic code and passes once each decoded
//! `Signed` retains only its own bytes. It is deterministic — it inspects the
//! decoded buffers' `capacity()` directly rather than sampling process RSS.

#![allow(clippy::expect_used, clippy::panic, clippy::cast_precision_loss)]

use std::collections::BTreeSet;

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    connection::{
        message::{
            BatchSyncResponse, RequestId, RequestedData, SyncDiff, SyncMessage, SyncResult,
        },
        test_utils::test_signer,
    },
    peer::id::PeerId,
    remote_heads::RemoteHeads,
};
use subduction_crypto::signed::Signed;

/// Build one signed loose commit + its blob.
async fn make_commit(id: SedimentreeId) -> (Signed<LooseCommit>, Blob) {
    let blob = Blob::new(vec![0xAB; 32]);
    let blob_meta = BlobMeta::new(&blob);
    let commit = LooseCommit::new(id, CommitId::new([0x11; 32]), BTreeSet::new(), blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
    (verified.into_signed(), blob)
}

/// A `BatchSyncResponse` with `N` missing commits must decode into buffers
/// whose total retained capacity is linear in the wire size, not quadratic.
#[tokio::test]
async fn batch_sync_response_decode_memory_is_linear() {
    const N: u32 = 1_000;

    let sed_id = SedimentreeId::new([1u8; 32]);

    // The decode's per-commit memory cost is independent of commit content,
    // so we seal one commit and clone it N times (each clone is right-sized,
    // ~200 B) instead of sealing N — keeps the test sub-second. Identical
    // entries are fine: the wire codec does not deduplicate.
    let commit = make_commit(sed_id).await;
    let missing_commits = vec![commit; N as usize];

    let response = BatchSyncResponse {
        req_id: RequestId {
            requestor: PeerId::new([9u8; 32]),
            nonce: 0xDEAD_BEEF,
        },
        id: sed_id,
        result: SyncResult::Ok(SyncDiff {
            missing_commits,
            missing_fragments: Vec::new(),
            requesting: RequestedData::default(),
        }),
        responder_heads: RemoteHeads {
            counter: 0,
            heads: Vec::new(),
        },
    };

    let bytes = SyncMessage::from(response).encode();

    let decoded = SyncMessage::try_decode(&bytes).expect("decode BatchSyncResponse");
    let SyncMessage::BatchSyncResponse(BatchSyncResponse {
        result: SyncResult::Ok(diff),
        ..
    }) = decoded
    else {
        panic!("expected Ok(BatchSyncResponse)");
    };

    assert_eq!(diff.missing_commits.len(), N as usize, "all commits decoded");

    // Total memory retained by the decoded commit buffers. The bug makes each
    // commit retain ~the whole remaining payload, so this sums to O(N²); the
    // fix makes each retain only its own bytes, so it sums to ~the wire size.
    let total_capacity: usize = diff
        .missing_commits
        .into_iter()
        .map(|(signed, _blob)| signed.into_bytes().capacity())
        .sum();

    let wire = bytes.len();
    let bound = wire * 4;
    let ratio = total_capacity as f64 / wire as f64;

    eprintln!(
        "N={N}: decoded total_capacity = {total_capacity} B, wire = {wire} B, ratio {ratio:.2}x \
         (quadratic would be ~{:.0}x)",
        f64::from(N + 1) / 2.0
    );

    assert!(
        total_capacity <= bound,
        "decoded commit buffers retain memory quadratic in N: \
         total_capacity = {total_capacity} B, wire = {wire} B (ratio {ratio:.0}x), \
         linear bound = {bound} B. Each decoded Signed must retain only its own \
         bytes, not the whole remaining payload."
    );
}
