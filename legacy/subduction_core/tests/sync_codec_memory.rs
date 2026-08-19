//! Decoding a `BatchSyncResponse` carrying N missing commits must use memory
//! linear in N: each decoded `Signed` retains only its own bytes, keeping
//! batch decode within wasm's bounded linear-memory heap.
//!
//! The check is deterministic: it inspects the decoded buffers' `capacity()`
//! directly rather than sampling process RSS.

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
        message::{BatchSyncResponse, RequestId, RequestedData, SyncDiff, SyncMessage, SyncResult},
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

    assert_eq!(
        diff.missing_commits.len(),
        N as usize,
        "all commits decoded"
    );

    // Total memory retained by the decoded commit buffers. With each commit
    // retaining only its own bytes, this sums to ~the wire size.
    let total_capacity: usize = diff
        .missing_commits
        .into_iter()
        .map(|(signed, _blob)| signed.into_bytes().capacity())
        .sum();

    let wire = bytes.len();
    let bound = wire * 4;
    let ratio = total_capacity as f64 / wire as f64;

    eprintln!(
        "N={N}: decoded total_capacity = {total_capacity} B, wire = {wire} B, \
         ratio {ratio:.2}x (linear bound {bound} B)"
    );

    assert!(
        total_capacity <= bound,
        "decoded commit buffers retain {total_capacity} B for {wire} B of wire \
         (ratio {ratio:.0}x), exceeding the linear bound of {bound} B. Each decoded \
         Signed must retain only its own bytes."
    );
}
