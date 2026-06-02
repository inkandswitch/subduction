//! Local-only behavior of the `store_*` / `add_*` write tiers that does not
//! require a connected peer:
//!
//! * `add_commits_batch` / `add_fragments_batch` (the round-trip combinators
//!   added alongside the `store_*` split): empty input is a no-op yielding an
//!   empty per-peer map; non-empty input persists everything. The wedged-peer
//!   propagation behavior of these combinators is covered in
//!   `addbatch_storage_durability.rs`.
//! * `store_commit`'s `FragmentRequested` boundary signal: `Some(..)` when the
//!   commit lands on a fragment boundary, `None` otherwise.

#![allow(clippy::expect_used)]

use std::collections::BTreeSet;

use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use subduction_core::{connection::test_utils::new_test_subduction, subduction::FragmentBatchItem};
use testresult::TestResult;

fn make_blob(seed: u8) -> Blob {
    let data: Vec<u8> = (0..64).map(|i| seed.wrapping_add(i)).collect();
    Blob::new(data)
}

// ---------------------------------------------------------------------------
// add_commits_batch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn add_commits_batch_empty_is_noop_and_empty_map() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();
    let sed_id = SedimentreeId::new([1u8; 32]);

    let per_peer = sd.add_commits_batch(sed_id, Vec::new(), None).await?;

    assert!(
        per_peer.is_empty(),
        "empty batch must yield an empty per-peer map"
    );
    assert!(
        sd.get_commits(sed_id).await.is_none(),
        "empty batch should not create a sedimentree"
    );

    Ok(())
}

#[tokio::test]
async fn add_commits_batch_persists_all_commits() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();
    let sed_id = SedimentreeId::new([2u8; 32]);

    // Non-boundary heads (first byte != 0) so nothing minimizes into a fragment.
    let inputs: Vec<(CommitId, BTreeSet<CommitId>, Blob)> = (0..5u8)
        .map(|i| (CommitId::new([i + 100; 32]), BTreeSet::new(), make_blob(i)))
        .collect();
    let expected_ids: BTreeSet<CommitId> = inputs.iter().map(|(id, _, _)| *id).collect();

    // No peers connected → an empty per-peer map, but the data must persist.
    let per_peer = sd.add_commits_batch(sed_id, inputs, None).await?;
    assert!(
        per_peer.is_empty(),
        "no connected peers → empty per-peer map"
    );

    let stored = sd
        .get_commits(sed_id)
        .await
        .expect("sedimentree must exist");
    let stored_ids: BTreeSet<CommitId> = stored
        .iter()
        .map(sedimentree_core::loose_commit::LooseCommit::head)
        .collect();
    assert_eq!(
        stored_ids, expected_ids,
        "exactly the input commits must be stored (by identity, not just count)"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// add_fragments_batch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn add_fragments_batch_empty_is_noop_and_empty_map() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();
    let sed_id = SedimentreeId::new([3u8; 32]);

    let per_peer = sd.add_fragments_batch(sed_id, Vec::new(), None).await?;

    assert!(
        per_peer.is_empty(),
        "empty batch must yield an empty per-peer map"
    );
    assert!(
        sd.get_fragments(sed_id).await.is_none(),
        "empty batch should not create a sedimentree"
    );

    Ok(())
}

#[tokio::test]
async fn add_fragments_batch_persists_all_fragments() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();
    let sed_id = SedimentreeId::new([4u8; 32]);

    let fragments: Vec<FragmentBatchItem> = (0..3u8)
        .map(|i| FragmentBatchItem {
            head: CommitId::new([i + 50; 32]),
            boundary: BTreeSet::from([CommitId::new([i + 150; 32])]),
            checkpoints: Vec::new(),
            blob: make_blob(i),
        })
        .collect();
    let expected_heads: BTreeSet<CommitId> = fragments.iter().map(|f| f.head).collect();

    let per_peer = sd.add_fragments_batch(sed_id, fragments, None).await?;
    assert!(
        per_peer.is_empty(),
        "no connected peers → empty per-peer map"
    );

    let stored = sd
        .get_fragments(sed_id)
        .await
        .expect("sedimentree must exist");
    let stored_heads: BTreeSet<CommitId> = stored
        .iter()
        .map(sedimentree_core::fragment::Fragment::head)
        .collect();
    assert_eq!(
        stored_heads, expected_heads,
        "exactly the input fragments must be stored (by head identity)"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// store_commit FragmentRequested boundary signal
// ---------------------------------------------------------------------------

/// A boundary commit (depth > 0 under `CountLeadingZeroBytes`, i.e. a leading
/// zero byte) must yield `Some(FragmentRequested)` for the same head.
#[tokio::test]
async fn store_commit_signals_fragment_requested_on_boundary() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();
    let sed_id = SedimentreeId::new([5u8; 32]);

    // Leading 0x00 byte → depth >= 1 → boundary.
    let head = CommitId::new([
        0u8, 0xAB, 0xCD, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
        22, 23, 24, 25, 26, 27, 28, 29,
    ]);

    let requested = sd
        .store_commit(sed_id, head, BTreeSet::new(), make_blob(1))
        .await?;

    let fragment_requested = requested.expect("a boundary commit must request a fragment (Some)");
    assert_eq!(
        fragment_requested.head(),
        head,
        "FragmentRequested must reference the boundary commit's head"
    );

    Ok(())
}

/// A non-boundary commit (depth 0, no leading zero byte) must yield `None`.
#[tokio::test]
async fn store_commit_no_fragment_requested_off_boundary() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();
    let sed_id = SedimentreeId::new([6u8; 32]);

    // First byte non-zero → depth 0 → not a boundary.
    let head = CommitId::new([0xFFu8; 32]);

    let requested = sd
        .store_commit(sed_id, head, BTreeSet::new(), make_blob(2))
        .await?;

    assert!(
        requested.is_none(),
        "a non-boundary commit must not request a fragment (None)"
    );

    Ok(())
}
