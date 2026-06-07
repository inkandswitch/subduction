//! `Sedimentree::diff_remote_fingerprints` on loose-commit-only histories
//! (no fragments). Covers identity/emptiness, disjoint chains, partial
//! overlap with prefix/fork shapes, DAG-ancestry pruning, and
//! one-round-convergence after mutual ingestion.

#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::panic)]

use std::collections::BTreeSet;

use sedimentree_core::{
    blob::{Blob, BlobMeta},
    depth::CountLeadingZeroBytes,
    crypto::fingerprint::FingerprintSeed,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};

const SED_ID: SedimentreeId = SedimentreeId::new([42u8; 32]);
const SEED: FingerprintSeed = FingerprintSeed::new(0x1234_5678, 0xDEAD_BEEF);

/// Commit whose head is `[seed; 32]` and whose blob bytes are also a
/// function of `seed`, so different seeds give different blob digests.
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

fn tree(commits: Vec<LooseCommit>) -> Sedimentree {
    Sedimentree::new(vec![], commits)
}

#[test]
fn identical_trees_have_empty_diff() {
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);
    let c = commit(3, &[head(2)]);

    let local = tree(vec![a.clone(), b.clone(), c.clone()]);
    let remote = tree(vec![a, b, c]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert!(diff.local_only_commits.is_empty());
    assert!(diff.remote_only_commit_fingerprints.is_empty());
}

#[test]
fn empty_local_full_remote_requests_all() {
    let local = tree(vec![]);
    let remote = tree(vec![commit(1, &[]), commit(2, &[head(1)])]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert_eq!(diff.local_only_commits.len(), 0);
    assert_eq!(diff.remote_only_commit_fingerprints.len(), 2);
}

#[test]
fn full_local_empty_remote_sends_all() {
    let local = tree(vec![commit(1, &[]), commit(2, &[head(1)])]);
    let remote = tree(vec![]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert_eq!(diff.local_only_commits.len(), 2);
    assert!(diff.remote_only_commit_fingerprints.is_empty());
}

/// Disjoint sets produce `local_only.len() == |local|` and
/// `remote_only.len() == |remote|`.
#[test]
fn disjoint_histories_send_full_sets_in_both_directions() {
    let local = tree(vec![
        commit(10, &[]),
        commit(11, &[head(10)]),
        commit(12, &[head(11)]),
    ]);
    let remote = tree(vec![
        commit(20, &[]),
        commit(21, &[head(20)]),
        commit(22, &[head(21)]),
    ]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert_eq!(diff.local_only_commits.len(), 3);
    assert_eq!(diff.remote_only_commit_fingerprints.len(), 3);
}

#[test]
fn disjoint_histories_unequal_sizes() {
    let mut local_commits = vec![commit(10, &[])];
    for i in 1..19u8 {
        local_commits.push(commit(10 + i, &[head(10 + i - 1)]));
    }
    assert_eq!(local_commits.len(), 19);
    let local = tree(local_commits);

    let mut remote_commits = vec![commit(100, &[])];
    for i in 1..20u8 {
        remote_commits.push(commit(100 + i, &[head(100 + i - 1)]));
    }
    assert_eq!(remote_commits.len(), 20);
    let remote = tree(remote_commits);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert_eq!(diff.local_only_commits.len(), 19);
    assert_eq!(diff.remote_only_commit_fingerprints.len(), 20);
}

/// Shared prefix A→B; local adds C, remote adds D. Each side sends its
/// divergent suffix only.
#[test]
fn forked_after_shared_prefix() {
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);
    let c = commit(3, &[head(2)]);
    let d = commit(4, &[head(2)]);

    let local = tree(vec![a.clone(), b.clone(), c]);
    let remote = tree(vec![a, b, d]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert_eq!(diff.local_only_commits.len(), 1);
    assert_eq!(*diff.local_only_commits[0].0, head(3));
    assert_eq!(diff.remote_only_commit_fingerprints.len(), 1);
}

#[test]
fn remote_is_prefix_of_local() {
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);
    let c = commit(3, &[head(2)]);
    let d = commit(4, &[head(3)]);

    let local = tree(vec![a.clone(), b.clone(), c.clone(), d]);
    let remote = tree(vec![a, b]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert_eq!(diff.local_only_commits.len(), 2);
    let sent_ids: BTreeSet<CommitId> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
    assert!(sent_ids.contains(&head(3)) && sent_ids.contains(&head(4)));
    assert!(!sent_ids.contains(&head(1)) && !sent_ids.contains(&head(2)));
    assert!(diff.remote_only_commit_fingerprints.is_empty());
}

#[test]
fn local_is_prefix_of_remote() {
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);
    let c = commit(3, &[head(2)]);
    let d = commit(4, &[head(3)]);

    let local = tree(vec![a.clone(), b.clone()]);
    let remote = tree(vec![a, b, c, d]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert!(diff.local_only_commits.is_empty());
    assert_eq!(diff.remote_only_commit_fingerprints.len(), 2);
}

/// If remote has C, local should not re-send C's ancestors A and B.
#[test]
fn ancestry_pruning_drops_ancestors_of_shared_tip() {
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);
    let c = commit(3, &[head(2)]);
    let d = commit(4, &[head(3)]);

    let local = tree(vec![a.clone(), b.clone(), c.clone(), d.clone()]);
    let remote = tree(vec![a, b, c]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert_eq!(diff.local_only_commits.len(), 1);
    assert_eq!(*diff.local_only_commits[0].0, head(4));
}

/// A remote claiming loose commit B carries no guarantee that the
/// remote also has B's ancestors — partial sync, restored-from-snapshot
/// peers, and other failure modes can leave a peer holding descendants
/// without ancestors. Without a fragment-based justification, local
/// must send A so that any peer in the "descendant without ancestor"
/// state can recover.
#[test]
fn loose_only_remote_does_not_imply_ancestors_are_remote_held() {
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);

    let local = tree(vec![a.clone(), b.clone()]);
    let remote = tree(vec![b]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert_eq!(diff.local_only_commits.len(), 1);
    assert_eq!(*diff.local_only_commits[0].0, head(1));
}

/// Two parallel chains rooted at the same point; remote has only chain
/// 1 (root, b1, c1). Local has both chains. With pure loose-commit
/// state (no fragments), only commits the remote doesn't list are
/// sent — chain 1 commits are excluded by the first filter, chain 2
/// commits are sent.
///
/// Note: even though `root` is shared, no fragment justifies pruning
/// b2/c2 transitively; they get sent.
#[test]
fn disjoint_branches_only_send_unique_commits() {
    let root = commit(1, &[]);
    let b1 = commit(10, &[head(1)]);
    let c1 = commit(11, &[head(10)]);
    let b2 = commit(20, &[head(1)]);
    let c2 = commit(21, &[head(20)]);

    let local = tree(vec![
        root.clone(),
        b1.clone(),
        c1.clone(),
        b2.clone(),
        c2.clone(),
    ]);
    let remote = tree(vec![root, b1, c1]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert_eq!(diff.local_only_commits.len(), 2);
    let sent_ids: BTreeSet<CommitId> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
    assert!(sent_ids.contains(&head(20)) && sent_ids.contains(&head(21)));
}

/// Diamond: `root → {b1, b2} → merge`. Remote has only `merge`. No
/// fragment justifies transitively pruning root, b1, or b2, so local
/// sends all three ancestors — necessary for the remote to catch up.
#[test]
fn diamond_dag_with_remote_holding_only_merge_sends_all_ancestors() {
    let root = commit(1, &[]);
    let b1 = commit(2, &[head(1)]);
    let b2 = commit(3, &[head(1)]);
    let merge = commit(4, &[head(2), head(3)]);

    let local = tree(vec![root, b1, b2, merge.clone()]);
    let remote = tree(vec![merge]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    let sent_ids: BTreeSet<CommitId> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
    assert_eq!(diff.local_only_commits.len(), 3);
    assert!(sent_ids.contains(&head(1)));
    assert!(sent_ids.contains(&head(2)));
    assert!(sent_ids.contains(&head(3)));
}

/// With loose-only state and no fragments, no ancestor pruning is
/// sound — every non-shared commit gets sent. The remote-side filter
/// still excludes commit 12 (the one the remote claims), leaving 14
/// to send.
#[test]
fn many_parallel_chains_no_loose_pruning() {
    let mut all_commits = Vec::new();
    for chain_idx in 0..5u8 {
        let base = 10 + chain_idx * 10;
        all_commits.push(commit(base, &[]));
        all_commits.push(commit(base + 1, &[head(base)]));
        all_commits.push(commit(base + 2, &[head(base + 1)]));
    }
    let local = tree(all_commits);
    assert_eq!(local.loose_commits().count(), 15);

    // Remote has only the last commit of the first chain.
    let remote = tree(vec![commit(12, &[head(11)])]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    // 15 total - 1 shared (commit 12) = 14 to send.
    assert_eq!(diff.local_only_commits.len(), 14);
}

// One-round-convergence-after-mutual-ingest is covered as a property in
// `tests/sync_convergence_props.rs::prop_mutual_ingest_converges_in_one_round`.

/// Minimize on a fragment-less tree must not change the fingerprint set.
#[test]
fn minimize_loose_only_preserves_fingerprints() {
    let local = tree(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);

    let depth_metric = CountLeadingZeroBytes;
    let minimized = local.minimize(&depth_metric);

    let summary_raw = local.fingerprint_summarize(&SEED);
    let summary_min = minimized.fingerprint_summarize(&SEED);

    assert_eq!(
        summary_raw.commit_fingerprints(),
        summary_min.commit_fingerprints(),
    );
    assert_eq!(
        summary_raw.fragment_fingerprints(),
        summary_min.fragment_fingerprints(),
    );
}
