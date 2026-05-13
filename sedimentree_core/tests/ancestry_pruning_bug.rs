//! Convergence tests for `Sedimentree::diff_remote_fingerprints`'s
//! fragment-aware ancestry pruning.
//!
//! # Failure mode the pruning must avoid
//!
//! A peer can hold a descendant without holding its ancestors —
//! partial sync, restored-from-snapshot peers, and various failure
//! modes all produce this state. If the responder's diff prunes
//! "ancestors of anything the requester claims" transitively, those
//! ancestors are silently dropped from `local_only_commits` and the
//! requester never receives them. Every subsequent diff reports "0
//! missing" and the gap is permanent.
//!
//! # Soundness rule
//!
//! `remote.commit_fingerprints` is a union of three populations:
//!
//!   - **Fragment head** on remote → remote holds head, checkpoints,
//!     and boundaries (sound up to the boundary horizon).
//!   - **Fragment boundary** on remote → remote holds the boundary
//!     commit, but **not** its parents (boundary is the horizon).
//!   - **Loose-commit head** on remote → no transitive guarantee
//!     about parents at all.
//!
//! Only fragment heads carry a transitive guarantee, and only down to
//! their boundaries. The pruning treats a local commit/fragment as a
//! walk root iff its head appears in `remote.fragment_fingerprints`,
//! walks the local loose-commit DAG in both directions from each
//! root, and stops at any non-root commit whose fingerprint is in
//! `remote.commit_fingerprints` (a boundary horizon, or a loose-commit
//! head we can't extend through).
//!
//! # Bandwidth trade-off
//!
//! When one peer has the fragment metadata and the other has neither
//! the fragment nor a matching head, the pruning falls back to "send
//! everything in `local_only_commits`" — duplicates that the receiver
//! has via the fragment blob. The cost is bounded by the fragment's
//! member count and is one-time: after the next sync both peers have
//! the fragment.

#![allow(
    clippy::byte_char_slices,
    clippy::doc_markdown,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_const_for_fn,
    missing_docs
)]

use std::collections::BTreeSet;

use sedimentree_core::{
    blob::{Blob, BlobMeta},
    crypto::fingerprint::FingerprintSeed,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};

fn sed_id() -> SedimentreeId {
    SedimentreeId::new([1u8; 32])
}

fn commit_id(seed: u8) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    CommitId::new(bytes)
}

fn loose(head: u8, parents: &[u8]) -> LooseCommit {
    let blob_meta = BlobMeta::new(&Blob::new(vec![head; 16]));
    let parent_set: BTreeSet<CommitId> = parents.iter().copied().map(commit_id).collect();
    LooseCommit::new(sed_id(), commit_id(head), parent_set, blob_meta)
}

fn fragment(head: u8, boundary: &[u8], checkpoints: &[u8], blob_bytes: u8) -> Fragment {
    let blob_meta = BlobMeta::new(&Blob::new(vec![blob_bytes; 16]));
    let boundary_set: BTreeSet<CommitId> = boundary.iter().copied().map(commit_id).collect();
    let checkpoint_vec: Vec<CommitId> = checkpoints.iter().copied().map(commit_id).collect();
    Fragment::new(
        sed_id(),
        commit_id(head),
        boundary_set,
        &checkpoint_vec,
        blob_meta,
    )
}

fn seed() -> FingerprintSeed {
    FingerprintSeed::new(0xAAAA_BBBB_CCCC_DDDD, 0x1234_5678_9ABC_DEF0)
}

// ============================================================================
// Loose-only ancestor: the responder must enumerate it as missing.
// ============================================================================

/// Linear chain A → B → C → D. Bad peer has B, C, D but is missing A.
/// Server has all four (loose, no fragments). The diff must report A
/// as missing — that's the only way the bad peer recovers.
#[test]
fn sends_missing_ancestor_when_descendant_only_remote_has() {
    let a = loose(b'A', &[]);
    let b = loose(b'B', &[b'A']);
    let c = loose(b'C', &[b'B']);
    let d = loose(b'D', &[b'C']);

    let server = Sedimentree::new(Vec::new(), vec![a, b.clone(), c.clone(), d.clone()]);
    let bad_client = Sedimentree::new(Vec::new(), vec![b, c, d]);

    let bad_summary = bad_client.fingerprint_summarize(&seed());
    let diff = server.diff_remote_fingerprints(&bad_summary);

    let ids: BTreeSet<CommitId> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
    assert!(
        ids.contains(&commit_id(b'A')),
        "missing ancestor A must be sent because no fragment-based \
         coverage justifies pruning it; bad peer can finally recover"
    );
    assert_eq!(diff.local_only_commits.len(), 1, "only A is missing");
}

/// Server holds the entire chain A → B → C → D. Bad peer has only the
/// tip D. All three ancestors should be sent.
#[test]
fn sends_entire_chain_when_remote_holds_only_tip() {
    let a = loose(b'A', &[]);
    let b = loose(b'B', &[b'A']);
    let c = loose(b'C', &[b'B']);
    let d = loose(b'D', &[b'C']);

    let server = Sedimentree::new(Vec::new(), vec![a, b, c, d.clone()]);
    let bad_client = Sedimentree::new(Vec::new(), vec![d]);

    let bad_summary = bad_client.fingerprint_summarize(&seed());
    let diff = server.diff_remote_fingerprints(&bad_summary);

    let ids: BTreeSet<CommitId> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
    assert_eq!(
        diff.local_only_commits.len(),
        3,
        "all three ancestors of D must be sent"
    );
    assert!(ids.contains(&commit_id(b'A')));
    assert!(ids.contains(&commit_id(b'B')));
    assert!(ids.contains(&commit_id(b'C')));
}

/// Branchy DAG: A → {B, C} → M. Bad peer has only M (the merge).
/// A, B, and C all get sent.
#[test]
fn sends_ancestors_in_branching_dag() {
    // Shape:
    //         A
    //        / \
    //       B   C
    //        \ /
    //         M (merge)
    let a = loose(b'A', &[]);
    let b = loose(b'B', &[b'A']);
    let c = loose(b'C', &[b'A']);
    let m = loose(b'M', &[b'B', b'C']);

    let server = Sedimentree::new(Vec::new(), vec![a, b, c, m.clone()]);
    let bad_client = Sedimentree::new(Vec::new(), vec![m]);

    let bad_summary = bad_client.fingerprint_summarize(&seed());
    let diff = server.diff_remote_fingerprints(&bad_summary);

    let ids: BTreeSet<CommitId> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
    assert_eq!(diff.local_only_commits.len(), 3);
    assert!(ids.contains(&commit_id(b'A')));
    assert!(ids.contains(&commit_id(b'B')));
    assert!(ids.contains(&commit_id(b'C')));
}

// ============================================================================
// Soundness: when peers are fully in sync, nothing is sent.
// ============================================================================

#[test]
fn fully_in_sync_peers_diff_is_empty() {
    let a = loose(b'A', &[]);
    let b = loose(b'B', &[b'A']);
    let c = loose(b'C', &[b'B']);
    let d = loose(b'D', &[b'C']);

    let server = Sedimentree::new(Vec::new(), vec![a.clone(), b.clone(), c.clone(), d.clone()]);
    let good_client = Sedimentree::new(Vec::new(), vec![a, b, c, d]);

    let good_summary = good_client.fingerprint_summarize(&seed());
    let diff = server.diff_remote_fingerprints(&good_summary);

    assert_eq!(diff.local_only_commits.len(), 0);
    assert_eq!(diff.local_only_fragments.len(), 0);
}

// ============================================================================
// Fragment-coverage preservation: when both peers have the fragment, the
// fragment-aware pruning correctly elides commits inside the fragment range.
// Convention: head = newest, boundary = oldest (sedimentree standard).
// ============================================================================

/// Linear A → B → C → D (oldest → newest). Both peers have loose
/// commits A, B, C, D AND a shared fragment F(head=D, boundary={A},
/// checkpoints={B,C}). Bad peer is missing nothing — diff is empty.
#[test]
fn shared_fragment_with_loose_commits_diff_is_empty() {
    let a = loose(b'A', &[]);
    let b = loose(b'B', &[b'A']);
    let c = loose(b'C', &[b'B']);
    let d = loose(b'D', &[b'C']);
    let frag = fragment(b'D', &[b'A'], &[b'B', b'C'], 1);

    let server = Sedimentree::new(
        vec![frag.clone()],
        vec![a.clone(), b.clone(), c.clone(), d.clone()],
    );
    let other = Sedimentree::new(vec![frag], vec![a, b, c, d]);

    let other_summary = other.fingerprint_summarize(&seed());
    let diff = server.diff_remote_fingerprints(&other_summary);

    assert_eq!(diff.local_only_commits.len(), 0);
    assert_eq!(diff.local_only_fragments.len(), 0);
}

/// Server has loose A → B → C → D AND fragment F(head=D, boundary={A},
/// checkpoints={B,C}). Remote has only the fragment (no loose
/// commits). B and C fall inside F's range and get pruned because
/// the walk from D (matched fragment head) stops at boundary A
/// (which is in `remote.commit_fingerprints`).
#[test]
fn fragment_aware_walk_prunes_commits_inside_fragment_range() {
    let a = loose(b'A', &[]);
    let b = loose(b'B', &[b'A']);
    let c = loose(b'C', &[b'B']);
    let d = loose(b'D', &[b'C']);
    let frag = fragment(b'D', &[b'A'], &[b'B', b'C'], 1);

    let server = Sedimentree::new(vec![frag.clone()], vec![a, b, c, d]);
    // Remote: only the fragment.
    let remote = Sedimentree::new(vec![frag], Vec::new());

    let remote_summary = remote.fingerprint_summarize(&seed());
    let diff = server.diff_remote_fingerprints(&remote_summary);

    // A is in remote.commit_fingerprints (boundary), so it's already
    // not in `local_only_commits` after the first filter. D is in
    // there too (head). B and C are only excluded if the fragment-aware
    // walk reaches them.
    let ids: BTreeSet<CommitId> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
    assert!(
        !ids.contains(&commit_id(b'B')),
        "B is inside fragment range (head=D, boundary=A); walk from D \
         through parents traverses to B and marks it covered"
    );
    assert!(
        !ids.contains(&commit_id(b'C')),
        "C is inside fragment range; walk from D traverses through C"
    );
}

/// **Critical correctness case**: server has loose P → B → H (P is
/// older, H is newer) AND fragment F(head=H, boundary={B}). Remote
/// has fragment F + loose B. P is older than the fragment boundary
/// so the remote doesn't have it. P must be sent.
///
/// The walk from H stops at B (which is in
/// `remote.commit_fingerprints` as the fragment boundary), so P is
/// never marked covered.
#[test]
fn does_not_prune_commit_past_fragment_boundary() {
    let p = loose(b'P', &[]);
    let b = loose(b'B', &[b'P']);
    let h = loose(b'H', &[b'B']);
    let frag = fragment(b'H', &[b'B'], &[], 1);

    let server = Sedimentree::new(vec![frag.clone()], vec![p, b.clone(), h]);
    let remote = Sedimentree::new(vec![frag], vec![b]);

    let remote_summary = remote.fingerprint_summarize(&seed());
    let diff = server.diff_remote_fingerprints(&remote_summary);

    let ids: BTreeSet<CommitId> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
    assert!(
        ids.contains(&commit_id(b'P')),
        "P is older than the fragment boundary; the fragment doesn't cover \
         it, so it must be sent"
    );
}

/// Asymmetric fragment with local matching the head: even though the
/// local doesn't have the fragment object, it has a loose commit D
/// whose head equals the remote's fragment head. The fragment-aware
/// walk uses D as a root, traverses through C → B and stops at A
/// (which is the remote's fragment boundary, in
/// `remote.commit_fingerprints`). Optimization preserved without
/// requiring the local to hold the fragment metadata.
#[test]
fn asymmetric_fragment_optimization_via_matching_local_head() {
    let a = loose(b'A', &[]);
    let b = loose(b'B', &[b'A']);
    let c = loose(b'C', &[b'B']);
    let d = loose(b'D', &[b'C']);
    let frag = fragment(b'D', &[b'A'], &[b'B', b'C'], 1);

    // Local has loose commits but no fragment.
    let local = Sedimentree::new(Vec::new(), vec![a, b, c, d]);
    // Remote has only the fragment.
    let remote = Sedimentree::new(vec![frag], Vec::new());

    let remote_summary = remote.fingerprint_summarize(&seed());
    let diff = local.diff_remote_fingerprints(&remote_summary);

    assert_eq!(
        diff.local_only_commits.len(),
        0,
        "local's loose D matches the remote's fragment head; the walk \
         covers B and C en route to boundary A — no duplicate sends"
    );
}

/// True asymmetric case: local has loose A, B, C only (no D, no
/// fragment). Remote has fragment F(head=D, boundary=A, ckpts=B,C).
/// Local has no walk root (no head matches D), so it falls back to
/// sending all local-only commits.
///
/// **This** is where the documented bandwidth cost shows up — the
/// receiver already has B and C inside the fragment blob.
#[test]
fn asymmetric_fragment_no_matching_head_does_send_duplicates() {
    let a = loose(b'A', &[]);
    let b = loose(b'B', &[b'A']);
    let c = loose(b'C', &[b'B']);
    // No D on local.
    let frag = fragment(b'D', &[b'A'], &[b'B', b'C'], 1);

    let local = Sedimentree::new(Vec::new(), vec![a, b, c]);
    let remote = Sedimentree::new(vec![frag], Vec::new());

    let remote_summary = remote.fingerprint_summarize(&seed());
    let diff = local.diff_remote_fingerprints(&remote_summary);

    let ids: BTreeSet<CommitId> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
    assert!(
        ids.contains(&commit_id(b'B')) || ids.contains(&commit_id(b'C')),
        "no walk root (local has neither fragment F nor a loose commit \
         with head=D); local sends B and/or C as duplicates that the \
         remote already has via the fragment"
    );
}
