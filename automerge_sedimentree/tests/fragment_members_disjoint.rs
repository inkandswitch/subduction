//! Regression test: a commit must belong to at most one fragment in the tree.
//!
//! Suspected bug in `sedimentree_core::commit::build_fragment_store`: under
//! high concurrency, the BFS that assigns members to a fragment can include
//! commits that are *also* members of a fragment reachable through the
//! current fragment's boundary. A commit appearing as a member of both a
//! fragment and one of its ancestor fragments would cause that change's ops
//! to be loaded twice during sedimentree reassembly, or worse, be silently
//! dropped depending on which path `minimize` follows.
//!
//! This test loads each egwalker test vector, runs `build_fragment_store`,
//! and asserts that for every produced fragment, its `members` set is
//! disjoint from the union of `members` of all fragments reachable through
//! its `boundary`.

#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)]

use automerge::Automerge;
use automerge_sedimentree::indexed::{IndexedSedimentreeAutomerge, OwnedParents};
use sedimentree_core::{
    collections::{Map, Set},
    commit::{CommitStore, CountLeadingZeroBytes, FragmentState},
    loose_commit::id::CommitId,
};
use testresult::TestResult;

const VECTORS: &[(&str, &[u8])] = &[
    ("A1", include_bytes!("../test-vectors/A1.am")),
    ("A2", include_bytes!("../test-vectors/A2.am")),
    ("C1", include_bytes!("../test-vectors/C1.am")),
    ("C2", include_bytes!("../test-vectors/C2.am")),
    ("S1", include_bytes!("../test-vectors/S1.am")),
    ("S2", include_bytes!("../test-vectors/S2.am")),
    ("S3", include_bytes!("../test-vectors/S3.am")),
];

/// Collect the union of `members` across every fragment reachable from
/// `start`'s boundary (transitively). Only follows boundary keys that are
/// themselves fragment heads in `known` — boundaries can also point at
/// depth-0 commits below the deepest level, which are not fragments.
fn ancestor_members(
    start: &FragmentState<OwnedParents>,
    known: &Map<CommitId, FragmentState<OwnedParents>>,
) -> Set<CommitId> {
    let mut acc: Set<CommitId> = Set::new();
    let mut to_visit: Vec<CommitId> = start.boundary().keys().copied().collect();
    let mut seen: Set<CommitId> = Set::new();

    while let Some(b) = to_visit.pop() {
        if !seen.insert(b) {
            continue;
        }
        if let Some(anc) = known.get(&b) {
            acc.extend(anc.members().iter().copied());
            to_visit.extend(anc.boundary().keys().copied());
        }
    }

    acc
}

#[test]
fn fragment_members_disjoint_from_ancestor_members() -> TestResult {
    let mut failures: Vec<String> = Vec::new();

    for (name, bytes) in VECTORS {
        let doc = Automerge::load(bytes)?;
        let metadata = doc.get_changes_meta(&[]);
        let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
        let heads: Vec<CommitId> = doc
            .get_heads()
            .iter()
            .map(|h| CommitId::new(h.0))
            .collect();

        let mut known: Map<CommitId, FragmentState<OwnedParents>> = Map::new();
        store
            .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
            .expect("build_fragment_store");

        let mut total_overlap = 0usize;
        let mut sample_overlaps: Vec<(CommitId, CommitId)> = Vec::new();

        for (head, state) in &known {
            let ancestor = ancestor_members(state, &known);
            for m in state.members() {
                if ancestor.contains(m) {
                    total_overlap += 1;
                    if sample_overlaps.len() < 5 {
                        sample_overlaps.push((*head, *m));
                    }
                }
            }
        }

        if total_overlap > 0 {
            failures.push(format!(
                "{name}: {total_overlap} member(s) appear in both a fragment and one of its ancestors. \
                 Sample (fragment_head, member): {sample_overlaps:?}"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "fragment members overlap with ancestor fragments:\n  {}",
        failures.join("\n  ")
    );

    Ok(())
}
