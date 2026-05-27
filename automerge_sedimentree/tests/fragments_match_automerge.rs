//! Verifies that [`Automerge::fragments`] produces fragments whose
//! `members` lists are disjoint from the `members` lists of all
//! ancestors reachable through `boundary`.
//!
//! Why this matters: the ingest adapter remaps loose-commit parents that
//! point at a non-head fragment member to the containing fragment's head,
//! so that `Sedimentree::minimize` doesn't treat the loose commit as
//! covered and prune it. That remap is only safe if a given change
//! appears in at most one ancestor chain — if a change appeared in both
//! a fragment F and one of F's transitive boundary ancestors, the remap
//! would be ambiguous and `minimize` could lose data.
//!
//! Scope: this test covers only the ancestor-overlap case. It does NOT
//! assert global uniqueness across the whole tree — concurrent sibling
//! fragments are allowed to share members (a change reachable from two
//! concurrent fragment heads whose `boundary` lists don't see each
//! other), and that's fine for the remap because no two such siblings
//! sit on the same ancestor chain.
//!
//! For each egwalker test vector this loads the document, asks Automerge
//! for all cached fragments (level ≥ 1), and checks the invariant by
//! walking each fragment's `boundary` breadth-first and asserting no
//! `members` overlap.
//! Level-0 fragments are single-member loose changes whose `boundary`
//! points at raw change parents (not fragment heads), so the
//! ancestor-walk model doesn't apply to them and they're excluded.
//!
//! [`Automerge::fragments`]: automerge::Automerge::fragments

#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)]

use std::collections::{HashMap, HashSet};

use automerge::{Automerge, ChangeHash};
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

#[test]
fn automerge_fragment_members_disjoint_from_ancestor_members() -> TestResult {
    let mut failures: Vec<String> = Vec::new();

    for (name, bytes) in VECTORS {
        let doc = Automerge::load(bytes)?;

        // Cached fragments only — level-0 fragments are single-member
        // loose changes whose boundary points at raw change parents
        // (not fragment heads), so the ancestor-walk model doesn't apply.
        let cached = doc.fragments(1..);
        let by_head: HashMap<ChangeHash, _> = cached.iter().map(|f| (f.head, f)).collect();

        let mut total_overlap = 0usize;
        let mut sample_overlaps: Vec<(ChangeHash, ChangeHash)> = Vec::new();

        for f in &cached {
            // Union members of every fragment reachable via boundary.
            let mut ancestor_members: HashSet<ChangeHash> = HashSet::new();
            let mut to_visit: Vec<ChangeHash> = f.boundary.clone();
            let mut seen: HashSet<ChangeHash> = HashSet::new();
            while let Some(b) = to_visit.pop() {
                if !seen.insert(b) {
                    continue;
                }
                if let Some(anc) = by_head.get(&b) {
                    ancestor_members.extend(anc.members.iter().copied());
                    to_visit.extend(anc.boundary.iter().copied());
                }
            }

            for m in &f.members {
                if ancestor_members.contains(m) {
                    total_overlap += 1;
                    if sample_overlaps.len() < 5 {
                        sample_overlaps.push((f.head, *m));
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
        "Automerge::fragments() produced overlapping members:\n  {}",
        failures.join("\n  ")
    );

    Ok(())
}
