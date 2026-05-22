//! Counterpart to `fragment_members_disjoint.rs`: confirms that the
//! ancestor-disjointness invariant holds when fragmentation is delegated to
//! `Automerge::fragments()` instead of `sedimentree_core::build_fragment_store`.
//!
//! For each egwalker test vector, asks Automerge for the cached fragments
//! (level >= 1) and verifies that for every fragment F, none of F's `members`
//! appear in the `members` of any fragment reachable through F's `boundary`.
//!
//! Note: this does NOT assert that every change belongs to exactly one
//! fragment across the whole tree. Automerge fragments can legitimately
//! share members across concurrent siblings (changes reachable from two
//! concurrent fragment heads whose parents don't see them). The bug we're
//! tracking is specifically the ancestor-overlap case, where a member of
//! a fragment also appears in one of its own ancestors — that would break
//! sedimentree's `minimize` and loose-commit parent remapping.

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
        let by_head: HashMap<ChangeHash, _> =
            cached.iter().map(|f| (f.head, f)).collect();

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

