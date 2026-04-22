//! `Fragment` generators.
//!
//! A fragment represents a sealed range of history anchored at a boundary commit. These
//! generators produce realistically-shaped fragments with configurable boundary, checkpoint, and
//! depth parameters — useful for benchmarking minimisation, merging, and diffing.

use std::collections::BTreeSet;

use sedimentree_core::{fragment::Fragment, loose_commit::id::CommitId};

use super::{blobs::synthetic_blob_meta, ids};

/// Construct a synthetic `Fragment` with configurable structural complexity.
///
/// | Parameter         | Effect                                                          |
/// |-------------------|-----------------------------------------------------------------|
/// | `head_seed`       | RNG seed; determines the head commit id and all derived seeds  |
/// | `boundary_count`  | Number of boundary commits (predecessors of this fragment)     |
/// | `checkpoint_count`| Number of checkpoints embedded in the fragment                 |
/// | `leading_zeros`   | Depth of the head commit (for depth-metric experiments)        |
///
/// All derived commit ids share the `leading_zeros` prefix for boundary commits and an
/// unconstrained prefix for checkpoint commits.
#[must_use]
pub fn synthetic_fragment(
    head_seed: u64,
    boundary_count: usize,
    checkpoint_count: usize,
    leading_zeros: usize,
) -> Fragment {
    let sedimentree_id = ids::sedimentree_id_from_seed(head_seed);
    let head = ids::commit_id_with_leading_zeros(leading_zeros, head_seed);

    let boundary: BTreeSet<CommitId> = (0..boundary_count)
        .map(|i| {
            ids::commit_id_with_leading_zeros(
                leading_zeros,
                head_seed.wrapping_add(100).wrapping_add(i as u64),
            )
        })
        .collect();

    let checkpoints: Vec<CommitId> = (0..checkpoint_count)
        .map(|i| ids::commit_id_from_seed(head_seed.wrapping_add(200).wrapping_add(i as u64)))
        .collect();

    let blob_meta = synthetic_blob_meta(head_seed.wrapping_add(300), 4096);

    Fragment::new(sedimentree_id, head, boundary, &checkpoints, blob_meta)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragment_has_expected_boundary_size() {
        let frag = synthetic_fragment(0, 3, 5, 1);
        assert_eq!(frag.boundary().len(), 3);
    }

    #[test]
    fn same_seed_same_fragment_head() {
        assert_eq!(
            synthetic_fragment(7, 2, 4, 1).head(),
            synthetic_fragment(7, 2, 4, 1).head(),
        );
    }

    #[test]
    fn different_seeds_different_heads() {
        assert_ne!(
            synthetic_fragment(1, 2, 4, 1).head(),
            synthetic_fragment(2, 2, 4, 1).head(),
        );
    }
}
