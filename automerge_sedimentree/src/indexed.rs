//! Pre-indexed [`CommitStore`] for Automerge documents.
//!
//! [`IndexedSedimentreeAutomerge`] pre-indexes all change metadata into a
//! map on construction (one O(n) pass), then serves every lookup in
//! **O(1)** (with `std`) or **O(log n)** (in `no_std`).
//!
//! The production ingest path uses [`Automerge::fragments`] /
//! [`Automerge::bundle_fragments`] (see [`crate::ingest`]). This module
//! exists as a [`CommitStore`]-based reference implementation that the
//! tests and benches in this crate compare against — it walks the
//! `build_fragment_store` algorithm directly, which makes it useful for
//! validating that the upstream fragmentizer produces equivalent output.
//!
//! [`Automerge::fragments`]: automerge::Automerge::fragments
//! [`Automerge::bundle_fragments`]: automerge::Automerge::bundle_fragments

use core::convert::Infallible;

use automerge::Automerge;
use sedimentree_core::{
    collections::{Map, Set},
    commit::{CommitStore, Parents},
    loose_commit::id::CommitId,
};

/// Pre-indexed parent digests for a single commit.
///
/// Owns its data rather than borrowing from the Automerge document, so
/// lookups against [`IndexedSedimentreeAutomerge`] don't keep a borrow of
/// the source `Automerge` alive.
#[derive(Debug, Clone)]
pub struct OwnedParents(Set<CommitId>);

impl Parents for OwnedParents {
    fn parents(&self) -> Set<CommitId> {
        self.0.clone()
    }
}

/// A pre-indexed [`CommitStore`] for Automerge documents.
///
/// Pre-indexes every change's parents into a map on construction (one
/// O(n) pass), then serves every [`CommitStore::lookup`] in **O(1)**
/// (with `std`) or **O(log n)** (in `no_std`). Driving
/// [`CommitStore::build_fragment_store`] over an indexed store therefore
/// runs in **O(n)** total rather than O(n²).
///
/// # Example
///
/// ```rust,no_run
/// use automerge_sedimentree::indexed::IndexedSedimentreeAutomerge;
/// use sedimentree_core::commit::{CommitStore, CountLeadingZeroBytes};
///
/// # fn example(doc: &automerge::Automerge) {
/// let store = IndexedSedimentreeAutomerge::from(doc);
/// // store.build_fragment_store(...) is now O(n) instead of O(n²)
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct IndexedSedimentreeAutomerge {
    index: Map<CommitId, OwnedParents>,
}

impl IndexedSedimentreeAutomerge {
    /// Build the index from an already-fetched slice of changes.
    ///
    /// Use this when you already have the changes (e.g., from
    /// `doc.get_changes(&[])`) to avoid calling `get_changes` a second time.
    #[must_use]
    pub fn from_changes(changes: &[automerge::Change]) -> Self {
        let mut index = Map::new();

        for change in changes {
            let id = CommitId::new(change.hash().0);
            let parents: Set<CommitId> = change
                .deps()
                .iter()
                .map(|dep| CommitId::new(dep.0))
                .collect();
            index.insert(id, OwnedParents(parents));
        }

        Self { index }
    }

    /// Build the index from lightweight change metadata.
    ///
    /// Prefer this over [`from_changes`](Self::from_changes) when you don't
    /// need the full `Change` objects (which carry serialized operations).
    /// Automerge's `get_changes_meta(&[])` returns metadata without
    /// reconstructing operation data, which is significantly cheaper —
    /// especially in debug builds where `get_changes` runs a redundant
    /// verification pass.
    #[must_use]
    pub fn from_metadata(metadata: &[automerge::ChangeMetadata<'_>]) -> Self {
        let mut index = Map::new();

        for meta in metadata {
            let id = CommitId::new(meta.hash.0);
            let parents: Set<CommitId> = meta.deps.iter().map(|dep| CommitId::new(dep.0)).collect();
            index.insert(id, OwnedParents(parents));
        }

        Self { index }
    }
}

impl From<&Automerge> for IndexedSedimentreeAutomerge {
    fn from(doc: &Automerge) -> Self {
        Self::from_metadata(&doc.get_changes_meta(&[]))
    }
}

impl CommitStore<'static> for IndexedSedimentreeAutomerge {
    type Node = OwnedParents;
    type LookupError = Infallible;

    fn lookup(&self, id: CommitId) -> Result<Option<Self::Node>, Self::LookupError> {
        Ok(self.index.get(&id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::{AutoCommit, ChangeHash, ObjType, transaction::Transactable};
    use sedimentree_core::commit::{CountLeadingZeroBytes, FragmentState};
    use testresult::TestResult;

    fn build_test_doc(num_changes: usize) -> TestResult<AutoCommit> {
        let mut doc = AutoCommit::new();
        let list = doc.put_object(automerge::ROOT, "items", ObjType::List)?;
        for i in 0..num_changes {
            doc.insert(&list, i, format!("item-{i}"))?;
        }
        Ok(doc)
    }

    #[test]
    fn roundtrip_fragment_decomposition() -> TestResult {
        let mut doc = build_test_doc(2000)?;
        let am_doc = doc.document();

        let original_bytes = am_doc.save();

        // Build the index from lightweight metadata (avoids expensive
        // change reconstruction + debug_assert doubling in get_changes).
        let metadata = am_doc.get_changes_meta(&[]);
        let change_count = metadata.len();
        assert!(change_count > 0, "document should have changes");

        let commit_store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
        let heads: Vec<CommitId> = am_doc
            .get_heads()
            .iter()
            .map(|h| CommitId::new(h.0))
            .collect();
        let strategy = CountLeadingZeroBytes;
        let mut known_states: Map<CommitId, FragmentState<OwnedParents>> = Map::new();

        let fragment_states =
            commit_store.build_fragment_store(&heads, &mut known_states, &strategy)?;

        // Only now fetch full changes for raw byte extraction.
        let changes = am_doc.get_changes(&[]);
        let changes_by_hash: Map<ChangeHash, &automerge::Change> =
            changes.iter().map(|c| (c.hash(), c)).collect();

        let mut fragment_blobs: Vec<Vec<u8>> = Vec::new();
        for state in &fragment_states {
            let mut blob = Vec::new();
            for member_digest in state.members() {
                let hash = ChangeHash(*member_digest.as_bytes());
                let change = changes_by_hash
                    .get(&hash)
                    .ok_or("member should be in changes_by_hash")?;
                blob.extend_from_slice(change.raw_bytes());
            }
            fragment_blobs.push(blob);
        }

        let covered: Set<CommitId> = known_states
            .values()
            .flat_map(|state| state.members().iter().copied())
            .collect();

        let uncovered_blobs: Vec<Vec<u8>> = changes
            .iter()
            .filter(|change| {
                let id = CommitId::new(change.hash().0);
                !covered.contains(&id)
            })
            .map(|change| change.raw_bytes().to_vec())
            .collect();

        let total_covered = covered.len() + uncovered_blobs.len();
        assert_eq!(
            total_covered,
            change_count,
            "covered ({}) + uncovered ({}) should equal total changes ({change_count})",
            covered.len(),
            uncovered_blobs.len()
        );

        let mut reassembled = Automerge::new();
        for blob in &fragment_blobs {
            reassembled.load_incremental(blob)?;
        }
        for blob in &uncovered_blobs {
            reassembled.load_incremental(blob)?;
        }

        let reassembled_bytes = reassembled.save();
        assert_eq!(am_doc.get_heads(), reassembled.get_heads());
        assert_eq!(change_count, reassembled.get_changes(&[]).len());
        assert_eq!(original_bytes, reassembled_bytes);
        Ok(())
    }
}
