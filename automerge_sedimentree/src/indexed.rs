//! Pre-indexed [`CommitStore`] for large Automerge documents.
//!
//! [`IndexedSedimentreeAutomerge`] pre-indexes all change metadata into a
//! [`HashMap`](std::collections::HashMap) on construction (one O(n) pass),
//! then serves every lookup in **O(1)**. Use this for documents with many
//! changes (>10k) where the quadratic cost of
//! [`SedimentreeAutomerge`](crate::SedimentreeAutomerge) becomes prohibitive.

use core::convert::Infallible;

use automerge::Automerge;
use sedimentree_core::{
    collections::Set,
    commit::{CommitStore, Parents},
    crypto::digest::Digest,
    loose_commit::LooseCommit,
};

/// Pre-indexed parent digests for a single commit.
///
/// Unlike [`SedimentreeChangeMetadata`](crate::SedimentreeChangeMetadata),
/// this type owns its data and does not borrow from the Automerge document.
/// Lookups against [`IndexedSedimentreeAutomerge`] are O(1) instead of the
/// O(n) linear scan that [`SedimentreeAutomerge`](crate::SedimentreeAutomerge)
/// incurs per call.
#[derive(Debug, Clone)]
pub struct OwnedParents(Set<Digest<LooseCommit>>);

impl Parents for OwnedParents {
    fn parents(&self) -> Set<Digest<LooseCommit>> {
        self.0.clone()
    }
}

/// A pre-indexed [`CommitStore`] for large Automerge documents.
///
/// [`SedimentreeAutomerge`](crate::SedimentreeAutomerge) calls
/// `doc.get_change_meta_by_hash()` on every [`CommitStore::lookup`], which
/// may be O(n) inside Automerge's internal storage. For a document with _n_
/// changes, `build_fragment_store` makes O(n) lookups, giving **O(n²)** total.
///
/// `IndexedSedimentreeAutomerge` pre-indexes all change metadata into a
/// [`HashMap`](std::collections::HashMap) on construction (one O(n) pass),
/// then serves every lookup in **O(1)**.
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
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
#[derive(Debug, Clone)]
pub struct IndexedSedimentreeAutomerge {
    index: sedimentree_core::collections::Map<Digest<LooseCommit>, OwnedParents>,
}

impl IndexedSedimentreeAutomerge {
    /// Build the index from an already-fetched slice of changes.
    ///
    /// Use this when you already have the changes (e.g., from
    /// `doc.get_changes(&[])`) to avoid calling `get_changes` a second time.
    #[must_use]
    pub fn from_changes(changes: &[automerge::Change]) -> Self {
        let mut index = sedimentree_core::collections::Map::with_capacity(changes.len());

        for change in changes {
            let digest = Digest::force_from_bytes(change.hash().0);
            let parents: Set<Digest<LooseCommit>> = change
                .deps()
                .iter()
                .map(|dep| Digest::force_from_bytes(dep.0))
                .collect();
            index.insert(digest, OwnedParents(parents));
        }

        Self { index }
    }
}

impl From<&Automerge> for IndexedSedimentreeAutomerge {
    fn from(doc: &Automerge) -> Self {
        Self::from_changes(&doc.get_changes(&[]))
    }
}

impl CommitStore<'static> for IndexedSedimentreeAutomerge {
    type Node = OwnedParents;
    type LookupError = Infallible;

    fn lookup(&self, digest: Digest<LooseCommit>) -> Result<Option<Self::Node>, Self::LookupError> {
        Ok(self.index.get(&digest).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::{AutoCommit, ChangeHash, ObjType, transaction::Transactable};
    use sedimentree_core::{
        collections::Map,
        commit::{CountLeadingZeroBytes, FragmentState},
    };
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
        let changes = am_doc.get_changes(&[]);
        let change_count = changes.len();
        assert!(change_count > 0, "document should have changes");

        let commit_store = IndexedSedimentreeAutomerge::from_changes(&changes);
        let heads: Vec<Digest<LooseCommit>> = am_doc
            .get_heads()
            .iter()
            .map(|h| Digest::force_from_bytes(h.0))
            .collect();
        let strategy = CountLeadingZeroBytes;
        let mut known_states: Map<Digest<LooseCommit>, FragmentState<OwnedParents>> = Map::new();

        let fragment_states =
            commit_store.build_fragment_store(&heads, &mut known_states, &strategy)?;

        let changes_by_hash: std::collections::HashMap<ChangeHash, &automerge::Change> =
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

        let covered: Set<Digest<LooseCommit>> = known_states
            .values()
            .flat_map(|state| state.members().iter().copied())
            .collect();

        let uncovered_blobs: Vec<Vec<u8>> = changes
            .iter()
            .filter(|change| {
                let digest = Digest::force_from_bytes(change.hash().0);
                !covered.contains(&digest)
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
