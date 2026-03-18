//! # Automerge integration for Sedimentree

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

use alloc::rc::Rc;
use core::{cell::RefCell, convert::Infallible};
use sedimentree_core::collections::Set;

use automerge::{AutoCommit, Automerge, ChangeHash, ChangeMetadata};
use sedimentree_core::{
    commit::{CommitStore, Parents},
    crypto::digest::Digest,
    loose_commit::LooseCommit,
};

/// A newtype wrapper around [`Automerge`] for use as a Sedimentree commit store.
#[derive(Debug, Clone)]
pub struct SedimentreeAutomerge<'a>(&'a Automerge);

impl<'a> From<&'a Automerge> for SedimentreeAutomerge<'a> {
    fn from(value: &'a Automerge) -> Self {
        Self(value)
    }
}

impl<'a> From<SedimentreeAutomerge<'a>> for &'a Automerge {
    fn from(value: SedimentreeAutomerge<'a>) -> Self {
        value.0
    }
}

impl<'a> CommitStore<'a> for SedimentreeAutomerge<'a> {
    type Node = SedimentreeChangeMetadata<'a>;
    type LookupError = Infallible;

    fn lookup(&self, digest: Digest<LooseCommit>) -> Result<Option<Self::Node>, Self::LookupError> {
        let change_hash = automerge::ChangeHash(*digest.as_bytes());
        let change_meta = self.0.get_change_meta_by_hash(&change_hash);
        Ok(change_meta.map(SedimentreeChangeMetadata::from))
    }
}

/// A newtype wrapper around [`Automerge`] for use as a Sedimentree commit store.
#[derive(Debug, Clone)]
pub struct SedimentreeAutoCommit(Rc<RefCell<AutoCommit>>);

impl From<Rc<RefCell<AutoCommit>>> for SedimentreeAutoCommit {
    fn from(value: Rc<RefCell<AutoCommit>>) -> Self {
        Self(value)
    }
}

impl From<SedimentreeAutoCommit> for Rc<RefCell<AutoCommit>> {
    fn from(value: SedimentreeAutoCommit) -> Self {
        value.0
    }
}

impl CommitStore<'static> for SedimentreeAutoCommit {
    type Node = SedimentreeChangeMetadata<'static>;
    type LookupError = Infallible;

    fn lookup(&self, digest: Digest<LooseCommit>) -> Result<Option<Self::Node>, Self::LookupError> {
        let change_hash = ChangeHash(*digest.as_bytes());
        let mut borrowed = self.0.borrow_mut();
        let change_meta = borrowed.get_change_meta_by_hash(&change_hash);
        Ok(change_meta.map(|x| SedimentreeChangeMetadata::from(x.into_owned())))
    }
}

/// A newtype wrapper around Automerge's [`ChangeMetadata`].
#[derive(Debug, Clone)]
pub struct SedimentreeChangeMetadata<'a>(ChangeMetadata<'a>);

impl<'a> From<ChangeMetadata<'a>> for SedimentreeChangeMetadata<'a> {
    fn from(value: ChangeMetadata<'a>) -> Self {
        Self(value)
    }
}

impl<'a> From<SedimentreeChangeMetadata<'a>> for ChangeMetadata<'a> {
    fn from(value: SedimentreeChangeMetadata<'a>) -> Self {
        value.0
    }
}

impl Parents for SedimentreeChangeMetadata<'_> {
    fn parents(&self) -> Set<Digest<LooseCommit>> {
        self.0
            .deps
            .iter()
            .map(|change_hash| Digest::force_from_bytes(change_hash.0))
            .collect()
    }
}

// ============================================================================
// Indexed (pre-hashed) variant for large documents
// ============================================================================

/// Pre-indexed parent digests for a single commit.
///
/// Unlike [`SedimentreeChangeMetadata`], this type owns its data and does not
/// borrow from the Automerge document. Lookups against
/// [`IndexedSedimentreeAutomerge`] are O(1) instead of the O(n) linear scan
/// that [`SedimentreeAutomerge`] incurs per call.
#[cfg(feature = "std")]
#[derive(Debug, Clone)]
pub struct OwnedParents(Set<Digest<LooseCommit>>);

#[cfg(feature = "std")]
impl Parents for OwnedParents {
    fn parents(&self) -> Set<Digest<LooseCommit>> {
        self.0.clone()
    }
}

/// A pre-indexed [`CommitStore`] for large Automerge documents.
///
/// [`SedimentreeAutomerge`] calls `doc.get_change_meta_by_hash()` on every
/// [`CommitStore::lookup`], which may be O(n) inside Automerge's internal
/// storage. For a document with _n_ changes, `build_fragment_store` makes
/// O(n) lookups, giving **O(n²)** total cost.
///
/// `IndexedSedimentreeAutomerge` pre-indexes all change metadata into a
/// [`HashMap`](std::collections::HashMap) on construction (one O(n) pass),
/// then serves every lookup in **O(1)**. Use this for documents with many
/// changes (>10k) where the quadratic cost becomes prohibitive.
///
/// # Example
///
/// ```rust,no_run
/// use automerge_sedimentree::IndexedSedimentreeAutomerge;
/// use sedimentree_core::commit::{CommitStore, CountLeadingZeroBytes};
///
/// # fn example(doc: &automerge::Automerge) {
/// let store = IndexedSedimentreeAutomerge::from(doc);
/// // store.build_fragment_store(...) is now O(n) instead of O(n²)
/// # }
/// ```
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
#[derive(Debug, Clone)]
pub struct IndexedSedimentreeAutomerge {
    index: sedimentree_core::collections::Map<Digest<LooseCommit>, OwnedParents>,
}

#[cfg(feature = "std")]
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

#[cfg(feature = "std")]
impl From<&Automerge> for IndexedSedimentreeAutomerge {
    fn from(doc: &Automerge) -> Self {
        Self::from_changes(&doc.get_changes(&[]))
    }
}

#[cfg(feature = "std")]
impl CommitStore<'static> for IndexedSedimentreeAutomerge {
    type Node = OwnedParents;
    type LookupError = Infallible;

    fn lookup(&self, digest: Digest<LooseCommit>) -> Result<Option<Self::Node>, Self::LookupError> {
        Ok(self.index.get(&digest).cloned())
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use automerge::{transaction::Transactable, AutoCommit, ObjType};
    use sedimentree_core::{
        collections::Map,
        commit::{CountLeadingZeroBytes, FragmentState},
    };

    /// Build an Automerge document with enough changes to produce multiple fragments.
    ///
    /// Uses `CountLeadingZeroBytes` which triggers a fragment boundary roughly
    /// every 256 changes (probability 1/256 of a leading zero byte in SHA-256).
    fn build_test_doc(num_changes: usize) -> AutoCommit {
        let mut doc = AutoCommit::new();
        let list = doc
            .put_object(automerge::ROOT, "items", ObjType::List)
            .expect("put_object");
        for i in 0..num_changes {
            doc.insert(&list, i, format!("item-{i}")).expect("insert");
        }
        doc
    }

    /// Decompose an Automerge document into fragment blobs + uncovered change blobs,
    /// then reassemble and verify the result matches the original.
    #[test]
    fn roundtrip_fragment_decomposition() {
        // 1. Build a document with enough changes to get multiple fragments
        let mut doc = build_test_doc(2000);
        let am_doc = doc.document();

        let original_bytes = am_doc.save();
        let changes = am_doc.get_changes(&[]);
        let change_count = changes.len();
        assert!(change_count > 0, "document should have changes");

        // 2. Build fragment structure
        let commit_store = IndexedSedimentreeAutomerge::from_changes(&changes);
        let heads: Vec<Digest<LooseCommit>> = am_doc
            .get_heads()
            .iter()
            .map(|h| Digest::force_from_bytes(h.0))
            .collect();
        let strategy = CountLeadingZeroBytes;
        let mut known_states: Map<Digest<LooseCommit>, FragmentState<OwnedParents>> = Map::new();

        let fragment_states = commit_store
            .build_fragment_store(&heads, &mut known_states, &strategy)
            .expect("build_fragment_store");

        // 3. Index changes by hash
        let changes_by_hash: std::collections::HashMap<ChangeHash, &automerge::Change> =
            changes.iter().map(|c| (c.hash(), c)).collect();

        // 4. Build fragment blobs
        let mut fragment_blobs: Vec<Vec<u8>> = Vec::new();
        for state in &fragment_states {
            let mut blob = Vec::new();
            for member_digest in state.members() {
                let hash = ChangeHash(*member_digest.as_bytes());
                let change = changes_by_hash
                    .get(&hash)
                    .expect("member should be in changes_by_hash");
                blob.extend_from_slice(change.raw_bytes());
            }
            fragment_blobs.push(blob);
        }

        // 5. Identify uncovered changes (not in any fragment's members)
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

        // Sanity: total members + uncovered should cover all changes
        let total_covered = covered.len() + uncovered_blobs.len();
        assert_eq!(
            total_covered,
            change_count,
            "covered ({}) + uncovered ({}) should equal total changes ({change_count})",
            covered.len(),
            uncovered_blobs.len()
        );

        // 6. Reassemble: load all fragment blobs + uncovered blobs into a fresh doc
        let mut reassembled = Automerge::new();
        for blob in &fragment_blobs {
            reassembled
                .load_incremental(blob)
                .expect("load_incremental for fragment blob");
        }
        for blob in &uncovered_blobs {
            reassembled
                .load_incremental(blob)
                .expect("load_incremental for uncovered blob");
        }

        // 7. Verify: the reassembled document should produce identical saved bytes
        let reassembled_bytes = reassembled.save();

        // Compare heads (the canonical way to check document equality)
        let original_heads = am_doc.get_heads();
        let reassembled_heads = reassembled.get_heads();
        assert_eq!(
            original_heads, reassembled_heads,
            "heads should match after roundtrip"
        );

        // Compare change counts
        let reassembled_changes = reassembled.get_changes(&[]);
        assert_eq!(
            change_count,
            reassembled_changes.len(),
            "change count should match after roundtrip"
        );

        // Compare saved bytes (strongest check — identical document state)
        assert_eq!(
            original_bytes.len(),
            reassembled_bytes.len(),
            "saved bytes length should match"
        );
        assert_eq!(
            original_bytes, reassembled_bytes,
            "saved bytes should be identical after roundtrip"
        );
    }

    /// Same as above but using `build_fragment_store_par` (parallel variant).
    #[cfg(feature = "rayon")]
    #[test]
    fn roundtrip_fragment_decomposition_parallel() {
        let mut doc = build_test_doc(2000);
        let am_doc = doc.document();

        let original_bytes = am_doc.save();
        let changes = am_doc.get_changes(&[]);
        let change_count = changes.len();

        let commit_store = IndexedSedimentreeAutomerge::from_changes(&changes);
        let heads: Vec<Digest<LooseCommit>> = am_doc
            .get_heads()
            .iter()
            .map(|h| Digest::force_from_bytes(h.0))
            .collect();
        let strategy = CountLeadingZeroBytes;
        let mut known_states: Map<Digest<LooseCommit>, FragmentState<OwnedParents>> = Map::new();

        let fragment_states = commit_store
            .build_fragment_store_par(&heads, &mut known_states, &strategy)
            .expect("build_fragment_store_par");

        let changes_by_hash: std::collections::HashMap<ChangeHash, &automerge::Change> =
            changes.iter().map(|c| (c.hash(), c)).collect();

        let mut fragment_blobs: Vec<Vec<u8>> = Vec::new();
        for state in &fragment_states {
            let mut blob = Vec::new();
            for member_digest in state.members() {
                let hash = ChangeHash(*member_digest.as_bytes());
                let change = changes_by_hash
                    .get(&hash)
                    .expect("member should be in changes_by_hash");
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
        assert_eq!(total_covered, change_count);

        let mut reassembled = Automerge::new();
        for blob in &fragment_blobs {
            reassembled
                .load_incremental(blob)
                .expect("load_incremental for fragment blob");
        }
        for blob in &uncovered_blobs {
            reassembled
                .load_incremental(blob)
                .expect("load_incremental for uncovered blob");
        }

        let reassembled_bytes = reassembled.save();

        assert_eq!(am_doc.get_heads(), reassembled.get_heads());
        assert_eq!(change_count, reassembled.get_changes(&[]).len());
        assert_eq!(original_bytes, reassembled_bytes);
    }
}
