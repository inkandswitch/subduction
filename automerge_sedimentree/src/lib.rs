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
