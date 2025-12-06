//! # Automerge integration for Sedimentree

#![cfg_attr(docsrs, feature(doc_cfg))]

use std::{cell::RefCell, collections::HashSet, convert::Infallible, hash::RandomState, rc::Rc};

use automerge::{AutoCommit, Automerge, ChangeHash, ChangeMetadata};
use sedimentree_core::{
    blob::Digest,
    commit::{CommitStore, Parents},
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

    fn lookup(&self, digest: Digest) -> Result<Option<Self::Node>, Self::LookupError> {
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

    fn lookup(&self, digest: Digest) -> Result<Option<Self::Node>, Self::LookupError> {
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
    type Hasher = RandomState;

    fn parents(&self) -> HashSet<Digest> {
        self.0
            .deps
            .iter()
            .map(|change_hash| Digest::from(change_hash.0))
            .collect()
    }
}
