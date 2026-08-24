//! Authorization capability: who may read, write, or delete a tree.
//!
//! Policies may perform IO (e.g. capability lookups), so verdicts are
//! async. The driver's effect executor consults the policy before every
//! storage operation; a denial surfaces as
//! [`StorageResult::Unauthorized`](subduction_protocol::storage::StorageResult::Unauthorized)
//! and the backend is never called — backends do not authorize.

use future_form::FutureForm;
use sedimentree_core::id::SedimentreeId;
use subduction_protocol::storage::Provenance;

/// Authorization for storage operations.
pub trait Policy<Async: FutureForm> {
    /// Authorize `action` on `tree` by `provenance`.
    fn authorize(
        &self,
        provenance: &Provenance,
        tree: SedimentreeId,
        action: StorageAction,
    ) -> Async::Future<'_, Verdict>;
}

/// What a storage operation wants to do — the policy check's input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageAction {
    /// Persist items into a tree.
    Write,

    /// Load items from a tree.
    Read,

    /// Delete a tree.
    Delete,
}

/// A policy verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The operation may proceed.
    Allow,

    /// The operation is denied.
    Deny,
}
