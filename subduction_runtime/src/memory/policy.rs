//! Trusting in-memory policy.

use future_form::{future_form, FutureForm, Local, Sendable};
use sedimentree_core::id::SedimentreeId;
use subduction_protocol::storage::Provenance;

use crate::policy::{Policy, StorageAction, Verdict};

/// Allows everything: the default policy for tests and trusted setups.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

#[future_form(Sendable, Local)]
impl<Async: FutureForm> Policy<Async> for AllowAll {
    fn authorize(
        &self,
        _provenance: &Provenance,
        _tree: SedimentreeId,
        _action: StorageAction,
    ) -> Async::Future<'_, Verdict> {
        Async::ready(Verdict::Allow)
    }
}
