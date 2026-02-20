//! Result of batch storage operations.

use alloc::vec::Vec;
use sedimentree_core::{crypto::digest::Digest, fragment::Fragment, loose_commit::LooseCommit};

/// Result of a batch save operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchResult {
    /// Digests of saved commits.
    pub commit_digests: Vec<Digest<LooseCommit>>,

    /// Digests of saved fragments.
    pub fragment_digests: Vec<Digest<Fragment>>,
}
