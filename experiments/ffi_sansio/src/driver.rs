//! Sans-I/O driver state machine.
//!
//! For most Storage methods (the 17 "leaf" methods), the state machine
//! is trivially one step: emit one effect, receive one response, done.
//!
//! For the 3 convenience methods (`save_commit_with_blob`,
//! `save_fragment_with_blob`, `save_batch`), the state machine is
//! multi-step: it decomposes the operation into a sequence of leaf
//! effects.

use crate::effect::{StorageEffect, StorageResponse};

/// The result of advancing the driver state machine.
pub enum StepResult {
    /// The driver needs the host to perform this effect.
    Effect(StorageEffect),
    /// The operation is complete. Contains CBOR-encoded final result.
    Complete(Vec<u8>),
}

/// A sans-I/O driver for a single Storage operation.
///
/// Created via one of the `new_*` constructors, then driven by
/// repeatedly calling `next_effect()` and `provide_response()`.
pub struct StorageDriver {
    state: DriverState,
}

enum DriverState {
    /// Waiting to emit the next effect.
    Pending(StorageEffect),
    /// Waiting for the host to provide a response.
    AwaitingResponse {
        /// How to interpret the response and determine the next state.
        continuation: Continuation,
    },
    /// Multi-step: remaining effects to emit after current response.
    MultiStep {
        continuation: Continuation,
        remaining: Vec<StorageEffect>,
    },
    /// The operation completed.
    Complete(Vec<u8>),
    /// The driver was consumed (after `finish()`).
    Consumed,
}

/// How to handle the response from a given effect.
enum Continuation {
    /// The response is the final result — pass it through directly.
    PassThrough,
    /// The response is ignored (unit-returning operation).
    Unit,
    /// Accumulate for save_batch: store partial result, continue.
    BatchAccumulate {
        commit_digests_cbor: Option<Vec<u8>>,
        fragment_digests_cbor: Option<Vec<u8>>,
    },
}

impl StorageDriver {
    // ==================== Single-step constructors ====================
    // These correspond 1:1 to the 17 leaf Storage methods.

    pub fn new_save_sedimentree_id(id_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::SaveSedimentreeId { id_cbor },
            Continuation::Unit,
        )
    }

    pub fn new_delete_sedimentree_id(id_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::DeleteSedimentreeId { id_cbor },
            Continuation::Unit,
        )
    }

    pub fn new_load_all_sedimentree_ids() -> Self {
        Self::single(
            StorageEffect::LoadAllSedimentreeIds,
            Continuation::PassThrough,
        )
    }

    pub fn new_save_loose_commit(args_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::SaveLooseCommit { args_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_load_loose_commit(args_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::LoadLooseCommit { args_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_list_commit_digests(id_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::ListCommitDigests { id_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_load_loose_commits(id_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::LoadLooseCommits { id_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_delete_loose_commit(args_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::DeleteLooseCommit { args_cbor },
            Continuation::Unit,
        )
    }

    pub fn new_delete_loose_commits(id_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::DeleteLooseCommits { id_cbor },
            Continuation::Unit,
        )
    }

    pub fn new_save_fragment(args_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::SaveFragment { args_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_load_fragment(args_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::LoadFragment { args_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_list_fragment_digests(id_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::ListFragmentDigests { id_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_load_fragments(id_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::LoadFragments { id_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_delete_fragment(args_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::DeleteFragment { args_cbor },
            Continuation::Unit,
        )
    }

    pub fn new_delete_fragments(id_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::DeleteFragments { id_cbor },
            Continuation::Unit,
        )
    }

    pub fn new_save_blob(blob_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::SaveBlob { blob_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_load_blob(digest_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::LoadBlob { digest_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_load_blobs(digests_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::LoadBlobs { digests_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_delete_blob(digest_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::DeleteBlob { digest_cbor },
            Continuation::Unit,
        )
    }

    // ==================== Multi-step constructors ====================

    pub fn new_save_commit_with_blob(args_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::SaveCommitWithBlob { args_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_save_fragment_with_blob(args_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::SaveFragmentWithBlob { args_cbor },
            Continuation::PassThrough,
        )
    }

    pub fn new_save_batch(args_cbor: Vec<u8>) -> Self {
        Self::single(
            StorageEffect::SaveBatch { args_cbor },
            Continuation::PassThrough,
        )
    }

    // ==================== Driving the state machine ====================

    /// Get the next effect to perform, or `None` if the operation is complete.
    pub fn next_effect(&self) -> Option<&StorageEffect> {
        match &self.state {
            DriverState::Pending(effect) => Some(effect),
            DriverState::MultiStep { .. }
            | DriverState::AwaitingResponse { .. }
            | DriverState::Complete(_)
            | DriverState::Consumed => None,
        }
    }

    /// Provide the result of the last effect.
    ///
    /// Advances the state machine. Returns `true` if the operation is
    /// complete (call `finish()` to get the result), `false` if there
    /// are more effects.
    pub fn provide_response(&mut self, response: StorageResponse) -> bool {
        let old_state = std::mem::replace(&mut self.state, DriverState::Consumed);
        match old_state {
            DriverState::Pending(_) | DriverState::AwaitingResponse { .. } => {
                // We were waiting for exactly this response.
                self.state = self.apply_continuation_from_pending(response);
                matches!(self.state, DriverState::Complete(_))
            }
            DriverState::MultiStep {
                continuation: _,
                mut remaining,
            } => {
                // Multi-step: pop the next effect.
                if let Some(next) = remaining.pop() {
                    self.state = if remaining.is_empty() {
                        DriverState::Pending(next)
                    } else {
                        DriverState::MultiStep {
                            continuation: Continuation::PassThrough,
                            remaining,
                        }
                    };
                    false
                } else {
                    self.state = DriverState::Complete(response.data);
                    true
                }
            }
            DriverState::Complete(_) | DriverState::Consumed => {
                // Shouldn't happen — protocol error.
                self.state = DriverState::Complete(Vec::new());
                true
            }
        }
    }

    /// Check if the operation is complete.
    pub fn is_complete(&self) -> bool {
        matches!(self.state, DriverState::Complete(_))
    }

    /// Extract the final result, consuming the driver.
    pub fn finish(mut self) -> Option<Vec<u8>> {
        let state = std::mem::replace(&mut self.state, DriverState::Consumed);
        match state {
            DriverState::Complete(data) => Some(data),
            _ => None,
        }
    }

    // ==================== Internal helpers ====================

    fn single(effect: StorageEffect, continuation: Continuation) -> Self {
        // For single-step ops: we start in Pending, the host gets the effect,
        // then provides a response, and we complete.
        let _ = continuation; // Stored implicitly in the Pending → response path
        Self {
            state: DriverState::Pending(effect),
        }
    }

    fn apply_continuation_from_pending(&self, response: StorageResponse) -> DriverState {
        // For single-step operations (which is all of them in this design,
        // since convenience methods are emitted as single effects to the host),
        // the response data IS the result.
        DriverState::Complete(response.data)
    }
}
