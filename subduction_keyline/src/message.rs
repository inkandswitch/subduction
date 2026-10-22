//! [`KeylineMessage`]: the keyline sync protocol's wire messages.
//!
//! The protocol moves one sedimentree, [`KEYLINE_TREE_ID`], so a message
//! never names a tree. The frames reuse [`SyncMessage`]'s encoding, under
//! this protocol's own schema:
//!
//! ```text
//! ╔══════════╦═══════════╦═════╦══════════════════════════════════╗
//! ║  Schema  ║ TotalSize ║ Tag ║ Payload (as SyncMessage encodes) ║
//! ║ SUL\x00  ║    4B     ║ 1B  ║ variable                         ║
//! ╚══════════╩═══════════╩═════╩══════════════════════════════════╝
//! ```
//!
//! | Variant        | Frame as                                        |
//! |----------------|-------------------------------------------------|
//! | `SyncRequest`  | `BatchSyncRequest` on the keyline tree          |
//! | `SyncResponse` | `BatchSyncResponse` with `SyncResult::Ok`        |
//! | `Commit`       | `LooseCommit` on the keyline tree               |
//!
//! A different schema keeps keyline frames out of the content sync handler:
//! wire envelopes dispatch on the first four bytes. Decoding accepts only
//! these three variants, and only on [`KEYLINE_TREE_ID`].

use alloc::vec::Vec;

use sedimentree_core::{
    blob::Blob,
    codec::{
        decode::Decode,
        encode::Encode,
        error::{DecodeError, InvalidEnumTag, InvalidSchema},
    },
    id::SedimentreeId,
    loose_commit::LooseCommit,
    sedimentree::FingerprintSummary,
};
use subduction_core::{
    connection::message::{
        BatchSyncRequest, BatchSyncResponse, MESSAGE_SCHEMA, RequestId, SyncDiff, SyncMessage,
        SyncResult, TryAsBatchSyncResponse, TryAsSubscribeRequest,
    },
    remote_heads::RemoteHeads,
};
use subduction_crypto::signed::Signed;

use crate::batch::KEYLINE_TREE_ID;

/// The keyline protocol's schema: the first four bytes of every frame.
pub const KEYLINE_SCHEMA: [u8; 4] = *b"SUL\x00";

/// Offset of the variant tag in a frame: after the schema and total size.
const TAG_OFFSET: usize = 8;

/// A keyline sync protocol message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeylineMessage {
    /// The sender's fingerprints of the keyline tree. Asks for what the
    /// sender lacks, and asks to receive new batches as they arrive.
    SyncRequest {
        /// Correlates the response.
        req_id: RequestId,

        /// The sender's commits, fingerprinted with a per-request seed.
        summary: FingerprintSummary,
    },

    /// The commits the requestor lacks, and fingerprints of the requestor's
    /// commits that the responder lacks.
    SyncResponse {
        /// The request this answers.
        req_id: RequestId,

        /// What each side is missing.
        diff: SyncDiff,
    },

    /// One batch commit: a new batch, or one the other side asked for.
    Commit {
        /// The signed commit.
        commit: Signed<LooseCommit>,

        /// The batch blob.
        blob: Blob,
    },
}

impl KeylineMessage {
    fn to_sync(&self) -> SyncMessage {
        match self {
            Self::SyncRequest { req_id, summary } => BatchSyncRequest {
                id: KEYLINE_TREE_ID,
                req_id: *req_id,
                fingerprint_summary: summary.clone(),
                subscribe: true,
            }
            .into(),
            Self::SyncResponse { req_id, diff } => BatchSyncResponse {
                req_id: *req_id,
                id: KEYLINE_TREE_ID,
                result: SyncResult::Ok(diff.clone()),
                responder_heads: RemoteHeads::default(),
            }
            .into(),
            Self::Commit { commit, blob } => SyncMessage::LooseCommit {
                id: KEYLINE_TREE_ID,
                commit: commit.clone(),
                blob: blob.clone(),
                sender_heads: RemoteHeads::default(),
            },
        }
    }

    fn from_sync(message: SyncMessage, tag: u8) -> Result<Self, DecodeError> {
        let unsupported = || {
            DecodeError::from(InvalidEnumTag {
                tag,
                type_name: "KeylineMessage",
            })
        };
        let on_tree = |id: SedimentreeId| {
            (id == KEYLINE_TREE_ID)
                .then_some(())
                .ok_or_else(unsupported)
        };

        match message {
            SyncMessage::BatchSyncRequest(BatchSyncRequest {
                id,
                req_id,
                fingerprint_summary,
                ..
            }) => {
                on_tree(id)?;
                Ok(Self::SyncRequest {
                    req_id,
                    summary: fingerprint_summary,
                })
            }
            SyncMessage::BatchSyncResponse(BatchSyncResponse {
                req_id,
                id,
                result: SyncResult::Ok(diff),
                ..
            }) => {
                on_tree(id)?;
                Ok(Self::SyncResponse { req_id, diff })
            }
            SyncMessage::LooseCommit {
                id, commit, blob, ..
            } => {
                on_tree(id)?;
                Ok(Self::Commit { commit, blob })
            }
            SyncMessage::BatchSyncResponse(_)
            | SyncMessage::Fragment { .. }
            | SyncMessage::RemoveSubscriptions(_)
            | SyncMessage::DataRequestRejected(_)
            | SyncMessage::HeadsUpdate { .. } => Err(unsupported()),
        }
    }
}

impl Encode for KeylineMessage {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = self.to_sync().encode();
        if let Some(schema) = bytes.get_mut(..KEYLINE_SCHEMA.len()) {
            schema.copy_from_slice(&KEYLINE_SCHEMA);
        }
        bytes
    }

    fn encoded_size(&self) -> usize {
        self.to_sync().encoded_size()
    }
}

impl Decode for KeylineMessage {
    const MIN_SIZE: usize = SyncMessage::MIN_SIZE;

    fn try_decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let schema: [u8; 4] = buf
            .get(..KEYLINE_SCHEMA.len())
            .and_then(|s| s.try_into().ok())
            .ok_or(DecodeError::MessageTooShort {
                type_name: "KeylineMessage",
                need: Self::MIN_SIZE,
                have: buf.len(),
            })?;

        if schema != KEYLINE_SCHEMA {
            return Err(InvalidSchema {
                expected: KEYLINE_SCHEMA,
                got: schema,
            }
            .into());
        }

        let tag = buf.get(TAG_OFFSET).copied().unwrap_or_default();
        let mut sync = Vec::from(buf);
        if let Some(schema) = sync.get_mut(..MESSAGE_SCHEMA.len()) {
            schema.copy_from_slice(&MESSAGE_SCHEMA);
        }
        Self::from_sync(SyncMessage::try_decode(&sync)?, tag)
    }
}

/// Keyline responses are handled by [`KeylineHandler`], never routed to
/// `Subduction`'s pending content round trips.
///
/// [`KeylineHandler`]: crate::handler::KeylineHandler
impl TryAsBatchSyncResponse for KeylineMessage {
    fn try_as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        None
    }
}

/// Keyline subscriptions are not content subscriptions, so they are never
/// forwarded upstream as such.
impl TryAsSubscribeRequest for KeylineMessage {
    fn try_as_subscribe_request(&self) -> Option<SedimentreeId> {
        None
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)]
mod tests {
    use super::*;

    use alloc::{collections::BTreeSet, vec};

    use future_form::Sendable;
    use futures::executor::block_on;
    use sedimentree_core::{
        blob::BlobMeta, crypto::fingerprint::FingerprintSeed, loose_commit::id::CommitId,
        sedimentree::Sedimentree,
    };
    use subduction_core::{connection::message::RequestedData, peer::id::PeerId};
    use subduction_crypto::signer::memory::MemorySigner;
    use testresult::TestResult;

    fn req_id() -> RequestId {
        RequestId {
            requestor: PeerId::new([3; 32]),
            nonce: 42,
        }
    }

    fn signed_commit(id: SedimentreeId) -> (Signed<LooseCommit>, Blob) {
        let blob = Blob::new(vec![1, 2, 3]);
        let commit = LooseCommit::new(
            id,
            CommitId::new([5; 32]),
            BTreeSet::new(),
            BlobMeta::new(&blob),
        );
        let key = MemorySigner::from_bytes(&[7; 32]);
        let sealed = block_on(Signed::seal::<Sendable, _>(&key, commit)).into_signed();
        (sealed, blob)
    }

    fn examples() -> Vec<KeylineMessage> {
        let (commit, blob) = signed_commit(KEYLINE_TREE_ID);
        let summary = Sedimentree::default().fingerprint_summarize(&FingerprintSeed::new(1, 2));
        vec![
            KeylineMessage::SyncRequest {
                req_id: req_id(),
                summary,
            },
            KeylineMessage::SyncResponse {
                req_id: req_id(),
                diff: SyncDiff {
                    missing_commits: vec![(commit.clone(), blob.clone())],
                    missing_fragments: vec![],
                    requesting: RequestedData {
                        commit_fingerprints: vec![],
                        fragment_fingerprints: vec![],
                    },
                },
            },
            KeylineMessage::Commit { commit, blob },
        ]
    }

    #[test]
    fn round_trips_under_its_own_schema() -> TestResult {
        for message in examples() {
            let bytes = message.encode();
            assert_eq!(bytes.get(..4), Some(KEYLINE_SCHEMA.as_slice()));
            assert_eq!(bytes.len(), message.encoded_size());
            assert_eq!(KeylineMessage::try_decode(&bytes)?, message);
        }
        Ok(())
    }

    #[test]
    fn content_sync_frames_are_not_keyline_frames() {
        for message in examples() {
            let content = message.to_sync().encode();
            assert!(matches!(
                KeylineMessage::try_decode(&content),
                Err(DecodeError::InvalidSchema(_))
            ));
        }
    }

    #[test]
    fn rejects_other_trees_and_variants() {
        let reframe = |message: &SyncMessage| {
            let mut bytes = message.encode();
            bytes[..4].copy_from_slice(&KEYLINE_SCHEMA);
            bytes
        };

        let (commit, blob) = signed_commit(SedimentreeId::new([9; 32]));
        let other_tree = SyncMessage::LooseCommit {
            id: SedimentreeId::new([9; 32]),
            commit,
            blob,
            sender_heads: RemoteHeads::default(),
        };
        assert!(matches!(
            KeylineMessage::try_decode(&reframe(&other_tree)),
            Err(DecodeError::InvalidEnumTag(_))
        ));

        let unauthorized = SyncMessage::BatchSyncResponse(BatchSyncResponse {
            req_id: req_id(),
            id: KEYLINE_TREE_ID,
            result: SyncResult::Unauthorized,
            responder_heads: RemoteHeads::default(),
        });
        assert!(matches!(
            KeylineMessage::try_decode(&reframe(&unauthorized)),
            Err(DecodeError::InvalidEnumTag(_))
        ));
    }
}
