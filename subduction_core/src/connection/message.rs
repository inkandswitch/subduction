//! The API contact messages to be sent over a [`Connection`].

use alloc::vec::Vec;

use sedimentree_core::{
    blob::{Blob, Digest},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
    sedimentree::SedimentreeSummary,
};

use crate::peer::id::PeerId;

/// The API contact messages to be sent over a [`Connection`].
#[derive(Debug, Clone, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(not(feature = "std"), derive(Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Message {
    /// A single loose commit being sent for a particular [`Sedimentree`].
    #[n(0)]
    LooseCommit {
        /// The ID of the [`Sedimentree`] that this commit belongs to.
        #[n(0)]
        id: SedimentreeId,

        /// The [`LooseCommit`] being sent.
        #[n(1)]
        commit: LooseCommit,

        /// The [`Blob`] containing the commit data.
        #[n(2)]
        blob: Blob,
    },

    /// A single fragment being sent for a particular [`Sedimentree`].
    #[n(1)]
    Fragment {
        /// The ID of the [`Sedimentree`] that this fragment belongs to.
        #[n(0)]
        id: SedimentreeId,

        /// The [`Fragment`] being sent.
        #[n(1)]
        fragment: Fragment,

        /// The [`Blob`] containing the fragment data.
        #[n(2)]
        blob: Blob,
    },

    /// A request for blobs by their [`Digest`]s.
    #[n(2)]
    BlobsRequest(#[n(0)] Vec<Digest>),

    /// A response to a [`BlobRequest`].
    #[n(3)]
    BlobsResponse(#[n(0)] Vec<Blob>),

    /// A request to "batch sync" an entire [`Sedimentree`].
    #[n(4)]
    BatchSyncRequest(#[n(0)] BatchSyncRequest),

    /// A response to a [`BatchSyncRequest`].
    #[n(5)]
    BatchSyncResponse(#[n(0)] BatchSyncResponse),
}

impl Message {
    /// Get the request ID for this message, if any.
    #[must_use]
    pub const fn request_id(&self) -> Option<RequestId> {
        match self {
            Message::BatchSyncRequest(BatchSyncRequest { req_id, .. })
            | Message::BatchSyncResponse(BatchSyncResponse { req_id, .. }) => Some(*req_id),
            Message::LooseCommit { .. }
            | Message::Fragment { .. }
            | Message::BlobsRequest(_)
            | Message::BlobsResponse(_) => None,
        }
    }

    /// Get the variant name of this message for logging purposes.
    #[must_use]
    pub const fn variant_name(&self) -> &'static str {
        match self {
            Message::LooseCommit { .. } => "LooseCommit",
            Message::Fragment { .. } => "Fragment",
            Message::BlobsRequest(_) => "BlobsRequest",
            Message::BlobsResponse(_) => "BlobsResponse",
            Message::BatchSyncRequest(_) => "BatchSyncRequest",
            Message::BatchSyncResponse(_) => "BatchSyncResponse",
        }
    }

    /// Get the sedimentree ID associated with this message, if any.
    #[must_use]
    pub const fn sedimentree_id(&self) -> Option<SedimentreeId> {
        match self {
            Message::LooseCommit { id, .. }
            | Message::Fragment { id, .. }
            | Message::BatchSyncRequest(BatchSyncRequest { id, .. })
            | Message::BatchSyncResponse(BatchSyncResponse { id, .. }) => Some(*id),
            Message::BlobsRequest(_) | Message::BlobsResponse(_) => None,
        }
    }
}

/// A request to sync a sedimentree in batch.
#[derive(Debug, Clone, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(not(feature = "std"), derive(Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BatchSyncRequest {
    /// The ID of the sedimentree to sync.
    #[n(0)]
    pub id: SedimentreeId,

    /// The unique ID of the request.
    #[n(1)]
    pub req_id: RequestId,

    /// The summary of the sedimentree that the requester has.
    #[n(2)]
    pub sedimentree_summary: SedimentreeSummary,
}

impl From<BatchSyncRequest> for Message {
    fn from(req: BatchSyncRequest) -> Self {
        Message::BatchSyncRequest(req)
    }
}

/// A response to a [`BatchSyncRequest`].
#[derive(Debug, Clone, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(not(feature = "std"), derive(Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BatchSyncResponse {
    /// The ID of the request that this is a response to.
    #[n(0)]
    pub req_id: RequestId,

    /// The ID of the sedimentree that was synced.
    #[n(1)]
    pub id: SedimentreeId,

    /// The diff for the remote peer.
    #[n(2)]
    pub diff: SyncDiff,
}

impl From<BatchSyncResponse> for Message {
    fn from(resp: BatchSyncResponse) -> Self {
        Message::BatchSyncResponse(resp)
    }
}

/// A unique identifier for a particular request.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestId {
    /// ID for the peer that initiated the request.
    #[n(0)]
    pub requestor: PeerId, // FIXME may be handled on Signed/Verified now?

    /// A nonce unique to this user and connection.
    #[n(1)]
    pub nonce: u64,
}

// TODO also make a version for the sender that is borrowed instead of owned.
/// The calculated difference for the remote peer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncDiff {
    /// Commits that we are missing and need to request from the peer.
    #[n(0)]
    pub missing_commits: Vec<(LooseCommit, Blob)>,

    /// Fragments that we are missing and need to request from the peer.
    #[n(1)]
    pub missing_fragments: Vec<(Fragment, Blob)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    mod message_request_id {
        use super::*;

        #[test]
        fn test_loose_commit_has_no_request_id() {
            let msg = Message::LooseCommit {
                id: SedimentreeId::new([1u8; 32]),
                commit: LooseCommit::new(
                    Digest::from([2u8; 32]),
                    Vec::new(),
                    sedimentree_core::blob::BlobMeta::new(&[]),
                ),
                blob: Blob::new(Vec::from([3u8; 16])),
            };
            assert_eq!(msg.request_id(), None);
        }

        #[test]
        fn test_fragment_has_no_request_id() {
            let msg = Message::Fragment {
                id: SedimentreeId::new([1u8; 32]),
                fragment: Fragment::new(
                    Digest::from([2u8; 32]),
                    Vec::new(),
                    Vec::new(),
                    sedimentree_core::blob::BlobMeta::new(&[]),
                ),
                blob: Blob::new(Vec::from([3u8; 16])),
            };
            assert_eq!(msg.request_id(), None);
        }

        #[test]
        fn test_blobs_request_has_no_request_id() {
            let msg = Message::BlobsRequest(vec![Digest::from([1u8; 32])]);
            assert_eq!(msg.request_id(), None);
        }

        #[test]
        fn test_blobs_response_has_no_request_id() {
            let msg = Message::BlobsResponse(vec![Blob::new(Vec::from([1u8; 16]))]);
            assert_eq!(msg.request_id(), None);
        }

        #[test]
        fn test_batch_sync_request_has_request_id() {
            let req_id = RequestId {
                requestor: PeerId::new([1u8; 32]),
                nonce: 42,
            };
            let msg = Message::BatchSyncRequest(BatchSyncRequest {
                id: SedimentreeId::new([2u8; 32]),
                req_id,
                sedimentree_summary: SedimentreeSummary::default(),
            });
            assert_eq!(msg.request_id(), Some(req_id));
        }

        #[test]
        fn test_batch_sync_response_has_request_id() {
            let req_id = RequestId {
                requestor: PeerId::new([1u8; 32]),
                nonce: 99,
            };
            let msg = Message::BatchSyncResponse(BatchSyncResponse {
                id: SedimentreeId::new([2u8; 32]),
                req_id,
                diff: SyncDiff {
                    missing_commits: Vec::new(),
                    missing_fragments: Vec::new(),
                },
            });
            assert_eq!(msg.request_id(), Some(req_id));
        }
    }

    mod request_id {
        use super::*;

        #[test]
        fn test_equality() {
            let req_id1 = RequestId {
                requestor: PeerId::new([1u8; 32]),
                nonce: 42,
            };
            let req_id2 = RequestId {
                requestor: PeerId::new([1u8; 32]),
                nonce: 42,
            };
            let req_id3 = RequestId {
                requestor: PeerId::new([1u8; 32]),
                nonce: 43,
            };

            assert_eq!(req_id1, req_id2);
            assert_ne!(req_id1, req_id3);
        }

        #[test]
        fn test_ordering_by_requestor_first() {
            let req_id1 = RequestId {
                requestor: PeerId::new([0u8; 32]),
                nonce: 100,
            };
            let req_id2 = RequestId {
                requestor: PeerId::new([1u8; 32]),
                nonce: 1,
            };

            // Ordering by requestor takes precedence
            assert!(req_id1 < req_id2);
        }

        #[test]
        fn test_ordering_by_nonce_when_requestor_equal() {
            let req_id1 = RequestId {
                requestor: PeerId::new([1u8; 32]),
                nonce: 1,
            };
            let req_id2 = RequestId {
                requestor: PeerId::new([1u8; 32]),
                nonce: 2,
            };

            assert!(req_id1 < req_id2);
        }
    }

    mod conversions {
        use super::*;

        #[test]
        fn test_batch_sync_request_into_message() {
            let req = BatchSyncRequest {
                id: SedimentreeId::new([1u8; 32]),
                req_id: RequestId {
                    requestor: PeerId::new([2u8; 32]),
                    nonce: 42,
                },
                sedimentree_summary: SedimentreeSummary::default(),
            };

            let msg: Message = req.clone().into();

            match msg {
                Message::BatchSyncRequest(inner) => {
                    assert_eq!(inner, req);
                }
                Message::LooseCommit { .. }
                | Message::Fragment { .. }
                | Message::BlobsRequest(_)
                | Message::BlobsResponse(_)
                | Message::BatchSyncResponse(_) => {
                    unreachable!("Expected BatchSyncRequest")
                }
            }
        }

        #[test]
        fn test_batch_sync_response_into_message() {
            let resp = BatchSyncResponse {
                id: SedimentreeId::new([1u8; 32]),
                req_id: RequestId {
                    requestor: PeerId::new([2u8; 32]),
                    nonce: 99,
                },
                diff: SyncDiff {
                    missing_commits: Vec::new(),
                    missing_fragments: Vec::new(),
                },
            };

            let msg: Message = resp.clone().into();

            match msg {
                Message::BatchSyncResponse(inner) => {
                    assert_eq!(inner, resp);
                }
                Message::LooseCommit { .. }
                | Message::Fragment { .. }
                | Message::BlobsRequest(_)
                | Message::BlobsResponse(_)
                | Message::BatchSyncRequest(_) => {
                    unreachable!("Expected BatchSyncResponse")
                }
            }
        }
    }

    mod sync_diff {
        use super::*;

        #[test]
        fn test_empty_sync_diff() {
            let diff = SyncDiff {
                missing_commits: Vec::new(),
                missing_fragments: Vec::new(),
            };

            assert_eq!(diff.missing_commits.len(), 0);
            assert_eq!(diff.missing_fragments.len(), 0);
        }

        #[test]
        fn test_sync_diff_with_commits() {
            let commit = LooseCommit::new(
                Digest::from([1u8; 32]),
                Vec::new(),
                sedimentree_core::blob::BlobMeta::new(&[]),
            );
            let blob = Blob::new(Vec::from([2u8; 16]));

            let diff = SyncDiff {
                missing_commits: vec![(commit.clone(), blob.clone())],
                missing_fragments: Vec::new(),
            };

            assert_eq!(diff.missing_commits.len(), 1);

            #[allow(clippy::unwrap_used)]
            {
                assert_eq!(diff.missing_commits.first().unwrap().0, commit);
            }
        }

        #[test]
        fn test_sync_diff_with_fragments() {
            let fragment = Fragment::new(
                Digest::from([2u8; 32]),
                Vec::new(),
                Vec::new(),
                sedimentree_core::blob::BlobMeta::new(&[]),
            );
            let blob = Blob::new(Vec::from([3u8; 16]));

            let diff = SyncDiff {
                missing_commits: Vec::new(),
                missing_fragments: vec![(fragment.clone(), blob.clone())],
            };

            assert_eq!(diff.missing_fragments.len(), 1);
        }
    }

    #[cfg(all(test, feature = "std", feature = "bolero"))]
    mod proptests {
        use super::*;

        #[test]
        fn prop_batch_sync_request_preserves_req_id() {
            bolero::check!()
                .with_arbitrary::<BatchSyncRequest>()
                .for_each(|req| {
                    let msg: Message = req.clone().into();
                    assert_eq!(msg.request_id(), Some(req.req_id));
                });
        }

        #[test]
        fn prop_batch_sync_response_preserves_req_id() {
            bolero::check!()
                .with_arbitrary::<BatchSyncResponse>()
                .for_each(|resp| {
                    let msg: Message = resp.clone().into();
                    assert_eq!(msg.request_id(), Some(resp.req_id));
                });
        }
    }
}
