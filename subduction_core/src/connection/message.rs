//! The API contact messages to be sent over a [`Connection`].

use alloc::vec::Vec;

use sedimentree_core::{
    blob::Blob,
    crypto::{digest::Digest, fingerprint::Fingerprint},
    fragment::{Fragment, id::FragmentId},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::FingerprintSummary,
};

use crate::{crypto::signed::Signed, peer::id::PeerId};

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

        /// The signed [`LooseCommit`] being sent.
        #[n(1)]
        commit: Signed<LooseCommit>,

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

        /// The signed [`Fragment`] being sent.
        #[n(1)]
        fragment: Signed<Fragment>,

        /// The [`Blob`] containing the fragment data.
        #[n(2)]
        blob: Blob,
    },

    /// A request for blobs by their [`Digest`]s.
    #[n(2)]
    BlobsRequest(#[n(0)] Vec<Digest<Blob>>),

    /// A response to a [`BlobRequest`].
    #[n(3)]
    BlobsResponse(#[n(0)] Vec<Blob>),

    /// A request to "batch sync" an entire [`Sedimentree`].
    #[n(4)]
    BatchSyncRequest(#[n(0)] BatchSyncRequest),

    /// A response to a [`BatchSyncRequest`].
    #[n(5)]
    BatchSyncResponse(#[n(0)] BatchSyncResponse),

    /// A request to remove subscriptions from specific sedimentrees.
    #[n(6)]
    RemoveSubscriptions(#[n(0)] RemoveSubscriptions),
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
            | Message::BlobsResponse(_)
            | Message::RemoveSubscriptions(_) => None,
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
            Message::RemoveSubscriptions(_) => "RemoveSubscriptions",
        }
    }

    /// Get the sedimentree ID associated with this message, if any.
    ///
    /// Returns `None` for messages that don't have a single associated ID
    /// (e.g., `BlobsRequest`, `RemoveSubscriptions` with multiple IDs).
    #[must_use]
    pub const fn sedimentree_id(&self) -> Option<SedimentreeId> {
        match self {
            Message::LooseCommit { id, .. }
            | Message::Fragment { id, .. }
            | Message::BatchSyncRequest(BatchSyncRequest { id, .. })
            | Message::BatchSyncResponse(BatchSyncResponse { id, .. }) => Some(*id),
            Message::BlobsRequest(_)
            | Message::BlobsResponse(_)
            | Message::RemoveSubscriptions(_) => None,
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

    /// Compact fingerprint summary of the requester's sedimentree.
    ///
    /// Uses SipHash-2-4 fingerprints with a per-request random seed
    /// instead of full structural data.
    #[n(2)]
    pub fingerprint_summary: FingerprintSummary,

    /// Whether to subscribe to future updates for this sedimentree.
    #[n(3)]
    pub subscribe: bool,
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

/// A request to remove subscriptions from specific sedimentrees.
#[derive(Debug, Clone, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(not(feature = "std"), derive(Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RemoveSubscriptions {
    /// The IDs of the sedimentrees to unsubscribe from.
    #[n(0)]
    pub ids: Vec<SedimentreeId>,
}

impl From<RemoveSubscriptions> for Message {
    fn from(unsub: RemoveSubscriptions) -> Self {
        Message::RemoveSubscriptions(unsub)
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
    ///
    /// This namespaces nonces so they only need to be unique per-peer rather than globally.
    /// Not redundant with connection-level auth or `Signed<T>` — `RequestId` must be
    /// matchable without accessing the connection, and these messages aren't individually signed.
    #[n(0)]
    pub requestor: PeerId,

    /// A nonce unique to this user and connection.
    #[n(1)]
    pub nonce: u64,
}

/// The calculated difference between two peers.
///
/// Contains both:
/// - Data to send to the requestor (`missing_commits`, `missing_fragments`)
/// - Data the responder is requesting back (`requesting`)
#[derive(Debug, Clone, PartialEq, Eq, Hash, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncDiff {
    /// Commits the requestor is missing (responder sends these).
    #[n(0)]
    pub missing_commits: Vec<(Signed<LooseCommit>, Blob)>,

    /// Fragments the requestor is missing (responder sends these).
    #[n(1)]
    pub missing_fragments: Vec<(Signed<Fragment>, Blob)>,

    /// Data the responder is requesting from the requestor.
    ///
    /// The requestor should send these commits and fragments back
    /// as individual [`Message::LooseCommit`] and [`Message::Fragment`] messages.
    #[n(2)]
    pub requesting: RequestedData,
}

/// Data that the responder is requesting from the requestor.
///
/// After receiving a [`BatchSyncResponse`], the requestor should send back
/// the commits and fragments identified here (fire-and-forget).
///
/// The fingerprints are echoed back from the requestor's original
/// [`FingerprintSummary`]. The requestor reverse-lookups each fingerprint
/// to find the corresponding local item.
#[derive(Debug, Clone, PartialEq, Eq, Hash, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestedData {
    /// Fingerprints of commits the responder needs from the requestor.
    #[n(0)]
    pub commit_fingerprints: Vec<Fingerprint<CommitId>>,

    /// Fingerprints of fragments the responder needs from the requestor.
    #[n(1)]
    pub fragment_fingerprints: Vec<Fingerprint<FragmentId>>,
}

impl RequestedData {
    /// Returns `true` if there is no data being requested.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.commit_fingerprints.is_empty() && self.fragment_fingerprints.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use alloc::collections::BTreeSet;

    use super::*;
    use alloc::vec;
    use sedimentree_core::crypto::fingerprint::FingerprintSeed;

    fn empty_fingerprint_summary() -> FingerprintSummary {
        FingerprintSummary::new(FingerprintSeed::new(0, 0), Vec::new(), Vec::new())
    }

    fn empty_requested_data() -> RequestedData {
        RequestedData {
            commit_fingerprints: Vec::new(),
            fragment_fingerprints: Vec::new(),
        }
    }

    mod message_request_id {
        use super::*;
        use crate::crypto::{signed::Signed, signer::MemorySigner};
        use future_form::Sendable;

        fn test_signer() -> MemorySigner {
            MemorySigner::from_bytes(&[42u8; 32])
        }

        #[tokio::test]
        async fn test_loose_commit_has_no_request_id() {
            let signer = test_signer();
            let commit = LooseCommit::new(
                Digest::from_bytes([2u8; 32]),
                Vec::new(),
                sedimentree_core::blob::BlobMeta::new(&[]),
            );
            let signed_commit = Signed::seal::<Sendable, _>(&signer, commit)
                .await
                .into_signed();
            let msg = Message::LooseCommit {
                id: SedimentreeId::new([1u8; 32]),
                commit: signed_commit,
                blob: Blob::new(Vec::from([3u8; 16])),
            };
            assert_eq!(msg.request_id(), None);
        }

        #[tokio::test]
        async fn test_fragment_has_no_request_id() {
            let signer = test_signer();
            let fragment = Fragment::new(
                Digest::from_bytes([2u8; 32]),
                BTreeSet::new(),
                Vec::new(),
                sedimentree_core::blob::BlobMeta::new(&[]),
            );
            let signed_fragment = Signed::seal::<Sendable, _>(&signer, fragment)
                .await
                .into_signed();
            let msg = Message::Fragment {
                id: SedimentreeId::new([1u8; 32]),
                fragment: signed_fragment,
                blob: Blob::new(Vec::from([3u8; 16])),
            };
            assert_eq!(msg.request_id(), None);
        }

        #[test]
        fn test_blobs_request_has_no_request_id() {
            let msg = Message::BlobsRequest(vec![Digest::from_bytes([1u8; 32])]);
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
                fingerprint_summary: empty_fingerprint_summary(),
                subscribe: false,
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
                    requesting: empty_requested_data(),
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
                fingerprint_summary: empty_fingerprint_summary(),
                subscribe: false,
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
                | Message::BatchSyncResponse(_)
                | Message::RemoveSubscriptions(_) => {
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
                    requesting: empty_requested_data(),
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
                | Message::BatchSyncRequest(_)
                | Message::RemoveSubscriptions(_) => {
                    unreachable!("Expected BatchSyncResponse")
                }
            }
        }
    }

    mod sync_diff {
        use super::*;
        use crate::crypto::{signed::Signed, signer::MemorySigner};
        use future_form::Sendable;

        fn test_signer() -> MemorySigner {
            MemorySigner::from_bytes(&[42u8; 32])
        }

        #[test]
        fn test_empty_sync_diff() {
            let diff = SyncDiff {
                missing_commits: Vec::new(),
                missing_fragments: Vec::new(),
                requesting: empty_requested_data(),
            };

            assert_eq!(diff.missing_commits.len(), 0);
            assert_eq!(diff.missing_fragments.len(), 0);
            assert!(diff.requesting.is_empty());
        }

        #[tokio::test]
        async fn test_sync_diff_with_commits() {
            let signer = test_signer();
            let commit = LooseCommit::new(
                Digest::from_bytes([1u8; 32]),
                Vec::new(),
                sedimentree_core::blob::BlobMeta::new(&[]),
            );
            let blob = Blob::new(Vec::from([2u8; 16]));
            let signed_commit = Signed::seal::<Sendable, _>(&signer, commit)
                .await
                .into_signed();

            let diff = SyncDiff {
                missing_commits: vec![(signed_commit.clone(), blob.clone())],
                missing_fragments: Vec::new(),
                requesting: empty_requested_data(),
            };

            assert_eq!(diff.missing_commits.len(), 1);

            #[allow(clippy::unwrap_used)]
            {
                assert_eq!(diff.missing_commits.first().unwrap().0, signed_commit);
            }
        }

        #[tokio::test]
        async fn test_sync_diff_with_fragments() {
            let signer = test_signer();
            let fragment = Fragment::new(
                Digest::from_bytes([2u8; 32]),
                BTreeSet::new(),
                Vec::new(),
                sedimentree_core::blob::BlobMeta::new(&[]),
            );
            let blob = Blob::new(Vec::from([3u8; 16]));
            let signed_fragment = Signed::seal::<Sendable, _>(&signer, fragment)
                .await
                .into_signed();

            let diff = SyncDiff {
                missing_commits: Vec::new(),
                missing_fragments: vec![(signed_fragment, blob)],
                requesting: empty_requested_data(),
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
