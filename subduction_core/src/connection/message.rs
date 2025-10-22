//! The API contact messages to be sent over a [`Connection`].

use sedimentree_core::{
    blob::{Blob, Digest},
    Fragment, LooseCommit, SedimentreeId, SedimentreeSummary,
};

use crate::peer::id::PeerId;

/// The API contact messages to be sent over a [`Connection`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Message {
    /// A single loose commit being sent for a particular [`Sedimentree`].
    LooseCommit {
        /// The ID of the [`Sedimentree`] that this commit belongs to.
        id: SedimentreeId,

        /// The [`LooseCommit`] being sent.
        commit: LooseCommit,

        /// The [`Blob`] containing the commit data.
        blob: Blob,
    },

    /// A single fragment being sent for a particular [`Sedimentree`].
    Fragment {
        /// The ID of the [`Sedimentree`] that this fragment belongs to.
        id: SedimentreeId,

        /// The [`Fragment`] being sent.
        fragment: Fragment,

        /// The [`Blob`] containing the fragment data.
        blob: Blob,
    },

    /// A request for blobs by their [`Digest`]s.
    BlobsRequest(Vec<Digest>),

    /// A response to a [`BlobRequest`].
    BlobsResponse(Vec<Blob>),

    /// A request to "batch sync" an entire [`Sedimentree`].
    BatchSyncRequest(BatchSyncRequest),

    /// A response to a [`BatchSyncRequest`].
    BatchSyncResponse(BatchSyncResponse),
}

impl Message {
    /// Get the request ID for this message, if any.
    #[must_use]
    pub const fn request_id(&self) -> Option<RequestId> {
        match self {
            Message::BatchSyncRequest(BatchSyncRequest { req_id, .. })
            | Message::BatchSyncResponse(BatchSyncResponse { req_id, .. }) => Some(*req_id),
            _ => None,
        }
    }
}

/// A request to sync a sedimentree in batch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BatchSyncRequest {
    /// The ID of the sedimentree to sync.
    pub id: SedimentreeId,

    /// The unique ID of the request.
    pub req_id: RequestId,

    /// The summary of the sedimentree that the requester has.
    pub sedimentree_summary: SedimentreeSummary,
}

impl From<BatchSyncRequest> for Message {
    fn from(req: BatchSyncRequest) -> Self {
        Message::BatchSyncRequest(req)
    }
}

/// A response to a [`BatchSyncRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BatchSyncResponse {
    /// The ID of the request that this is a response to.
    pub req_id: RequestId,

    /// The ID of the sedimentree that was synced.
    pub id: SedimentreeId,

    /// The diff for the remote peer.
    pub diff: SyncDiff,
}

impl From<BatchSyncResponse> for Message {
    fn from(resp: BatchSyncResponse) -> Self {
        Message::BatchSyncResponse(resp)
    }
}

/// A unique identifier for a particular request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestId {
    /// ID for the peer that initiated the request.
    pub requestor: PeerId,

    /// A nonce unique to this user and connection.
    pub nonce: u128,
}

// TODO also make a version for the sender that is borrowed instead of owned.
/// The calculated difference for the remote peer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncDiff {
    /// Commits that we are missing and need to request from the peer.
    pub missing_commits: Vec<(LooseCommit, Blob)>,

    /// Fragments that we are missing and need to request from the peer.
    pub missing_fragments: Vec<(Fragment, Blob)>,
}
