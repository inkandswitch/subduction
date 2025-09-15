//! Manage connections to peers in the network.

use std::time::Duration;

use crate::peer::{id::PeerId, metadata::PeerMetadata};
use futures::Future;
use sedimentree_core::{Blob, Chunk, Digest, LooseCommit, SedimentreeId, SedimentreeSummary};
use thiserror::Error;

/// A trait representing a connection to a peer in the network.
///
/// It is assumed that a [`Connection`] is authenticated to a particular peer.
/// Encrypting this channel is also strongly recommended.
pub trait Connection: Clone {
    /// A problem when gracefully disconnecting.
    type DisconnectionError: core::error::Error;

    /// A problem when sending a message.
    type SendError: core::error::Error;

    /// A problem when receiving a message.
    type RecvError: core::error::Error;

    /// A problem with a roundtrip call.
    type CallError: core::error::Error;

    /// A unique identifier for this connection.
    ///
    /// This number should be a counter or random number.
    /// We assume that the smae ID is never reused for different connections.
    /// For this reason, it is not recommended to use or derive from the peer ID on its own.
    fn connection_id(&self) -> usize;

    /// The peer ID of the remote peer.
    fn peer_id(&self) -> PeerId;

    /// The metadata of the remote peer, if any.
    fn peer_metadata(&self) -> Option<PeerMetadata>;

    /// Disconnect from the peer gracefully.
    fn disconnect(&mut self) -> impl Future<Output = Result<(), Self::DisconnectionError>>;

    /// Send a message.
    fn send(&self, message: Message) -> impl Future<Output = Result<(), Self::SendError>>;

    /// Receive a message.
    fn recv(&self) -> impl Future<Output = Result<Message, Self::RecvError>>;

    /// Get the next request ID e.g. for a [`call`].
    fn next_request_id(&self) -> impl Future<Output = RequestId>;

    /// Make a synchronous call to the peer, expecting a response.
    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> impl Future<Output = Result<BatchSyncResponse, Self::CallError>>;
}

/// A trait for connections that can be re-established if they drop.
pub trait Reconnection: Connection {
    /// The address type used to connect to the peer.
    ///
    /// For example, this may be a string, a handle, a public key, and so on.
    type Address;

    /// A problem when creating the connection.
    type ConnectError: core::error::Error;

    /// A problem when running the connection.
    type RunError: core::error::Error;

    /// Setup the connection, but don't run it.
    fn connect(
        addr: Self::Address,
        timeout: Duration,
        peer_id: PeerId,
        conn_id: usize,
    ) -> impl Future<Output = Result<Box<Self>, Self::ConnectError>>;

    /// Run the connection send/receive loop.
    fn run(&self) -> impl Future<Output = Result<(), Self::RunError>>;
}

/// A policy for allowing or disallowing connections from peers.
pub trait ConnectionPolicy {
    /// Check if a connection from the given peer is allowed.
    fn allowed_to_connect(&self, peer: &PeerId) -> Result<(), ConnectionDisallowed>;
}

/// An error indicating that a connection is disallowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, Hash)]
#[error("Connection disallowed")]
pub struct ConnectionDisallowed;

/// A random challenge to be signed by a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Challenge([u8; 32]);

/// A trait for generating and verifying challenges for peers.
pub trait PeerChallenge {
    /// Generate a random challenge for the given peer.
    fn generate_challenge(&self, peer_id: &PeerId) -> Challenge; // NOTE store this locally in e.g. ring buffer + expiry

    /// Verify a signed challenge from the given peer.
    fn verify_challenge(&self, peer_id: &PeerId, signature: [u8; 32]) -> bool; // FIXME ed25519_dalek::signature
}

// FIXME shoukd have a borrowed version?
/// The calculated difference for the remote peer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncDiff {
    /// Commits that we are missing and need to request from the peer.
    pub missing_commits: Vec<(LooseCommit, Blob)>,

    /// Chunks that we are missing and need to request from the peer.
    pub missing_chunks: Vec<(Chunk, Blob)>,
}

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

    /// A single chunk being sent for a particular [`Sedimentree`].
    Chunk {
        /// The ID of the [`Sedimentree`] that this chunk belongs to.
        id: SedimentreeId,

        /// The [`Chunk`] being sent.
        chunk: Chunk,

        /// The [`Blob`] containing the chunk data.
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
    pub fn request_id(&self) -> Option<RequestId> {
        match self {
            Message::BatchSyncRequest(BatchSyncRequest { req_id, .. }) => Some(*req_id),
            Message::BatchSyncResponse(BatchSyncResponse { req_id, .. }) => Some(*req_id),
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
