//! Manage connections to peers in the network.

use crate::peer::{id::PeerId, metadata::PeerMetadata};
use futures::Future;
use sedimentree_core::{Blob, Chunk, Digest, LooseCommit, SedimentreeId, SedimentreeSummary};
use thiserror::Error;

/// A trait representing a connection to a peer in the network.
///
/// It is assumed that a [`Connection`] is authenticated to a particular peer.
/// Encrypting this channel is also strongly recommended.
pub trait Connection: Clone {
    /// A problem when interacting with the network connection.
    type Error: core::error::Error;

    /// A problem when gracefully disconnecting.
    type DisconnectionError: core::error::Error;

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
    fn send(&self, message: Message) -> impl Future<Output = Result<(), Self::Error>>; // FIXME err type

    /// Receive a message.
    fn recv(&self) -> impl Future<Output = Result<Message, Self::Error>>; // FIXME err type

    // /// Send an incremental update to the peer.
    // ///
    // /// This is the low-level primitive for sending incremental updates.
    // fn send_incremental_update(
    //     &self,
    //     commits: &[&LooseCommit],
    //     chunks: &[&Chunk],
    // ) -> impl Future<Output = Result<(), Self::Error>>;

    // /// Send a single loose commit to the peer.
    // fn send_loose_commit(
    //     &self,
    //     commit: &LooseCommit,
    // ) -> impl Future<Output = Result<(), Self::Error>> {
    //     async { self.send_incremental_update(&[commit], &[]).await }
    // }

    // /// Send a single chunk to the peer.
    // fn send_chunk(&self, chunk: &Chunk) -> impl Future<Output = Result<(), Self::Error>> {
    //     async { self.send_incremental_update(&[], &[chunk]).await }
    // }

    // /// Receive an incremental update from the peer.
    // ///
    // /// This is the low-level primitive for receiving incremental updates.
    // fn recv_incremental_update(
    //     &self,
    //     commits: &[LooseCommit],
    //     chunk_summaries: &[ChunkSummary],
    // ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Request a batch sync over this connection.
    fn request_batch_sync(
        &self,
        id: SedimentreeId,
        our_sedimentree_summary: &SedimentreeSummary,
    ) -> impl Future<Output = Result<SyncDiff, Self::Error>>;

    // fn call(&self, msg: &ToSend<'_>) -> impl Future<Output = Result<Response, Self::Error>>;

    // /// Receive a batch sync over this connection.
    // fn recv_batch_sync(
    //     &self,
    //     their_sedimentree_summary: &SedimentreeSummary,
    // ) -> impl Future<Output = Result<SyncDiff<'_>, Self::Error>>;

    // /// Request blobs over this connection.
    // fn request_blobs(
    //     &self,
    //     digests: &[Digest],
    // ) -> impl Future<Output = Result<Vec<Blob>, Self::Error>>;

    // /// Receive blobs over this connection.
    // fn recv_blobs(&self, blobs: &[Blob]) -> impl Future<Output = Result<(), Self::Error>>;
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Message {
    LooseCommit {
        id: SedimentreeId,
        commit: LooseCommit,
        blob: Blob,
    },
    Chunk {
        id: SedimentreeId,
        chunk: Chunk,
        blob: Blob,
    },
    BatchSyncRequest {
        req_id: RequestId,
        id: SedimentreeId,
        sedimentree_summary: SedimentreeSummary,
    },
    BatchSyncResponse {
        req_id: RequestId,
        id: SedimentreeId,
        diff: SyncDiff,
    },
    BlobRequest {
        req_id: RequestId,
        digests: Vec<Digest>,
    },
    BlobResponse {
        req_id: RequestId,
        blobs: Vec<Blob>,
    },
}

impl Message {
    /// Get the request ID for this message, if any.
    pub fn request_id(&self) -> Option<RequestId> {
        match self {
            Message::BatchSyncRequest { req_id, .. } => Some(*req_id),
            Message::BatchSyncResponse { req_id, .. } => Some(*req_id),
            Message::BlobRequest { req_id, .. } => Some(*req_id),
            Message::BlobResponse { req_id, .. } => Some(*req_id),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestId {
    pub requestor: PeerId,
    pub nonce: u32,
}
