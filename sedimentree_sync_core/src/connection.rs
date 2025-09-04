use crate::peer::{id::PeerId, metadata::PeerMetadata};
use futures::Future;
use sedimentree_core::{Blob, Chunk, ChunkSummary, Digest, LooseCommit, SedimentreeSummary};
use thiserror::Error;

pub trait Connection {
    type Error: core::error::Error;
    type DisconnectionError: core::error::Error;

    fn connection_id(&self) -> usize;

    fn peer_id(&self) -> PeerId;
    fn peer_metadata(&self) -> Option<PeerMetadata>;

    fn disconnect(&mut self) -> impl Future<Output = Result<(), Self::DisconnectionError>>;

    fn send_incremental_update(
        &self,
        commits: &[&LooseCommit],
        chunks: &[&Chunk],
    ) -> impl Future<Output = Result<(), Self::Error>>;

    fn send_loose_commit(
        &self,
        commit: &LooseCommit,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        async { self.send_incremental_update(&[commit], &[]).await }
    }

    fn send_chunk(&self, chunk: &Chunk) -> impl Future<Output = Result<(), Self::Error>> {
        async { self.send_incremental_update(&[], &[chunk]).await }
    }

    fn recv_incremental_update(
        &self,
        commits: &[LooseCommit],
        chunk_summaries: &[ChunkSummary],
    ) -> impl Future<Output = Result<(), Self::Error>>;

    fn request_batch_sync(
        &self,
        our_sedimentree_summary: &SedimentreeSummary,
    ) -> impl Future<Output = Result<SyncDiff<'_>, Self::Error>>;

    fn recv_batch_sync(
        &self,
        their_sedimentree_summary: &SedimentreeSummary,
    ) -> impl Future<Output = Result<SyncDiff<'_>, Self::Error>>;

    fn request_blobs(
        &self,
        digests: &[Digest],
    ) -> impl Future<Output = Result<Vec<Blob>, Self::Error>>;

    fn recv_blobs(&self, blobs: &[Blob]) -> impl Future<Output = Result<(), Self::Error>>;
}

pub trait ConnectionPolicy {
    fn allowed_to_connect(&self, peer: &PeerId) -> Result<(), ConnectionDisallowed>;
}

pub struct SyncDiff<'a> {
    pub missing_commits: &'a [(LooseCommit, Blob)],
    pub missing_chunks: &'a [(Chunk, Blob)],
}

#[derive(Debug, Clone, PartialEq, Eq, Error, Hash)]
#[error("Connection disallowed")]
pub struct ConnectionDisallowed;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Challenge([u8; 32]);

pub trait PeerChallenge {
    fn generate_challenge(&self, peer_id: &PeerId) -> Challenge; // NOTE store this locally in e.g. ring buffer + expiry
    fn verify_challenge(&self, peer_id: &PeerId, signature: [u8; 32]) -> bool; // FIXME ed25519_dalek::signature
}
