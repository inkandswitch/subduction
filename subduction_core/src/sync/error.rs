//! Error types for the top-level `Subduction`.

use sedimentree_core::{future::FutureKind, storage::Storage, Digest};
use thiserror::Error;

use crate::connection::{Connection, ConnectionDisallowed};

/// An error that can occur during I/O operations.
///
/// This covers storage and network connection errors.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Error)]
pub enum IoError<F: FutureKind, S: Storage<F>, C: Connection<F>> {
    /// An error occurred while using storage.
    #[error(transparent)]
    Storage(S::Error),

    /// An error occurred while sending data on the connection.
    #[error(transparent)]
    ConnSend(C::SendError),

    /// An error occurred while receiving data from the connection.
    #[error(transparent)]
    ConnRecv(C::RecvError),

    /// An error occurred during a roundtrip call on the connection.
    #[error(transparent)]
    ConnCall(C::CallError),

    /// The connection was disallowed by the [`ConnectionPolicy`] policy.
    #[error(transparent)]
    ConnPolicy(#[from] ConnectionDisallowed),
}

/// An error that can occur while handling a blob request.
#[derive(Debug, Error)]
pub enum BlobRequestErr<F: FutureKind, S: Storage<F>, C: Connection<F>> {
    /// An IO error occurred while handling the blob request.
    #[error("IO error: {0}")]
    IoError(#[from] IoError<F, S, C>),

    /// Some requested blobs were missing locally.
    #[error("Missing blobs: {0:?}")]
    MissingBlobs(Vec<Digest>),
}

/// An error that can occur while handling a batch sync request.
#[derive(Debug, Error)]
pub enum ListenError<F: FutureKind, S: Storage<F>, C: Connection<F>> {
    /// An IO error occurred while handling the batch sync request.
    #[error(transparent)]
    IoError(#[from] IoError<F, S, C>),

    /// Missing blobs associated with local chunks or commits.
    #[error("Missing blobs associated to local chunks & commits: {0:?}")]
    MissingBlobs(Vec<Digest>),
}
