//! Error types for the top-level `Subduction`.

use alloc::vec::Vec;

use future_form::FutureForm;
use sedimentree_core::{blob::Blob, crypto::digest::Digest, id::SedimentreeId};
use thiserror::Error;

use sedimentree_core::codec::{decode::Decode, encode::Encode};

use crate::{
    connection::{
        Connection, Roundtrip,
        message::{BatchSyncRequest, BatchSyncResponse},
    },
    peer::id::PeerId,
    storage::traits::Storage,
};
use subduction_crypto::verified_meta::BlobMismatch;

/// The peer is not authorized to perform the requested operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
#[error("peer {peer} not authorized to access sedimentree {sedimentree_id}")]
pub struct Unauthorized {
    /// The peer that attempted the operation.
    pub peer: PeerId,

    /// The sedimentree they attempted to access.
    pub sedimentree_id: SedimentreeId,
}

/// An error indicating that a [`Sedimentree`] could not be hydrated from storage.
#[derive(Debug, Clone, Copy, Error)]
pub enum HydrationError<F: FutureForm, S: Storage<F>> {
    /// An error occurred while loading all sedimentree IDs.
    #[error("hydration error when loading all sedimentree IDs: {0}")]
    LoadAllIdsError(#[source] S::Error),

    /// An error occurred while loading loose commits.
    #[error("hydration error when loading loose commits: {0}")]
    LoadLooseCommitsError(#[source] S::Error),

    /// An error occurred while loading fragments.
    #[error("hydration error when loading fragments: {0}")]
    LoadFragmentsError(#[source] S::Error),
}

/// An error that can occur during I/O operations.
///
/// This covers storage and network connection errors.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Error)]
pub enum IoError<
    F: FutureForm + ?Sized,
    S: Storage<F>,
    C: Connection<F, W> + Roundtrip<F, BatchSyncRequest, BatchSyncResponse>,
    W: Encode + Decode,
> {
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

    /// The blob content doesn't match the claimed metadata.
    #[error(transparent)]
    BlobMismatch(#[from] BlobMismatch),
}

/// An error that can occur while handling a blob request.
#[derive(Debug, Error)]
pub enum BlobRequestErr<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F, W> + Roundtrip<F, BatchSyncRequest, BatchSyncResponse>,
    W: Encode + Decode,
> {
    /// An IO error occurred while handling the blob request.
    #[error("IO error: {0}")]
    IoError(#[from] IoError<F, S, C, W>),

    /// Some requested blobs were missing locally.
    #[error("Missing blobs: {0:?}")]
    MissingBlobs(Vec<Digest<Blob>>),
}

/// An error that can occur while handling a batch sync request.
#[derive(Debug, Error)]
pub enum ListenError<
    F: FutureForm + ?Sized,
    S: Storage<F>,
    C: Connection<F, W> + Roundtrip<F, BatchSyncRequest, BatchSyncResponse>,
    W: Encode + Decode,
> {
    /// An IO error occurred while handling the batch sync request.
    #[error(transparent)]
    IoError(#[from] IoError<F, S, C, W>),

    /// Tried to send a message to a closed channel.
    #[error("tried to send to closed channel")]
    TrySendError,
}

/// An error that can occur during registration of a new connection.
#[derive(Debug, Clone, Error, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
pub enum RegistrationError<D> {
    /// The connection was disallowed by the [`ConnectionPolicy`].
    #[error("connection disallowed: {0}")]
    ConnectionDisallowed(D),

    /// Tried to send a message to a closed channel.
    #[error("tried to send to closed channel")]
    SendToClosedChannel,
}

/// An error that can occur during attachment.
#[derive(Debug, Error)]
pub enum AttachError<
    F: FutureForm + ?Sized,
    S: Storage<F>,
    C: Connection<F, W> + Roundtrip<F, BatchSyncRequest, BatchSyncResponse>,
    W: Encode + Decode,
    D,
> {
    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] IoError<F, S, C, W>),

    /// The connection was not allowed.
    #[error("registration error: {0}")]
    Registration(#[from] RegistrationError<D>),
}

/// An error that can occur during local write operations.
#[derive(Debug, Error)]
pub enum WriteError<
    F: FutureForm + ?Sized,
    S: Storage<F>,
    C: Connection<F, W> + Roundtrip<F, BatchSyncRequest, BatchSyncResponse>,
    W: Encode + Decode,
    PutErr,
> {
    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] IoError<F, S, C, W>),

    /// The storage policy rejected the write.
    #[error("put disallowed: {0}")]
    PutDisallowed(PutErr),

    /// A required blob was not provided.
    #[error("missing blob: {0}")]
    MissingBlob(Digest<Blob>),
}

/// An error that can occur when sending requested data to a peer.
#[derive(Debug, Error)]
pub enum SendRequestedDataError<
    F: FutureForm + ?Sized,
    S: Storage<F>,
    C: Connection<F, W> + Roundtrip<F, BatchSyncRequest, BatchSyncResponse>,
    W: Encode + Decode,
> {
    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] IoError<F, S, C, W>),

    /// The peer is not authorized to access the requested sedimentree.
    #[error(transparent)]
    Unauthorized(#[from] Unauthorized),
}

/// Error when a sync request is rejected by the remote peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum SyncRejected {
    /// The sedimentree was not found on the remote peer.
    #[error("sedimentree {0} not found on remote peer")]
    NotFound(SedimentreeId),

    /// Not authorized to access the sedimentree.
    #[error("not authorized to access sedimentree {0}")]
    Unauthorized(SedimentreeId),
}
