//! Error types for the top-level `Subduction`.

use future_form::FutureForm;
use sedimentree_core::{blob::Blob, crypto::digest::Digest, id::SedimentreeId};
use thiserror::Error;

use crate::{
    connection::{Connection, managed::CallError},
    peer::id::PeerId,
    storage::traits::Storage,
};

/// No multiplexer was found for a connected peer.
///
use sedimentree_core::codec::{decode::Decode, encode::Encode};
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
pub enum HydrationError<Async: FutureForm, Store: Storage<Async>> {
    /// An error occurred while loading all sedimentree IDs.
    #[error("hydration error when loading all sedimentree IDs: {0}")]
    LoadAllIdsError(#[source] Store::Error),

    /// An error occurred while loading loose commits.
    #[error("hydration error when loading loose commits: {0}")]
    LoadLooseCommitsError(#[source] Store::Error),

    /// An error occurred while loading fragments.
    #[error("hydration error when loading fragments: {0}")]
    LoadFragmentsError(#[source] Store::Error),
}

/// An error that can occur during I/O operations.
///
/// This covers storage and network connection errors.
#[derive(Debug, Error)]
pub enum IoError<
    Async: FutureForm + ?Sized,
    Store: Storage<Async>,
    Conn: Connection<Async, WireMsg>,
    WireMsg: Encode + Decode,
> {
    /// An error occurred while using storage.
    #[error(transparent)]
    Storage(Store::Error),

    /// An error occurred while sending data on the connection.
    #[error(transparent)]
    ConnSend(Conn::SendError),

    /// An error occurred while receiving data from the connection.
    #[error(transparent)]
    ConnRecv(Conn::RecvError),

    /// An error occurred during a roundtrip call on the connection.
    #[error(transparent)]
    ConnCall(CallError<Conn::SendError>),

    /// The blob content doesn't match the claimed metadata.
    #[error(transparent)]
    BlobMismatch(#[from] BlobMismatch),
}

/// An error that can occur while handling a batch sync request.
#[derive(Debug, Error)]
pub enum ListenError<
    Async: FutureForm + ?Sized,
    Store: Storage<Async>,
    Conn: Connection<Async, WireMsg>,
    WireMsg: Encode + Decode,
> {
    /// An IO error occurred while handling the batch sync request.
    #[error(transparent)]
    IoError(#[from] IoError<Async, Store, Conn, WireMsg>),

    /// Tried to send a message to a closed channel.
    #[error("tried to send to closed channel")]
    TrySendError,
}

/// An error that can occur when adding a new connection.
#[derive(Debug, Clone, Error, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
pub enum AddConnectionError<D> {
    /// The connection was disallowed by the [`ConnectionPolicy`].
    #[error("connection disallowed: {0}")]
    ConnectionDisallowed(D),

    /// Tried to send a message to a closed channel.
    #[error("tried to send to closed channel")]
    SendToClosedChannel,
}

/// An error that can occur during local write operations.
#[derive(Debug, Error)]
pub enum WriteError<
    Async: FutureForm + ?Sized,
    Store: Storage<Async>,
    Conn: Connection<Async, WireMsg>,
    WireMsg: Encode + Decode,
    PutErr = core::convert::Infallible,
> {
    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] IoError<Async, Store, Conn, WireMsg>),

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
    Async: FutureForm + ?Sized,
    Store: Storage<Async>,
    Conn: Connection<Async, WireMsg>,
    WireMsg: Encode + Decode,
> {
    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] IoError<Async, Store, Conn, WireMsg>),

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
