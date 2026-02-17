//! Error types for the top-level `Subduction`.

use alloc::{string::String, vec::Vec};

use future_form::FutureForm;
use keyhive_core::{
    crypto::signed::SigningError as KeyhiveSigningError,
    principal::individual::ReceivePrekeyOpError,
};
use sedimentree_core::{blob::Blob, crypto::digest::Digest, id::SedimentreeId};
use subduction_keyhive::{
    error::VerificationError as KeyhiveVerificationError, peer_id::KeyhivePeerId,
};
use thiserror::Error;

use crate::{connection::Connection, peer::id::PeerId, storage::traits::Storage};

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
pub enum IoError<F: FutureForm + ?Sized, S: Storage<F>, C: Connection<F>> {
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
}

/// An error that can occur while handling a blob request.
#[derive(Debug, Error)]
pub enum BlobRequestErr<F: FutureForm, S: Storage<F>, C: Connection<F>> {
    /// An IO error occurred while handling the blob request.
    #[error("IO error: {0}")]
    IoError(#[from] IoError<F, S, C>),

    /// Some requested blobs were missing locally.
    #[error("Missing blobs: {0:?}")]
    MissingBlobs(Vec<Digest<Blob>>),
}

/// An error that can occur while handling a batch sync request.
#[derive(Debug, Error)]
pub enum ListenError<F: FutureForm + ?Sized, S: Storage<F>, C: Connection<F>> {
    /// An IO error occurred while handling the batch sync request.
    #[error(transparent)]
    IoError(#[from] IoError<F, S, C>),

    /// Missing blobs associated with local fragments or commits.
    #[error("Missing blobs associated to local fragments & commits: {0:?}")]
    MissingBlobs(Vec<Digest<Blob>>),

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
pub enum AttachError<F: FutureForm + ?Sized, S: Storage<F>, C: Connection<F>, D> {
    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] IoError<F, S, C>),

    /// The connection was not allowed.
    #[error("registration error: {0}")]
    Registration(#[from] RegistrationError<D>),
}

/// An error that can occur during local write operations.
#[derive(Debug, Error)]
pub enum WriteError<F: FutureForm + ?Sized, S: Storage<F>, C: Connection<F>, PutErr> {
    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] IoError<F, S, C>),

    /// The storage policy rejected the write.
    #[error("put disallowed: {0}")]
    PutDisallowed(PutErr),
}

/// An error that can occur when sending requested data to a peer.
#[derive(Debug, Error)]
pub enum SendRequestedDataError<F: FutureForm + ?Sized, S: Storage<F>, C: Connection<F>> {
    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] IoError<F, S, C>),

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

/// Errors that can occur during keyhive sync operations.
#[derive(Debug, Error)]
pub enum KeyhiveSyncError<F: FutureForm + ?Sized, C: Connection<F>> {
    /// Failed to send a keyhive message over the connection.
    #[error("failed to send keyhive message")]
    Send(#[source] C::SendError),

    /// Keyhive storage operation failed.
    #[error("keyhive storage error")]
    Storage(#[from] subduction_keyhive::error::StorageError),

    /// Message signing failed.
    #[error("signing failed")]
    Signing(#[from] KeyhiveSigningError),

    /// Message verification failed.
    #[error("verification failed")]
    Verification(#[from] KeyhiveVerificationError),

    /// The peer is not connected.
    #[error("peer not connected: {0:?}")]
    PeerNotConnected(PeerId),

    /// The peer is unknown (no contact card available).
    #[error("unknown peer (no contact card): {0:?}")]
    UnknownPeer(KeyhivePeerId),

    /// Received an unexpected message type.
    #[error("unexpected message type: expected {expected}, got {actual}")]
    UnexpectedMessageType {
        /// The expected message type.
        expected: &'static str,
        /// The actual message type.
        actual: &'static str,
    },

    /// Failed to convert peer ID to keyhive identifier.
    #[error("invalid peer identifier")]
    InvalidIdentifier(#[source] ed25519_dalek::SignatureError),

    /// Failed to ingest a contact card.
    #[error("failed to receive contact card")]
    ReceiveContactCard(#[from] ReceivePrekeyOpError),

    /// CBOR serialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// CBOR deserialization failed.
    #[error("deserialization error: {0}")]
    Deserialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(test, feature = "std", feature = "bolero"))]
    mod proptests {
        use alloc::{format, string::String};

        use super::*;

        #[test]
        fn prop_display_produces_non_empty_string() {
            bolero::check!()
                .with_type::<RegistrationError<String>>()
                .for_each(|err| {
                    let display = format!("{err}");
                    assert!(!display.is_empty());
                });
        }
    }
}
