//! Error types for the keyhive protocol.

use alloc::string::String;

use thiserror::Error;

use crate::peer_id::KeyhivePeerId;

/// Errors that can occur during message signing. `E` is the keyhive's error.
#[derive(Debug, Error)]
pub enum SigningError<E: core::error::Error + 'static> {
    /// The signing operation failed.
    #[error("signing failed")]
    SigningFailed(#[source] E),
}

/// Errors that can occur during message verification.
#[derive(Debug, Error)]
pub enum VerificationError {
    /// The signed frame is not a keyhive `Signed<Bytes>`.
    #[error("undecodable signed frame")]
    SignedFrame(#[source] keyhive_codec::error::DecodeError),

    /// The signed payload is not a protocol message.
    #[error("undecodable message")]
    Payload(#[source] WireDecodeError),

    /// The signature is invalid.
    #[error("invalid signature")]
    InvalidSignature,

    /// The sender ID doesn't match the signing key.
    #[error("sender ID mismatch: expected {expected}, got {actual}")]
    SenderMismatch {
        /// The expected sender (from message metadata).
        expected: KeyhivePeerId,
        /// The actual sender (from the signature).
        actual: KeyhivePeerId,
    },
}

/// Errors that can occur during protocol operations. `SendErr` is the
/// connection's send error and `KeyhiveErr` the keyhive's error.
#[derive(Debug, Error)]
pub enum ProtocolError<
    SendErr: core::error::Error + 'static,
    KeyhiveErr: core::error::Error + 'static,
> {
    /// Failed to send a message.
    #[error("send error")]
    Send(#[source] SendErr),

    /// Message signing failed.
    #[error("signing error")]
    Signing(#[from] SigningError<KeyhiveErr>),

    /// Message verification failed.
    #[error("verification error")]
    Verification(#[from] VerificationError),

    /// The peer is unknown (no contact card).
    #[error("unknown peer: {0}")]
    UnknownPeer(KeyhivePeerId),

    /// The message type was unexpected.
    #[error("unexpected message type: expected {expected}, got {actual}")]
    UnexpectedMessageType {
        /// The expected message type.
        expected: &'static str,
        /// The actual message type.
        actual: &'static str,
    },

    /// Failed to convert peer ID to identifier.
    #[error("failed to convert peer ID")]
    InvalidIdentifier(#[source] keyhive_crypto::identifier::InvalidIdentifier),

    /// A keyhive operation failed.
    #[error("keyhive operation failed")]
    Keyhive(#[source] KeyhiveErr),

    /// Failed to receive contact card.
    #[error("failed to receive contact card")]
    ReceiveContactCard(#[source] KeyhiveErr),

    /// Some received events do not decode. The others were ingested.
    #[error("{undecodable} of {total} events do not decode")]
    UndecodableEvents {
        /// How many events do not decode.
        undecodable: usize,

        /// How many events were received.
        total: usize,
    },

    /// A storage operation failed.
    #[error("storage error")]
    Storage(#[from] StorageError),
}

/// Bytes that are not a keyhive protocol value: a [`Message`], a
/// [`SignedMessage`], or a [`KeyhivePeerId`] within them.
///
/// [`Message`]: crate::Message
/// [`SignedMessage`]: crate::SignedMessage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WireDecodeError {
    /// The bytes break the canonical codec: too short, an unknown tag, a
    /// length that overruns the buffer, or trailing bytes.
    #[error(transparent)]
    Codec(#[from] sedimentree_core::codec::error::DecodeError),

    /// A peer ID's suffix is not UTF-8.
    #[error("peer ID suffix is not UTF-8")]
    Suffix(#[from] core::str::Utf8Error),
}

/// Errors that can occur during storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Failed to save data.
    #[error("failed to save: {0}")]
    Save(String),

    /// Failed to load data.
    #[error("failed to load: {0}")]
    Load(String),

    /// Failed to delete data.
    #[error("failed to delete: {0}")]
    Delete(String),
}
