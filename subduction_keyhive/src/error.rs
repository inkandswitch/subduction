//! Error types for the keyhive protocol.

use alloc::string::String;

use thiserror::Error;

use crate::peer_id::KeyhivePeerId;

/// Errors that can occur during message signing.
#[derive(Debug, Error)]
pub enum SigningError {
    /// Failed to serialize the message payload.
    #[error("failed to serialize payload: {0}")]
    Serialization(String),

    /// The signing operation failed.
    #[error("signing failed")]
    SigningFailed(#[source] keyhive_core::crypto::signed::SigningError),
}

/// Errors that can occur during message verification.
#[derive(Debug, Error)]
pub enum VerificationError {
    /// Failed to deserialize the signed message.
    #[error("failed to deserialize signed message: {0}")]
    Deserialization(String),

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

/// Errors that can occur during protocol operations.
#[derive(Debug, Error)]
pub enum ProtocolError<SendErr>
where
    SendErr: core::error::Error + 'static,
{
    /// Failed to send a message.
    #[error("send error")]
    Send(#[source] SendErr),

    /// Message signing failed.
    #[error("signing error")]
    Signing(#[from] SigningError),

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
    InvalidIdentifier(#[source] ed25519_dalek::SignatureError),

    /// Failed to receive contact card.
    #[error("failed to receive contact card")]
    ReceiveContactCard(#[source] keyhive_core::principal::individual::ReceivePrekeyOpError),

    /// Serialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Deserialization failed.
    #[error("deserialization error: {0}")]
    Deserialization(String),
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

    /// Failed to serialize data.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Failed to deserialize data.
    #[error("deserialization error: {0}")]
    Deserialization(String),
}
