//! Error types for the keyhive protocol.

use alloc::string::String;
use core::convert::Infallible;

use thiserror::Error;

use crate::peer_id::KeyhivePeerId;

/// Errors that can occur during message signing.
#[derive(Debug, Error)]
pub enum SigningError {
    /// Failed to serialize the message payload (minicbor).
    #[error("failed to serialize payload")]
    CborEncode(#[from] minicbor::encode::Error<Infallible>),

    /// Failed to serialize the message payload (serde).
    #[error("failed to serialize payload")]
    SerdeEncode(#[from] minicbor_serde::error::EncodeError<Infallible>),

    /// The signing operation failed.
    #[error("signing failed")]
    SigningFailed(#[source] keyhive_core::crypto::signed::SigningError),
}

/// Errors that can occur during message verification.
#[derive(Debug, Error)]
pub enum VerificationError {
    /// Failed to deserialize the signed message (minicbor).
    #[error("failed to deserialize signed message")]
    CborDecode(#[from] minicbor::decode::Error),

    /// Failed to deserialize the signed message (serde).
    #[error("failed to deserialize signed message")]
    SerdeDecode(#[from] minicbor_serde::error::DecodeError),

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

    /// Failed to serialize data (minicbor).
    #[error("serialization error")]
    CborEncode(#[from] minicbor::encode::Error<Infallible>),

    /// Failed to serialize data (serde).
    #[error("serialization error")]
    SerdeEncode(#[from] minicbor_serde::error::EncodeError<Infallible>),

    /// Failed to deserialize data.
    #[error("deserialization error")]
    Deserialization(#[source] minicbor::decode::Error),

    /// Failed to deserialize data (serde).
    #[error("deserialization error")]
    SerdeDecode(#[from] minicbor_serde::error::DecodeError),
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

    /// Failed to serialize data (minicbor).
    #[error("serialization error")]
    CborEncode(#[from] minicbor::encode::Error<Infallible>),

    /// Failed to serialize data (serde).
    #[error("serialization error")]
    SerdeEncode(#[from] minicbor_serde::error::EncodeError<Infallible>),

    /// Failed to deserialize data (minicbor).
    #[error("deserialization error")]
    CborDecode(#[from] minicbor::decode::Error),

    /// Failed to deserialize data (serde).
    #[error("deserialization error")]
    SerdeDecode(#[from] minicbor_serde::error::DecodeError),
}
