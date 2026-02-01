//! Error types for the keyhive protocol.

use alloc::string::{String, ToString};

use thiserror::Error;

use crate::peer_id::KeyhivePeerId;

/// Errors that can occur during message signing.
#[derive(Debug, Error)]
pub enum SigningError {
    /// Failed to serialize the message payload.
    #[error("failed to serialize payload: {0}")]
    Serialization(String),

    /// The signing operation failed.
    #[error("signing failed: {0}")]
    SigningFailed(String),
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

    /// Failed to parse the peer ID.
    #[error("failed to parse peer ID: {0}")]
    InvalidPeerId(String),
}

/// Errors that can occur during protocol operations.
#[derive(Debug, Error)]
pub enum ProtocolError<SendErr, RecvErr>
where
    SendErr: core::error::Error + 'static,
    RecvErr: core::error::Error + 'static,
{
    /// Failed to send a message.
    #[error("send error: {0}")]
    Send(#[source] SendErr),

    /// Failed to receive a message.
    #[error("receive error: {0}")]
    Recv(#[source] RecvErr),

    /// Message signing failed.
    #[error("signing error: {0}")]
    Signing(#[from] SigningError),

    /// Message verification failed.
    #[error("verification error: {0}")]
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

    /// Keyhive operation failed.
    #[error("keyhive error: {0}")]
    Keyhive(String),
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

/// Errors that can occur during event ingestion.
#[derive(Debug, Error)]
pub enum IngestError {
    /// Failed to ingest one or more events.
    #[error("failed to ingest {pending_count} events")]
    PendingEvents {
        /// Number of events still pending.
        pending_count: usize,
    },

    /// Failed to deserialize event bytes.
    #[error("failed to deserialize event: {0}")]
    Deserialization(String),

    /// Keyhive rejected the event.
    #[error("keyhive rejected event: {0}")]
    Rejected(String),
}
