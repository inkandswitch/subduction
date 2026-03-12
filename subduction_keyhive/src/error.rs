//! Error types for the keyhive protocol.

extern crate alloc;

use alloc::boxed::Box;

use thiserror::Error;

#[cfg(feature = "std")]
use crate::peer_id::KeyhivePeerId;

/// CBOR serialization error (newtype around ciborium).
#[derive(Debug, Error)]
#[error("CBOR serialization error: {0}")]
pub struct CborSerError(
    #[source] pub ciborium::ser::Error<alloc::boxed::Box<dyn core::error::Error + Send + Sync>>,
);

impl CborSerError {
    /// Wrap a concrete ciborium serialization error.
    ///
    /// The writer-specific IO error is erased into a boxed trait object
    /// so that `CborSerError` doesn't depend on the writer type.
    pub fn from_writer<W: core::error::Error + Send + Sync + 'static>(
        err: ciborium::ser::Error<W>,
    ) -> Self {
        match err {
            ciborium::ser::Error::Io(e) => Self(ciborium::ser::Error::Io(Box::new(e))),
            ciborium::ser::Error::Value(s) => Self(ciborium::ser::Error::Value(s)),
        }
    }
}

/// CBOR deserialization error (newtype around ciborium).
#[derive(Debug, Error)]
#[error("CBOR deserialization error: {0}")]
pub struct CborDeError(
    #[source] pub ciborium::de::Error<alloc::boxed::Box<dyn core::error::Error + Send + Sync>>,
);

/// Wrapper to give `ciborium_io::EndOfFile` an `Error` impl.
///
/// ciborium's no-std reader error type (`EndOfFile`) only implements `Debug`,
/// not `Display`/`Error`. This newtype bridges the gap.
#[derive(Clone, Copy, Debug, Error)]
#[error("unexpected end of CBOR input")]
pub struct CborEndOfFile;

impl CborDeError {
    /// Wrap a concrete ciborium deserialization error.
    ///
    /// The reader-specific IO error is erased into a boxed trait object
    /// so that `CborDeError` doesn't depend on the reader type.
    pub fn from_reader<R: core::error::Error + Send + Sync + 'static>(
        err: ciborium::de::Error<R>,
    ) -> Self {
        match err {
            ciborium::de::Error::Io(e) => Self(ciborium::de::Error::Io(Box::new(e))),
            ciborium::de::Error::Syntax(offset) => Self(ciborium::de::Error::Syntax(offset)),
            ciborium::de::Error::Semantic(offset, msg) => {
                Self(ciborium::de::Error::Semantic(offset, msg))
            }
            ciborium::de::Error::RecursionLimitExceeded => {
                Self(ciborium::de::Error::RecursionLimitExceeded)
            }
        }
    }

    /// Wrap a ciborium deserialization error from a `&[u8]` reader.
    ///
    /// Depending on whether `ciborium-io` has `std` enabled (due to Cargo
    /// feature unification), the IO error type for `&[u8]` readers is
    /// either `ciborium_io::EndOfFile` (no-std) or `std::io::Error` (std).
    /// Neither can be statically assumed, so this method accepts any
    /// `Debug` type and always substitutes [`CborEndOfFile`] for the `Io`
    /// variant — the original error is only "unexpected end of input".
    #[must_use]
    pub fn from_slice<E: core::fmt::Debug>(err: ciborium::de::Error<E>) -> Self {
        match err {
            ciborium::de::Error::Io(_) => Self(ciborium::de::Error::Io(Box::new(CborEndOfFile))),
            ciborium::de::Error::Syntax(offset) => Self(ciborium::de::Error::Syntax(offset)),
            ciborium::de::Error::Semantic(offset, msg) => {
                Self(ciborium::de::Error::Semantic(offset, msg))
            }
            ciborium::de::Error::RecursionLimitExceeded => {
                Self(ciborium::de::Error::RecursionLimitExceeded)
            }
        }
    }
}

/// Errors that can occur during message signing.
#[cfg(feature = "std")]
#[derive(Debug, Error)]
pub enum SigningError {
    /// Failed to serialize the message payload.
    #[error("failed to serialize payload")]
    Serialization(#[source] CborSerError),

    /// The signing operation failed.
    #[error("signing failed")]
    SigningFailed(#[source] keyhive_core::crypto::signed::SigningError),
}

/// Errors that can occur during message verification.
#[cfg(feature = "std")]
#[derive(Debug, Error)]
pub enum VerificationError {
    /// Failed to deserialize the signed message.
    #[error("failed to deserialize signed message")]
    Deserialization(#[source] CborDeError),

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
#[cfg(feature = "std")]
#[derive(Debug, Error)]
pub enum ProtocolError<SendErr: core::error::Error + 'static> {
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

    /// A storage operation failed.
    #[error("storage error")]
    Storage(#[source] StorageError),

    /// CBOR serialization failed.
    #[error("serialization error")]
    Serialization(#[source] CborSerError),

    /// CBOR deserialization failed.
    #[error("deserialization error")]
    Deserialization(#[source] CborDeError),
}

/// Errors that can occur during storage operations.
///
/// Storage backend errors are erased into `Box<dyn Error + Send + Sync>`
/// so that `StorageError` stays concrete and doesn't leak the backend type.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Failed to save data.
    #[error("failed to save")]
    Save(#[source] Box<dyn core::error::Error + Send + Sync>),

    /// Failed to load data.
    #[error("failed to load")]
    Load(#[source] Box<dyn core::error::Error + Send + Sync>),

    /// Failed to delete data.
    #[error("failed to delete")]
    Delete(#[source] Box<dyn core::error::Error + Send + Sync>),

    /// Failed to serialize data.
    #[error("serialization error")]
    Serialization(#[source] CborSerError),

    /// Failed to deserialize data.
    #[error("deserialization error")]
    Deserialization(#[source] CborDeError),

    /// Keyhive archive ingestion failed.
    #[error("archive ingestion failed")]
    ArchiveIngestion(#[source] Box<dyn core::error::Error + Send + Sync>),
}
