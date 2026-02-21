//! Error types for the canonical binary codec.

use thiserror::Error;

/// Errors that can occur during encoding or decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CodecError {
    /// Buffer is too short to contain the expected data.
    #[error("buffer too short: need {need} bytes, have {have}")]
    BufferTooShort {
        /// Minimum bytes needed.
        need: usize,
        /// Actual bytes available.
        have: usize,
    },

    /// Schema header doesn't match expected value.
    #[error("invalid schema: expected {expected:?}, got {got:?}")]
    InvalidSchema {
        /// Expected schema bytes.
        expected: [u8; 4],
        /// Actual schema bytes found.
        got: [u8; 4],
    },

    /// Protocol version is not supported.
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    /// Ed25519 signature verification failed.
    #[error("invalid signature")]
    InvalidSignature,

    /// Ed25519 verifying key is invalid.
    #[error("invalid verifying key")]
    InvalidVerifyingKey,

    /// Array elements are not in sorted order.
    #[error("array not sorted at index {index}")]
    UnsortedArray {
        /// Index where sort violation was detected.
        index: usize,
    },

    /// Enum discriminant/tag is not recognized.
    #[error("invalid enum tag {tag:#04x} for {type_name}")]
    InvalidEnumTag {
        /// The invalid tag value.
        tag: u8,
        /// Name of the enum type.
        type_name: &'static str,
    },

    /// Declared size doesn't match actual data.
    #[error("size mismatch: declared {declared}, actual {actual}")]
    SizeMismatch {
        /// Size declared in the message.
        declared: usize,
        /// Actual size of the data.
        actual: usize,
    },

    /// Context value (e.g., SedimentreeId) doesn't match signed payload.
    #[error("context mismatch: {field}")]
    ContextMismatch {
        /// Description of what mismatched.
        field: &'static str,
    },

    /// BlobMeta size exceeds maximum allowed.
    #[error("blob too large: {size} bytes, max {max}")]
    BlobTooLarge {
        /// Size of the blob.
        size: u64,
        /// Maximum allowed size.
        max: u64,
    },

    /// Array has too many elements.
    #[error("{field} has too many elements: {count}, max {max}")]
    ArrayTooLarge {
        /// Number of elements.
        count: usize,
        /// Maximum allowed.
        max: usize,
        /// Name of the array field.
        field: &'static str,
    },

    /// Duplicate element in array.
    #[error("duplicate element in {field} at index {index}")]
    DuplicateElement {
        /// Index where duplicate was found.
        index: usize,
        /// Name of the array field.
        field: &'static str,
    },
}
