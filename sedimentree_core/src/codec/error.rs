//! Error types for the canonical binary codec.

use thiserror::Error;

/// What type of data was being read when a buffer underflow occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadingType {
    /// Reading a u8.
    U8,
    /// Reading a u16.
    U16,
    /// Reading a u32.
    U32,
    /// Reading a u64.
    U64,
    /// Reading a fixed-size array.
    Array {
        /// Size of the array.
        size: usize,
    },
    /// Reading a byte slice.
    Slice {
        /// Length of the slice.
        len: usize,
    },
}

impl core::fmt::Display for ReadingType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::U8 => write!(f, "u8"),
            Self::U16 => write!(f, "u16"),
            Self::U32 => write!(f, "u32"),
            Self::U64 => write!(f, "u64"),
            Self::Array { size } => write!(f, "[u8; {size}]"),
            Self::Slice { len } => write!(f, "&[u8] (len {len})"),
        }
    }
}

/// Errors that can occur during decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DecodeError {
    /// Buffer is too short when reading a specific primitive type.
    #[error(
        "buffer too short reading {reading} at offset {offset}: need {need} bytes, have {have}"
    )]
    BufferTooShort {
        /// What type was being read.
        reading: ReadingType,
        /// Offset where the read was attempted.
        offset: usize,
        /// Minimum bytes needed from offset.
        need: usize,
        /// Actual bytes available from offset.
        have: usize,
    },

    /// Message is smaller than minimum required size for its type.
    #[error("{type_name} too short: need {need} bytes, have {have}")]
    MessageTooShort {
        /// Name of the message/payload type.
        type_name: &'static str,
        /// Minimum bytes required.
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
