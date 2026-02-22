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

/// Buffer underflow when reading a primitive type.
///
/// Returned by primitive decode functions (`u8`, `u16`, etc.) that can only fail
/// due to insufficient bytes. Converts into [`DecodeError`] via `?`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("buffer too short reading {reading} at offset {offset}: need {need} bytes, have {have}")]
pub struct BufferTooShort {
    /// What type was being read.
    pub reading: ReadingType,
    /// Offset where the read was attempted.
    pub offset: usize,
    /// Minimum bytes needed from offset.
    pub need: usize,
    /// Actual bytes available from offset.
    pub have: usize,
}

/// Schema header doesn't match expected value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("invalid schema: expected {expected:?}, got {got:?}")]
pub struct InvalidSchema {
    /// Expected schema bytes.
    pub expected: [u8; 4],
    /// Actual schema bytes found.
    pub got: [u8; 4],
}

/// Array elements are not in sorted order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("array not sorted at index {index}")]
pub struct UnsortedArray {
    /// Index where sort violation was detected.
    pub index: usize,
}

/// Enum discriminant/tag is not recognized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("invalid enum tag {tag:#04x} for {type_name}")]
pub struct InvalidEnumTag {
    /// The invalid tag value.
    pub tag: u8,
    /// Name of the enum type.
    pub type_name: &'static str,
}

/// Declared size doesn't match actual data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("size mismatch: declared {declared}, actual {actual}")]
pub struct SizeMismatch {
    /// Size declared in the message.
    pub declared: usize,
    /// Actual size of the data.
    pub actual: usize,
}

/// [`BlobMeta`](crate::blob::BlobMeta) size exceeds maximum allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("blob too large: {size} bytes, max {max}")]
pub struct BlobTooLarge {
    /// Size of the blob.
    pub size: u64,
    /// Maximum allowed size.
    pub max: u64,
}

/// Array has too many elements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("{field} has too many elements: {count}, max {max}")]
pub struct ArrayTooLarge {
    /// Number of elements.
    pub count: usize,
    /// Maximum allowed.
    pub max: usize,
    /// Name of the array field.
    pub field: &'static str,
}

/// Duplicate element in array.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("duplicate element in {field} at index {index}")]
pub struct DuplicateElement {
    /// Index where duplicate was found.
    pub index: usize,
    /// Name of the array field.
    pub field: &'static str,
}

/// Errors that can occur during decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DecodeError {
    /// Buffer is too short when reading a specific primitive type.
    #[error(transparent)]
    BufferTooShort(#[from] BufferTooShort),

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
    #[error(transparent)]
    InvalidSchema(#[from] InvalidSchema),

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
    #[error(transparent)]
    UnsortedArray(#[from] UnsortedArray),

    /// Enum discriminant/tag is not recognized.
    #[error(transparent)]
    InvalidEnumTag(#[from] InvalidEnumTag),

    /// Declared size doesn't match actual data.
    #[error(transparent)]
    SizeMismatch(#[from] SizeMismatch),

    /// [`BlobMeta`](crate::blob::BlobMeta) size exceeds maximum allowed.
    #[error(transparent)]
    BlobTooLarge(#[from] BlobTooLarge),

    /// Array has too many elements.
    #[error(transparent)]
    ArrayTooLarge(#[from] ArrayTooLarge),

    /// Duplicate element in array.
    #[error(transparent)]
    DuplicateElement(#[from] DuplicateElement),
}
