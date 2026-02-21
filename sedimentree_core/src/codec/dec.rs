//! Decoding trait for signed payloads.

use super::{error::CodecError, Schema};

/// Decode a type from its canonical binary representation.
///
/// Types implementing this trait can be parsed from received bytes.
pub trait Decode: Schema + Sized {
    /// Minimum valid encoded size (for early rejection).
    ///
    /// This is the size of the full signed message (schema + issuer + fields + signature).
    const MIN_SIZE: usize;

    /// Decode type-specific fields from the buffer.
    ///
    /// `buf` contains only the fields portion (after schema + issuer,
    /// before signature). The implementation should parse and validate
    /// all fields, including checking sort order for arrays.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if the buffer is malformed, too short,
    /// contains invalid values, or fails validation (e.g., unsorted arrays).
    fn try_decode_fields(buf: &[u8], ctx: &Self::Context) -> Result<Self, CodecError>;
}
