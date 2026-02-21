//! Decoding for the canonical binary codec.

use super::{error::CodecError, schema::Schema};

// ============================================================================
// Decode Trait
// ============================================================================

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

// ============================================================================
// Helper Functions
// ============================================================================

/// Decode a u8.
#[inline]
pub fn u8(buf: &[u8], offset: usize) -> Result<u8, CodecError> {
    buf.get(offset).copied().ok_or(CodecError::BufferTooShort {
        need: offset + 1,
        have: buf.len(),
    })
}

/// Decode a u16 from big-endian bytes.
#[inline]
pub fn u16(buf: &[u8], offset: usize) -> Result<u16, CodecError> {
    let bytes: [u8; 2] = buf
        .get(offset..offset + 2)
        .and_then(|s| s.try_into().ok())
        .ok_or(CodecError::BufferTooShort {
            need: offset + 2,
            have: buf.len(),
        })?;
    Ok(u16::from_be_bytes(bytes))
}

/// Decode a u32 from big-endian bytes.
#[inline]
pub fn u32(buf: &[u8], offset: usize) -> Result<u32, CodecError> {
    let bytes: [u8; 4] = buf
        .get(offset..offset + 4)
        .and_then(|s| s.try_into().ok())
        .ok_or(CodecError::BufferTooShort {
            need: offset + 4,
            have: buf.len(),
        })?;
    Ok(u32::from_be_bytes(bytes))
}

/// Decode a u64 from big-endian bytes.
#[inline]
pub fn u64(buf: &[u8], offset: usize) -> Result<u64, CodecError> {
    let bytes: [u8; 8] = buf
        .get(offset..offset + 8)
        .and_then(|s| s.try_into().ok())
        .ok_or(CodecError::BufferTooShort {
            need: offset + 8,
            have: buf.len(),
        })?;
    Ok(u64::from_be_bytes(bytes))
}

/// Decode a fixed-size array.
#[inline]
pub fn array<const N: usize>(buf: &[u8], offset: usize) -> Result<[u8; N], CodecError> {
    buf.get(offset..offset + N)
        .and_then(|s| s.try_into().ok())
        .ok_or(CodecError::BufferTooShort {
            need: offset + N,
            have: buf.len(),
        })
}

/// Get a slice of bytes.
#[inline]
pub fn slice(buf: &[u8], offset: usize, len: usize) -> Result<&[u8], CodecError> {
    buf.get(offset..offset + len)
        .ok_or(CodecError::BufferTooShort {
            need: offset + len,
            have: buf.len(),
        })
}

/// Verify that a slice of fixed-size elements is sorted ascending.
///
/// Returns `Ok(())` if sorted, or `Err(CodecError::UnsortedArray)` with
/// the index of the first out-of-order element.
pub fn verify_sorted<const N: usize>(elements: &[[u8; N]]) -> Result<(), CodecError> {
    for i in 1..elements.len() {
        if elements[i - 1] >= elements[i] {
            return Err(CodecError::UnsortedArray { index: i });
        }
    }
    Ok(())
}
