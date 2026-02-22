//! Decoding for the canonical binary codec.

use super::{
    error::{BufferTooShort, DecodeError, ReadingType, UnsortedArray},
    schema::Schema,
};

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
    /// Returns [`DecodeError`] if the buffer is malformed, too short,
    /// contains invalid values, or fails validation (e.g., unsorted arrays).
    fn try_decode_fields(buf: &[u8]) -> Result<Self, DecodeError>;
}

/// Decode a u8.
///
/// # Errors
///
/// Returns [`BufferTooShort`] if there's not enough data at `offset`.
#[inline]
pub fn u8(buf: &[u8], offset: usize) -> Result<u8, BufferTooShort> {
    buf.get(offset).copied().ok_or(BufferTooShort {
        reading: ReadingType::U8,
        offset,
        need: 1,
        have: buf.len().saturating_sub(offset),
    })
}

/// Decode a u16 from big-endian bytes.
///
/// # Errors
///
/// Returns [`BufferTooShort`] if there are fewer than 2 bytes at `offset`.
#[inline]
pub fn u16(buf: &[u8], offset: usize) -> Result<u16, BufferTooShort> {
    let bytes: [u8; 2] = buf
        .get(offset..offset + 2)
        .and_then(|s| s.try_into().ok())
        .ok_or(BufferTooShort {
            reading: ReadingType::U16,
            offset,
            need: 2,
            have: buf.len().saturating_sub(offset),
        })?;
    Ok(u16::from_be_bytes(bytes))
}

/// Decode a u32 from big-endian bytes.
///
/// # Errors
///
/// Returns [`BufferTooShort`] if there are fewer than 4 bytes at `offset`.
#[inline]
pub fn u32(buf: &[u8], offset: usize) -> Result<u32, BufferTooShort> {
    let bytes: [u8; 4] = buf
        .get(offset..offset + 4)
        .and_then(|s| s.try_into().ok())
        .ok_or(BufferTooShort {
            reading: ReadingType::U32,
            offset,
            need: 4,
            have: buf.len().saturating_sub(offset),
        })?;
    Ok(u32::from_be_bytes(bytes))
}

/// Decode a u64 from big-endian bytes.
///
/// # Errors
///
/// Returns [`BufferTooShort`] if there are fewer than 8 bytes at `offset`.
#[inline]
pub fn u64(buf: &[u8], offset: usize) -> Result<u64, BufferTooShort> {
    let bytes: [u8; 8] = buf
        .get(offset..offset + 8)
        .and_then(|s| s.try_into().ok())
        .ok_or(BufferTooShort {
            reading: ReadingType::U64,
            offset,
            need: 8,
            have: buf.len().saturating_sub(offset),
        })?;
    Ok(u64::from_be_bytes(bytes))
}

/// Decode a fixed-size array.
///
/// # Errors
///
/// Returns [`BufferTooShort`] if there are fewer than `N` bytes at `offset`.
#[inline]
pub fn array<const N: usize>(buf: &[u8], offset: usize) -> Result<[u8; N], BufferTooShort> {
    buf.get(offset..offset + N)
        .and_then(|s| s.try_into().ok())
        .ok_or(BufferTooShort {
            reading: ReadingType::Array { size: N },
            offset,
            need: N,
            have: buf.len().saturating_sub(offset),
        })
}

/// Get a slice of bytes.
///
/// # Errors
///
/// Returns [`BufferTooShort`] if there are fewer than `len` bytes at `offset`.
#[inline]
pub fn slice(buf: &[u8], offset: usize, len: usize) -> Result<&[u8], BufferTooShort> {
    buf.get(offset..offset + len).ok_or(BufferTooShort {
        reading: ReadingType::Slice { len },
        offset,
        need: len,
        have: buf.len().saturating_sub(offset),
    })
}

/// Verify that a slice of fixed-size elements is sorted ascending.
///
/// # Errors
///
/// Returns [`UnsortedArray`] with the index of the first out-of-order element.
pub fn verify_sorted<const N: usize>(elements: &[[u8; N]]) -> Result<(), UnsortedArray> {
    for (i, window) in elements.windows(2).enumerate() {
        let [prev, curr] = window else {
            // windows(2) always yields slices of length 2, but the compiler doesn't know
            continue;
        };
        if prev >= curr {
            return Err(UnsortedArray { index: i + 1 });
        }
    }
    Ok(())
}
