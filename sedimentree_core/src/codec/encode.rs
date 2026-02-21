//! Encoding for the canonical binary codec.

use alloc::vec::Vec;

use super::schema::Schema;

/// Encode a type to its canonical binary representation.
///
/// Types implementing this trait can be signed and transmitted.
/// The encoding must be deterministic for signature verification.
pub trait Encode: Schema {
    /// Encode type-specific fields to the buffer.
    ///
    /// This is called after the schema and issuer have been written.
    /// The implementation should append its fields to `buf`.
    fn encode_fields(&self, ctx: &Self::Context, buf: &mut Vec<u8>);

    /// Size of the encoded fields (for buffer pre-allocation).
    fn fields_size(&self, ctx: &Self::Context) -> usize;

    /// Total size of the signed message.
    ///
    /// This is `4 (schema) + 32 (issuer) + fields_size + 64 (signature)`.
    fn signed_size(&self, ctx: &Self::Context) -> usize {
        4 + 32 + self.fields_size(ctx) + 64
    }
}

/// Encode a u8.
#[inline]
pub fn u8(value: u8, buf: &mut Vec<u8>) {
    buf.push(value);
}

/// Encode a u16 as big-endian.
#[inline]
pub fn u16(value: u16, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Encode a u32 as big-endian.
#[inline]
pub fn u32(value: u32, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Encode a u64 as big-endian.
#[inline]
pub fn u64(value: u64, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Encode raw bytes.
#[inline]
pub fn bytes(value: &[u8], buf: &mut Vec<u8>) {
    buf.extend_from_slice(value);
}

/// Encode a fixed-size array.
#[inline]
pub fn array<const N: usize>(value: &[u8; N], buf: &mut Vec<u8>) {
    buf.extend_from_slice(value);
}
