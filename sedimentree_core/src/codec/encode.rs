//! Encoding for the canonical binary codec.

use alloc::vec::Vec;

use super::schema::Schema;

/// Encode a type to its canonical binary representation.
///
/// This is a simple trait for types that can be serialized to bytes.
/// The encoding must be deterministic.
pub trait Encode {
    /// Encode to a new `Vec<u8>`.
    fn encode(&self) -> Vec<u8>;

    /// Size of the encoded representation (for buffer pre-allocation).
    fn encoded_size(&self) -> usize;
}

/// Extension trait for types that have both a schema header and encodable fields.
///
/// This is used for signed protocol messages like [`LooseCommit`] and [`Fragment`]
/// that need a schema prefix for type identification.
///
/// [`LooseCommit`]: crate::loose_commit::LooseCommit
/// [`Fragment`]: crate::fragment::Fragment
pub trait EncodeFields: Schema {
    /// Encode type-specific fields to the buffer.
    ///
    /// This is called after the schema and issuer have been written.
    /// The implementation should append its fields to `buf`.
    fn encode_fields(&self, buf: &mut Vec<u8>);

    /// Size of the encoded fields (for buffer pre-allocation).
    fn fields_size(&self) -> usize;

    /// Total size of the signed message.
    ///
    /// This is `4 (schema) + 32 (issuer) + fields_size + 64 (signature)`.
    fn signed_size(&self) -> usize {
        4 + 32 + self.fields_size() + 64
    }
}

/// Blanket impl: anything with [`Schema`] + [`EncodeFields`] gets [`Encode`] for free.
impl<T: Schema + EncodeFields> Encode for T {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + self.fields_size());
        buf.extend_from_slice(&Self::SCHEMA);
        self.encode_fields(&mut buf);
        buf
    }

    fn encoded_size(&self) -> usize {
        4 + self.fields_size()
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
