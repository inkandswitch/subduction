//! Encoding trait for signed payloads.

use alloc::vec::Vec;

use super::Schema;

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
