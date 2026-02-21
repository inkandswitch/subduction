//! Canonical binary codec for signed payloads.
//!
//! This module provides the [`Codec`] trait and supporting types for encoding
//! and decoding types in a deterministic binary format suitable for signing,
//! network transmission, and persistent storage.
//!
//! # Format Overview
//!
//! All signed types share a common structure:
//!
//! ```text
//! ┌─────────────────────── Payload ───────────────────────┬─ Seal ─┐
//! ╔════════╦══════════╦═══════════════════════════════════╦════════╗
//! ║ Schema ║ IssuerVK ║         Type-Specific Fields      ║  Sig   ║
//! ║   4B   ║   32B    ║            (variable)             ║  64B   ║
//! ╚════════╩══════════╩═══════════════════════════════════╩════════╝
//! ```
//!
//! - **Schema**: 4-byte header identifying type and version (e.g., `STC\x00`)
//! - **IssuerVK**: Ed25519 verifying key of the signer (32 bytes)
//! - **Fields**: Type-specific data encoded by the [`Codec`] implementation
//! - **Signature**: Ed25519 signature over the payload (64 bytes)
//!
//! # Encoding Conventions
//!
//! - All integers are **big-endian** (network byte order)
//! - All arrays are **sorted ascending** by byte order
//! - Sizes use fixed-width integers (not variable-length encoding)
//!
//! # Schema Headers
//!
//! | Prefix | Crate | Types |
//! |--------|-------|-------|
//! | `ST` | `sedimentree_core` | `LooseCommit`, `Fragment` |
//! | `SU` | `subduction_core` | `Challenge`, `Response`, `Message` |

pub mod decode;
pub mod encode;

use alloc::vec::Vec;

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

/// A type with a canonical binary codec for signing and serialization.
///
/// Types implementing this trait can be wrapped in `Signed<T>` for
/// cryptographic signing and verification.
///
/// # Layout
///
/// The full signed format is:
///
/// ```text
/// Schema (4B) ++ IssuerVK (32B) ++ Fields (variable) ++ Signature (64B)
/// ```
///
/// The `Codec` trait handles encoding/decoding the _Fields_ portion.
/// The schema, issuer, and signature are handled by `Signed<T>`.
///
/// # Context
///
/// Some types require additional context for encoding (e.g., `LooseCommit`
/// needs a `SedimentreeId` to bind the signature to a document). The
/// [`Context`](Self::Context) associated type captures this requirement.
pub trait Codec: Sized {
    /// Context required for encoding/decoding.
    ///
    /// Use `()` for types that don't need additional context.
    type Context;

    /// 4-byte schema header identifying the type and version.
    ///
    /// Format: `[prefix0, prefix1, type_byte, version_byte]`
    ///
    /// - `ST` prefix for sedimentree types
    /// - `SU` prefix for subduction types
    const SCHEMA: [u8; 4];

    /// Minimum valid encoded size (for early rejection).
    ///
    /// This is the size of the full signed message (schema + issuer + fields + signature).
    const MIN_SIZE: usize;

    /// Encode type-specific fields to the buffer.
    ///
    /// This is called after the schema and issuer have been written.
    /// The implementation should append its fields to `buf`.
    fn encode_fields(&self, ctx: &Self::Context, buf: &mut Vec<u8>);

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
    fn decode_fields(buf: &[u8], ctx: &Self::Context) -> Result<Self, CodecError>;

    /// Size of the encoded fields (for buffer pre-allocation).
    fn fields_size(&self, ctx: &Self::Context) -> usize;

    /// Total size of the signed message.
    ///
    /// This is `4 (schema) + 32 (issuer) + fields_size + 64 (signature)`.
    fn signed_size(&self, ctx: &Self::Context) -> usize {
        4 + 32 + self.fields_size(ctx) + 64
    }
}
