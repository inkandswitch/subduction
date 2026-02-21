//! Type identity for signed payloads.

/// Type identity for signed payloads.
///
/// This trait provides the 4-byte schema header that identifies a type
/// in the wire format. It's shared between encoding and decoding.
pub trait Schema {
    /// Context required for encoding/decoding.
    ///
    /// Use `()` for types that don't need additional context.
    /// Some types (e.g., `LooseCommit`) require a `SedimentreeId` to bind
    /// the signature to a specific document.
    type Context;

    /// 4-byte schema header identifying the type and version.
    ///
    /// Format: `[prefix0, prefix1, type_byte, version_byte]`
    ///
    /// - `ST` prefix for sedimentree types
    /// - `SU` prefix for subduction types
    const SCHEMA: [u8; 4];
}
