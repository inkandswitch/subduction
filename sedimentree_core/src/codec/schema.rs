//! Type identity for signed payloads.

/// Prefix for sedimentree types.
pub const SEDIMENTREE_PREFIX: [u8; 2] = *b"ST";

/// Prefix for subduction types.
pub const SUBDUCTION_PREFIX: [u8; 2] = *b"SU";

/// Type identity for signed payloads.
///
/// This trait provides the schema header that identifies a type
/// in the wire format. It's shared between encoding and decoding.
pub trait Schema {
    /// Context required for encoding/decoding.
    ///
    /// Use `()` for types that don't need additional context.
    /// Some types (e.g., `LooseCommit`) require a `SedimentreeId` to bind
    /// the signature to a specific document.
    type Context;

    /// 2-byte crate prefix (e.g., [`SEDIMENTREE_PREFIX`] or [`SUBDUCTION_PREFIX`]).
    const PREFIX: [u8; 2];

    /// Type identifier byte within the crate namespace.
    const TYPE_BYTE: u8;

    /// Schema version byte for forward compatibility.
    const VERSION: u8;

    /// Combined 4-byte schema header: `[prefix0, prefix1, type_byte, version]`.
    const SCHEMA: [u8; 4] = [
        Self::PREFIX[0],
        Self::PREFIX[1],
        Self::TYPE_BYTE,
        Self::VERSION,
    ];
}
