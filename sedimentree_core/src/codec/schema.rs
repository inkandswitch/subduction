//! Schema identity for payloads.

/// Prefix for sedimentree types.
pub const SEDIMENTREE_PREFIX: [u8; 2] = *b"ST";

/// Prefix for subduction types.
pub const SUBDUCTION_PREFIX: [u8; 2] = *b"SU";

/// Schema identity tag.
///
/// This trait provides the schema header that identifies a type
/// in the wire format. It's shared between encoding and decoding.
///
/// # Discriminant
///
/// Types that share a schema (e.g., multiple signed variants within
/// one protocol) use [`DISCRIMINANT`](Self::DISCRIMINANT) to
/// distinguish themselves. The discriminant is included in the
/// [`Signed<T>`] wire bytes between the schema header and the issuer,
/// making it part of the signed region. This prevents tag-swapping
/// attacks where a valid signature is reused for a different variant.
///
/// The combination `SCHEMA || DISCRIMINANT` must be globally unique
/// across all signed types.
///
/// [`Signed<T>`]: https://docs.rs/subduction_crypto/latest/subduction_crypto/signed/struct.Signed.html
pub trait Schema {
    /// 2-byte protocol prefix (e.g., [`SEDIMENTREE_PREFIX`] or [`SUBDUCTION_PREFIX`]).
    const PREFIX: [u8; 2];

    /// Type identifier byte within the protocol namespace.
    const TYPE_BYTE: u8;

    /// Schema version byte for forward compatibility.
    const VERSION: u8;

    /// Variant discriminant within a shared schema namespace.
    ///
    /// When multiple signed types share the same `SCHEMA` (e.g.,
    /// `Challenge` and `Response` both under `SUH\x00`), each type
    /// sets a distinct discriminant. The discriminant byte appears
    /// on the wire between the schema header and the issuer, and
    /// is part of the signed region.
    ///
    /// Defaults to `None` for types that are the sole signed type
    /// within their schema namespace.
    const DISCRIMINANT: Option<u8> = None;

    /// Combined 4-byte schema header: `[prefix0, prefix1, type_byte, version]`.
    const SCHEMA: [u8; 4] = [
        Self::PREFIX[0],
        Self::PREFIX[1],
        Self::TYPE_BYTE,
        Self::VERSION,
    ];
}
