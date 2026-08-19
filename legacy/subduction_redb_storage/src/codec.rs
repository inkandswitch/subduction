//! The tagged compound value stored in each B+tree slot.
//!
//! ```text
//! 0x00 ++ meta_len:u32be ++ meta ++ blob    (inline)
//! 0x01 ++ meta                              (external)
//! ```
//!
//! Inline values carry the blob bytes after the `Signed<T>` meta. External
//! values store *only* the meta: the blob lives in a content-addressed file
//! named by the digest inside the meta's own `BlobMeta`, so the value never
//! duplicates the digest.

/// Value tag: blob bytes stored inline after the meta.
const TAG_INLINE: u8 = 0x00;

/// Value tag: blob stored externally; the value holds only the meta.
const TAG_EXTERNAL: u8 = 0x01;

/// A decoded compound value, before external blob resolution.
pub(crate) enum DecodedCompound {
    /// Blob bytes were stored inline.
    Inline {
        /// `Signed<T>` wire bytes.
        meta: Vec<u8>,
        /// Blob contents.
        blob: Vec<u8>,
    },

    /// Blob lives in an external content-addressed file, named by the
    /// digest inside the meta's own `BlobMeta` (no duplication).
    External {
        /// `Signed<T>` wire bytes.
        meta: Vec<u8>,
    },
}

/// Encode an inline compound value: `0x00 ++ meta_len:u32be ++ meta ++ blob`.
pub(crate) fn encode_inline(meta: &[u8], blob: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + meta.len() + blob.len());
    out.push(TAG_INLINE);
    #[allow(clippy::cast_possible_truncation)]
    out.extend_from_slice(&(meta.len() as u32).to_be_bytes());
    out.extend_from_slice(meta);
    out.extend_from_slice(blob);
    out
}

/// Encode an external compound value: `0x01 ++ meta`.
///
/// No length prefix and no blob reference: the meta is the remainder of
/// the value, and the blob's digest/size are already inside the signed
/// payload's `BlobMeta`.
pub(crate) fn encode_external(meta: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + meta.len());
    out.push(TAG_EXTERNAL);
    out.extend_from_slice(meta);
    out
}

/// Decode a compound value. `None` on a malformed buffer.
pub(crate) fn decode_compound(bytes: &[u8]) -> Option<DecodedCompound> {
    let tag = *bytes.first()?;

    match tag {
        TAG_INLINE => {
            let len_bytes: [u8; 4] = bytes.get(1..5)?.try_into().ok()?;
            let meta_len = u32::from_be_bytes(len_bytes) as usize;
            Some(DecodedCompound::Inline {
                meta: bytes.get(5..5 + meta_len)?.to_vec(),
                blob: bytes.get(5 + meta_len..)?.to_vec(),
            })
        }
        TAG_EXTERNAL => Some(DecodedCompound::External {
            meta: bytes.get(1..)?.to_vec(),
        }),
        _ => None,
    }
}

/// Whether a compound value stores its blob in an external file (`true`) or
/// inline (`false`). `None` on an empty/unknown-tag buffer. Cheaper than
/// [`split_meta`] when only the tag is needed (no copy).
pub(crate) fn is_external(bytes: &[u8]) -> Option<bool> {
    match *bytes.first()? {
        TAG_INLINE => Some(false),
        TAG_EXTERNAL => Some(true),
        _ => None,
    }
}

/// Copy out just the `meta` (`Signed<T>` wire bytes) of a compound value,
/// ignoring any inline blob, and report whether the blob is stored
/// externally (`true`) or inline (`false`). `None` on a malformed buffer.
pub(crate) fn split_meta(bytes: &[u8]) -> Option<(bool, Vec<u8>)> {
    let tag = *bytes.first()?;
    match tag {
        TAG_INLINE => {
            let len_bytes: [u8; 4] = bytes.get(1..5)?.try_into().ok()?;
            let meta_len = u32::from_be_bytes(len_bytes) as usize;
            bytes.get(5..5 + meta_len).map(|m| (false, m.to_vec()))
        }
        TAG_EXTERNAL => bytes.get(1..).map(|m| (true, m.to_vec())),
        _ => None,
    }
}
