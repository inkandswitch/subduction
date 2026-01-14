//! Helpers for working with hexadecimal strings.

use alloc::vec::Vec;

/// Decode a hexadecimal string into a vector of bytes.
#[must_use]
pub fn decode_hex(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || !bytes.len().is_multiple_of(2) {
        return None;
    }

    bytes
        .chunks_exact(2)
        .map(|c| {
            #[allow(clippy::get_first)] // Clearer together
            let hi = decode_hex_nibble(*c.get(0)?)?;
            let lo = decode_hex_nibble(*c.get(1)?)?;
            Some((hi << 4) | lo)
        })
        .collect()
}

#[inline]
const fn decode_hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
