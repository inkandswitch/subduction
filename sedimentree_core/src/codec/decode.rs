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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_sorted_empty() {
        let elements: &[[u8; 32]] = &[];
        assert!(verify_sorted(elements).is_ok());
    }

    #[test]
    fn verify_sorted_single() {
        let elements: &[[u8; 32]] = &[[0x42; 32]];
        assert!(verify_sorted(elements).is_ok());
    }

    #[test]
    fn verify_sorted_ascending() {
        let elements: &[[u8; 32]] = &[[0x10; 32], [0x20; 32], [0x30; 32]];
        assert!(verify_sorted(elements).is_ok());
    }

    #[test]
    fn verify_sorted_descending_rejected() {
        let elements: &[[u8; 32]] = &[[0x30; 32], [0x20; 32], [0x10; 32]];
        assert!(matches!(
            verify_sorted(elements),
            Err(UnsortedArray { index: 1 })
        ));
    }

    #[test]
    fn verify_sorted_duplicates_rejected() {
        let elements: &[[u8; 32]] = &[[0x10; 32], [0x20; 32], [0x20; 32]];
        assert!(matches!(
            verify_sorted(elements),
            Err(UnsortedArray { index: 2 })
        ));
    }
}

#[cfg(all(test, feature = "bolero"))]
mod proptests {
    use super::*;
    use crate::codec::encode;
    use alloc::vec::Vec;

    /// Fuzz u8 decoder - should never panic on any input.
    #[test]
    fn u8_does_not_panic() {
        bolero::check!()
            .with_arbitrary::<(Vec<u8>, usize)>()
            .for_each(|(bytes, offset)| {
                let _ = u8(bytes, *offset);
            });
    }

    /// Fuzz u16 decoder - should never panic on any input.
    #[test]
    fn u16_does_not_panic() {
        bolero::check!()
            .with_arbitrary::<(Vec<u8>, usize)>()
            .for_each(|(bytes, offset)| {
                let _ = u16(bytes, *offset);
            });
    }

    /// Fuzz u32 decoder - should never panic on any input.
    #[test]
    fn u32_does_not_panic() {
        bolero::check!()
            .with_arbitrary::<(Vec<u8>, usize)>()
            .for_each(|(bytes, offset)| {
                let _ = u32(bytes, *offset);
            });
    }

    /// Fuzz u64 decoder - should never panic on any input.
    #[test]
    fn u64_does_not_panic() {
        bolero::check!()
            .with_arbitrary::<(Vec<u8>, usize)>()
            .for_each(|(bytes, offset)| {
                let _ = u64(bytes, *offset);
            });
    }

    /// Fuzz array decoder - should never panic on any input.
    #[test]
    fn array_does_not_panic() {
        bolero::check!()
            .with_arbitrary::<(Vec<u8>, usize)>()
            .for_each(|(bytes, offset)| {
                let _ = array::<32>(bytes, *offset);
            });
    }

    /// Fuzz slice decoder - should never panic on any input.
    #[test]
    fn slice_does_not_panic() {
        bolero::check!()
            .with_arbitrary::<(Vec<u8>, usize, usize)>()
            .for_each(|(bytes, offset, len)| {
                let _ = slice(bytes, *offset, *len);
            });
    }

    /// Round-trip: encode then decode u8.
    #[test]
    fn u8_round_trip() {
        bolero::check!()
            .with_arbitrary::<core::primitive::u8>()
            .for_each(|value| {
                let mut buf = Vec::new();
                encode::u8(*value, &mut buf);
                let decoded = u8(&buf, 0).expect("decode should succeed");
                assert_eq!(decoded, *value);
            });
    }

    /// Round-trip: encode then decode u16.
    #[test]
    fn u16_round_trip() {
        bolero::check!()
            .with_arbitrary::<core::primitive::u16>()
            .for_each(|value| {
                let mut buf = Vec::new();
                encode::u16(*value, &mut buf);
                let decoded = u16(&buf, 0).expect("decode should succeed");
                assert_eq!(decoded, *value);
            });
    }

    /// Round-trip: encode then decode u32.
    #[test]
    fn u32_round_trip() {
        bolero::check!()
            .with_arbitrary::<core::primitive::u32>()
            .for_each(|value| {
                let mut buf = Vec::new();
                encode::u32(*value, &mut buf);
                let decoded = u32(&buf, 0).expect("decode should succeed");
                assert_eq!(decoded, *value);
            });
    }

    /// Round-trip: encode then decode u64.
    #[test]
    fn u64_round_trip() {
        bolero::check!()
            .with_arbitrary::<core::primitive::u64>()
            .for_each(|value| {
                let mut buf = Vec::new();
                encode::u64(*value, &mut buf);
                let decoded = u64(&buf, 0).expect("decode should succeed");
                assert_eq!(decoded, *value);
            });
    }

    /// Round-trip: encode then decode array.
    #[test]
    fn array_round_trip() {
        bolero::check!()
            .with_arbitrary::<[u8; 32]>()
            .for_each(|value| {
                let mut buf = Vec::new();
                encode::array(value, &mut buf);
                let decoded: [u8; 32] = array(&buf, 0).expect("decode should succeed");
                assert_eq!(&decoded, value);
            });
    }

    /// verify_sorted accepts any sorted array.
    #[test]
    fn verify_sorted_accepts_sorted() {
        bolero::check!()
            .with_arbitrary::<Vec<[u8; 32]>>()
            .for_each(|elements| {
                let mut sorted = elements.clone();
                sorted.sort();
                sorted.dedup();
                assert!(verify_sorted(&sorted).is_ok());
            });
    }
}
