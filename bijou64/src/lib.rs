//! Bijective variable-length encoding for unsigned 64-bit integers.
//!
//! bijou64 (**BIJ**ective **O**ffset **U64**) encodes `u64` values into 1–9
//! bytes using a tag-byte prefix scheme derived from [VARU64], modified with
//! per-tier offsets to achieve **structural canonicality** — each value has
//! exactly one encoding, and each encoding has exactly one value. This is
//! [bijective numeration] applied to VARU64's tag-byte framing.
//!
//! See the [specification](https://github.com/inkandswitch/subduction/blob/main/bijou64/SPEC.md)
//! for the full format definition, design rationale, and test vectors.
//!
//! # Encoding
//!
//! The first byte determines the encoding:
//!
//! - `0x00..=0xF7` (0–247): the byte _is_ the value. One byte total.
//! - `0xF8..=0xFF` (248–255): length tag. Additional bytes = `tag - 247`.
//!   Payload is big-endian `value - OFFSET[tier]`.
//!
//! The offset for each tier is the first value not representable by the
//! previous tier, making all tier ranges disjoint by construction:
//!
//! ```text
//! ┌───────────┬──────────────────┬──────────────────────────────────────────────────┐
//! │ Tag       │ Additional bytes │ Value range                                      │
//! ├───────────┼──────────────────┼──────────────────────────────────────────────────┤
//! │ 0x00-0xF7 │ 0                │ 0 -- 247                                         │
//! │ 0xF8      │ 1                │ 248 -- 503                                       │
//! │ 0xF9      │ 2                │ 504 -- 66,039                                    │
//! │ 0xFA      │ 3                │ 66,040 -- 16,843,255                             │
//! │ 0xFB      │ 4                │ 16,843,256 -- 4,311,810,551                      │
//! │ 0xFC      │ 5                │ 4,311,810,552 -- 1,103,823,438,327               │
//! │ 0xFD      │ 6                │ 1,103,823,438,328 -- 282,578,800,148,983         │
//! │ 0xFE      │ 7                │ 282,578,800,148,984 -- 72,340,172,838,076,919    │
//! │ 0xFF      │ 8                │ 72,340,172,838,076,920 -- u64::MAX               │
//! └───────────┴──────────────────┴──────────────────────────────────────────────────┘
//! ```
//!
//! # Canonicality
//!
//! Unlike [VARU64], which requires a runtime check to reject overlong
//! encodings, bijou64 achieves canonicality structurally: each tier's value
//! range is disjoint, so no byte sequence can decode to a value representable
//! in a shorter form. The only decoder error conditions are buffer underflow
//! and arithmetic overflow on tier 8.
//!
//! # Examples
//!
//! ```
//! let mut buf = Vec::new();
//! bijou64::encode(300, &mut buf);
//! assert_eq!(buf, [0xF8, 0x34]); // tag 248, payload 300 - 248 = 52
//!
//! let (value, len) = bijou64::decode(&buf).unwrap();
//! assert_eq!(value, 300);
//! assert_eq!(len, 2);
//! ```
//!
//! [VARU64]: https://github.com/AljoschaMeyer/varu64-rs
//! [bijective numeration]: https://en.wikipedia.org/wiki/Bijective_numeration

#![no_std]
#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

extern crate alloc;

#[allow(unused_imports)] // vec! macro used in tests
use alloc::{vec, vec::Vec};

/// Maximum number of bytes a `bijou64` encoding can occupy.
pub const MAX_BYTES: usize = 9;

/// Tag byte threshold: values below this are encoded as a single byte.
const TAG_THRESHOLD: u8 = 248;

/// Number of multi-byte tiers (tags 248–255).
const NUM_TIERS: usize = 8;

/// Computes the tier offset for tier `n`.
///
/// Each tier's offset is the first value not representable by the previous
/// tier. Recurrence: `offset(n) = offset(n-1) + 256^(n-1)` for `n >= 2`,
/// with `offset(1) = 248` and `offset(0) = 0`.
const fn tier_offset(n: usize) -> u64 {
    if n == 0 {
        return 0;
    }
    if n == 1 {
        return TAG_THRESHOLD as u64;
    }

    let mut result = TAG_THRESHOLD as u64;
    let mut power = 1u64; // 256^0
    let mut i = 2;
    while i <= n {
        power = power.saturating_mul(256);
        result = result.saturating_add(power);
        i += 1;
    }
    result
}

/// Per-tier offsets.
///
/// `OFFSETS[t]` is the first value that requires tier `t` (1-indexed).
/// Index 0 is unused (tier 0 values are encoded as the tag byte itself).
const OFFSETS: [u64; NUM_TIERS + 1] = [
    tier_offset(0),
    tier_offset(1),
    tier_offset(2),
    tier_offset(3),
    tier_offset(4),
    tier_offset(5),
    tier_offset(6),
    tier_offset(7),
    tier_offset(8),
];

/// Per-tier upper bounds (exclusive).
///
/// A value belongs to tier `t` if `OFFSETS[t] <= value < BOUNDS[t]`.
/// `BOUNDS[t] == OFFSETS[t + 1]` for tiers 1–7. Tier 8 extends to
/// `u64::MAX` (the decoder handles overflow via `checked_add`).
const BOUNDS: [u64; NUM_TIERS + 1] = [
    tier_offset(1), // tier 0 upper bound = tier 1 offset
    tier_offset(2),
    tier_offset(3),
    tier_offset(4),
    tier_offset(5),
    tier_offset(6),
    tier_offset(7),
    tier_offset(8),
    u64::MAX, // tier 8 extends to u64::MAX
];

/// Returns the encoded length of `value` in bytes (1–9).
///
/// # Examples
///
/// ```
/// assert_eq!(bijou64::encoded_len(0), 1);
/// assert_eq!(bijou64::encoded_len(247), 1);
/// assert_eq!(bijou64::encoded_len(248), 2);
/// assert_eq!(bijou64::encoded_len(503), 2);
/// assert_eq!(bijou64::encoded_len(504), 3);
/// assert_eq!(bijou64::encoded_len(u64::MAX), 9);
/// ```
#[must_use]
pub const fn encoded_len(value: u64) -> usize {
    if value < BOUNDS[0] {
        1
    } else if value < BOUNDS[1] {
        2
    } else if value < BOUNDS[2] {
        3
    } else if value < BOUNDS[3] {
        4
    } else if value < BOUNDS[4] {
        5
    } else if value < BOUNDS[5] {
        6
    } else if value < BOUNDS[6] {
        7
    } else if value < BOUNDS[7] {
        8
    } else {
        9
    }
}

/// Encodes `value` as a `bijou64`, appending bytes to `buf`.
///
/// # Panics
///
/// Cannot panic in practice: [`encode_array`]'s exhaustive `if`/`else`
/// chain guarantees the returned length is in `1..=MAX_BYTES`.
///
/// # Examples
///
/// ```
/// let mut buf = Vec::new();
/// bijou64::encode(42, &mut buf);
/// assert_eq!(buf, [0x2A]);
///
/// buf.clear();
/// bijou64::encode(248, &mut buf);
/// assert_eq!(buf, [0xF8, 0x00]);
/// ```
#[allow(clippy::expect_used)] // Invariant: encode_array's exhaustive if/else guarantees len ∈ 1..=MAX_BYTES
pub fn encode(value: u64, buf: &mut Vec<u8>) {
    let (arr, len) = encode_array(value);
    buf.extend_from_slice(
        arr.get(..len)
            .expect("encode_array returned out-of-range len"),
    );
}

/// Encodes `value` as a `bijou64` into a fixed-size array.
///
/// Returns `(bytes, len)` where `bytes` is a 9-byte array with the
/// encoding in `bytes[..len]`.
///
/// # Examples
///
/// ```
/// let (bytes, len) = bijou64::encode_array(300);
/// assert_eq!(&bytes[..len], &[0xF8, 0x34]);
/// ```
#[must_use]
pub const fn encode_array(value: u64) -> ([u8; MAX_BYTES], usize) {
    if value < BOUNDS[0] {
        // Tier 0: single byte is the value. Mask is a no-op (value < 248)
        // but satisfies clippy::cast_possible_truncation without an allow.
        return ([(value & 0xFF) as u8, 0, 0, 0, 0, 0, 0, 0, 0], 1);
    }

    // For multi-byte tiers, compute tag + big-endian (value - offset).
    // Fully unrolled: each arm uses only literal indices.
    if value < BOUNDS[1] {
        let be = (value - OFFSETS[1]).to_be_bytes();
        ([0xF8, be[7], 0, 0, 0, 0, 0, 0, 0], 2)
    } else if value < BOUNDS[2] {
        let be = (value - OFFSETS[2]).to_be_bytes();
        ([0xF9, be[6], be[7], 0, 0, 0, 0, 0, 0], 3)
    } else if value < BOUNDS[3] {
        let be = (value - OFFSETS[3]).to_be_bytes();
        ([0xFA, be[5], be[6], be[7], 0, 0, 0, 0, 0], 4)
    } else if value < BOUNDS[4] {
        let be = (value - OFFSETS[4]).to_be_bytes();
        ([0xFB, be[4], be[5], be[6], be[7], 0, 0, 0, 0], 5)
    } else if value < BOUNDS[5] {
        let be = (value - OFFSETS[5]).to_be_bytes();
        ([0xFC, be[3], be[4], be[5], be[6], be[7], 0, 0, 0], 6)
    } else if value < BOUNDS[6] {
        let be = (value - OFFSETS[6]).to_be_bytes();
        ([0xFD, be[2], be[3], be[4], be[5], be[6], be[7], 0, 0], 7)
    } else if value < BOUNDS[7] {
        let be = (value - OFFSETS[7]).to_be_bytes();
        (
            [0xFE, be[1], be[2], be[3], be[4], be[5], be[6], be[7], 0],
            8,
        )
    } else {
        let be = (value - OFFSETS[8]).to_be_bytes();
        (
            [0xFF, be[0], be[1], be[2], be[3], be[4], be[5], be[6], be[7]],
            9,
        )
    }
}

/// Decodes a `bijou64` from the front of `buf`.
///
/// Returns `(value, bytes_consumed)` on success.
///
/// # Errors
///
/// - [`DecodeError::BufferTooShort`] if `buf` has fewer bytes than the
///   encoding requires.
/// - [`DecodeError::Overflow`] if a tier 8 payload, when added to
///   `OFFSETS[8]`, exceeds `u64::MAX`.
///
/// # Examples
///
/// ```
/// // Single-byte value
/// let (v, n) = bijou64::decode(&[0x2A]).unwrap();
/// assert_eq!((v, n), (42, 1));
///
/// // Multi-byte value with trailing data
/// let (v, n) = bijou64::decode(&[0xF8, 0x34, 0xFF]).unwrap();
/// assert_eq!((v, n), (300, 2));
/// ```
#[allow(clippy::many_single_char_names)] // byte destructuring in slice patterns
pub const fn decode(buf: &[u8]) -> Result<(u64, usize), DecodeError> {
    let Some((&tag, rest)) = buf.split_first() else {
        return Err(DecodeError::BufferTooShort);
    };

    if tag < TAG_THRESHOLD {
        return Ok((tag as u64, 1));
    }

    // Read big-endian payload and add tier offset. Slice-pattern matching
    // proves to the compiler that enough bytes exist in each arm, and
    // `u64::from_be_bytes` reconstructs the payload without manual shifts.
    let (offset, payload, consumed) = match tag {
        0xF8 => match rest {
            &[a, ..] => (OFFSETS[1], u64::from_be_bytes([0, 0, 0, 0, 0, 0, 0, a]), 2),
            _ => return Err(DecodeError::BufferTooShort),
        },
        0xF9 => match rest {
            &[a, b, ..] => (OFFSETS[2], u64::from_be_bytes([0, 0, 0, 0, 0, 0, a, b]), 3),
            _ => return Err(DecodeError::BufferTooShort),
        },
        0xFA => match rest {
            &[a, b, c, ..] => (OFFSETS[3], u64::from_be_bytes([0, 0, 0, 0, 0, a, b, c]), 4),
            _ => return Err(DecodeError::BufferTooShort),
        },
        0xFB => match rest {
            &[a, b, c, d, ..] => (OFFSETS[4], u64::from_be_bytes([0, 0, 0, 0, a, b, c, d]), 5),
            _ => return Err(DecodeError::BufferTooShort),
        },
        0xFC => match rest {
            &[a, b, c, d, e, ..] => (OFFSETS[5], u64::from_be_bytes([0, 0, 0, a, b, c, d, e]), 6),
            _ => return Err(DecodeError::BufferTooShort),
        },
        0xFD => match rest {
            &[a, b, c, d, e, f, ..] => {
                (OFFSETS[6], u64::from_be_bytes([0, 0, a, b, c, d, e, f]), 7)
            }
            _ => return Err(DecodeError::BufferTooShort),
        },
        0xFE => match rest {
            &[a, b, c, d, e, f, g, ..] => {
                (OFFSETS[7], u64::from_be_bytes([0, a, b, c, d, e, f, g]), 8)
            }
            _ => return Err(DecodeError::BufferTooShort),
        },
        // 0xFF — only remaining value since tag >= TAG_THRESHOLD (248)
        _ => match rest {
            &[a, b, c, d, e, f, g, h, ..] => {
                (OFFSETS[8], u64::from_be_bytes([a, b, c, d, e, f, g, h]), 9)
            }
            _ => return Err(DecodeError::BufferTooShort),
        },
    };

    match offset.checked_add(payload) {
        Some(value) => Ok((value, consumed)),
        None => Err(DecodeError::Overflow),
    }
}

/// Errors that can occur when decoding a `bijou64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The input buffer is shorter than the encoding requires.
    #[error("buffer too short for bijou64 encoding")]
    BufferTooShort,

    /// The decoded value exceeds `u64::MAX` (tier 8 only).
    #[error("bijou64 tier 8 payload overflows u64")]
    Overflow,
}

#[cfg(test)]
#[allow(clippy::indexing_slicing, clippy::needless_range_loop, clippy::panic)]
mod tests {
    use super::*;

    type TestResult = Result<(), DecodeError>;

    mod offset_table {
        use super::*;

        #[test]
        fn recurrence() {
            assert_eq!(OFFSETS[0], 0);
            assert_eq!(OFFSETS[1], 248);

            let mut power = 1u64; // 256^0
            for i in 2..=NUM_TIERS {
                power *= 256;
                assert_eq!(
                    OFFSETS[i],
                    OFFSETS[i - 1] + power,
                    "OFFSETS[{i}] does not satisfy recurrence"
                );
            }
        }

        #[test]
        fn known_values() {
            assert_eq!(OFFSETS[1], 248);
            assert_eq!(OFFSETS[2], 504);
            assert_eq!(OFFSETS[3], 0x1_01F8);
            assert_eq!(OFFSETS[4], 0x101_01F8);
            assert_eq!(OFFSETS[5], 0x1_0101_01F8);
            assert_eq!(OFFSETS[6], 0x101_0101_01F8);
            assert_eq!(OFFSETS[7], 0x1_0101_0101_01F8);
            assert_eq!(OFFSETS[8], 0x101_0101_0101_01F8);
        }

        #[test]
        fn bounds_are_consistent() {
            assert_eq!(BOUNDS[0], 248);
            for i in 1..NUM_TIERS {
                assert_eq!(
                    BOUNDS[i],
                    OFFSETS[i + 1],
                    "BOUNDS[{i}] should equal OFFSETS[{}]",
                    i + 1
                );
            }
            assert_eq!(BOUNDS[NUM_TIERS], u64::MAX);
        }
    }

    mod round_trip {
        use super::*;

        /// Every tier boundary (min and max) round-trips at the correct length.
        #[test]
        fn all_tier_boundaries() -> TestResult {
            for tier in 1..=NUM_TIERS {
                let min_val = OFFSETS[tier];
                let max_val = if tier < NUM_TIERS {
                    OFFSETS[tier + 1] - 1
                } else {
                    u64::MAX
                };
                let expected_len = 1 + tier;

                for &value in &[min_val, max_val] {
                    let mut buf = Vec::new();
                    encode(value, &mut buf);
                    assert_eq!(
                        buf.len(),
                        expected_len,
                        "tier {tier} value {value}: expected {expected_len} bytes, got {}",
                        buf.len()
                    );

                    let (decoded, consumed) = decode(&buf)?;
                    assert_eq!(decoded, value, "tier {tier} round-trip failed for {value}");
                    assert_eq!(consumed, expected_len);
                }
            }
            Ok(())
        }
    }

    mod errors {
        use super::*;

        #[test]
        fn empty_buffer() {
            assert_eq!(decode(&[]), Err(DecodeError::BufferTooShort));
        }

        #[test]
        fn truncated_at_every_tier() {
            // For each multi-byte tier, provide the tag byte plus one fewer
            // payload byte than required.
            for tier in 1..=NUM_TIERS {
                let tag = u8::try_from(247 + tier).unwrap_or(0xFF);
                let mut buf = vec![tag];
                // tier needs `tier` payload bytes; provide `tier - 1`
                buf.extend(core::iter::repeat_n(0x00u8, tier - 1));

                assert_eq!(
                    decode(&buf),
                    Err(DecodeError::BufferTooShort),
                    "tier {tier} (tag 0x{tag:02X}) with {}-byte payload \
                     should be BufferTooShort",
                    tier - 1
                );
            }
        }

        #[test]
        fn tier8_overflow() {
            // Tag 0xFF = tier 8, payload = all 0xFF = u64::MAX
            // OFFSETS[8] + u64::MAX overflows
            let buf = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
            assert_eq!(decode(&buf), Err(DecodeError::Overflow));
        }

        #[test]
        fn tier8_overflow_exact_boundary() -> TestResult {
            // Smallest tier 8 payload that overflows:
            // OFFSETS[8] = 0x101_0101_0101_01F8
            // Max valid payload = u64::MAX - OFFSETS[8] = 0xFEFE_FEFE_FEFE_FE07
            // One above that overflows.
            let max_payload = u64::MAX - OFFSETS[8];
            let overflow_payload = max_payload + 1; // = 0xFEFE_FEFE_FEFE_FE08
            let be = overflow_payload.to_be_bytes();
            let buf = [0xFF, be[0], be[1], be[2], be[3], be[4], be[5], be[6], be[7]];
            assert_eq!(decode(&buf), Err(DecodeError::Overflow));

            // One below: the max valid payload should decode to u64::MAX
            let be_max = max_payload.to_be_bytes();
            let buf_max = [
                0xFF, be_max[0], be_max[1], be_max[2], be_max[3], be_max[4], be_max[5], be_max[6],
                be_max[7],
            ];
            let (value, consumed) = decode(&buf_max)?;
            assert_eq!(value, u64::MAX);
            assert_eq!(consumed, 9);
            Ok(())
        }

        #[test]
        fn trailing_bytes_not_consumed() -> TestResult {
            let (v, n) = decode(&[0x2A, 0xDE, 0xAD])?;
            assert_eq!((v, n), (42, 1));

            let (v, n) = decode(&[0xF8, 0x34, 0xBE, 0xEF])?;
            assert_eq!((v, n), (300, 2));
            Ok(())
        }
    }

    mod exhaustive {
        use super::*;

        #[test]
        fn tier0() -> TestResult {
            for value in 0..248u64 {
                let mut buf = Vec::new();
                encode(value, &mut buf);
                assert_eq!(buf.len(), 1);
                assert_eq!(buf[0], u8::try_from(value).unwrap_or(0));

                let (decoded, consumed) = decode(&buf)?;
                assert_eq!(decoded, value);
                assert_eq!(consumed, 1);
            }
            Ok(())
        }

        #[test]
        fn tier1() -> TestResult {
            for value in 248..504u64 {
                let mut buf = Vec::new();
                encode(value, &mut buf);
                assert_eq!(buf.len(), 2, "value {value} should encode in 2 bytes");
                assert_eq!(buf[0], 0xF8);

                let (decoded, consumed) = decode(&buf)?;
                assert_eq!(decoded, value, "round-trip failed for {value}");
                assert_eq!(consumed, 2);
            }
            Ok(())
        }

        #[test]
        fn tier2() -> TestResult {
            for value in 504..66_040u64 {
                let mut buf = Vec::new();
                encode(value, &mut buf);
                assert_eq!(buf.len(), 3, "value {value} should encode in 3 bytes");
                assert_eq!(buf[0], 0xF9);

                let (decoded, consumed) = decode(&buf)?;
                assert_eq!(decoded, value, "round-trip failed for {value}");
                assert_eq!(consumed, 3);
            }
            Ok(())
        }
    }

    mod boundaries {
        use super::*;

        #[test]
        fn offset_triples() -> TestResult {
            // For each tier boundary OFFSET[n], verify that:
            //   OFFSET[n] - 1  encodes in tier n-1 (shorter)
            //   OFFSET[n]      encodes in tier n
            //   OFFSET[n] + 1  encodes in tier n (same length)
            for tier in 1..=NUM_TIERS {
                let offset = OFFSETS[tier];
                let tier_len = 1 + tier;
                let prev_len = if tier == 1 { 1 } else { 1 + (tier - 1) };

                // OFFSET[n] - 1: last value of the previous tier
                let below = offset - 1;
                let mut buf = Vec::new();
                encode(below, &mut buf);
                assert_eq!(
                    buf.len(),
                    prev_len,
                    "OFFSETS[{tier}] - 1 = {below}: expected {prev_len} bytes, got {}",
                    buf.len()
                );
                let (v, n) = decode(&buf)?;
                assert_eq!(v, below);
                assert_eq!(n, prev_len);

                // OFFSET[n]: first value of this tier
                buf.clear();
                encode(offset, &mut buf);
                assert_eq!(
                    buf.len(),
                    tier_len,
                    "OFFSETS[{tier}] = {offset}: expected {tier_len} bytes, got {}",
                    buf.len()
                );
                let (v, n) = decode(&buf)?;
                assert_eq!(v, offset);
                assert_eq!(n, tier_len);

                // OFFSET[n] + 1: second value of this tier
                if offset < u64::MAX {
                    buf.clear();
                    encode(offset + 1, &mut buf);
                    assert_eq!(
                        buf.len(),
                        tier_len,
                        "OFFSETS[{tier}] + 1 = {}: expected {tier_len} bytes, got {}",
                        offset + 1,
                        buf.len()
                    );
                    let (v, n) = decode(&buf)?;
                    assert_eq!(v, offset + 1);
                    assert_eq!(n, tier_len);
                }
            }
            Ok(())
        }

        #[test]
        fn all_zero_payloads() -> TestResult {
            // [tag, 0x00, ..., 0x00] should decode to OFFSETS[tier] for each tier
            for tier in 1..=NUM_TIERS {
                let tag = u8::try_from(247 + tier).unwrap_or(0xFF);
                let mut buf = vec![tag];
                buf.extend(core::iter::repeat_n(0x00u8, tier));

                let (value, consumed) = decode(&buf)?;
                assert_eq!(
                    value, OFFSETS[tier],
                    "tier {tier} all-zeros payload: expected OFFSETS[{tier}] = {}, got {value}",
                    OFFSETS[tier]
                );
                assert_eq!(consumed, 1 + tier);

                // Round-trip: re-encoding should produce the same bytes
                let mut re = Vec::new();
                encode(value, &mut re);
                assert_eq!(re, buf, "tier {tier} all-zeros round-trip mismatch");
            }
            Ok(())
        }

        #[test]
        fn all_ones_payloads() -> TestResult {
            // [tag, 0xFF, ..., 0xFF] should decode to the tier's maximum value
            for tier in 1..=NUM_TIERS {
                let tag = u8::try_from(247 + tier).unwrap_or(0xFF);
                let mut buf = vec![tag];
                buf.extend(core::iter::repeat_n(0xFFu8, tier));

                let result = decode(&buf);

                if tier < NUM_TIERS {
                    // Tiers 1-7: all-ones payload = 256^tier - 1, so
                    // value = OFFSETS[tier] + (256^tier - 1) = OFFSETS[tier+1] - 1
                    let (value, consumed) = result?;
                    let expected = OFFSETS[tier + 1] - 1;
                    assert_eq!(
                        value, expected,
                        "tier {tier} all-ones payload: expected {expected}, got {value}"
                    );
                    assert_eq!(consumed, 1 + tier);

                    // Round-trip
                    let mut re = Vec::new();
                    encode(value, &mut re);
                    assert_eq!(re, buf, "tier {tier} all-ones round-trip mismatch");
                } else {
                    // Tier 8: all-ones payload = u64::MAX, OFFSETS[8] + u64::MAX overflows
                    assert_eq!(
                        result,
                        Err(DecodeError::Overflow),
                        "tier 8 all-ones should overflow"
                    );
                }
            }
            Ok(())
        }
    }

    mod bijectivity {
        use super::*;

        #[test]
        fn overlong_encoding_decodes_to_different_value() -> TestResult {
            // For each tier 1..7, take a value that belongs to that tier and
            // manually encode it in the *next* tier's format (wider tag, same
            // numeric payload without re-adding the offset). Because the
            // decoder adds OFFSETS[tier+1] instead of OFFSETS[tier], the
            // decoded value must differ — the offset shift structurally
            // prevents overlong encodings from round-tripping.
            for tier in 1..NUM_TIERS {
                let value = OFFSETS[tier]; // first value in this tier
                let payload = value - OFFSETS[tier]; // == 0

                // Forge: use next tier's tag with the same payload bytes
                let wider_tier = tier + 1;
                let tag = u8::try_from(247 + wider_tier).unwrap_or(0xFF);
                let mut forged = vec![tag];
                let be = payload.to_be_bytes();
                forged.extend_from_slice(be.get(8 - wider_tier..).unwrap_or(&[]));

                let (decoded, _) = decode(&forged)?;

                // The forged encoding should decode to OFFSETS[wider_tier],
                // not the original value (unless they happen to be equal,
                // which they never are since OFFSETS is strictly increasing).
                assert_ne!(
                    decoded, value,
                    "tier {tier}: overlong encoding of {value} decoded back \
                     to {value} — bijectivity violated"
                );
                assert_eq!(
                    decoded, OFFSETS[wider_tier],
                    "tier {tier}: forged payload 0 in tier {wider_tier} should \
                     decode to OFFSETS[{wider_tier}]"
                );
            }
            Ok(())
        }
    }

    mod streaming {
        use super::*;

        #[test]
        fn consecutive_decode() -> TestResult {
            // Encode two values back-to-back into a single buffer, then
            // decode them sequentially using the returned `consumed` offset.
            let values: &[(u64, u64)] = &[
                (0, 0),
                (42, 300),
                (248, 504),
                (0x101_01F8, u64::MAX),
                (u64::MAX, 0),
            ];

            for &(a, b) in values {
                let mut buf = Vec::new();
                encode(a, &mut buf);
                encode(b, &mut buf);

                let (decoded_a, consumed_a) = decode(&buf)?;
                assert_eq!(decoded_a, a, "first value mismatch for ({a}, {b})");

                let (decoded_b, consumed_b) = decode(&buf[consumed_a..])?;
                assert_eq!(decoded_b, b, "second value mismatch for ({a}, {b})");
                assert_eq!(
                    consumed_a + consumed_b,
                    buf.len(),
                    "total consumed mismatch for ({a}, {b})"
                );
            }
            Ok(())
        }
    }

    mod encode_api {
        use super::*;

        #[test]
        fn appends_to_non_empty_buffer() -> TestResult {
            let mut buf = vec![0xDE, 0xAD];
            encode(300, &mut buf);

            // Original bytes preserved
            assert_eq!(&buf[..2], &[0xDE, 0xAD]);

            // Appended encoding is correct
            let (value, consumed) = decode(&buf[2..])?;
            assert_eq!(value, 300);
            assert_eq!(consumed, 2);
            assert_eq!(buf.len(), 4); // 2 prefix + 2 encoding
            Ok(())
        }
    }

    mod test_vectors {
        use super::*;

        /// Test vectors: `(value, expected_bytes)`.
        ///
        /// These vectors should be replicated in any second implementation
        /// (e.g., TypeScript) to verify encoding compatibility.
        const VECTORS: &[(u64, &[u8])] = &[
            // Tier 0: single byte
            (0, &[0x00]),
            (1, &[0x01]),
            (42, &[0x2A]),
            (97, &[0x61]),
            (127, &[0x7F]),
            (128, &[0x80]),
            (150, &[0x96]),
            (247, &[0xF7]),
            // Tier 1: tag 0xF8 + 1 byte
            (248, &[0xF8, 0x00]),
            (249, &[0xF8, 0x01]),
            (300, &[0xF8, 0x34]),
            (503, &[0xF8, 0xFF]),
            // Tier 2: tag 0xF9 + 2 bytes
            (504, &[0xF9, 0x00, 0x00]),
            (1_000, &[0xF9, 0x01, 0xF0]),
            (65_535, &[0xF9, 0xFE, 0x07]),
            (66_039, &[0xF9, 0xFF, 0xFF]),
            // Tier 3: tag 0xFA + 3 bytes
            (0x1_01F8, &[0xFA, 0x00, 0x00, 0x00]),
            (0x1_05B8, &[0xFA, 0x00, 0x03, 0xC0]),
            (0x101_01F7, &[0xFA, 0xFF, 0xFF, 0xFF]),
            // Tier 4: tag 0xFB + 4 bytes
            (0x101_01F8, &[0xFB, 0x00, 0x00, 0x00, 0x00]),
            (0x1_0101_01F7, &[0xFB, 0xFF, 0xFF, 0xFF, 0xFF]),
            // Tier 5: tag 0xFC + 5 bytes
            (0x1_0101_01F8, &[0xFC, 0x00, 0x00, 0x00, 0x00, 0x00]),
            (0x101_0101_01F7, &[0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
            // Tier 6: tag 0xFD + 6 bytes
            (0x101_0101_01F8, &[0xFD, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            (
                0x1_0101_0101_01F7,
                &[0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
            // Tier 7: tag 0xFE + 7 bytes
            (
                0x1_0101_0101_01F8,
                &[0xFE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                0x101_0101_0101_01F7,
                &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
            // Tier 8: tag 0xFF + 8 bytes
            (
                0x101_0101_0101_01F8,
                &[0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                u64::MAX,
                &[0xFF, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0x07],
            ),
        ];

        #[test]
        fn encode_vectors() {
            for &(value, expected) in VECTORS {
                let mut buf = Vec::new();
                super::encode(value, &mut buf);
                assert_eq!(
                    buf.as_slice(),
                    expected,
                    "encode({value}) mismatch: got {buf:02X?}, expected {expected:02X?}"
                );
            }
        }

        #[test]
        fn decode_vectors() -> TestResult {
            for &(expected_value, bytes) in VECTORS {
                let (value, consumed) = super::decode(bytes)?;
                assert_eq!(
                    value, expected_value,
                    "decode({bytes:02X?}): got {value}, expected {expected_value}"
                );
                assert_eq!(consumed, bytes.len());
            }
            Ok(())
        }
    }

    #[cfg(feature = "bolero")]
    mod property {
        use super::*;

        #[test]
        #[cfg_attr(miri, ignore)]
        fn round_trip() {
            bolero::check!().with_arbitrary::<u64>().for_each(|&value| {
                let mut buf = Vec::new();
                encode(value, &mut buf);
                let (decoded, consumed) = decode(&buf).unwrap_or_else(|e| {
                    panic!("round-trip decode failed for {value}: {e}");
                });
                assert_eq!(decoded, value, "round-trip failed for {value}");
                assert_eq!(consumed, buf.len());
            });
        }

        #[test]
        #[cfg_attr(miri, ignore)]
        fn encoded_len_matches() {
            bolero::check!().with_arbitrary::<u64>().for_each(|&value| {
                let mut buf = Vec::new();
                encode(value, &mut buf);
                assert_eq!(encoded_len(value), buf.len());
            });
        }

        #[test]
        #[cfg_attr(miri, ignore)]
        fn encode_array_matches() {
            bolero::check!().with_arbitrary::<u64>().for_each(|&value| {
                let mut buf = Vec::new();
                encode(value, &mut buf);
                let (arr, len) = encode_array(value);
                assert_eq!(arr.get(..len), Some(buf.as_slice()));
            });
        }

        #[test]
        #[cfg_attr(miri, ignore)]
        fn decode_never_panics() {
            bolero::check!()
                .with_arbitrary::<Vec<u8>>()
                .for_each(|buf| {
                    // Should return Ok or Err, never panic
                    let _ = decode(buf);
                });
        }

        /// `a < b ⟹ encode(a) < encode(b)` lexicographically.
        #[test]
        #[cfg_attr(miri, ignore)]
        fn lexicographic_order() {
            bolero::check!()
                .with_arbitrary::<(u64, u64)>()
                .for_each(|&(a, b)| {
                    let (enc_a, len_a) = encode_array(a);
                    let (enc_b, len_b) = encode_array(b);
                    let slice_a = &enc_a[..len_a];
                    let slice_b = &enc_b[..len_b];
                    assert_eq!(
                        a.cmp(&b),
                        slice_a.cmp(slice_b),
                        "order mismatch: {a} vs {b}, \
                         encoded {slice_a:02X?} vs {slice_b:02X?}",
                    );
                });
        }

        #[test]
        #[cfg_attr(miri, ignore)]
        fn bijective() {
            bolero::check!()
                .with_arbitrary::<Vec<u8>>()
                .for_each(|buf| {
                    if let Ok((value, consumed)) = decode(buf) {
                        let mut re_encoded = Vec::new();
                        encode(value, &mut re_encoded);
                        assert_eq!(
                            re_encoded.as_slice(),
                            buf.get(..consumed).unwrap_or_default(),
                            "bijection violated: decode({:02X?}) = {value}, \
                             re-encode = {:02X?}",
                            buf.get(..consumed).unwrap_or_default(),
                            re_encoded
                        );
                    }
                });
        }
    }
}
