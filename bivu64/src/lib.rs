//! Bijective variable-length encoding for unsigned 64-bit integers.
//!
//! `bivu64` encodes `u64` values into 1–9 bytes using a tag-byte prefix scheme
//! derived from [VARU64], modified with per-tier offsets to achieve
//! **structural canonicality** — each value has exactly one encoding, and each
//! encoding has exactly one value. This is [bijective numeration] applied to
//! VARU64's tag-byte framing.
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
//! encodings, `bivu64` achieves canonicality structurally: each tier's value
//! range is disjoint, so no byte sequence can decode to a value representable
//! in a shorter form. The only decoder error conditions are buffer underflow
//! and arithmetic overflow on tier 8.
//!
//! # Examples
//!
//! ```
//! let mut buf = Vec::new();
//! bivu64::encode(300, &mut buf);
//! assert_eq!(buf, [0xF8, 0x34]); // tag 248, payload 300 - 248 = 52
//!
//! let (value, len) = bivu64::decode(&buf).unwrap();
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

/// Maximum number of bytes a `bivu64` encoding can occupy.
pub const MAX_BYTES: usize = 9;

/// Tag byte threshold: values below this are encoded as a single byte.
const TAG_THRESHOLD: u8 = 248;

/// Number of multi-byte tiers (tags 248–255).
const NUM_TIERS: usize = 8;

/// Per-tier offsets.
///
/// `OFFSETS[t]` is the first value that requires tier `t` (1-indexed).
/// Recurrence: `OFFSETS[n] = OFFSETS[n-1] + 256^(n-1)` for `n >= 2`,
/// with `OFFSETS[1] = 248`.
///
/// Index 0 is unused (tier 0 values are encoded as the tag byte itself).
#[allow(clippy::indexing_slicing)] // indices bounded by NUM_TIERS; .get() unavailable in const
const OFFSETS: [u64; NUM_TIERS + 1] = {
    let mut table = [0u64; NUM_TIERS + 1];
    table[1] = TAG_THRESHOLD as u64;

    let mut i = 2;
    let mut power = 1u64; // 256^0
    while i <= NUM_TIERS {
        power = power.saturating_mul(256);
        table[i] = table[i - 1].saturating_add(power);
        i += 1;
    }

    table
};

/// Per-tier upper bounds (exclusive).
///
/// A value belongs to tier `t` if `OFFSETS[t] <= value < BOUNDS[t]`.
/// `BOUNDS[t] == OFFSETS[t + 1]` for tiers 1–7. Tier 8 extends to
/// `u64::MAX` (the decoder handles overflow via `checked_add`).
#[allow(clippy::indexing_slicing)] // indices bounded by NUM_TIERS; .get() unavailable in const
const BOUNDS: [u64; NUM_TIERS + 1] = {
    let mut table = [0u64; NUM_TIERS + 1];
    table[0] = TAG_THRESHOLD as u64;

    let mut i = 1;
    while i < NUM_TIERS {
        table[i] = OFFSETS[i + 1];
        i += 1;
    }

    table[NUM_TIERS] = u64::MAX;
    table
};

/// Errors that can occur when decoding a `bivu64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// The input buffer is shorter than the encoding requires.
    BufferTooShort,

    /// The decoded value exceeds `u64::MAX` (tier 8 only).
    Overflow,
}

impl core::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BufferTooShort => f.write_str("buffer too short for bivu64 encoding"),
            Self::Overflow => f.write_str("bivu64 tier 8 payload overflows u64"),
        }
    }
}

/// Returns the encoded length of `value` in bytes (1–9).
///
/// # Examples
///
/// ```
/// assert_eq!(bivu64::encoded_len(0), 1);
/// assert_eq!(bivu64::encoded_len(247), 1);
/// assert_eq!(bivu64::encoded_len(248), 2);
/// assert_eq!(bivu64::encoded_len(503), 2);
/// assert_eq!(bivu64::encoded_len(504), 3);
/// assert_eq!(bivu64::encoded_len(u64::MAX), 9);
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

/// Encodes `value` as a `bivu64`, appending bytes to `buf`.
///
/// # Examples
///
/// ```
/// let mut buf = Vec::new();
/// bivu64::encode(42, &mut buf);
/// assert_eq!(buf, [0x2A]);
///
/// buf.clear();
/// bivu64::encode(248, &mut buf);
/// assert_eq!(buf, [0xF8, 0x00]);
/// ```
#[allow(clippy::indexing_slicing)] // tier is 1..=8; all indices provably in bounds
pub fn encode(value: u64, buf: &mut Vec<u8>) {
    if value < u64::from(TAG_THRESHOLD) {
        #[allow(clippy::cast_possible_truncation)] // value < 248
        buf.push(value as u8);
        return;
    }

    let tier = tier_for(value);
    let tag = 247 + tier;

    #[allow(clippy::cast_possible_truncation)] // tag <= 255
    buf.push(tag as u8);

    let payload = value - OFFSETS[tier];
    let be = payload.to_be_bytes();
    buf.extend_from_slice(&be[8 - tier..]);
}

/// Encodes `value` as a `bivu64` into a fixed-size array.
///
/// Returns `(bytes, len)` where `bytes` is a 9-byte array with the
/// encoding in `bytes[..len]`.
///
/// # Examples
///
/// ```
/// let (bytes, len) = bivu64::encode_array(300);
/// assert_eq!(&bytes[..len], &[0xF8, 0x34]);
/// ```
#[must_use]
#[allow(clippy::indexing_slicing)] // tier is 1..=8; all indices provably in bounds
#[allow(clippy::cast_possible_truncation)] // value < 248 or tag <= 255
pub const fn encode_array(value: u64) -> ([u8; MAX_BYTES], usize) {
    let mut out = [0u8; MAX_BYTES];

    if value < TAG_THRESHOLD as u64 {
        out[0] = value as u8;
        return (out, 1);
    }

    let tier = tier_for(value);
    out[0] = (247 + tier) as u8;

    let payload = value - OFFSETS[tier];
    let be = payload.to_be_bytes();

    let start = 8 - tier;
    let mut i = 0;
    while i < tier {
        out[1 + i] = be[start + i];
        i += 1;
    }

    (out, 1 + tier)
}

/// Decodes a `bivu64` from the front of `buf`.
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
/// let (v, n) = bivu64::decode(&[0x2A]).unwrap();
/// assert_eq!((v, n), (42, 1));
///
/// // Multi-byte value with trailing data
/// let (v, n) = bivu64::decode(&[0xF8, 0x34, 0xFF]).unwrap();
/// assert_eq!((v, n), (300, 2));
/// ```
#[allow(clippy::indexing_slicing)] // all indices guarded by length checks above their use
pub const fn decode(buf: &[u8]) -> Result<(u64, usize), DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::BufferTooShort);
    }

    let tag = buf[0];

    if tag < TAG_THRESHOLD {
        return Ok((tag as u64, 1));
    }

    let additional = (tag - 247) as usize; // 1..=8

    if buf.len() < 1 + additional {
        return Err(DecodeError::BufferTooShort);
    }

    // Read big-endian payload
    let mut payload = 0u64;
    let mut i = 0;
    while i < additional {
        payload = (payload << 8) | (buf[1 + i] as u64);
        i += 1;
    }

    // OFFSETS[additional] + payload, with overflow check for tier 8
    match OFFSETS[additional].checked_add(payload) {
        Some(value) => Ok((value, 1 + additional)),
        None => Err(DecodeError::Overflow),
    }
}

/// Returns the tier (1–8) for a multi-byte value.
///
/// # Panics
///
/// Panics if `value < 248` (single-byte values have no tier).
#[allow(clippy::indexing_slicing)] // t bounded by NUM_TIERS loop guard
const fn tier_for(value: u64) -> usize {
    debug_assert!(value >= TAG_THRESHOLD as u64);

    // Walk tiers 1..8, return first where value < BOUNDS[tier]
    let mut t = 1;
    while t < NUM_TIERS {
        if value < BOUNDS[t] {
            return t;
        }
        t += 1;
    }

    NUM_TIERS // tier 8
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

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
            assert_eq!(OFFSETS[3], 66_040);
            assert_eq!(OFFSETS[4], 16_843_256);
            assert_eq!(OFFSETS[5], 4_311_810_552);
            assert_eq!(OFFSETS[6], 1_103_823_438_328);
            assert_eq!(OFFSETS[7], 282_578_800_148_984);
            assert_eq!(OFFSETS[8], 72_340_172_838_076_920);
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

        #[test]
        fn zero() {
            let mut buf = Vec::new();
            encode(0, &mut buf);
            assert_eq!(buf, [0x00]);

            let (v, n) = decode(&buf).expect("decode failed");
            assert_eq!((v, n), (0, 1));
        }

        #[test]
        fn max_single_byte() {
            let mut buf = Vec::new();
            encode(247, &mut buf);
            assert_eq!(buf, [0xF7]);

            let (v, n) = decode(&buf).expect("decode failed");
            assert_eq!((v, n), (247, 1));
        }

        #[test]
        fn tier1_min() {
            let mut buf = Vec::new();
            encode(248, &mut buf);
            assert_eq!(buf, [0xF8, 0x00]);

            let (v, n) = decode(&buf).expect("decode failed");
            assert_eq!((v, n), (248, 2));
        }

        #[test]
        fn tier1_max() {
            let mut buf = Vec::new();
            encode(503, &mut buf);
            assert_eq!(buf, [0xF8, 0xFF]);

            let (v, n) = decode(&buf).expect("decode failed");
            assert_eq!((v, n), (503, 2));
        }

        #[test]
        fn tier2_min() {
            let mut buf = Vec::new();
            encode(504, &mut buf);
            assert_eq!(buf, [0xF9, 0x00, 0x00]);

            let (v, n) = decode(&buf).expect("decode failed");
            assert_eq!((v, n), (504, 3));
        }

        #[test]
        fn value_300() {
            let mut buf = Vec::new();
            encode(300, &mut buf);
            assert_eq!(buf, [0xF8, 0x34]); // 300 - 248 = 52 = 0x34

            let (v, n) = decode(&buf).expect("decode failed");
            assert_eq!((v, n), (300, 2));
        }

        #[test]
        fn value_1000() {
            let mut buf = Vec::new();
            encode(1000, &mut buf);
            assert_eq!(buf, [0xF9, 0x01, 0xF0]); // 1000 - 504 = 496 = 0x01F0

            let (v, n) = decode(&buf).expect("decode failed");
            assert_eq!((v, n), (1000, 3));
        }

        #[test]
        fn u64_max() {
            let mut buf = Vec::new();
            encode(u64::MAX, &mut buf);
            assert_eq!(buf.len(), 9);
            assert_eq!(buf[0], 0xFF); // tag 255 = tier 8

            let (v, n) = decode(&buf).expect("decode failed");
            assert_eq!((v, n), (u64::MAX, 9));
        }

        #[test]
        fn all_tier_boundaries() {
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

                    let (decoded, consumed) = decode(&buf).expect("decode failed");
                    assert_eq!(decoded, value, "tier {tier} round-trip failed for {value}");
                    assert_eq!(consumed, expected_len);
                }
            }
        }
    }

    mod encoded_len {
        use super::*;

        #[test]
        fn matches_actual() {
            let test_values = [
                0,
                1,
                127,
                128,
                247,
                248,
                503,
                504,
                1000,
                65_535,
                66_039,
                66_040,
                16_843_255,
                16_843_256,
                u32::MAX as u64,
                4_311_810_551,
                4_311_810_552,
                u64::MAX,
            ];

            for &value in &test_values {
                let mut buf = Vec::new();
                encode(value, &mut buf);
                assert_eq!(
                    super::encoded_len(value),
                    buf.len(),
                    "encoded_len({value}) mismatch"
                );
            }
        }

        #[test]
        fn encode_array_matches_encode() {
            let test_values = [0, 42, 247, 248, 300, 504, 1000, 66_040, u64::MAX];

            for &value in &test_values {
                let mut buf = Vec::new();
                encode(value, &mut buf);

                let (arr, len) = encode_array(value);
                assert_eq!(
                    &arr[..len],
                    buf.as_slice(),
                    "encode_array({value}) mismatch"
                );
            }
        }
    }

    mod errors {
        use super::*;

        #[test]
        fn empty_buffer() {
            assert_eq!(decode(&[]), Err(DecodeError::BufferTooShort));
        }

        #[test]
        fn truncated_multi_byte() {
            // Tag 0xF9 means 2 additional bytes, but only 1 provided
            assert_eq!(decode(&[0xF9, 0x00]), Err(DecodeError::BufferTooShort));
        }

        #[test]
        fn tier8_overflow() {
            // Tag 0xFF = tier 8, payload = all 0xFF = u64::MAX
            // OFFSETS[8] + u64::MAX overflows
            let buf = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
            assert_eq!(decode(&buf), Err(DecodeError::Overflow));
        }

        #[test]
        fn trailing_bytes_not_consumed() {
            let (v, n) = decode(&[0x2A, 0xDE, 0xAD]).expect("decode failed");
            assert_eq!((v, n), (42, 1));

            let (v, n) = decode(&[0xF8, 0x34, 0xBE, 0xEF]).expect("decode failed");
            assert_eq!((v, n), (300, 2));
        }
    }

    mod exhaustive {
        use super::*;

        #[test]
        fn tier0() {
            for value in 0..248u64 {
                let mut buf = Vec::new();
                encode(value, &mut buf);
                assert_eq!(buf.len(), 1);

                #[allow(clippy::cast_possible_truncation)]
                {
                    assert_eq!(buf[0], value as u8);
                }

                let (decoded, consumed) = decode(&buf).expect("decode failed");
                assert_eq!(decoded, value);
                assert_eq!(consumed, 1);
            }
        }

        #[test]
        fn tier1() {
            for value in 248..504u64 {
                let mut buf = Vec::new();
                encode(value, &mut buf);
                assert_eq!(buf.len(), 2, "value {value} should encode in 2 bytes");
                assert_eq!(buf[0], 0xF8);

                let (decoded, consumed) = decode(&buf).expect("decode failed");
                assert_eq!(decoded, value, "round-trip failed for {value}");
                assert_eq!(consumed, 2);
            }
        }

        #[test]
        fn tier2() {
            for value in 504..66_040u64 {
                let mut buf = Vec::new();
                encode(value, &mut buf);
                assert_eq!(buf.len(), 3, "value {value} should encode in 3 bytes");
                assert_eq!(buf[0], 0xF9);

                let (decoded, consumed) = decode(&buf).expect("decode failed");
                assert_eq!(decoded, value, "round-trip failed for {value}");
                assert_eq!(consumed, 3);
            }
        }
    }

    mod boundaries {
        use super::*;

        #[test]
        fn offset_triples() {
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
                let (v, n) = decode(&buf).expect("decode OFFSET[n]-1 failed");
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
                let (v, n) = decode(&buf).expect("decode OFFSET[n] failed");
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
                    let (v, n) = decode(&buf).expect("decode OFFSET[n]+1 failed");
                    assert_eq!(v, offset + 1);
                    assert_eq!(n, tier_len);
                }
            }
        }

        #[test]
        fn all_zero_payloads() {
            // [tag, 0x00, ..., 0x00] should decode to OFFSETS[tier] for each tier
            for tier in 1..=NUM_TIERS {
                let tag = (247 + tier) as u8;
                let mut buf = vec![tag];
                buf.extend(core::iter::repeat(0x00u8).take(tier));

                let (value, consumed) = decode(&buf).unwrap_or_else(|e| {
                    panic!("decode all-zeros tier {tier} failed: {e}");
                });
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
        }

        #[test]
        fn all_ones_payloads() {
            // [tag, 0xFF, ..., 0xFF] should decode to the tier's maximum value
            for tier in 1..=NUM_TIERS {
                let tag = (247 + tier) as u8;
                let mut buf = vec![tag];
                buf.extend(core::iter::repeat(0xFFu8).take(tier));

                let result = decode(&buf);

                if tier < NUM_TIERS {
                    // Tiers 1-7: all-ones payload = 256^tier - 1, so
                    // value = OFFSETS[tier] + (256^tier - 1) = OFFSETS[tier+1] - 1
                    let (value, consumed) = result.unwrap_or_else(|e| {
                        panic!("decode all-ones tier {tier} failed: {e}");
                    });
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
        }
    }

    mod bijectivity {
        use super::*;

        #[test]
        fn overlong_encoding_decodes_to_different_value() {
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
                let tag = (247 + wider_tier) as u8;
                let mut forged = vec![tag];
                let be = payload.to_be_bytes();
                forged.extend_from_slice(&be[8 - wider_tier..]);

                let (decoded, _) = decode(&forged).expect("forged decode failed");

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
        }

        #[test]
        fn no_two_byte_sequences_decode_to_same_value() {
            // For each tier boundary value, verify that its canonical encoding
            // is the *only* encoding that decodes to it. Specifically, no
            // wider tier can produce the same value, because the wider tier's
            // minimum (OFFSETS[wider]) already exceeds the value.
            for tier in 1..=NUM_TIERS {
                let value = OFFSETS[tier];

                // Canonical encoding
                let mut canonical = Vec::new();
                encode(value, &mut canonical);
                assert_eq!(canonical.len(), 1 + tier);

                // Any wider tier's minimum decoded value is OFFSETS[wider] > value
                for wider in (tier + 1)..=NUM_TIERS {
                    assert!(
                        OFFSETS[wider] > value,
                        "OFFSETS[{wider}] <= OFFSETS[{tier}]: tier ranges overlap"
                    );
                }

                // Any narrower tier's maximum decoded value is OFFSETS[tier] - 1 < value
                // (except tier 0 whose max is 247)
                if tier > 1 {
                    let narrower_max = OFFSETS[tier] - 1;
                    assert!(
                        narrower_max < value,
                        "narrower tier max ({narrower_max}) >= tier {tier} min ({value})"
                    );
                }
            }
        }

        #[test]
        fn tier_ranges_are_contiguous_and_disjoint() {
            // Verify that tier ranges cover [0, u64::MAX] with no gaps or overlaps.
            // Tier 0: [0, OFFSETS[1])
            // Tier n: [OFFSETS[n], OFFSETS[n+1])  for n in 1..8
            // Tier 8: [OFFSETS[8], u64::MAX]
            assert_eq!(BOUNDS[0], OFFSETS[1], "tier 0 upper bound != tier 1 offset");
            let mut prev_end = BOUNDS[0];

            for tier in 1..=NUM_TIERS {
                assert_eq!(
                    OFFSETS[tier],
                    prev_end,
                    "gap between tier {} and tier {tier}",
                    tier - 1
                );

                if tier < NUM_TIERS {
                    prev_end = OFFSETS[tier + 1];
                } else {
                    // Tier 8 must be able to represent u64::MAX
                    let max_payload = u64::MAX - OFFSETS[tier];
                    let tier_capacity = if tier < 8 {
                        (1u64 << (8 * tier)) - 1
                    } else {
                        u64::MAX
                    };
                    assert!(
                        max_payload <= tier_capacity,
                        "tier 8 cannot represent u64::MAX: \
                         max_payload={max_payload}, capacity={tier_capacity}"
                    );
                }
            }
        }
    }

    mod test_vectors {
        use super::*;

        /// Test vectors: (value, expected_bytes).
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
            (66_040, &[0xFA, 0x00, 0x00, 0x00]),
            (67_000, &[0xFA, 0x00, 0x03, 0xC0]),
            (16_843_255, &[0xFA, 0xFF, 0xFF, 0xFF]),
            // Tier 4: tag 0xFB + 4 bytes
            (16_843_256, &[0xFB, 0x00, 0x00, 0x00, 0x00]),
            (4_311_810_551, &[0xFB, 0xFF, 0xFF, 0xFF, 0xFF]),
            // Tier 8: tag 0xFF + 8 bytes
            (
                72_340_172_838_076_920,
                &[0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                u64::MAX,
                &[0xFF, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0x07],
            ),
        ];

        #[test]
        fn encode() {
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
        fn decode() {
            for &(expected_value, bytes) in VECTORS {
                let (value, consumed) = super::decode(bytes).unwrap_or_else(|e| {
                    panic!("decode({bytes:02X?}) failed: {e}");
                });
                assert_eq!(
                    value, expected_value,
                    "decode({bytes:02X?}): got {value}, expected {expected_value}"
                );
                assert_eq!(consumed, bytes.len());
            }
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
                let (decoded, consumed) = decode(&buf).expect("decode failed");
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
                assert_eq!(&arr[..len], buf.as_slice());
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

        #[test]
        #[cfg_attr(miri, ignore)]
        fn bijective() {
            bolero::check!()
                .with_arbitrary::<Vec<u8>>()
                .for_each(|buf| {
                    if let Ok((value, consumed)) = decode(buf) {
                        // Re-encode and verify it produces the same bytes
                        let mut re_encoded = Vec::new();
                        encode(value, &mut re_encoded);
                        assert_eq!(
                            re_encoded.as_slice(),
                            &buf[..consumed],
                            "bijection violated: decode({:02X?}) = {value}, \
                             re-encode = {:02X?}",
                            &buf[..consumed],
                            re_encoded
                        );
                    }
                });
        }
    }
}
