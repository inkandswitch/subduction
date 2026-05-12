//! Comprehensive property tests for the bespoke binary codec.
//!
//! The codec lives in [`sedimentree_core::codec`] and provides the
//! canonical encoding for all signed messages and wire-level envelopes
//! across the workspace. This integration test file exercises:
//!
//! - Primitive encoder/decoder round-trips (`u8`, `u16`, `u32`, `u64`,
//!   `array`, `slice`)
//! - The `verify_sorted` invariant (strict ascending order, duplicate
//!   rejection)
//! - The schema/encode/decode trait machinery via the two concrete
//!   [`DecodeFields`] implementors that live in this crate
//!   ([`LooseCommit`] and [`Fragment`])
//! - Adversarial inputs: never panic on arbitrary bytes; bit-flip
//!   robustness; truncation; trailing bytes; tampered headers
//!
//! The high-level invariants we want to hold _everywhere_:
//!
//! 1. **Round-trip**: `encode → decode == identity`
//! 2. **Size invariants**: `encoded.len() == encoded_size()` and
//!    `consumed == decoded.fields_size()`. The latter is the
//!    invariant `Signed::try_decode` silently relies on for
//!    `bytes.truncate(actual_size)` correctness.
//! 3. **No panics**: arbitrary bytes through any decoder must produce
//!    a structured error rather than a panic, abort, or hang.
//! 4. **Tampering produces errors, not silent acceptance**: schema
//!    bytes, length prefixes, and payload bytes that are corrupted in
//!    transit must yield an error or a different (but still validly
//!    decoded) value — never an out-of-bounds access or assertion
//!    failure.
//!
//! The bolero house style here matches what's in
//! `sedimentree_core/src/codec/decode.rs` and
//! `sedimentree_core/src/loose_commit.rs`: `with_arbitrary` for
//! domain types, explicit `panic!("decode should succeed: {e}")` on
//! the unhappy path of a round-trip, and `_ = decoder(&bytes)` for
//! never-panic fuzz.

#![cfg(feature = "bolero")]
#![allow(clippy::panic, clippy::expect_used, clippy::indexing_slicing)]

use sedimentree_core::{
    codec::{
        decode::{self, DecodeFields},
        encode::{self, Encode, EncodeFields},
        error::{DecodeError, ReadingType, UnsortedArray},
        schema::Schema,
    },
    fragment::Fragment,
    loose_commit::LooseCommit,
};

// ── Primitive round-trips ───────────────────────────────────────────────
//
// These complement the in-module proptests in
// `codec/decode.rs::proptests` by checking encode-then-decode at
// non-zero offsets and inside larger buffers, the way real callers
// actually use them.

/// Encode a `u8` after some prefix bytes; decode should find it at the
/// declared offset regardless of prefix content.
#[test]
fn u8_round_trip_with_prefix() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, u8)>()
        .for_each(|(prefix, value)| {
            let mut buf = prefix.clone();
            let offset = buf.len();
            encode::u8(*value, &mut buf);
            assert_eq!(decode::u8(&buf, offset).expect("in bounds"), *value);
        });
}

#[test]
fn u16_round_trip_with_prefix() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, u16)>()
        .for_each(|(prefix, value)| {
            let mut buf = prefix.clone();
            let offset = buf.len();
            encode::u16(*value, &mut buf);
            assert_eq!(decode::u16(&buf, offset).expect("in bounds"), *value);
        });
}

#[test]
fn u32_round_trip_with_prefix() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, u32)>()
        .for_each(|(prefix, value)| {
            let mut buf = prefix.clone();
            let offset = buf.len();
            encode::u32(*value, &mut buf);
            assert_eq!(decode::u32(&buf, offset).expect("in bounds"), *value);
        });
}

#[test]
fn u64_round_trip_with_prefix() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, u64)>()
        .for_each(|(prefix, value)| {
            let mut buf = prefix.clone();
            let offset = buf.len();
            encode::u64(*value, &mut buf);
            assert_eq!(decode::u64(&buf, offset).expect("in bounds"), *value);
        });
}

#[test]
fn array_round_trip_with_prefix() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, [u8; 32])>()
        .for_each(|(prefix, value)| {
            let mut buf = prefix.clone();
            let offset = buf.len();
            encode::array(value, &mut buf);
            assert_eq!(
                decode::array::<32>(&buf, offset).expect("in bounds"),
                *value
            );
        });
}

/// Endianness check: u16 BE — the most-significant byte must be at
/// the lower offset.
#[test]
#[allow(clippy::cast_possible_truncation)]
fn u16_is_big_endian() {
    bolero::check!().with_arbitrary::<u16>().for_each(|value| {
        let mut buf = Vec::new();
        encode::u16(*value, &mut buf);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], (*value >> 8) as u8);
        assert_eq!(buf[1], *value as u8);
    });
}

// ── Slice / array decoder pathological inputs ──────────────────────────

/// A `len` so large that `offset + len` overflows `usize` must error,
/// not panic. The `slice` decoder uses `checked_add` so this is the
/// regression net.
#[test]
fn slice_handles_offset_plus_len_overflow() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, usize)>()
        .for_each(|(bytes, offset)| {
            // Pick a `len` that, combined with `offset`, would overflow.
            let len = usize::MAX - offset.saturating_sub(1);
            let _result = decode::slice(bytes, *offset, len);
        });
}

/// `slice(buf, offset, 0)` should always succeed and return `&[]`,
/// regardless of buffer size or offset (as long as offset itself is in
/// range — currently, an offset beyond the buffer plus len 0 yields
/// the right result by virtue of `buf.get(offset..offset)` returning
/// `Some(&[])` when `offset == buf.len()`, and `None` otherwise).
#[test]
fn slice_zero_len_at_buf_end_is_ok() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let result = decode::slice(bytes, bytes.len(), 0);
            assert!(result.is_ok());
            assert!(result.expect("just checked").is_empty());
        });
}

// ── verify_sorted ──────────────────────────────────────────────────────

/// `verify_sorted` accepts iff the input is strictly ascending.
///
/// This is the canonical-encoding invariant for `LooseCommit::parents`,
/// `Fragment::boundary`, `Fragment::checkpoints`, etc. Strict ascending
/// order doubles as a duplicate detector — duplicate parents would
/// allow two distinct encodings of the same logical commit.
#[test]
fn verify_sorted_strict_ascending_property() {
    bolero::check!()
        .with_arbitrary::<Vec<[u8; 32]>>()
        .for_each(|elements| {
            let result = decode::verify_sorted(elements);

            // Compute the truth via windows().
            let is_strict_ascending = elements.windows(2).all(|w| match w {
                [a, b] => a < b,
                _ => true,
            });

            match (result, is_strict_ascending) {
                (Ok(()), true) | (Err(_), false) => {} // agreement
                (Ok(()), false) => panic!("verify_sorted accepted unsorted input: {elements:?}"),
                (Err(e), true) => panic!("verify_sorted rejected sorted input: {e}"),
            }
        });
}

/// `verify_sorted` rejects any input that contains a duplicate.
#[test]
fn verify_sorted_rejects_duplicates() {
    bolero::check!()
        .with_arbitrary::<(Vec<[u8; 32]>, [u8; 32])>()
        .for_each(|(prefix, dup)| {
            // Build a sorted prefix, then append `dup` twice.
            let mut sorted: Vec<[u8; 32]> = prefix.clone();
            sorted.sort_unstable();
            sorted.dedup();
            sorted.push(*dup);
            sorted.push(*dup);
            sorted.sort_unstable();

            // The presence of a duplicate guarantees rejection.
            let result = decode::verify_sorted(&sorted);
            assert!(
                matches!(result, Err(UnsortedArray { .. })),
                "expected UnsortedArray for input containing a duplicate"
            );
        });
}

/// `verify_sorted` reports the index of the *second* element of the
/// first out-of-order pair (i.e., the position where the violation
/// becomes detectable). This is the index-1 offset asserted by
/// `verify_sorted_descending_rejected`/`_duplicates_rejected`.
#[test]
fn verify_sorted_violation_index_is_one_based() {
    bolero::check!()
        .with_arbitrary::<(Vec<[u8; 32]>, usize)>()
        .for_each(|(elements, _seed)| {
            if let Err(UnsortedArray { index }) = decode::verify_sorted(elements) {
                assert!(index >= 1, "violation index must be >= 1");
                assert!(
                    index < elements.len(),
                    "violation index {} must be < len {}",
                    index,
                    elements.len()
                );
                let prev = &elements[index - 1];
                let curr = &elements[index];
                assert!(
                    prev >= curr,
                    "reported violation is not actually a violation"
                );
            }
        });
}

// ── Schema invariants ─────────────────────────────────────────────────

/// The `SCHEMA` constant is a deterministic function of the protocol
/// prefix, type byte, and version. Encoding a `T` always begins with
/// `T::SCHEMA`.
#[test]
fn loose_commit_encoding_starts_with_schema() {
    bolero::check!()
        .with_arbitrary::<LooseCommit>()
        .for_each(|commit| {
            let bytes = commit.encode();
            assert_eq!(&bytes[..4], &LooseCommit::SCHEMA);
            assert_eq!(LooseCommit::SCHEMA, *b"STC\x00");
        });
}

#[test]
fn fragment_encoding_starts_with_schema() {
    bolero::check!()
        .with_arbitrary::<Fragment>()
        .for_each(|fragment| {
            let bytes = fragment.encode();
            assert_eq!(&bytes[..4], &Fragment::SCHEMA);
            assert_eq!(Fragment::SCHEMA, *b"STF\x00");
        });
}

// ── DecodeFields universal contract ───────────────────────────────────
//
// Any `T: EncodeFields + DecodeFields` must satisfy:
//
//   1.  encode_fields → try_decode_fields == Ok((same value, len(buf)))
//   2.  decoded.fields_size() == consumed
//   3.  re-encoding the decoded value yields byte-for-byte equality
//
// (1) is the round-trip. (2) is the truncate-correctness invariant.
// (3) is the determinism invariant (no spurious bits in the encoder).

#[test]
fn loose_commit_round_trip_is_byte_identical() {
    bolero::check!()
        .with_arbitrary::<LooseCommit>()
        .for_each(|commit| {
            let mut buf1 = Vec::new();
            commit.encode_fields(&mut buf1);
            match LooseCommit::try_decode_fields(&buf1) {
                Ok((decoded, consumed)) => {
                    assert_eq!(consumed, buf1.len(), "must consume all bytes");
                    assert_eq!(consumed, decoded.fields_size(), "consumed == fields_size()");
                    let mut buf2 = Vec::new();
                    decoded.encode_fields(&mut buf2);
                    assert_eq!(
                        buf1, buf2,
                        "re-encoding the decoded value must produce identical bytes"
                    );
                }
                Err(e) => panic!("decode must succeed for valid encoded data: {e}"),
            }
        });
}

#[test]
fn fragment_round_trip_is_byte_identical() {
    bolero::check!()
        .with_arbitrary::<Fragment>()
        .for_each(|fragment| {
            let mut buf1 = Vec::new();
            fragment.encode_fields(&mut buf1);
            match Fragment::try_decode_fields(&buf1) {
                Ok((decoded, consumed)) => {
                    assert_eq!(consumed, buf1.len(), "must consume all bytes");
                    assert_eq!(consumed, decoded.fields_size(), "consumed == fields_size()");
                    let mut buf2 = Vec::new();
                    decoded.encode_fields(&mut buf2);
                    assert_eq!(
                        buf1, buf2,
                        "re-encoding the decoded value must produce identical bytes"
                    );
                }
                Err(e) => panic!("decode must succeed for valid encoded data: {e}"),
            }
        });
}

/// Encoding into a non-empty buffer appends — it does not corrupt
/// existing bytes.
#[test]
fn loose_commit_encode_fields_appends() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, LooseCommit)>()
        .for_each(|(prefix, commit)| {
            let mut buf = prefix.clone();
            commit.encode_fields(&mut buf);
            assert!(buf.starts_with(prefix));
            assert_eq!(buf.len(), prefix.len() + commit.fields_size());
        });
}

#[test]
fn fragment_encode_fields_appends() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, Fragment)>()
        .for_each(|(prefix, fragment)| {
            let mut buf = prefix.clone();
            fragment.encode_fields(&mut buf);
            assert!(buf.starts_with(prefix));
            assert_eq!(buf.len(), prefix.len() + fragment.fields_size());
        });
}

// ── Trailing-byte handling for fields ─────────────────────────────────
//
// `try_decode_fields` accepts trailing bytes — that's why it returns
// `(T, consumed)`. The signed-message machinery relies on this so
// that `Signed<T>` can find the signature immediately after the
// fields. Verify the property explicitly.

#[test]
fn loose_commit_decode_with_trailing_bytes() {
    bolero::check!()
        .with_arbitrary::<(LooseCommit, Vec<u8>)>()
        .for_each(|(commit, trailing)| {
            let mut buf = Vec::new();
            commit.encode_fields(&mut buf);
            let canonical_len = buf.len();
            buf.extend_from_slice(trailing);

            match LooseCommit::try_decode_fields(&buf) {
                Ok((decoded, consumed)) => {
                    assert_eq!(
                        &decoded, commit,
                        "trailing bytes must not corrupt the value"
                    );
                    assert_eq!(consumed, canonical_len, "consumed must equal canonical len");
                }
                Err(e) => panic!("decode must accept canonical bytes + trailing garbage: {e}"),
            }
        });
}

#[test]
fn fragment_decode_with_trailing_bytes() {
    bolero::check!()
        .with_arbitrary::<(Fragment, Vec<u8>)>()
        .for_each(|(fragment, trailing)| {
            let mut buf = Vec::new();
            fragment.encode_fields(&mut buf);
            let canonical_len = buf.len();
            buf.extend_from_slice(trailing);

            match Fragment::try_decode_fields(&buf) {
                Ok((decoded, consumed)) => {
                    assert_eq!(
                        &decoded, fragment,
                        "trailing bytes must not corrupt the value"
                    );
                    assert_eq!(consumed, canonical_len);
                }
                Err(e) => panic!("decode must accept canonical bytes + trailing garbage: {e}"),
            }
        });
}

// ── Truncation robustness ──────────────────────────────────────────────
//
// If we drop bytes from the *end* of a valid encoding, the decoder
// must error rather than panic. This catches off-by-one bugs in the
// length checks and any indexing that doesn't go through `Result`.

#[test]
fn loose_commit_truncation_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(LooseCommit, u16)>()
        .for_each(|(commit, drop_count)| {
            let mut buf = Vec::new();
            commit.encode_fields(&mut buf);
            let new_len = buf.len().saturating_sub(*drop_count as usize);
            buf.truncate(new_len);

            // Either decode succeeds (if `drop_count == 0`) or returns
            // an error. It must not panic.
            let _result = LooseCommit::try_decode_fields(&buf);
        });
}

#[test]
fn fragment_truncation_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(Fragment, u16)>()
        .for_each(|(fragment, drop_count)| {
            let mut buf = Vec::new();
            fragment.encode_fields(&mut buf);
            let new_len = buf.len().saturating_sub(*drop_count as usize);
            buf.truncate(new_len);

            let _result = Fragment::try_decode_fields(&buf);
        });
}

// ── Bit-flip robustness ───────────────────────────────────────────────
//
// Flipping a single bit anywhere in a valid encoding must yield
// either:
//
//  - a successful decode (to a *different* value), or
//  - a structured `DecodeError`.
//
// It must never panic.

#[test]
fn loose_commit_single_bit_flip_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(LooseCommit, u32, u8)>()
        .for_each(|(commit, byte_idx_seed, bit_idx_seed)| {
            let mut buf = Vec::new();
            commit.encode_fields(&mut buf);
            if buf.is_empty() {
                return;
            }
            let byte_idx = (*byte_idx_seed as usize) % buf.len();
            let bit_idx = bit_idx_seed % 8;
            buf[byte_idx] ^= 1 << bit_idx;

            let _result = LooseCommit::try_decode_fields(&buf);
        });
}

#[test]
fn fragment_single_bit_flip_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(Fragment, u32, u8)>()
        .for_each(|(fragment, byte_idx_seed, bit_idx_seed)| {
            let mut buf = Vec::new();
            fragment.encode_fields(&mut buf);
            if buf.is_empty() {
                return;
            }
            let byte_idx = (*byte_idx_seed as usize) % buf.len();
            let bit_idx = bit_idx_seed % 8;
            buf[byte_idx] ^= 1 << bit_idx;

            let _result = Fragment::try_decode_fields(&buf);
        });
}

// ── Equal bytes ⇒ equal values ────────────────────────────────────────
//
// If `a.encode() == b.encode()`, then `a == b` (assuming both round-trip).
// Combined with determinism above, this proves the codec is a *bijection*
// from values to byte strings — which is exactly what canonical
// encoding requires.

#[test]
fn loose_commit_equal_bytes_implies_equal_values() {
    bolero::check!()
        .with_arbitrary::<(LooseCommit, LooseCommit)>()
        .for_each(|(a, b)| {
            let bytes_a = a.encode();
            let bytes_b = b.encode();
            if bytes_a == bytes_b {
                assert_eq!(a, b, "equal bytes must imply equal values");
            }
        });
}

#[test]
fn fragment_equal_bytes_implies_equal_values() {
    bolero::check!()
        .with_arbitrary::<(Fragment, Fragment)>()
        .for_each(|(a, b)| {
            let bytes_a = a.encode();
            let bytes_b = b.encode();
            if bytes_a == bytes_b {
                assert_eq!(a, b, "equal bytes must imply equal values");
            }
        });
}

// ── Concatenation parsing ──────────────────────────────────────────────
//
// If we encode `[a, b, c, ...]` back-to-back into a single buffer and
// parse them sequentially using the `consumed` byte count, we must
// recover the original sequence. This is the property that lets
// `Signed<T>` find the signature immediately after the fields.

#[test]
fn loose_commit_sequence_parses_with_consumed_offset() {
    bolero::check!()
        .with_arbitrary::<Vec<LooseCommit>>()
        .for_each(|commits| {
            let mut buf = Vec::new();
            for c in commits {
                c.encode_fields(&mut buf);
            }

            let mut offset = 0;
            let mut decoded: Vec<LooseCommit> = Vec::new();
            for _ in commits {
                match LooseCommit::try_decode_fields(&buf[offset..]) {
                    Ok((c, consumed)) => {
                        decoded.push(c);
                        offset += consumed;
                    }
                    Err(e) => {
                        panic!("sequential decode must succeed for back-to-back encodings: {e}")
                    }
                }
            }

            assert_eq!(offset, buf.len(), "must have consumed exactly the buffer");
            assert_eq!(decoded.len(), commits.len());
            for (i, c) in commits.iter().enumerate() {
                assert_eq!(&decoded[i], c, "element {i} must round-trip");
            }
        });
}

// ── Domain-level invariants exposed at the codec boundary ─────────────

/// The wire encoding of `LooseCommit::parents` is a `BTreeSet`-style
/// strictly ascending sequence. Encoding a value whose `parents` set
/// has duplicates is structurally impossible (the Rust `BTreeSet`
/// dedups), but the *decode* path must still reject hand-crafted
/// buffers with duplicate parents.
#[test]
fn loose_commit_with_duplicate_parents_in_buffer_is_rejected() {
    bolero::check!()
        .with_arbitrary::<(LooseCommit, [u8; 32])>()
        .for_each(|(commit, dup_parent)| {
            // Re-encode but inject the dup parent into the parents region.
            let mut buf = Vec::new();
            commit.encode_fields(&mut buf);

            // We can mechanically insert a duplicate parent only if the
            // commit has at least 1 parent already to clone its bytes.
            // For simplicity, build a hand-crafted buffer:
            //   sed_id(32) + head(32) + blob_digest(32) + parent_count(1=2) +
            //   bijou64(blob_size) + parent[0](32) + parent[0](32) — the duplicate
            let mut crafted = Vec::new();
            crafted.extend_from_slice(commit.sedimentree_id().as_bytes());
            crafted.extend_from_slice(commit.head().as_bytes());
            crafted.extend_from_slice(commit.blob_meta().digest().as_bytes());
            crafted.push(2u8); // parent_count = 2
            bijou64::encode(commit.blob_meta().size_bytes(), &mut crafted);
            crafted.extend_from_slice(dup_parent);
            crafted.extend_from_slice(dup_parent); // duplicate

            let result = LooseCommit::try_decode_fields(&crafted);
            assert!(
                matches!(result, Err(DecodeError::UnsortedArray(_))),
                "duplicate parents must be rejected as UnsortedArray, got {result:?}"
            );
        });
}

// ── Random-byte never-panic suite ─────────────────────────────────────
//
// These complement the in-module fuzz tests with longer-running coverage
// at the integration-test level, where bolero's seed corpus accumulates
// across runs.

#[test]
fn random_bytes_through_loose_commit_never_panics() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = LooseCommit::try_decode_fields(bytes);
        });
}

#[test]
fn random_bytes_through_fragment_never_panics() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = Fragment::try_decode_fields(bytes);
        });
}
