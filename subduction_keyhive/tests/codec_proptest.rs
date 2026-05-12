//! Comprehensive property tests for the [`KeyhiveMessage`] wire codec.
//!
//! `KeyhiveMessage` is the simplest envelope in the workspace: it wraps
//! an opaque CBOR payload in a `SUK\x00` frame:
//!
//! ```text
//! ┌────────┬───────────┬─────────────────┐
//! │ Schema │ TotalSize │  CBOR Payload   │
//! │   4B   │    4B     │     variable    │
//! └────────┴───────────┴─────────────────┘
//! ```
//!
//! There is no tag byte and no signature at this layer — the inner
//! CBOR payload is signed/verified by `keyhive_core` separately.
//!
//! What we want to hold:
//!
//! 1. **Round-trip** for arbitrary payload bytes.
//! 2. **No panics** on arbitrary input bytes.
//! 3. **Truncation** at any prefix length yields a structured error.
//! 4. **Trailing bytes** are rejected (`SizeMismatch`).
//! 5. **Schema tampering** yields `InvalidSchema`.
//! 6. **`total_size` mismatch** yields `SizeMismatch`.

#![cfg(feature = "bolero")]
#![allow(clippy::panic, clippy::expect_used, clippy::indexing_slicing)]

use std::vec::Vec;

use sedimentree_core::codec::{
    decode::Decode,
    encode::Encode,
    error::{DecodeError, InvalidSchema, SizeMismatch},
};
use subduction_keyhive::wire::{KEYHIVE_SCHEMA, KeyhiveMessage};

// ── Round-trip ────────────────────────────────────────────────────────

#[test]
fn keyhive_round_trip() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|payload| {
            let msg = KeyhiveMessage::new(payload.clone());
            let encoded = msg.encode();
            let decoded = KeyhiveMessage::try_decode(&encoded).expect("round-trip decode");
            assert_eq!(decoded.payload(), payload.as_slice());
            assert_eq!(decoded, msg);
        });
}

#[test]
fn keyhive_encoded_size_matches_actual_length() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|payload| {
            let msg = KeyhiveMessage::new(payload.clone());
            assert_eq!(msg.encoded_size(), msg.encode().len());
        });
}

/// Encoded form starts with the `SUK\x00` schema.
#[test]
fn keyhive_encoded_starts_with_schema() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|payload| {
            let msg = KeyhiveMessage::new(payload.clone());
            let encoded = msg.encode();
            assert_eq!(&encoded[..4], &KEYHIVE_SCHEMA);
        });
}

/// The 4-byte big-endian `total_size` immediately after the schema is
/// equal to the buffer length.
#[test]
fn keyhive_encoded_total_size_matches_buffer_length() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|payload| {
            let msg = KeyhiveMessage::new(payload.clone());
            let encoded = msg.encode();
            let total_size_bytes: [u8; 4] = encoded[4..8].try_into().expect("4 bytes");
            let declared = u32::from_be_bytes(total_size_bytes) as usize;
            assert_eq!(declared, encoded.len());
        });
}

// ── No panics ─────────────────────────────────────────────────────────

#[test]
fn keyhive_random_bytes_never_panic() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = KeyhiveMessage::try_decode(bytes);
        });
}

// ── Truncation ────────────────────────────────────────────────────────

#[test]
fn keyhive_truncation_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, u32)>()
        .for_each(|(payload, drop_count)| {
            let msg = KeyhiveMessage::new(payload.clone());
            let mut encoded = msg.encode();
            let new_len = encoded.len().saturating_sub(*drop_count as usize);
            encoded.truncate(new_len);
            let _result = KeyhiveMessage::try_decode(&encoded);
        });
}

/// Any truncation by 1 or more bytes from a canonical encoding yields
/// an error (size mismatch or message-too-short, depending on how
/// much was dropped).
#[test]
fn keyhive_truncation_yields_error() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, u32)>()
        .for_each(|(payload, drop_count)| {
            // Skip the no-truncation case.
            if *drop_count == 0 {
                return;
            }
            let msg = KeyhiveMessage::new(payload.clone());
            let mut encoded = msg.encode();
            let drop = (*drop_count as usize).min(encoded.len());
            if drop == 0 {
                return;
            }
            let new_len = encoded.len() - drop;
            encoded.truncate(new_len);

            let result = KeyhiveMessage::try_decode(&encoded);
            assert!(
                result.is_err(),
                "truncated keyhive message must error, got Ok"
            );
        });
}

// ── Trailing bytes ────────────────────────────────────────────────────

#[test]
fn keyhive_trailing_bytes_rejected() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, Vec<u8>)>()
        .for_each(|(payload, trailing)| {
            if trailing.is_empty() {
                return;
            }
            let msg = KeyhiveMessage::new(payload.clone());
            let mut encoded = msg.encode();
            encoded.extend_from_slice(trailing);

            let result = KeyhiveMessage::try_decode(&encoded);
            assert!(
                matches!(result, Err(DecodeError::SizeMismatch(SizeMismatch { .. }))),
                "trailing bytes must yield SizeMismatch, got {result:?}"
            );
        });
}

// ── Schema tampering ──────────────────────────────────────────────────

#[test]
fn keyhive_wrong_schema_rejected() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, [u8; 4])>()
        .for_each(|(payload, bad_schema)| {
            if bad_schema == &KEYHIVE_SCHEMA {
                return;
            }
            let msg = KeyhiveMessage::new(payload.clone());
            let mut encoded = msg.encode();
            encoded[..4].copy_from_slice(bad_schema);

            let result = KeyhiveMessage::try_decode(&encoded);
            assert!(
                matches!(
                    result,
                    Err(DecodeError::InvalidSchema(InvalidSchema { .. }))
                ),
                "wrong schema must yield InvalidSchema, got {result:?}"
            );
        });
}

// ── total_size tampering ─────────────────────────────────────────────

/// Corrupting the `total_size` u32 to anything other than the actual
/// buffer length yields `SizeMismatch`.
#[test]
fn keyhive_corrupted_total_size_rejected() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, u32)>()
        .for_each(|(payload, fake_size)| {
            let msg = KeyhiveMessage::new(payload.clone());
            let mut encoded = msg.encode();
            #[allow(clippy::cast_possible_truncation)]
            if (*fake_size as usize) == encoded.len() {
                return;
            }
            encoded[4..8].copy_from_slice(&fake_size.to_be_bytes());

            let result = KeyhiveMessage::try_decode(&encoded);
            assert!(
                matches!(result, Err(DecodeError::SizeMismatch(SizeMismatch { .. }))),
                "corrupted total_size must yield SizeMismatch, got {result:?}"
            );
        });
}

// ── Bit-flip ─────────────────────────────────────────────────────────

#[test]
fn keyhive_single_bit_flip_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, u32, u8)>()
        .for_each(|(payload, byte_idx_seed, bit_idx_seed)| {
            let msg = KeyhiveMessage::new(payload.clone());
            let mut encoded = msg.encode();
            if encoded.is_empty() {
                return;
            }
            let byte_idx = (*byte_idx_seed as usize) % encoded.len();
            let bit_idx = bit_idx_seed % 8;
            encoded[byte_idx] ^= 1 << bit_idx;
            let _result = KeyhiveMessage::try_decode(&encoded);
        });
}

// ── Constants ────────────────────────────────────────────────────────

#[test]
fn keyhive_schema_constant() {
    assert_eq!(KEYHIVE_SCHEMA, *b"SUK\x00");
}
