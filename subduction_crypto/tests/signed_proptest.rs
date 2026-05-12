//! Comprehensive property tests for [`Signed<T>`].
//!
//! `Signed<T>` is the universal envelope format used by the Subduction
//! protocol family. Its wire layout is:
//!
//! ```text
//! ┌─────────────── signed region (covered by Ed25519 sig) ──────────────┐
//! │ schema(4) │ disc?(0|1) │ issuer(32) │ fields(N) │      │ sig(64)    │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! The discriminant byte (when present) is part of the signed region —
//! this prevents tag-swap attacks where a valid sig is reused for a
//! different variant under the same `SCHEMA`. Two payload types are
//! defined here to exercise both code paths:
//!
//! - [`PlainPayload`] — no discriminant
//! - [`TaggedPayload`] / [`OtherTaggedPayload`] — same `SCHEMA`,
//!   different `DISCRIMINANT`
//!
//! What we want to hold:
//!
//! 1. **Round-trip**: `seal → as_bytes → try_decode → try_verify → payload == T`
//! 2. **Byte preservation**: `try_decode` followed by `as_bytes` is the identity
//! 3. **Truncation correctness**: when extra bytes are appended,
//!    `try_decode` truncates the stored buffer to exactly the
//!    canonical signed length, *and* the resulting verification still
//!    succeeds. (This is the invariant that protects `SyncMessage`'s
//!    embedding of `Signed<T>` byte-for-byte.)
//! 4. **No panics**: arbitrary bytes through `try_decode` always
//!    yields a structured `DecodeError` rather than a panic.
//! 5. **Tampering detection**: any single bit-flip in the signed
//!    region, or in the signature, must cause verification to fail
//!    (or decode to fail outright). Non-tampered bytes verify.
//! 6. **Cross-type rejection**: bytes signed as `TaggedPayload` must
//!    not decode as `OtherTaggedPayload` even though they share a
//!    `SCHEMA`. The discriminant must be enforced.

#![cfg(feature = "bolero")]
#![allow(clippy::panic, clippy::expect_used, clippy::indexing_slicing)]

use std::vec::Vec;

use ed25519_dalek::{Signer as _, SigningKey};
use sedimentree_core::codec::{
    decode::{self, DecodeFields},
    encode::{self, EncodeFields},
    error::{DecodeError, InvalidDiscriminant, InvalidSchema},
    schema::{self, Schema},
};
use subduction_crypto::signed::{
    MIN_SIGNED_SIZE, SCHEMA_SIZE, SIGNATURE_SIZE, Signed, VERIFYING_KEY_SIZE,
};

// ── Test payloads ──────────────────────────────────────────────────────

/// Payload without a discriminant. Wire size: schema(4) + issuer(32) +
/// value(8) + sig(64) = 108 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, arbitrary::Arbitrary)]
struct PlainPayload {
    value: u64,
}

impl Schema for PlainPayload {
    const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
    const TYPE_BYTE: u8 = b'1';
    const VERSION: u8 = 0;
    // No DISCRIMINANT
}

impl EncodeFields for PlainPayload {
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        encode::u64(self.value, buf);
    }

    fn fields_size(&self) -> usize {
        8
    }
}

impl DecodeFields for PlainPayload {
    const MIN_SIGNED_SIZE: usize = SCHEMA_SIZE + VERIFYING_KEY_SIZE + 8 + SIGNATURE_SIZE;

    fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let value = decode::u64(buf, 0)?;
        Ok((Self { value }, 8))
    }
}

/// Payload with a discriminant. Wire size: schema(4) + disc(1) +
/// issuer(32) + value(8) + sig(64) = 109 bytes.
///
/// Shares `SCHEMA` with [`OtherTaggedPayload`] but has a different
/// discriminant — so a `Signed<TaggedPayload>` must not decode as a
/// `Signed<OtherTaggedPayload>` and vice versa.
#[derive(Debug, Clone, Copy, PartialEq, Eq, arbitrary::Arbitrary)]
struct TaggedPayload {
    value: u64,
}

impl Schema for TaggedPayload {
    const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
    const TYPE_BYTE: u8 = b'2';
    const VERSION: u8 = 0;
    const DISCRIMINANT: Option<u8> = Some(0xAA);
}

impl EncodeFields for TaggedPayload {
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        encode::u64(self.value, buf);
    }

    fn fields_size(&self) -> usize {
        8
    }
}

impl DecodeFields for TaggedPayload {
    const MIN_SIGNED_SIZE: usize = SCHEMA_SIZE + 1 + VERIFYING_KEY_SIZE + 8 + SIGNATURE_SIZE;

    fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let value = decode::u64(buf, 0)?;
        Ok((Self { value }, 8))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, arbitrary::Arbitrary)]
struct OtherTaggedPayload {
    value: u64,
}

impl Schema for OtherTaggedPayload {
    const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
    const TYPE_BYTE: u8 = b'2'; // same SCHEMA as TaggedPayload
    const VERSION: u8 = 0;
    const DISCRIMINANT: Option<u8> = Some(0xBB); // different discriminant
}

impl EncodeFields for OtherTaggedPayload {
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        encode::u64(self.value, buf);
    }

    fn fields_size(&self) -> usize {
        8
    }
}

impl DecodeFields for OtherTaggedPayload {
    const MIN_SIGNED_SIZE: usize = SCHEMA_SIZE + 1 + VERIFYING_KEY_SIZE + 8 + SIGNATURE_SIZE;

    fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let value = decode::u64(buf, 0)?;
        Ok((Self { value }, 8))
    }
}

/// Variable-length payload exercising the case where
/// `try_decode_fields` returns a `consumed` value that depends on the
/// payload contents. This is the trickiest case for
/// `Signed::try_decode`'s truncation logic.
///
/// Wire format: `len(2)` (u16 BE) + `data[len]`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VariablePayload {
    data: Vec<u8>,
}

impl<'a> arbitrary::Arbitrary<'a> for VariablePayload {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Cap the length to keep test runs reasonable.
        let len = u.int_in_range(0..=512_u32)? as usize;
        let mut data = Vec::with_capacity(len);
        for _ in 0..len {
            data.push(u.arbitrary()?);
        }
        Ok(Self { data })
    }
}

impl Schema for VariablePayload {
    const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
    const TYPE_BYTE: u8 = b'3';
    const VERSION: u8 = 0;
}

impl EncodeFields for VariablePayload {
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        #[allow(clippy::cast_possible_truncation)]
        encode::u16(self.data.len() as u16, buf);
        buf.extend_from_slice(&self.data);
    }

    fn fields_size(&self) -> usize {
        2 + self.data.len()
    }
}

impl DecodeFields for VariablePayload {
    const MIN_SIGNED_SIZE: usize = SCHEMA_SIZE + VERIFYING_KEY_SIZE + 2 + SIGNATURE_SIZE;

    fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let len = decode::u16(buf, 0)? as usize;
        let payload_start = 2;
        let payload_end = payload_start + len;
        if buf.len() < payload_end {
            return Err(DecodeError::MessageTooShort {
                type_name: "VariablePayload",
                need: payload_end,
                have: buf.len(),
            });
        }
        let data = buf[payload_start..payload_end].to_vec();
        Ok((Self { data }, payload_end))
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Synchronously seal a payload (mirrors `Signed::seal` internals
/// without requiring a tokio runtime in proptests).
fn seal_sync<T: Schema + EncodeFields + DecodeFields>(key_bytes: [u8; 32], payload: &T) -> Vec<u8> {
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let issuer = signing_key.verifying_key();

    let disc_size = usize::from(T::DISCRIMINANT.is_some());
    let total_size =
        SCHEMA_SIZE + disc_size + VERIFYING_KEY_SIZE + payload.fields_size() + SIGNATURE_SIZE;
    let mut bytes = Vec::with_capacity(total_size);

    bytes.extend_from_slice(&T::SCHEMA);
    if let Some(disc) = T::DISCRIMINANT {
        bytes.push(disc);
    }
    bytes.extend_from_slice(issuer.as_bytes());
    payload.encode_fields(&mut bytes);

    let signature = signing_key.sign(&bytes);
    bytes.extend_from_slice(&signature.to_bytes());

    debug_assert_eq!(bytes.len(), total_size);
    bytes
}

// ── Round-trip ────────────────────────────────────────────────────────

#[test]
fn plain_round_trip_seal_decode_verify() {
    bolero::check!()
        .with_arbitrary::<(PlainPayload, [u8; 32])>()
        .for_each(|(payload, key_bytes)| {
            let bytes = seal_sync(*key_bytes, payload);
            let signed = Signed::<PlainPayload>::try_decode(bytes).expect("decode");
            let verified = signed.try_verify().expect("verify");
            assert_eq!(verified.payload(), payload);
        });
}

#[test]
fn tagged_round_trip_seal_decode_verify() {
    bolero::check!()
        .with_arbitrary::<(TaggedPayload, [u8; 32])>()
        .for_each(|(payload, key_bytes)| {
            let bytes = seal_sync(*key_bytes, payload);
            let signed = Signed::<TaggedPayload>::try_decode(bytes).expect("decode");
            let verified = signed.try_verify().expect("verify");
            assert_eq!(verified.payload(), payload);
        });
}

#[test]
fn variable_round_trip_seal_decode_verify() {
    bolero::check!()
        .with_arbitrary::<(VariablePayload, [u8; 32])>()
        .for_each(|(payload, key_bytes)| {
            let bytes = seal_sync(*key_bytes, payload);
            let signed = Signed::<VariablePayload>::try_decode(bytes).expect("decode");
            let verified = signed.try_verify().expect("verify");
            assert_eq!(verified.payload(), payload);
        });
}

// ── Byte preservation through try_decode ──────────────────────────────
//
// The most important invariant: `try_decode` must store *exactly* the
// canonical signed bytes. If it truncates too aggressively, the
// signature appears at the wrong place and `try_verify` fails. If
// it truncates not enough, downstream parsers (e.g., a sequential
// `SyncMessage` decoder) get out of sync.

#[test]
fn plain_decode_preserves_canonical_bytes() {
    bolero::check!()
        .with_arbitrary::<(PlainPayload, [u8; 32])>()
        .for_each(|(payload, key_bytes)| {
            let original = seal_sync(*key_bytes, payload);
            let signed = Signed::<PlainPayload>::try_decode(original.clone()).expect("decode");
            assert_eq!(signed.as_bytes(), original.as_slice());
        });
}

#[test]
fn tagged_decode_preserves_canonical_bytes() {
    bolero::check!()
        .with_arbitrary::<(TaggedPayload, [u8; 32])>()
        .for_each(|(payload, key_bytes)| {
            let original = seal_sync(*key_bytes, payload);
            let signed = Signed::<TaggedPayload>::try_decode(original.clone()).expect("decode");
            assert_eq!(signed.as_bytes(), original.as_slice());
        });
}

#[test]
fn variable_decode_preserves_canonical_bytes() {
    bolero::check!()
        .with_arbitrary::<(VariablePayload, [u8; 32])>()
        .for_each(|(payload, key_bytes)| {
            let original = seal_sync(*key_bytes, payload);
            let signed = Signed::<VariablePayload>::try_decode(original.clone()).expect("decode");
            assert_eq!(signed.as_bytes(), original.as_slice());
        });
}

// ── Trailing-byte truncation ──────────────────────────────────────────
//
// `Signed::try_decode` must handle a buffer with *trailing bytes*
// beyond the canonical signed message — it should truncate to the
// canonical length and verification should still succeed. This is
// what `SyncMessage::LooseCommit` relies on: the `Signed<LooseCommit>`
// is followed by a blob, and `try_decode` is called on the full
// payload buffer.

#[test]
fn plain_decode_truncates_trailing_bytes() {
    bolero::check!()
        .with_arbitrary::<(PlainPayload, [u8; 32], Vec<u8>)>()
        .for_each(|(payload, key_bytes, trailing)| {
            let canonical = seal_sync(*key_bytes, payload);
            let canonical_len = canonical.len();
            let mut with_trailing = canonical.clone();
            with_trailing.extend_from_slice(trailing);

            let signed =
                Signed::<PlainPayload>::try_decode(with_trailing).expect("decode with trailing");
            assert_eq!(
                signed.as_bytes().len(),
                canonical_len,
                "truncation must produce canonical-length bytes"
            );
            assert_eq!(signed.as_bytes(), &canonical[..]);
            // Verification must still succeed.
            signed.try_verify().expect("verify after truncation");
        });
}

#[test]
fn tagged_decode_truncates_trailing_bytes() {
    bolero::check!()
        .with_arbitrary::<(TaggedPayload, [u8; 32], Vec<u8>)>()
        .for_each(|(payload, key_bytes, trailing)| {
            let canonical = seal_sync(*key_bytes, payload);
            let canonical_len = canonical.len();
            let mut with_trailing = canonical.clone();
            with_trailing.extend_from_slice(trailing);

            let signed =
                Signed::<TaggedPayload>::try_decode(with_trailing).expect("decode with trailing");
            assert_eq!(signed.as_bytes().len(), canonical_len);
            signed.try_verify().expect("verify after truncation");
        });
}

#[test]
fn variable_decode_truncates_trailing_bytes() {
    bolero::check!()
        .with_arbitrary::<(VariablePayload, [u8; 32], Vec<u8>)>()
        .for_each(|(payload, key_bytes, trailing)| {
            let canonical = seal_sync(*key_bytes, payload);
            let canonical_len = canonical.len();
            let mut with_trailing = canonical.clone();
            with_trailing.extend_from_slice(trailing);

            let signed =
                Signed::<VariablePayload>::try_decode(with_trailing).expect("decode with trailing");
            assert_eq!(signed.as_bytes().len(), canonical_len);
            signed.try_verify().expect("verify after truncation");
        });
}

// ── Truncation robustness ─────────────────────────────────────────────
//
// Dropping bytes from the *end* of a canonical encoding must yield a
// structured error — never a panic.

#[test]
fn plain_decode_truncated_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(PlainPayload, [u8; 32], u8)>()
        .for_each(|(payload, key_bytes, drop_count)| {
            let mut bytes = seal_sync(*key_bytes, payload);
            let new_len = bytes.len().saturating_sub(*drop_count as usize);
            bytes.truncate(new_len);
            let _result = Signed::<PlainPayload>::try_decode(bytes);
        });
}

#[test]
fn tagged_decode_truncated_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(TaggedPayload, [u8; 32], u8)>()
        .for_each(|(payload, key_bytes, drop_count)| {
            let mut bytes = seal_sync(*key_bytes, payload);
            let new_len = bytes.len().saturating_sub(*drop_count as usize);
            bytes.truncate(new_len);
            let _result = Signed::<TaggedPayload>::try_decode(bytes);
        });
}

#[test]
fn variable_decode_truncated_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(VariablePayload, [u8; 32], u8)>()
        .for_each(|(payload, key_bytes, drop_count)| {
            let mut bytes = seal_sync(*key_bytes, payload);
            let new_len = bytes.len().saturating_sub(*drop_count as usize);
            bytes.truncate(new_len);
            let _result = Signed::<VariablePayload>::try_decode(bytes);
        });
}

// ── Random-bytes never-panic ──────────────────────────────────────────

#[test]
fn random_bytes_through_plain_signed_never_panics() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = Signed::<PlainPayload>::try_decode(bytes.clone());
        });
}

#[test]
fn random_bytes_through_tagged_signed_never_panics() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = Signed::<TaggedPayload>::try_decode(bytes.clone());
        });
}

#[test]
fn random_bytes_through_variable_signed_never_panics() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = Signed::<VariablePayload>::try_decode(bytes.clone());
        });
}

// ── Schema tampering ──────────────────────────────────────────────────

/// Replacing the schema header with anything other than the expected
/// 4-byte sequence yields `InvalidSchema`.
#[test]
fn plain_schema_tamper_yields_invalid_schema() {
    bolero::check!()
        .with_arbitrary::<(PlainPayload, [u8; 32], [u8; 4])>()
        .for_each(|(payload, key_bytes, bad_schema)| {
            // Skip the case where the bad schema happens to be valid.
            if bad_schema == &PlainPayload::SCHEMA {
                return;
            }
            let mut bytes = seal_sync(*key_bytes, payload);
            bytes[..4].copy_from_slice(bad_schema);
            let result = Signed::<PlainPayload>::try_decode(bytes);
            assert!(
                matches!(
                    result,
                    Err(DecodeError::InvalidSchema(InvalidSchema { .. }))
                ),
                "expected InvalidSchema, got {result:?}"
            );
        });
}

/// Replacing the discriminant byte with anything other than the
/// expected value yields `InvalidDiscriminant`.
#[test]
fn tagged_discriminant_tamper_yields_invalid_discriminant() {
    bolero::check!()
        .with_arbitrary::<(TaggedPayload, [u8; 32], u8)>()
        .for_each(|(payload, key_bytes, bad_disc)| {
            if Some(*bad_disc) == TaggedPayload::DISCRIMINANT {
                return;
            }
            let mut bytes = seal_sync(*key_bytes, payload);
            bytes[SCHEMA_SIZE] = *bad_disc;
            let result = Signed::<TaggedPayload>::try_decode(bytes);
            assert!(
                matches!(
                    result,
                    Err(DecodeError::InvalidDiscriminant(InvalidDiscriminant { .. }))
                ),
                "expected InvalidDiscriminant, got {result:?}"
            );
        });
}

/// Bytes signed as `TaggedPayload` (`disc=0xAA`) must not decode as
/// `OtherTaggedPayload` (`disc=0xBB`) — the schema bytes are
/// identical but the discriminant differs. This is the tag-swap
/// attack the discriminant defends against.
#[test]
fn tag_swap_attack_is_rejected() {
    bolero::check!()
        .with_arbitrary::<([u8; 32], u64)>()
        .for_each(|(key_bytes, value)| {
            let payload = TaggedPayload { value: *value };
            let bytes = seal_sync(*key_bytes, &payload);

            // Sanity check: the byte at SCHEMA_SIZE is the
            // TaggedPayload discriminant.
            assert_eq!(bytes[SCHEMA_SIZE], 0xAA);

            // Decoding as OtherTaggedPayload must reject due to
            // discriminant mismatch.
            let result = Signed::<OtherTaggedPayload>::try_decode(bytes);
            assert!(
                matches!(
                    result,
                    Err(DecodeError::InvalidDiscriminant(InvalidDiscriminant { .. }))
                ),
                "tag-swap must be rejected by discriminant check, got {result:?}"
            );
        });
}

// ── Bit-flip tampering ────────────────────────────────────────────────
//
// Any single bit-flip in the signed region must cause verification
// to fail. (Or, if the flip is inside the schema/discriminant, decode
// fails earlier.) Either outcome is fine — what we forbid is a
// successful verification of tampered bytes.

#[test]
fn plain_single_bit_flip_breaks_verification() {
    bolero::check!()
        .with_arbitrary::<(PlainPayload, [u8; 32], u32, u8)>()
        .for_each(|(payload, key_bytes, byte_idx_seed, bit_idx_seed)| {
            let mut bytes = seal_sync(*key_bytes, payload);
            if bytes.is_empty() {
                return;
            }
            let byte_idx = (*byte_idx_seed as usize) % bytes.len();
            let bit_idx = bit_idx_seed % 8;
            bytes[byte_idx] ^= 1 << bit_idx;

            // Either decode fails or verify fails — never both succeed.
            // A decode failure is acceptable; we only assert on the Ok branch.
            if let Ok(signed) = Signed::<PlainPayload>::try_decode(bytes) {
                let verify_result = signed.try_verify();
                assert!(
                    verify_result.is_err(),
                    "tampered bytes must not verify (byte_idx={byte_idx}, bit_idx={bit_idx})"
                );
            }
        });
}

#[test]
fn tagged_single_bit_flip_breaks_verification() {
    bolero::check!()
        .with_arbitrary::<(TaggedPayload, [u8; 32], u32, u8)>()
        .for_each(|(payload, key_bytes, byte_idx_seed, bit_idx_seed)| {
            let mut bytes = seal_sync(*key_bytes, payload);
            if bytes.is_empty() {
                return;
            }
            let byte_idx = (*byte_idx_seed as usize) % bytes.len();
            let bit_idx = bit_idx_seed % 8;
            bytes[byte_idx] ^= 1 << bit_idx;

            if let Ok(signed) = Signed::<TaggedPayload>::try_decode(bytes) {
                let verify_result = signed.try_verify();
                assert!(verify_result.is_err(), "tampered bytes must not verify");
            }
        });
}

#[test]
fn variable_single_bit_flip_breaks_verification() {
    bolero::check!()
        .with_arbitrary::<(VariablePayload, [u8; 32], u32, u8)>()
        .for_each(|(payload, key_bytes, byte_idx_seed, bit_idx_seed)| {
            let mut bytes = seal_sync(*key_bytes, payload);
            if bytes.is_empty() {
                return;
            }
            let byte_idx = (*byte_idx_seed as usize) % bytes.len();
            let bit_idx = bit_idx_seed % 8;
            bytes[byte_idx] ^= 1 << bit_idx;

            if let Ok(signed) = Signed::<VariablePayload>::try_decode(bytes) {
                let verify_result = signed.try_verify();
                assert!(verify_result.is_err(), "tampered bytes must not verify");
            }
        });
}

// ── Wire-layout invariants ────────────────────────────────────────────

/// `as_bytes()`, `payload_bytes()`, `fields_bytes()`, and
/// `signature()` agree on positions: `payload_bytes` ⊆ `as_bytes`,
/// `fields_bytes` ⊆ `payload_bytes`, signature is at the end of `as_bytes`.
#[test]
fn wire_layout_slices_agree() {
    bolero::check!()
        .with_arbitrary::<(PlainPayload, [u8; 32])>()
        .for_each(|(payload, key_bytes)| {
            let bytes = seal_sync(*key_bytes, payload);
            let signed = Signed::<PlainPayload>::try_decode(bytes.clone()).expect("decode");

            assert_eq!(signed.as_bytes(), &bytes[..]);

            let payload_bytes = signed.payload_bytes();
            assert_eq!(payload_bytes.len(), bytes.len() - SIGNATURE_SIZE);
            assert_eq!(payload_bytes, &bytes[..bytes.len() - SIGNATURE_SIZE]);

            let fields_bytes = signed.fields_bytes();
            assert_eq!(fields_bytes.len(), payload.fields_size());
            assert_eq!(
                fields_bytes,
                &bytes[SCHEMA_SIZE + VERIFYING_KEY_SIZE..bytes.len() - SIGNATURE_SIZE]
            );

            let sig_bytes = signed.signature().to_bytes();
            assert_eq!(&sig_bytes[..], &bytes[bytes.len() - SIGNATURE_SIZE..]);
        });
}

#[test]
fn wire_layout_slices_agree_tagged() {
    bolero::check!()
        .with_arbitrary::<(TaggedPayload, [u8; 32])>()
        .for_each(|(payload, key_bytes)| {
            let bytes = seal_sync(*key_bytes, payload);
            let signed = Signed::<TaggedPayload>::try_decode(bytes.clone()).expect("decode");

            // For a tagged payload, fields starts after schema + disc
            // (1 byte) + issuer.
            let fields_bytes = signed.fields_bytes();
            let expected_start = SCHEMA_SIZE + 1 + VERIFYING_KEY_SIZE;
            let expected_end = bytes.len() - SIGNATURE_SIZE;
            assert_eq!(fields_bytes, &bytes[expected_start..expected_end]);
            assert_eq!(fields_bytes.len(), payload.fields_size());
        });
}
