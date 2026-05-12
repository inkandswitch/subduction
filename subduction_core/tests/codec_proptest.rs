//! Comprehensive property tests for the binary codec at the
//! `subduction_core` layer.
//!
//! This file covers four wire-level types:
//!
//! - [`Challenge`] — `Signed<Challenge>` with `SUH\x00` schema and
//!   discriminant `0x00`. Fields: `audience(33) + timestamp(8) + nonce(16)`.
//! - [`Response`] — `Signed<Response>` with `SUH\x00` schema and
//!   discriminant `0x01`. Fields: `challenge_digest(32) + timestamp(8)`.
//! - [`HandshakeMessage`] — outer envelope dispatching on the byte
//!   after the schema. `0x00` → `Signed<Challenge>`, `0x01` →
//!   `Signed<Response>`, `0x02` → unsigned `Rejection`.
//! - [`SyncMessage`] — outer envelope `SUM\x00 + total_size(u32) +
//!   tag(u8) + payload`. Nine variants.
//!
//! What we want to hold:
//!
//! 1. **Round-trip** for every shape.
//! 2. **No panics** on arbitrary bytes for every decoder.
//! 3. **Truncation** at any prefix length yields a structured error.
//! 4. **Trailing bytes** are rejected by `SyncMessage` (strict
//!    `total_size == bytes.len()` check) and by control-tag handshake
//!    messages, but truncated by `Signed<T>`.
//! 5. **Schema/tag tampering** yields `InvalidSchema` or
//!    `InvalidEnumTag`, never panic, never silent acceptance.
//! 6. **Tag-swap protection**: bytes signed as `Challenge` (disc =
//!    0x00) must not decode as `Response` (disc = 0x01).

#![cfg(feature = "bolero")]
#![allow(clippy::panic, clippy::expect_used, clippy::indexing_slicing)]

use std::vec::Vec;

use ed25519_dalek::{Signer as _, SigningKey};
use sedimentree_core::codec::{
    decode::DecodeFields,
    encode::{Encode, EncodeFields},
    error::{DecodeError, InvalidDiscriminant, InvalidEnumTag, InvalidSchema, SizeMismatch},
    schema::Schema,
};
use subduction_core::{
    connection::message::{MESSAGE_SCHEMA, SyncMessage},
    handshake::{
        HandshakeMessage,
        challenge::Challenge,
        rejection::{Rejection, RejectionReason},
        response::Response,
    },
};
use subduction_crypto::signed::{SCHEMA_SIZE, SIGNATURE_SIZE, Signed, VERIFYING_KEY_SIZE};

// ── Helpers ────────────────────────────────────────────────────────────

/// Sign a `T` synchronously using an ed25519 key derived from the
/// given seed. This produces the canonical wire bytes and is what
/// `Signed::seal` does internally (just without the async/Signer
/// abstraction).
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

    bytes
}

// ════════════════════════════════════════════════════════════════════════
// Challenge
// ════════════════════════════════════════════════════════════════════════

#[test]
fn challenge_fields_round_trip() {
    bolero::check!()
        .with_arbitrary::<Challenge>()
        .for_each(|challenge| {
            let mut buf = Vec::new();
            challenge.encode_fields(&mut buf);
            match Challenge::try_decode_fields(&buf) {
                Ok((decoded, consumed)) => {
                    assert_eq!(&decoded, challenge);
                    assert_eq!(consumed, buf.len(), "consumed must equal canonical size");
                    assert_eq!(consumed, decoded.fields_size());
                }
                Err(e) => panic!("decode must succeed for valid Challenge bytes: {e}"),
            }
        });
}

#[test]
fn challenge_fields_size_matches_encoded_length() {
    bolero::check!()
        .with_arbitrary::<Challenge>()
        .for_each(|challenge| {
            let mut buf = Vec::new();
            challenge.encode_fields(&mut buf);
            assert_eq!(buf.len(), challenge.fields_size());
        });
}

#[test]
fn challenge_decode_fields_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = Challenge::try_decode_fields(bytes);
        });
}

#[test]
fn challenge_signed_round_trip() {
    bolero::check!()
        .with_arbitrary::<(Challenge, [u8; 32])>()
        .for_each(|(challenge, key_bytes)| {
            let bytes = seal_sync(*key_bytes, challenge);
            let signed = Signed::<Challenge>::try_decode(bytes).expect("decode");
            let verified = signed.try_verify().expect("verify");
            assert_eq!(verified.payload(), challenge);
        });
}

#[test]
fn challenge_signed_decode_random_bytes_no_panic() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = Signed::<Challenge>::try_decode(bytes.clone());
        });
}

/// A `Challenge`-shaped buffer with an out-of-range audience tag
/// (anything besides 0x00 or 0x01) must yield `InvalidEnumTag`.
#[test]
fn challenge_invalid_audience_tag_is_rejected() {
    bolero::check!()
        .with_arbitrary::<(Challenge, u8)>()
        .for_each(|(challenge, bad_tag)| {
            if *bad_tag == 0x00 || *bad_tag == 0x01 {
                return;
            }
            let mut buf = Vec::new();
            challenge.encode_fields(&mut buf);
            // Audience tag is the very first byte of the fields.
            buf[0] = *bad_tag;
            let result = Challenge::try_decode_fields(&buf);
            assert!(
                matches!(
                    result,
                    Err(DecodeError::InvalidEnumTag(InvalidEnumTag { .. }))
                ),
                "expected InvalidEnumTag for bad audience tag {bad_tag:#04x}, got {result:?}"
            );
        });
}

/// `Signed<Challenge>::try_decode` of bytes signed as `Response`
/// (same `SUH\x00` schema, different discriminant) must reject.
///
/// The exact error depends on size: `Response::MIN_SIGNED_SIZE` (141)
/// is smaller than `Challenge::MIN_SIGNED_SIZE` (158), so the early
/// length check fires before the discriminant check on real Response
/// bytes. Either error path is acceptable — what we forbid is a
/// successful decode.
#[test]
fn challenge_rejects_response_bytes() {
    bolero::check!()
        .with_arbitrary::<(Response, [u8; 32])>()
        .for_each(|(response, key_bytes)| {
            let bytes = seal_sync(*key_bytes, response);
            // Sanity: byte 4 should be Response::DISCRIMINANT (0x01).
            assert_eq!(bytes[SCHEMA_SIZE], 0x01);
            let result = Signed::<Challenge>::try_decode(bytes);
            assert!(
                matches!(
                    result,
                    Err(DecodeError::InvalidDiscriminant(InvalidDiscriminant { .. })
                        | DecodeError::MessageTooShort { .. })
                ),
                "expected InvalidDiscriminant or MessageTooShort when decoding Response bytes as Challenge, got {result:?}"
            );
        });
}

/// Stronger version: pad the Response bytes out to Challenge's
/// minimum size first, so we *do* hit the discriminant check.
#[test]
fn challenge_rejects_response_bytes_padded_via_discriminant() {
    bolero::check!()
        .with_arbitrary::<(Response, [u8; 32])>()
        .for_each(|(response, key_bytes)| {
            let mut bytes = seal_sync(*key_bytes, response);
            // Pad to Challenge::MIN_SIGNED_SIZE so the discriminant
            // check is reached before the length check fails.
            while bytes.len() < <Challenge as DecodeFields>::MIN_SIGNED_SIZE {
                bytes.push(0u8);
            }
            assert_eq!(bytes[SCHEMA_SIZE], 0x01);
            let result = Signed::<Challenge>::try_decode(bytes);
            assert!(
                matches!(
                    result,
                    Err(DecodeError::InvalidDiscriminant(InvalidDiscriminant { .. }))
                ),
                "expected InvalidDiscriminant after padding, got {result:?}"
            );
        });
}

#[test]
fn challenge_signed_truncation_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(Challenge, [u8; 32], u8)>()
        .for_each(|(challenge, key_bytes, drop_count)| {
            let mut bytes = seal_sync(*key_bytes, challenge);
            let new_len = bytes.len().saturating_sub(*drop_count as usize);
            bytes.truncate(new_len);
            let _result = Signed::<Challenge>::try_decode(bytes);
        });
}

#[test]
fn challenge_signed_bit_flip_breaks_verification() {
    bolero::check!()
        .with_arbitrary::<(Challenge, [u8; 32], u32, u8)>()
        .for_each(|(challenge, key_bytes, byte_idx_seed, bit_idx_seed)| {
            let mut bytes = seal_sync(*key_bytes, challenge);
            if bytes.is_empty() {
                return;
            }
            let byte_idx = (*byte_idx_seed as usize) % bytes.len();
            let bit_idx = bit_idx_seed % 8;
            bytes[byte_idx] ^= 1 << bit_idx;

            if let Ok(signed) = Signed::<Challenge>::try_decode(bytes) {
                let verify = signed.try_verify();
                assert!(verify.is_err(), "tampered bytes must not verify");
            }
        });
}

// ════════════════════════════════════════════════════════════════════════
// Response
// ════════════════════════════════════════════════════════════════════════

#[test]
fn response_fields_round_trip() {
    bolero::check!()
        .with_arbitrary::<Response>()
        .for_each(|response| {
            let mut buf = Vec::new();
            response.encode_fields(&mut buf);
            match Response::try_decode_fields(&buf) {
                Ok((decoded, consumed)) => {
                    assert_eq!(&decoded, response);
                    assert_eq!(consumed, buf.len());
                    assert_eq!(consumed, decoded.fields_size());
                }
                Err(e) => panic!("decode must succeed for valid Response bytes: {e}"),
            }
        });
}

#[test]
fn response_fields_size_matches_encoded_length() {
    bolero::check!()
        .with_arbitrary::<Response>()
        .for_each(|response| {
            let mut buf = Vec::new();
            response.encode_fields(&mut buf);
            assert_eq!(buf.len(), response.fields_size());
        });
}

#[test]
fn response_decode_fields_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = Response::try_decode_fields(bytes);
        });
}

#[test]
fn response_signed_round_trip() {
    bolero::check!()
        .with_arbitrary::<(Response, [u8; 32])>()
        .for_each(|(response, key_bytes)| {
            let bytes = seal_sync(*key_bytes, response);
            let signed = Signed::<Response>::try_decode(bytes).expect("decode");
            let verified = signed.try_verify().expect("verify");
            assert_eq!(verified.payload(), response);
        });
}

#[test]
fn response_signed_decode_random_bytes_no_panic() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = Signed::<Response>::try_decode(bytes.clone());
        });
}

/// Decoding Challenge bytes as Response must always reject. Because
/// Challenge bytes are *larger* than `Response::MIN_SIGNED_SIZE`, the
/// discriminant check is always reached.
#[test]
fn response_rejects_challenge_bytes_via_discriminant() {
    bolero::check!()
        .with_arbitrary::<(Challenge, [u8; 32])>()
        .for_each(|(challenge, key_bytes)| {
            let bytes = seal_sync(*key_bytes, challenge);
            assert_eq!(bytes[SCHEMA_SIZE], 0x00);
            let result = Signed::<Response>::try_decode(bytes);
            assert!(
                matches!(
                    result,
                    Err(DecodeError::InvalidDiscriminant(InvalidDiscriminant { .. }))
                ),
                "expected InvalidDiscriminant when decoding Challenge bytes as Response, got {result:?}"
            );
        });
}

// ════════════════════════════════════════════════════════════════════════
// HandshakeMessage envelope
// ════════════════════════════════════════════════════════════════════════

/// Decoding wire bytes of a sealed `Challenge` must produce
/// `HandshakeMessage::SignedChallenge`.
#[test]
fn handshake_decodes_signed_challenge() {
    bolero::check!()
        .with_arbitrary::<(Challenge, [u8; 32])>()
        .for_each(|(challenge, key_bytes)| {
            let bytes = seal_sync(*key_bytes, challenge);
            match HandshakeMessage::try_decode(&bytes) {
                Ok(HandshakeMessage::SignedChallenge(signed)) => {
                    let verified = signed.try_verify().expect("verify");
                    assert_eq!(verified.payload(), challenge);
                }
                Ok(other) => panic!("expected SignedChallenge, got {other:?}"),
                Err(e) => panic!("decode must succeed: {e}"),
            }
        });
}

#[test]
fn handshake_decodes_signed_response() {
    bolero::check!()
        .with_arbitrary::<(Response, [u8; 32])>()
        .for_each(|(response, key_bytes)| {
            let bytes = seal_sync(*key_bytes, response);
            match HandshakeMessage::try_decode(&bytes) {
                Ok(HandshakeMessage::SignedResponse(signed)) => {
                    let verified = signed.try_verify().expect("verify");
                    assert_eq!(verified.payload(), response);
                }
                Ok(other) => panic!("expected SignedResponse, got {other:?}"),
                Err(e) => panic!("decode must succeed: {e}"),
            }
        });
}

#[test]
fn handshake_decodes_rejection_round_trip() {
    bolero::check!()
        .with_arbitrary::<Rejection>()
        .for_each(|rejection| {
            let msg = HandshakeMessage::Rejection(*rejection);
            let bytes = msg.encode();
            match HandshakeMessage::try_decode(&bytes) {
                Ok(HandshakeMessage::Rejection(decoded)) => {
                    assert_eq!(&decoded, rejection);
                }
                Ok(other) => panic!("expected Rejection, got {other:?}"),
                Err(e) => panic!("decode must succeed for canonical rejection: {e}"),
            }
        });
}

#[test]
fn handshake_random_bytes_never_panic() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = HandshakeMessage::try_decode(bytes);
        });
}

/// An envelope whose 4-byte schema isn't `SUH\x00` is rejected as
/// `InvalidSchema`.
#[test]
fn handshake_wrong_schema_rejected() {
    bolero::check!()
        .with_arbitrary::<(Vec<u8>, [u8; 4])>()
        .for_each(|(payload, bad_schema)| {
            if bad_schema == &Challenge::SCHEMA {
                return;
            }
            let mut bytes = Vec::with_capacity(4 + payload.len());
            bytes.extend_from_slice(bad_schema);
            bytes.extend_from_slice(payload);
            // Need at least 5 bytes to reach the tag-validation step.
            if bytes.len() < 5 {
                bytes.resize(5, 0);
            }

            let result = HandshakeMessage::try_decode(&bytes);
            // Must error — the schema check fires before the tag check.
            assert!(
                matches!(result, Err(DecodeError::InvalidSchema(_))),
                "wrong schema must yield InvalidSchema, got {result:?}"
            );
        });
}

/// A `SUH\x00`-prefixed envelope with an unknown tag byte (not
/// 0x00/0x01/0x02) yields `InvalidEnumTag`.
#[test]
fn handshake_unknown_tag_rejected() {
    bolero::check!().with_arbitrary::<u8>().for_each(|bad_tag| {
        if *bad_tag == 0x00 || *bad_tag == 0x01 || *bad_tag == 0x02 {
            return;
        }
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&Challenge::SCHEMA);
        bytes.push(*bad_tag);

        let result = HandshakeMessage::try_decode(&bytes);
        assert!(
            matches!(result, Err(DecodeError::InvalidEnumTag(_))),
            "unknown tag {bad_tag:#04x} must yield InvalidEnumTag, got {result:?}"
        );
    });
}

#[test]
fn handshake_truncation_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(Challenge, [u8; 32], u8)>()
        .for_each(|(challenge, key_bytes, drop_count)| {
            let mut bytes = seal_sync(*key_bytes, challenge);
            let new_len = bytes.len().saturating_sub(*drop_count as usize);
            bytes.truncate(new_len);
            let _result = HandshakeMessage::try_decode(&bytes);
        });
}

// ── Rejection ──────────────────────────────────────────────────────────

/// Every `RejectionReason` variant decodes from its canonical tag
/// byte.
#[test]
fn rejection_reason_round_trip_all_variants() {
    use std::convert::TryFrom;
    for tag in 0u8..=3u8 {
        let parsed = RejectionReason::try_from(tag).expect("known tag");
        assert_eq!(parsed as u8, tag);
    }
}

#[test]
fn rejection_reason_unknown_tag_rejected() {
    use std::convert::TryFrom;
    bolero::check!().with_arbitrary::<u8>().for_each(|tag| {
        if *tag <= 3 {
            return;
        }
        let result = RejectionReason::try_from(*tag);
        assert!(result.is_err(), "tag {tag} must be rejected");
    });
}

// ════════════════════════════════════════════════════════════════════════
// SyncMessage
// ════════════════════════════════════════════════════════════════════════
//
// SyncMessage is the workhorse of the sync protocol. It has 9 variants,
// embeds `Signed<LooseCommit>` and `Signed<Fragment>` byte-for-byte,
// and uses bijou64 for blob-length prefixes. Its decoder is the most
// complex consumer of the codec.

#[test]
fn sync_message_round_trip() {
    bolero::check!()
        .with_arbitrary::<SyncMessage>()
        .for_each(|msg| {
            let encoded = msg.encode();
            let decoded = SyncMessage::try_decode(&encoded)
                .expect("round-trip decode of arbitrary SyncMessage must succeed");
            assert_eq!(msg, &decoded);
        });
}

#[test]
fn sync_message_encoded_size_matches_actual() {
    bolero::check!()
        .with_arbitrary::<SyncMessage>()
        .for_each(|msg| {
            let encoded = msg.encode();
            assert_eq!(encoded.len(), msg.encoded_size());
        });
}

#[test]
fn sync_message_decode_random_bytes_never_panics() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = SyncMessage::try_decode(bytes);
        });
}

#[test]
fn sync_message_truncation_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(SyncMessage, u32)>()
        .for_each(|(msg, drop_count)| {
            let mut encoded = msg.encode();
            let new_len = encoded.len().saturating_sub(*drop_count as usize);
            encoded.truncate(new_len);
            let _result = SyncMessage::try_decode(&encoded);
        });
}

/// Single-bit-flip anywhere in the message must not panic.
#[test]
fn sync_message_single_bit_flip_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(SyncMessage, u32, u8)>()
        .for_each(|(msg, byte_idx_seed, bit_idx_seed)| {
            let mut encoded = msg.encode();
            if encoded.is_empty() {
                return;
            }
            let byte_idx = (*byte_idx_seed as usize) % encoded.len();
            let bit_idx = bit_idx_seed % 8;
            encoded[byte_idx] ^= 1 << bit_idx;
            let _result = SyncMessage::try_decode(&encoded);
        });
}

/// Trailing bytes after a canonical `SyncMessage` are rejected via
/// `SizeMismatch`. (This is *stricter* than `Signed::try_decode`,
/// which truncates trailing bytes.)
#[test]
fn sync_message_trailing_bytes_rejected() {
    bolero::check!()
        .with_arbitrary::<(SyncMessage, Vec<u8>)>()
        .for_each(|(msg, trailing)| {
            if trailing.is_empty() {
                return;
            }
            let mut encoded = msg.encode();
            encoded.extend_from_slice(trailing);

            let result = SyncMessage::try_decode(&encoded);
            assert!(
                matches!(result, Err(DecodeError::SizeMismatch(SizeMismatch { .. }))),
                "expected SizeMismatch for SyncMessage with trailing bytes, got {result:?}"
            );
        });
}

#[test]
fn sync_message_wrong_schema_rejected() {
    bolero::check!()
        .with_arbitrary::<(SyncMessage, [u8; 4])>()
        .for_each(|(msg, bad_schema)| {
            if bad_schema == &MESSAGE_SCHEMA {
                return;
            }
            let mut encoded = msg.encode();
            encoded[..4].copy_from_slice(bad_schema);
            let result = SyncMessage::try_decode(&encoded);
            assert!(
                matches!(
                    result,
                    Err(DecodeError::InvalidSchema(InvalidSchema { .. }))
                ),
                "wrong schema must yield InvalidSchema, got {result:?}"
            );
        });
}

/// A `SUM\x00`-prefixed envelope with `total_size` corrupted to a
/// value that doesn't match the buffer length yields `SizeMismatch`.
#[test]
fn sync_message_corrupted_total_size_rejected() {
    bolero::check!()
        .with_arbitrary::<(SyncMessage, u32)>()
        .for_each(|(msg, fake_total_size)| {
            let mut encoded = msg.encode();
            // Skip the case where the fake size happens to be valid.
            #[allow(clippy::cast_possible_truncation)]
            if (*fake_total_size as usize) == encoded.len() {
                return;
            }
            encoded[4..8].copy_from_slice(&fake_total_size.to_be_bytes());
            let result = SyncMessage::try_decode(&encoded);
            // Either SizeMismatch (the size doesn't match) or some
            // downstream parsing error from interpreting the now-wrong
            // tag byte. Whatever — must not panic, must not succeed.
            //
            // If by sheer luck the decoder still succeeded (because the
            // corrupted total_size matches what a valid prefix would
            // describe), it must at least round-trip back to bytes equal
            // to the canonical form. But this is a real edge case;
            // usually it errors. When it doesn't round-trip, the decoder
            // accepted bytes it shouldn't have — flag for review.
            if let Ok(decoded) = result {
                let re_encoded = decoded.encode();
                assert_eq!(
                    re_encoded, encoded,
                    "decoder accepted corrupted total_size {fake_total_size} \
                     but re-encoding produces different bytes"
                );
            }
        });
}

/// A `SUM\x00`-prefixed envelope with a tag byte outside the
/// supported range (0x00–0x08) yields `InvalidEnumTag`.
#[test]
fn sync_message_unknown_tag_rejected() {
    bolero::check!().with_arbitrary::<u8>().for_each(|bad_tag| {
        if *bad_tag <= 0x08 {
            return;
        }
        let mut bytes = Vec::with_capacity(9);
        bytes.extend_from_slice(&MESSAGE_SCHEMA);
        bytes.extend_from_slice(&9u32.to_be_bytes()); // total_size
        bytes.push(*bad_tag);
        // No payload — this triggers the tag validation before
        // any payload decoding.

        let result = SyncMessage::try_decode(&bytes);
        assert!(
            matches!(result, Err(DecodeError::InvalidEnumTag(_))),
            "unknown tag {bad_tag:#04x} must yield InvalidEnumTag, got {result:?}"
        );
    });
}
