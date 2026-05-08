//! Comprehensive property tests for the ephemeral message codec.
//!
//! `subduction_ephemeral` exposes two wire-level types:
//!
//! - [`EphemeralPayload`] — `Signed<EphemeralPayload>` with `SUE\x00`
//!   schema and discriminant `0x00`. Fields: `topic(32) + nonce(8) +
//!   timestamp(8) + bijou64(payload_len) + payload[len]`. The blob
//!   length prefix is `bijou64`-encoded, so this is the most
//!   variable-length-aware codec in the system.
//! - [`EphemeralMessage`] — outer envelope dispatching on the byte
//!   after the schema. `0x00` → `Signed<EphemeralPayload>` (the bytes
//!   *are* the Signed wire format), `0x01`–`0x03` → unsigned topic
//!   list control messages.
//!
//! What we want to hold:
//!
//! 1. **Round-trip** for every shape, including empty payloads and
//!    payloads at bijou64 tier boundaries.
//! 2. **No panics** on arbitrary bytes.
//! 3. **Truncation** at any prefix length yields a structured error.
//! 4. **Trailing bytes** are rejected by control-tag messages
//!    (`SizeMismatch`) but truncated by `Signed::try_decode`.
//! 5. **Schema/tag tampering** yields `InvalidSchema` /
//!    `InvalidEnumTag`, never panic.
//! 6. **Empty topic list** is rejected (the [`NonEmpty`] invariant).

#![cfg(feature = "bolero")]
#![allow(clippy::panic, clippy::expect_used, clippy::indexing_slicing)]

use std::vec::Vec;

use ed25519_dalek::{Signer as _, SigningKey};
use nonempty::NonEmpty;
use sedimentree_core::codec::{
    decode::{Decode, DecodeFields},
    encode::{Encode, EncodeFields},
    error::{DecodeError, InvalidSchema, SizeMismatch},
    schema::Schema,
};
use subduction_core::timestamp::TimestampSeconds;
use subduction_crypto::signed::{Signed, SCHEMA_SIZE, SIGNATURE_SIZE, VERIFYING_KEY_SIZE};
use subduction_ephemeral::{
    message::{EphemeralMessage, EphemeralPayload, EPHEMERAL_SCHEMA},
    topic::Topic,
};

// ── Generators ────────────────────────────────────────────────────────
//
// EphemeralPayload doesn't currently derive Arbitrary. Build it from
// arbitrary primitives directly. The payload length is bounded so
// proptests don't degenerate into huge buffers.

#[derive(Debug, Clone, arbitrary::Arbitrary)]
struct GenInputs {
    topic_bytes: [u8; 32],
    nonce: u64,
    /// Capped at 1024 by the payload generator to keep test runs fast.
    timestamp_secs: u64,
    /// Bounded payload to keep proptests fast. We cover the full range
    /// of bijou64 encoded lengths (1–9 bytes) at lengths 0..255 and
    /// 248..1024 — the tier-0/tier-1 boundary is 248.
    payload_seed: u16,
    payload_filler: u8,
}

fn build_payload(inputs: &GenInputs) -> EphemeralPayload {
    let len = (inputs.payload_seed as usize) % 1024;
    EphemeralPayload {
        id: Topic::new(inputs.topic_bytes),
        nonce: inputs.nonce,
        timestamp: TimestampSeconds::new(inputs.timestamp_secs),
        payload: vec![inputs.payload_filler; len],
    }
}

fn seal_sync<T: Schema + EncodeFields + DecodeFields>(
    key_bytes: [u8; 32],
    payload: &T,
) -> Vec<u8> {
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
// EphemeralPayload (the signable inner type)
// ════════════════════════════════════════════════════════════════════════

#[test]
fn ephemeral_payload_fields_round_trip() {
    bolero::check!()
        .with_arbitrary::<GenInputs>()
        .for_each(|inputs| {
            let payload = build_payload(inputs);
            let mut buf = Vec::new();
            payload.encode_fields(&mut buf);
            match EphemeralPayload::try_decode_fields(&buf) {
                Ok((decoded, consumed)) => {
                    assert_eq!(decoded, payload);
                    assert_eq!(consumed, buf.len(), "consumed must equal canonical size");
                    assert_eq!(consumed, decoded.fields_size());
                }
                Err(e) => panic!("decode must succeed for valid EphemeralPayload bytes: {e}"),
            }
        });
}

#[test]
fn ephemeral_payload_fields_size_matches_encoded_length() {
    bolero::check!()
        .with_arbitrary::<GenInputs>()
        .for_each(|inputs| {
            let payload = build_payload(inputs);
            let mut buf = Vec::new();
            payload.encode_fields(&mut buf);
            assert_eq!(buf.len(), payload.fields_size());
        });
}

#[test]
fn ephemeral_payload_decode_fields_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = EphemeralPayload::try_decode_fields(bytes);
        });
}

#[test]
fn ephemeral_payload_round_trip_with_trailing_bytes() {
    bolero::check!()
        .with_arbitrary::<(GenInputs, Vec<u8>)>()
        .for_each(|(inputs, trailing)| {
            let payload = build_payload(inputs);
            let mut buf = Vec::new();
            payload.encode_fields(&mut buf);
            let canonical_len = buf.len();
            buf.extend_from_slice(trailing);

            match EphemeralPayload::try_decode_fields(&buf) {
                Ok((decoded, consumed)) => {
                    assert_eq!(decoded, payload);
                    assert_eq!(consumed, canonical_len);
                }
                Err(e) => panic!("decode must accept canonical bytes + trailing: {e}"),
            }
        });
}

#[test]
fn ephemeral_payload_truncation_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(GenInputs, u16)>()
        .for_each(|(inputs, drop_count)| {
            let payload = build_payload(inputs);
            let mut buf = Vec::new();
            payload.encode_fields(&mut buf);
            let new_len = buf.len().saturating_sub(*drop_count as usize);
            buf.truncate(new_len);
            let _result = EphemeralPayload::try_decode_fields(&buf);
        });
}

// ── Signed<EphemeralPayload> ──────────────────────────────────────────

#[test]
fn ephemeral_signed_round_trip() {
    bolero::check!()
        .with_arbitrary::<(GenInputs, [u8; 32])>()
        .for_each(|(inputs, key_bytes)| {
            let payload = build_payload(inputs);
            let bytes = seal_sync(*key_bytes, &payload);
            let signed = Signed::<EphemeralPayload>::try_decode(bytes).expect("decode");
            let verified = signed.try_verify().expect("verify");
            assert_eq!(verified.payload(), &payload);
        });
}

#[test]
fn ephemeral_signed_decode_random_bytes_no_panic() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = Signed::<EphemeralPayload>::try_decode(bytes.clone());
        });
}

#[test]
fn ephemeral_signed_truncates_trailing_bytes() {
    bolero::check!()
        .with_arbitrary::<(GenInputs, [u8; 32], Vec<u8>)>()
        .for_each(|(inputs, key_bytes, trailing)| {
            let payload = build_payload(inputs);
            let canonical = seal_sync(*key_bytes, &payload);
            let canonical_len = canonical.len();
            let mut with_trailing = canonical.clone();
            with_trailing.extend_from_slice(trailing);

            let signed = Signed::<EphemeralPayload>::try_decode(with_trailing)
                .expect("decode with trailing");
            assert_eq!(signed.as_bytes().len(), canonical_len);
            signed.try_verify().expect("verify after truncation");
        });
}

#[test]
fn ephemeral_signed_truncation_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(GenInputs, [u8; 32], u16)>()
        .for_each(|(inputs, key_bytes, drop_count)| {
            let payload = build_payload(inputs);
            let mut bytes = seal_sync(*key_bytes, &payload);
            let new_len = bytes.len().saturating_sub(*drop_count as usize);
            bytes.truncate(new_len);
            let _result = Signed::<EphemeralPayload>::try_decode(bytes);
        });
}

#[test]
fn ephemeral_signed_bit_flip_breaks_verification() {
    bolero::check!()
        .with_arbitrary::<(GenInputs, [u8; 32], u32, u8)>()
        .for_each(|(inputs, key_bytes, byte_idx_seed, bit_idx_seed)| {
            let payload = build_payload(inputs);
            let mut bytes = seal_sync(*key_bytes, &payload);
            if bytes.is_empty() {
                return;
            }
            let byte_idx = (*byte_idx_seed as usize) % bytes.len();
            let bit_idx = bit_idx_seed % 8;
            bytes[byte_idx] ^= 1 << bit_idx;

            if let Ok(signed) = Signed::<EphemeralPayload>::try_decode(bytes) {
                assert!(
                    signed.try_verify().is_err(),
                    "tampered Ephemeral bytes must not verify"
                );
            }
        });
}

// ════════════════════════════════════════════════════════════════════════
// EphemeralMessage envelope
// ════════════════════════════════════════════════════════════════════════

/// Wire bytes from a sealed `Signed<EphemeralPayload>` decode as
/// `EphemeralMessage::Ephemeral`.
#[test]
fn ephemeral_message_decodes_signed_ephemeral() {
    bolero::check!()
        .with_arbitrary::<(GenInputs, [u8; 32])>()
        .for_each(|(inputs, key_bytes)| {
            let payload = build_payload(inputs);
            let bytes = seal_sync(*key_bytes, &payload);
            match EphemeralMessage::try_decode(&bytes) {
                Ok(EphemeralMessage::Ephemeral(signed)) => {
                    let verified = signed.try_verify().expect("verify");
                    assert_eq!(verified.payload(), &payload);
                }
                Ok(other) => panic!("expected Ephemeral, got {other:?}"),
                Err(e) => panic!("decode must succeed: {e}"),
            }
        });
}

#[test]
fn ephemeral_message_subscribe_round_trip() {
    bolero::check!()
        .with_arbitrary::<(Vec<[u8; 32]>, [u8; 32])>()
        .for_each(|(extra_topics, head_topic)| {
            let mut topics = NonEmpty::new(Topic::new(*head_topic));
            for t in extra_topics {
                topics.push(Topic::new(*t));
            }
            // u16 limits the count.
            if topics.len() > u16::MAX as usize {
                return;
            }
            let msg = EphemeralMessage::Subscribe { topics };
            let encoded = msg.encode();
            let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");
            assert_eq!(decoded, msg);
        });
}

#[test]
fn ephemeral_message_unsubscribe_round_trip() {
    bolero::check!()
        .with_arbitrary::<(Vec<[u8; 32]>, [u8; 32])>()
        .for_each(|(extra_topics, head_topic)| {
            let mut topics = NonEmpty::new(Topic::new(*head_topic));
            for t in extra_topics {
                topics.push(Topic::new(*t));
            }
            if topics.len() > u16::MAX as usize {
                return;
            }
            let msg = EphemeralMessage::Unsubscribe { topics };
            let encoded = msg.encode();
            let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");
            assert_eq!(decoded, msg);
        });
}

#[test]
fn ephemeral_message_subscribe_rejected_round_trip() {
    bolero::check!()
        .with_arbitrary::<(Vec<[u8; 32]>, [u8; 32])>()
        .for_each(|(extra_topics, head_topic)| {
            let mut topics = NonEmpty::new(Topic::new(*head_topic));
            for t in extra_topics {
                topics.push(Topic::new(*t));
            }
            if topics.len() > u16::MAX as usize {
                return;
            }
            let msg = EphemeralMessage::SubscribeRejected { topics };
            let encoded = msg.encode();
            let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");
            assert_eq!(decoded, msg);
        });
}

#[test]
fn ephemeral_message_random_bytes_never_panic() {
    bolero::check!()
        .with_arbitrary::<Vec<u8>>()
        .for_each(|bytes| {
            let _result = EphemeralMessage::try_decode(bytes);
        });
}

#[test]
fn ephemeral_message_wrong_schema_rejected() {
    bolero::check!()
        .with_arbitrary::<([u8; 4], [u8; 32])>()
        .for_each(|(bad_schema, topic)| {
            if bad_schema == &EPHEMERAL_SCHEMA {
                return;
            }
            let topics = NonEmpty::new(Topic::new(*topic));
            let msg = EphemeralMessage::Subscribe { topics };
            let mut encoded = msg.encode();
            encoded[..4].copy_from_slice(bad_schema);
            let result = EphemeralMessage::try_decode(&encoded);
            assert!(
                matches!(result, Err(DecodeError::InvalidSchema(InvalidSchema { .. }))),
                "wrong schema must yield InvalidSchema, got {result:?}"
            );
        });
}

#[test]
fn ephemeral_message_unknown_tag_rejected() {
    bolero::check!()
        .with_arbitrary::<u8>()
        .for_each(|bad_tag| {
            // Known tags are 0x00 (Ephemeral discriminant), 0x01, 0x02, 0x03.
            if matches!(*bad_tag, 0x00 | 0x01 | 0x02 | 0x03) {
                return;
            }
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&EPHEMERAL_SCHEMA);
            bytes.push(*bad_tag);

            let result = EphemeralMessage::try_decode(&bytes);
            assert!(
                matches!(result, Err(DecodeError::InvalidEnumTag(_))),
                "unknown tag {bad_tag:#04x} must yield InvalidEnumTag, got {result:?}"
            );
        });
}

/// Trailing bytes after a control-tag message yield `SizeMismatch`.
#[test]
fn ephemeral_message_control_trailing_bytes_rejected() {
    bolero::check!()
        .with_arbitrary::<([u8; 32], Vec<u8>)>()
        .for_each(|(topic, trailing)| {
            if trailing.is_empty() {
                return;
            }
            let topics = NonEmpty::new(Topic::new(*topic));
            let msg = EphemeralMessage::Subscribe { topics };
            let mut encoded = msg.encode();
            encoded.extend_from_slice(trailing);

            let result = EphemeralMessage::try_decode(&encoded);
            assert!(
                matches!(result, Err(DecodeError::SizeMismatch(SizeMismatch { .. }))),
                "trailing bytes on control message must yield SizeMismatch, got {result:?}"
            );
        });
}

/// An empty topic list (count=0) is rejected.
#[test]
fn ephemeral_message_empty_topic_list_rejected() {
    for tag in [0x01u8, 0x02u8, 0x03u8] {
        let mut buf = Vec::new();
        buf.extend_from_slice(&EPHEMERAL_SCHEMA);
        buf.push(tag);
        buf.extend_from_slice(&0_u16.to_be_bytes()); // count = 0

        let result = EphemeralMessage::try_decode(&buf);
        assert!(
            matches!(result, Err(DecodeError::MessageTooShort { .. })),
            "empty topic list (tag {tag:#04x}) must be rejected, got {result:?}"
        );
    }
}

#[test]
fn ephemeral_message_truncation_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(GenInputs, [u8; 32], u16)>()
        .for_each(|(inputs, key_bytes, drop_count)| {
            let payload = build_payload(inputs);
            let mut bytes = seal_sync(*key_bytes, &payload);
            let new_len = bytes.len().saturating_sub(*drop_count as usize);
            bytes.truncate(new_len);
            let _result = EphemeralMessage::try_decode(&bytes);
        });
}

#[test]
fn ephemeral_message_bit_flip_does_not_panic() {
    bolero::check!()
        .with_arbitrary::<(GenInputs, [u8; 32], u32, u8)>()
        .for_each(|(inputs, key_bytes, byte_idx_seed, bit_idx_seed)| {
            let payload = build_payload(inputs);
            let mut bytes = seal_sync(*key_bytes, &payload);
            if bytes.is_empty() {
                return;
            }
            let byte_idx = (*byte_idx_seed as usize) % bytes.len();
            let bit_idx = bit_idx_seed % 8;
            bytes[byte_idx] ^= 1 << bit_idx;
            let _result = EphemeralMessage::try_decode(&bytes);
        });
}

/// Schema header constants are consistent.
#[test]
fn ephemeral_schema_constants() {
    assert_eq!(EPHEMERAL_SCHEMA, EphemeralPayload::SCHEMA);
    assert_eq!(EPHEMERAL_SCHEMA, *b"SUE\x00");
}
