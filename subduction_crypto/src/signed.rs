//! Signed payloads.
//!
//! This module provides [`Signed<T>`], a wrapper for payloads that have been
//! cryptographically signed with an Ed25519 key.
//!
//! # Wire Format
//!
//! All signed payloads use a canonical binary format:
//!
//! ```text
//! ┌──────────────────────────── Payload ────────────────────────────────┬─ Seal ─┐
//! ╔════════╦═══════╦══════════╦═══════════════════════════════════════╦════════╗
//! ║ Schema ║ Disc? ║ IssuerVK ║          Type-Specific Fields         ║  Sig   ║
//! ║   4B   ║ 0|1B  ║   32B    ║             (variable)                ║  64B   ║
//! ╚════════╩═══════╩══════════╩═══════════════════════════════════════╩════════╝
//! ```
//!
//! - **Schema**: 4-byte header identifying type and version
//! - **Disc?**: Optional 1-byte discriminant (from [`Schema::DISCRIMINANT`])
//!   distinguishing types that share a schema (e.g., `Challenge` and `Response`
//!   under `SUH\x00`). Present only when `T::DISCRIMINANT.is_some()`.
//! - **`IssuerVK`**: `Ed25519` verifying key of the signer (32 bytes)
//! - **Fields**: Type-specific data encoded by the [`EncodeFields`] implementation
//! - **Signature**: `Ed25519` signature over bytes `[0..len-64]`

use alloc::vec::Vec;
use core::{cmp::Ordering, marker::PhantomData};

use ed25519_dalek::{Signature, VerifyingKey};
use sedimentree_core::codec::{
    decode::DecodeFields,
    encode::EncodeFields,
    error::{DecodeError, InvalidDiscriminant, InvalidSchema},
    schema::Schema,
};
use thiserror::Error;

use crate::{signer::Signer, verified_signature::VerifiedSignature};

/// Size of the schema header.
pub const SCHEMA_SIZE: usize = 4;

/// Size of an Ed25519 verifying key.
pub const VERIFYING_KEY_SIZE: usize = 32;

/// Size of an Ed25519 signature.
pub const SIGNATURE_SIZE: usize = 64;

/// Minimum size of any signed message (schema + issuer + signature, no fields).
pub const MIN_SIGNED_SIZE: usize = SCHEMA_SIZE + VERIFYING_KEY_SIZE + SIGNATURE_SIZE;

/// A signed payload with its issuer and signature.
///
/// # Type-State Pattern
///
/// This type participates in a type-state flow that encodes verification at the type level:
///
/// ```text
/// Local:    T  ──seal──►  VerifiedSignature<T>  ──into_signed──►  Signed<T> (wire)
/// Remote:   Signed<T>  ──try_verify──►  VerifiedSignature<T>
/// Storage:  Signed<T>  ──decode_payload──►  T  (trusted, no wrapper)
/// ```
///
/// - [`Signed<T>`] holds a signature that **may not have been verified**
/// - [`VerifiedSignature<T>`] is a witness that the signature **is valid**
///
/// # No Direct Payload Access
///
/// `Signed<T>` intentionally does not expose a `payload(&self) -> &T` method.
/// This forces callers to go through [`try_verify`](Self::try_verify) to access
/// the payload, preventing "verify and forget" bugs where verification is called
/// but its result is ignored.
///
/// To access the payload, verify first:
///
/// ```ignore
/// let verified = signed.try_verify(&binding)?;
/// let payload: &T = verified.payload();
/// ```
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Signed<T: Schema + EncodeFields + DecodeFields> {
    /// Cached issuer verifying key (also at bytes[4..36]).
    issuer: VerifyingKey,

    /// Cached signature (also at bytes[len-64..]).
    signature: Signature,

    /// Full wire bytes: schema(4) + issuer(32) + fields(N) + signature(64).
    bytes: Vec<u8>,

    _marker: PhantomData<T>,
}

impl<T: Schema + EncodeFields + DecodeFields> Clone for Signed<T> {
    fn clone(&self) -> Self {
        Self {
            issuer: self.issuer,
            signature: self.signature,
            bytes: self.bytes.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: Schema + EncodeFields + DecodeFields> Signed<T> {
    /// Get the issuer's verifying key.
    #[must_use]
    pub const fn issuer(&self) -> VerifyingKey {
        self.issuer
    }

    /// Get the full wire bytes.
    ///
    /// This includes the schema, optional discriminant, issuer, fields,
    /// and signature.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Get the payload bytes (everything before the signature).
    ///
    /// This is the data that was signed:
    /// `schema + discriminant? + issuer + fields`.
    #[must_use]
    pub fn payload_bytes(&self) -> &[u8] {
        self.bytes
            .get(..self.bytes.len().saturating_sub(SIGNATURE_SIZE))
            .unwrap_or(&[])
    }

    /// Get the fields bytes (after schema + discriminant + issuer, before signature).
    #[must_use]
    pub fn fields_bytes(&self) -> &[u8] {
        let start = SCHEMA_SIZE + usize::from(T::DISCRIMINANT.is_some()) + VERIFYING_KEY_SIZE;
        let end = self.bytes.len().saturating_sub(SIGNATURE_SIZE);
        self.bytes.get(start..end).unwrap_or(&[])
    }

    /// Get the signature.
    #[must_use]
    pub const fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Verify the signature and decode the payload.
    ///
    /// This delegates to [`VerifiedSignature::try_from_signed`], which is the
    /// canonical way to verify signatures.
    ///
    /// # Errors
    ///
    /// Returns an error if the signature is invalid or the payload cannot be decoded.
    pub fn try_verify(&self) -> Result<VerifiedSignature<T>, VerificationError> {
        VerifiedSignature::try_from_signed(self)
    }

    /// Decode the payload from trusted storage without signature verification.
    ///
    /// Use this only for data from trusted sources (e.g., local storage
    /// that was populated via a verified path).
    ///
    /// For untrusted data, use [`try_verify`](Self::try_verify) instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be decoded.
    pub fn try_decode_trusted_payload(&self) -> Result<T, DecodeError> {
        T::try_decode_fields(self.fields_bytes()).map(|(payload, _)| payload)
    }

    /// Decode from wire bytes.
    ///
    /// This validates the schema header and extracts the issuer and signature,
    /// but does NOT verify the signature. Use [`try_verify`](Self::try_verify)
    /// to verify.
    ///
    /// The buffer may contain trailing bytes beyond the signed message; only
    /// the actual message bytes (determined by decoding the payload) are retained.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The buffer is too short
    /// - The schema header doesn't match `T::SCHEMA`
    /// - The verifying key is invalid
    /// - The payload cannot be decoded
    pub fn try_decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::try_decode_prefix(bytes).map(|(signed, _)| signed)
    }

    /// Decode one signed message from the front of `bytes`, returning the
    /// decoded value and the number of bytes it consumed (`actual_size`).
    ///
    /// The consumed length lets callers decode a sequence of messages packed
    /// back-to-back (e.g. the commits and fragments in a batch sync response).
    ///
    /// Validates the header but does NOT verify the signature; use
    /// [`try_verify`](Self::try_verify) for that.
    ///
    /// # Errors
    ///
    /// Same conditions as [`try_decode`](Self::try_decode).
    pub fn try_decode_prefix(bytes: &[u8]) -> Result<(Self, usize), DecodeError> {
        let (issuer, signature, actual_size) = Self::decode_header(bytes)?;

        // Copy out this message's bytes, leaving the rest of `bytes` for the
        // caller to continue decoding.
        let message = bytes
            .get(..actual_size)
            .ok_or(DecodeError::MessageTooShort {
                type_name: core::any::type_name::<T>(),
                need: actual_size,
                have: bytes.len(),
            })?
            .to_vec();

        Ok((
            Self {
                issuer,
                signature,
                bytes: message,
                _marker: PhantomData,
            },
            actual_size,
        ))
    }

    /// Validate the wire header and compute the signed message's total size.
    ///
    /// Validates the schema and optional discriminant, extracts the issuer key
    /// and signature, and decodes the fields to determine `actual_size` (schema,
    /// discriminant, issuer, fields, and signature). Does not verify the signature.
    fn decode_header(bytes: &[u8]) -> Result<(VerifyingKey, Signature, usize), DecodeError> {
        // Check minimum size
        if bytes.len() < T::MIN_SIGNED_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: core::any::type_name::<T>(),
                need: T::MIN_SIGNED_SIZE,
                have: bytes.len(),
            });
        }

        // Validate schema
        let schema: [u8; SCHEMA_SIZE] = bytes
            .get(0..SCHEMA_SIZE)
            .and_then(|s| s.try_into().ok())
            .ok_or(DecodeError::MessageTooShort {
            type_name: core::any::type_name::<T>(),
            need: SCHEMA_SIZE,
            have: bytes.len(),
        })?;
        if schema != T::SCHEMA {
            return Err(InvalidSchema {
                expected: T::SCHEMA,
                got: schema,
            }
            .into());
        }

        let disc_size = usize::from(T::DISCRIMINANT.is_some());

        // Validate discriminant (if present)
        if let Some(expected_disc) = T::DISCRIMINANT {
            let got_disc = *bytes.get(SCHEMA_SIZE).ok_or(DecodeError::MessageTooShort {
                type_name: core::any::type_name::<T>(),
                need: SCHEMA_SIZE + 1,
                have: bytes.len(),
            })?;
            if got_disc != expected_disc {
                return Err(InvalidDiscriminant {
                    expected: expected_disc,
                    got: got_disc,
                }
                .into());
            }
        }

        // Extract issuer
        let issuer_start = SCHEMA_SIZE + disc_size;
        let issuer_bytes: [u8; VERIFYING_KEY_SIZE] = bytes
            .get(issuer_start..issuer_start + VERIFYING_KEY_SIZE)
            .and_then(|s| s.try_into().ok())
            .ok_or(DecodeError::MessageTooShort {
                type_name: core::any::type_name::<T>(),
                need: issuer_start + VERIFYING_KEY_SIZE,
                have: bytes.len(),
            })?;
        let issuer = VerifyingKey::from_bytes(&issuer_bytes)
            .map_err(|_| DecodeError::InvalidVerifyingKey)?;

        // Decode the payload to determine the actual message size.
        // The fields start after schema + discriminant + issuer, and we need
        // to parse them to know where they end (variable-length arrays).
        let fields_start = issuer_start + VERIFYING_KEY_SIZE;
        let fields_bytes = bytes
            .get(fields_start..)
            .ok_or(DecodeError::MessageTooShort {
                type_name: core::any::type_name::<T>(),
                need: fields_start + 1,
                have: bytes.len(),
            })?;
        let (_payload, fields_size) = T::try_decode_fields(fields_bytes)?;

        // Calculate the actual message size and validate we have enough bytes
        let actual_size =
            SCHEMA_SIZE + disc_size + VERIFYING_KEY_SIZE + fields_size + SIGNATURE_SIZE;
        if bytes.len() < actual_size {
            return Err(DecodeError::MessageTooShort {
                type_name: core::any::type_name::<T>(),
                need: actual_size,
                have: bytes.len(),
            });
        }

        // Extract signature from the correct position
        let sig_start = fields_start + fields_size;
        let sig_bytes: [u8; SIGNATURE_SIZE] = bytes
            .get(sig_start..sig_start + SIGNATURE_SIZE)
            .and_then(|s| s.try_into().ok())
            .ok_or(DecodeError::MessageTooShort {
                type_name: core::any::type_name::<T>(),
                need: SIGNATURE_SIZE,
                have: bytes.len().saturating_sub(sig_start),
            })?;
        let signature = Signature::from_bytes(&sig_bytes);

        Ok((issuer, signature, actual_size))
    }

    /// Consume and return the wire bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Seal a payload with the given signer's cryptographic signature.
    ///
    /// Returns a [`VerifiedSignature<T>`] since we know our own signature is valid.
    /// Use [`.into_signed()`](VerifiedSignature::into_signed) to get the [`Signed<T>`]
    /// for wire transmission.
    ///
    /// # Arguments
    ///
    /// * `signer` - The signer to use
    /// * `payload` - The payload to sign
    pub async fn seal<Async: future_form::FutureForm, Sign: Signer<Async>>(
        signer: &Sign,
        payload: T,
    ) -> VerifiedSignature<T> {
        let issuer = signer.verifying_key();

        // Pre-allocate
        let total_size = payload.signed_size();
        let mut bytes = Vec::with_capacity(total_size);

        // Write schema
        bytes.extend_from_slice(&T::SCHEMA);

        // Write discriminant (if present)
        if let Some(disc) = T::DISCRIMINANT {
            bytes.push(disc);
        }

        // Write issuer
        bytes.extend_from_slice(issuer.as_bytes());

        // Write fields
        payload.encode_fields(&mut bytes);

        // Sign the payload (everything so far)
        let signature = signer.sign(&bytes).await;

        // Append signature
        bytes.extend_from_slice(&signature.to_bytes());

        debug_assert_eq!(bytes.len(), total_size);

        let result = Self {
            issuer,
            signature,
            bytes,
            _marker: PhantomData,
        };

        // We just signed it, so we know it's valid — no need to verify
        VerifiedSignature::from_parts(result, payload)
    }

    /// Create a signed payload from raw components.
    ///
    /// This is a low-level constructor for testing and deserialization.
    /// Most callers should use [`seal`](Self::seal) instead.
    ///
    /// # Arguments
    ///
    /// * `issuer` - The verifying key of the signer
    /// * `signature` - The Ed25519 signature
    /// * `payload` - The payload
    #[must_use]
    pub fn from_parts(issuer: VerifyingKey, signature: Signature, payload: &T) -> Self {
        let total_size = payload.signed_size();
        let mut bytes = Vec::with_capacity(total_size);

        bytes.extend_from_slice(&T::SCHEMA);
        bytes.extend_from_slice(issuer.as_bytes());
        payload.encode_fields(&mut bytes);
        bytes.extend_from_slice(&signature.to_bytes());

        Self {
            issuer,
            signature,
            bytes,
            _marker: PhantomData,
        }
    }
}

impl<T: Schema + EncodeFields + DecodeFields> PartialEq for Signed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl<T: Schema + EncodeFields + DecodeFields> Eq for Signed<T> {}

impl<T: Schema + EncodeFields + DecodeFields> core::hash::Hash for Signed<T> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl<T: Schema + EncodeFields + DecodeFields> PartialOrd for Signed<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Schema + EncodeFields + DecodeFields> Ord for Signed<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

/// Errors that can occur during signature verification.
#[derive(Debug, Clone, Copy, Error)]
pub enum VerificationError {
    /// Invalid signature error.
    #[error("invalid signature")]
    InvalidSignature,

    /// Codec error (decoding failed).
    #[error("codec error: {0}")]
    Codec(#[from] DecodeError),
}

#[cfg(feature = "arbitrary")]
impl<'a, T: Schema + EncodeFields + DecodeFields + arbitrary::Arbitrary<'a>>
    arbitrary::Arbitrary<'a> for Signed<T>
{
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        use ed25519_dalek::{Signer as _, SigningKey};

        // Generate arbitrary payload
        let payload: T = u.arbitrary()?;

        // Generate a random signing key from arbitrary bytes
        let key_bytes: [u8; 32] = u.arbitrary()?;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let issuer = signing_key.verifying_key();

        // Encode the payload
        let fields_size = payload.fields_size();
        let disc_size = usize::from(T::DISCRIMINANT.is_some());
        let total_size =
            SCHEMA_SIZE + disc_size + VERIFYING_KEY_SIZE + fields_size + SIGNATURE_SIZE;
        let mut bytes = Vec::with_capacity(total_size);

        bytes.extend_from_slice(&T::SCHEMA);
        if let Some(disc) = T::DISCRIMINANT {
            bytes.push(disc);
        }
        bytes.extend_from_slice(issuer.as_bytes());
        payload.encode_fields(&mut bytes);

        // Sign the payload
        let signature = signing_key.sign(&bytes);
        bytes.extend_from_slice(&signature.to_bytes());

        Ok(Self {
            issuer,
            signature,
            bytes,
            _marker: PhantomData,
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use alloc::vec::Vec;

    use sedimentree_core::codec::{
        decode::{self, DecodeFields},
        encode::{self, EncodeFields},
        error::DecodeError,
        schema::{self, Schema},
    };
    use testresult::TestResult;

    use crate::signer::memory::MemorySigner;

    use super::*;

    /// Minimal test payload for testing Signed round-trips.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, arbitrary::Arbitrary)]
    struct TestPayload {
        value: u64,
    }

    impl Schema for TestPayload {
        const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
        const TYPE_BYTE: u8 = b'T';
        const VERSION: u8 = 0;
    }

    impl EncodeFields for TestPayload {
        fn encode_fields(&self, buf: &mut Vec<u8>) {
            encode::u64(self.value, buf);
        }

        fn fields_size(&self) -> usize {
            8
        }
    }

    impl DecodeFields for TestPayload {
        const MIN_SIGNED_SIZE: usize = 4 + 32 + 8 + 64; // schema + issuer + value + signature

        fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
            let value = decode::u64(buf, 0)?;
            Ok((Self { value }, 8))
        }
    }

    fn test_signer(seed: u8) -> MemorySigner {
        MemorySigner::from_bytes(&[seed; 32])
    }

    #[tokio::test]
    async fn seal_produces_verifiable_bytes() -> TestResult {
        let signer = test_signer(1);
        let payload = TestPayload { value: 42 };

        // Seal the payload
        let verified = Signed::seal::<future_form::Sendable, _>(&signer, payload).await;
        let sealed = verified.into_signed();

        // Get the wire bytes
        let bytes = sealed.as_bytes().to_vec();

        // Parse from wire bytes
        let parsed = Signed::<TestPayload>::try_decode(&bytes)?;

        // Verify the signature and decode
        let verified = parsed.try_verify()?;

        assert_eq!(verified.payload(), &payload);
        assert_eq!(verified.issuer(), signer.verifying_key());
        Ok(())
    }

    #[tokio::test]
    async fn seal_wire_format_is_correct() {
        let signer = test_signer(1);
        let payload = TestPayload {
            value: 0x1234_5678_9ABC_DEF0,
        };

        let verified = Signed::seal::<future_form::Sendable, _>(&signer, payload).await;
        let bytes = verified.signed().as_bytes();

        // Check total size: 4 (schema) + 32 (issuer) + 8 (value) + 64 (signature) = 108
        assert_eq!(bytes.len(), 108);

        // Check schema header
        assert_eq!(&bytes[0..4], &TestPayload::SCHEMA);

        // Check issuer
        assert_eq!(&bytes[4..36], signer.verifying_key().as_bytes());

        // Check value (big-endian u64)
        assert_eq!(&bytes[36..44], &0x1234_5678_9ABC_DEF0_u64.to_be_bytes());

        // Signature is at bytes[44..108]
        assert_eq!(bytes.len() - 44, 64);
    }

    #[tokio::test]
    async fn tampered_bytes_fail_verification() -> TestResult {
        let signer = test_signer(1);
        let payload = TestPayload { value: 42 };

        let verified = Signed::seal::<future_form::Sendable, _>(&signer, payload).await;
        let mut bytes = verified.signed().as_bytes().to_vec();

        // Tamper with the payload (change the value)
        bytes[36] ^= 0xFF;

        let parsed = Signed::<TestPayload>::try_decode(&bytes)?;
        let result = parsed.try_verify();

        assert!(result.is_err(), "tampered bytes should fail verification");
        Ok(())
    }

    /// Seal → `as_bytes` → `try_decode` → `as_bytes` must produce identical bytes.
    ///
    /// This catches truncation bugs where `try_decode` computes the wrong
    /// `actual_size` and silently truncates the stored bytes.
    #[test]
    fn prop_seal_roundtrip_preserves_bytes() {
        bolero::check!()
            .with_arbitrary::<(TestPayload, [u8; 32])>()
            .for_each(|(payload, key_bytes)| {
                let signing_key = ed25519_dalek::SigningKey::from_bytes(key_bytes);
                let issuer = signing_key.verifying_key();

                // Build signed bytes (synchronous, same as Signed::seal internals)
                let fields_size = payload.fields_size();
                let total_size = SCHEMA_SIZE + VERIFYING_KEY_SIZE + fields_size + SIGNATURE_SIZE;
                let mut original_bytes = Vec::with_capacity(total_size);
                original_bytes.extend_from_slice(&TestPayload::SCHEMA);
                original_bytes.extend_from_slice(issuer.as_bytes());
                payload.encode_fields(&mut original_bytes);
                let signature = <ed25519_dalek::SigningKey as ed25519_dalek::Signer<_>>::sign(
                    &signing_key,
                    &original_bytes,
                );
                original_bytes.extend_from_slice(&signature.to_bytes());

                // Round-trip through try_decode
                let decoded = Signed::<TestPayload>::try_decode(&original_bytes)
                    .expect("decode should succeed for sealed bytes");

                assert_eq!(
                    decoded.as_bytes(),
                    &original_bytes[..],
                    "try_decode must preserve exact byte identity"
                );
            });
    }

    /// `try_decode` of sealed bytes produces a `Signed` whose `try_verify` succeeds.
    ///
    /// This catches corruption in the `try_decode` path that would break
    /// signature verification on reload (e.g., wrong truncation point).
    #[test]
    fn prop_try_decode_of_sealed_bytes_verifies() {
        bolero::check!()
            .with_arbitrary::<(TestPayload, [u8; 32])>()
            .for_each(|(payload, key_bytes)| {
                let signing_key = ed25519_dalek::SigningKey::from_bytes(key_bytes);
                let issuer = signing_key.verifying_key();

                let fields_size = payload.fields_size();
                let total_size = SCHEMA_SIZE + VERIFYING_KEY_SIZE + fields_size + SIGNATURE_SIZE;
                let mut bytes = Vec::with_capacity(total_size);
                bytes.extend_from_slice(&TestPayload::SCHEMA);
                bytes.extend_from_slice(issuer.as_bytes());
                payload.encode_fields(&mut bytes);
                let signature = <ed25519_dalek::SigningKey as ed25519_dalek::Signer<_>>::sign(
                    &signing_key,
                    &bytes,
                );
                bytes.extend_from_slice(&signature.to_bytes());

                let decoded =
                    Signed::<TestPayload>::try_decode(&bytes).expect("decode should succeed");

                let verified = decoded
                    .try_verify()
                    .expect("verification must succeed for sealed bytes after try_decode");

                assert_eq!(verified.payload(), payload);
            });
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::indexing_slicing, clippy::panic)]
mod regression {
    //! Deterministic regression tests for [`Signed<T>`] codec, trait
    //! impls, and layout constants.

    use alloc::vec::Vec;

    use ed25519_dalek::{Signer as _, SigningKey};
    use sedimentree_core::codec::{
        decode::{self, DecodeFields},
        encode::{self, EncodeFields},
        error::DecodeError,
        schema::{self, Schema},
    };

    use super::{MIN_SIGNED_SIZE, SCHEMA_SIZE, SIGNATURE_SIZE, Signed, VERIFYING_KEY_SIZE};

    // ── Test payloads ─────────────────────────────────────────────────

    /// Minimal payload for these tests. Wire layout: schema(4) +
    /// issuer(32) + value(8) + sig(64) = 108 bytes. Uses a schema byte
    /// distinct from `super::tests::TestPayload` so the two test
    /// modules don't share schema state.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct Payload {
        value: u64,
    }

    impl Schema for Payload {
        const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
        const TYPE_BYTE: u8 = b'R';
        const VERSION: u8 = 0;
    }

    impl EncodeFields for Payload {
        fn encode_fields(&self, buf: &mut Vec<u8>) {
            encode::u64(self.value, buf);
        }

        fn fields_size(&self) -> usize {
            8
        }
    }

    impl DecodeFields for Payload {
        const MIN_SIGNED_SIZE: usize = SCHEMA_SIZE + VERIFYING_KEY_SIZE + 8 + SIGNATURE_SIZE;

        fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
            let value = decode::u64(buf, 0)?;
            Ok((Self { value }, 8))
        }
    }

    /// Build canonical signed bytes for `Payload` synchronously, using
    /// the same layout as `Signed::seal`.
    pub(crate) fn seal_payload(key_bytes: [u8; 32], value: u64) -> Vec<u8> {
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let issuer = signing_key.verifying_key();
        let payload = Payload { value };

        let total_size = SCHEMA_SIZE + VERIFYING_KEY_SIZE + payload.fields_size() + SIGNATURE_SIZE;
        let mut bytes = Vec::with_capacity(total_size);

        bytes.extend_from_slice(&Payload::SCHEMA);
        bytes.extend_from_slice(issuer.as_bytes());
        payload.encode_fields(&mut bytes);
        let signature = signing_key.sign(&bytes);
        bytes.extend_from_slice(&signature.to_bytes());
        bytes
    }

    // ── Signed::into_bytes ────────────────────────────────────────────

    /// `into_bytes` must return the canonical wire encoding, consuming
    /// the `Signed<T>` value, and must agree with `as_bytes`.
    #[test]
    fn into_bytes_returns_full_wire_bytes() {
        let canonical = seal_payload([7u8; 32], 0xDEAD_BEEF);
        let signed = Signed::<Payload>::try_decode(&canonical).expect("decode of canonical bytes");

        let via_as_bytes: Vec<u8> = signed.as_bytes().to_vec();
        let via_into_bytes: Vec<u8> = signed.into_bytes();

        assert_eq!(
            via_into_bytes, canonical,
            "into_bytes must return the canonical wire encoding"
        );
        assert_eq!(
            via_into_bytes, via_as_bytes,
            "into_bytes must agree with as_bytes"
        );
        assert_eq!(
            via_into_bytes.len(),
            108,
            "Payload canonical wire size is 108 bytes"
        );
    }

    // ── Signed::try_decode error field arithmetic ─────────────────────
    //
    // These error paths are guarded by the outer `T::MIN_SIGNED_SIZE`
    // check at the top of `try_decode`. In normal use they're
    // unreachable because `T::MIN_SIGNED_SIZE` is always ≥ the
    // intermediate-bound expressions. To exercise them we deliberately
    // under-specify a payload's `MIN_SIGNED_SIZE` so the outer check
    // doesn't catch the short input, driving execution down to the
    // in-line bounds checks.
    //
    // This pins a defense-in-depth contract: if a downstream
    // `DecodeFields` impl gets its `MIN_SIGNED_SIZE` wrong, the inner
    // bounds checks must still produce structured errors with accurate
    // `need` values rather than panicking or reporting garbage.

    /// Lies about `MIN_SIGNED_SIZE`, claiming `0`. Forces the outer
    /// pre-check to pass for any input length, exposing the in-line
    /// discriminant-bounds check.
    #[derive(Debug, Clone, Copy)]
    struct UnderSpecifiedTagged {
        value: u64,
    }

    impl Schema for UnderSpecifiedTagged {
        const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
        const TYPE_BYTE: u8 = b'U';
        const VERSION: u8 = 0;
        const DISCRIMINANT: Option<u8> = Some(0xAA);
    }

    impl EncodeFields for UnderSpecifiedTagged {
        fn encode_fields(&self, buf: &mut Vec<u8>) {
            encode::u64(self.value, buf);
        }
        fn fields_size(&self) -> usize {
            8
        }
    }

    impl DecodeFields for UnderSpecifiedTagged {
        // Deliberately too small. The real minimum would be
        // SCHEMA_SIZE + 1 + VERIFYING_KEY_SIZE + 8 + SIGNATURE_SIZE = 109.
        // We claim 0 so the outer check always passes.
        const MIN_SIGNED_SIZE: usize = 0;

        fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
            let value = decode::u64(buf, 0)?;
            Ok((Self { value }, 8))
        }
    }

    /// Same trick for an untagged payload, exposing the missing-issuer
    /// and missing-fields-region bounds checks.
    #[derive(Debug, Clone, Copy)]
    struct UnderSpecifiedPlain {
        value: u64,
    }

    impl Schema for UnderSpecifiedPlain {
        const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
        const TYPE_BYTE: u8 = b'P';
        const VERSION: u8 = 0;
    }

    impl EncodeFields for UnderSpecifiedPlain {
        fn encode_fields(&self, buf: &mut Vec<u8>) {
            encode::u64(self.value, buf);
        }
        fn fields_size(&self) -> usize {
            8
        }
    }

    impl DecodeFields for UnderSpecifiedPlain {
        const MIN_SIGNED_SIZE: usize = 0;

        fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
            let value = decode::u64(buf, 0)?;
            Ok((Self { value }, 8))
        }
    }

    /// With an under-specified `MIN_SIGNED_SIZE`, feeding exactly
    /// `SCHEMA_SIZE` bytes (just the schema header, no discriminant)
    /// makes `try_decode` reach the in-line discriminant bounds check,
    /// which must report `need = SCHEMA_SIZE + 1` (= 5).
    #[test]
    fn try_decode_reports_correct_need_for_missing_discriminant() {
        let mut bytes = Vec::with_capacity(SCHEMA_SIZE);
        bytes.extend_from_slice(&UnderSpecifiedTagged::SCHEMA);
        assert_eq!(bytes.len(), SCHEMA_SIZE);

        let result = Signed::<UnderSpecifiedTagged>::try_decode(&bytes);
        match result {
            Err(DecodeError::MessageTooShort { need, have, .. }) => {
                assert_eq!(
                    need,
                    SCHEMA_SIZE + 1,
                    "need must be SCHEMA_SIZE + 1 (= {})",
                    SCHEMA_SIZE + 1
                );
                assert_eq!(have, SCHEMA_SIZE, "have must be SCHEMA_SIZE");
            }
            other => panic!("expected MessageTooShort, got {other:?}"),
        }
    }

    /// With an under-specified `MIN_SIGNED_SIZE` and a buffer of
    /// `SCHEMA_SIZE` bytes for an *untagged* payload, the schema check
    /// passes (we wrote the right schema) and execution reaches the
    /// issuer bounds check. `need` must be
    /// `issuer_start + VERIFYING_KEY_SIZE = 4 + 32 = 36`.
    #[test]
    fn try_decode_reports_correct_need_for_missing_issuer() {
        let mut bytes = Vec::with_capacity(SCHEMA_SIZE);
        bytes.extend_from_slice(&UnderSpecifiedPlain::SCHEMA);
        assert_eq!(bytes.len(), SCHEMA_SIZE);

        let result = Signed::<UnderSpecifiedPlain>::try_decode(&bytes);
        match result {
            Err(DecodeError::MessageTooShort { need, have, .. }) => {
                assert_eq!(
                    need,
                    SCHEMA_SIZE + VERIFYING_KEY_SIZE,
                    "need must be issuer_start + VERIFYING_KEY_SIZE (= {})",
                    SCHEMA_SIZE + VERIFYING_KEY_SIZE
                );
                assert_eq!(have, SCHEMA_SIZE, "have must be SCHEMA_SIZE");
            }
            other => panic!("expected MessageTooShort, got {other:?}"),
        }
    }

    /// With an under-specified `MIN_SIGNED_SIZE` and a buffer of
    /// exactly the right size to pass the schema + issuer checks but
    /// have an empty fields region (i.e., `bytes.len() == fields_start`),
    /// `bytes.get(fields_start..)` returns an empty slice, not `None`.
    /// So execution continues into `try_decode_fields(empty_slice)`,
    /// which fails inside `decode::u64`. The fields-region
    /// `need: fields_start + 1` arm is unreachable from any
    /// well-formed caller of `try_decode` — it sits behind a
    /// `bytes.len() < fields_start` condition that contradicts
    /// having reached this point. We assert the reachable
    /// alternative: an empty fields region produces a downstream
    /// decode error (not a panic, not a successful decode).
    #[test]
    fn try_decode_empty_fields_region_errors_cleanly() {
        // Exactly SCHEMA_SIZE + VERIFYING_KEY_SIZE bytes — passes the
        // issuer bounds check, then `try_decode_fields` is called with
        // an empty slice and fails.
        let mut bytes = Vec::with_capacity(SCHEMA_SIZE + VERIFYING_KEY_SIZE);
        bytes.extend_from_slice(&UnderSpecifiedPlain::SCHEMA);
        bytes.extend_from_slice(&[0u8; VERIFYING_KEY_SIZE]);
        assert_eq!(bytes.len(), SCHEMA_SIZE + VERIFYING_KEY_SIZE);

        let result = Signed::<UnderSpecifiedPlain>::try_decode(&bytes);
        assert!(
            result.is_err(),
            "decode of bytes with empty fields region must fail"
        );
    }

    // ── Signed::PartialEq ────────────────────────────────────────────

    /// Two `Signed<T>` values with identical canonical bytes must be
    /// `==`; two with different payloads or different signers must
    /// be `!=`.
    #[test]
    fn partial_eq_distinguishes_different_signed_values() {
        let a = Signed::<Payload>::try_decode(&seal_payload([1u8; 32], 100)).expect("decode a");
        let a_again =
            Signed::<Payload>::try_decode(&seal_payload([1u8; 32], 100)).expect("decode a_again");
        let b = Signed::<Payload>::try_decode(&seal_payload([1u8; 32], 200))
            .expect("decode b (different value)");
        let c = Signed::<Payload>::try_decode(&seal_payload([2u8; 32], 100))
            .expect("decode c (different signer)");

        assert_eq!(a, a, "self-equality must hold");
        assert_eq!(a, a_again, "two seals of identical content must be equal");

        assert_ne!(a, b, "different payloads must not be equal");
        assert_ne!(a, c, "different signers must not be equal");
    }

    // ── try_decode trailing-byte truncation ───────────────────────────
    //
    // `try_decode` accepts buffers containing trailing bytes beyond
    // the canonical encoding (e.g. when `Signed<T>` is embedded inside
    // a larger message like `SyncMessage`, which hands `try_decode`
    // the full payload buffer including subsequent items). The
    // existing bolero proptest covers this with random trailing bytes;
    // this is a deterministic complement that runs in <1ms.

    /// `try_decode` of canonical bytes followed by arbitrary trailing
    /// data must succeed, truncate to the canonical length, and
    /// verify cleanly.
    #[test]
    fn try_decode_succeeds_with_trailing_bytes() {
        let canonical = seal_payload([42u8; 32], 0xABCD);
        let canonical_len = canonical.len();
        let mut with_trailing = canonical.clone();
        with_trailing.extend_from_slice(&[0xFF; 256]);

        let signed = Signed::<Payload>::try_decode(&with_trailing)
            .expect("decode must succeed when extra bytes trail the canonical encoding");
        assert_eq!(
            signed.as_bytes().len(),
            canonical_len,
            "try_decode must truncate trailing bytes to the canonical length"
        );
        assert_eq!(signed.as_bytes(), canonical.as_slice());
        signed
            .try_verify()
            .expect("verification must succeed after truncation");
    }

    // ── Hash + PartialOrd implementations ─────────────────────────────
    //
    // `Hash` and `PartialOrd` are required for using `Signed<T>` as
    // keys in `BTreeMap` / `HashMap` and for ordering. Pin both
    // contracts deterministically.

    /// `Signed<T>`'s `Hash` impl must produce different hashes for
    /// different signed values.
    #[test]
    fn hash_distinguishes_different_values() {
        use core::hash::{BuildHasher as _, Hasher as _};
        use std::collections::hash_map::RandomState;

        let a = Signed::<Payload>::try_decode(&seal_payload([5u8; 32], 100)).expect("decode a");
        let b = Signed::<Payload>::try_decode(&seal_payload([5u8; 32], 200)).expect("decode b");

        let builder = RandomState::new();
        let mut ha = builder.build_hasher();
        let mut hb = builder.build_hasher();
        core::hash::Hash::hash(&a, &mut ha);
        core::hash::Hash::hash(&b, &mut hb);

        assert_ne!(
            ha.finish(),
            hb.finish(),
            "Signed::hash must distinguish different signed values"
        );
    }

    /// `Signed<T>`'s `PartialOrd` must produce `Some(_)` for any two
    /// values, since `Ord` defines a total order.
    #[test]
    fn partial_cmp_is_total() {
        let a = Signed::<Payload>::try_decode(&seal_payload([6u8; 32], 1)).expect("decode a");
        let b = Signed::<Payload>::try_decode(&seal_payload([6u8; 32], 2)).expect("decode b");

        assert!(
            a.partial_cmp(&a).is_some(),
            "partial_cmp(self, self) must be Some (= Equal)"
        );
        assert!(
            a.partial_cmp(&b).is_some(),
            "partial_cmp(a, b) must be Some (= Less or Greater)"
        );
        assert!(
            b.partial_cmp(&a).is_some(),
            "partial_cmp(b, a) must be Some (= Less or Greater)"
        );
        assert_ne!(
            a.partial_cmp(&b),
            b.partial_cmp(&a),
            "partial_cmp must respect antisymmetry for distinct values"
        );
    }

    // ── Free-standing MIN_SIGNED_SIZE constant ───────────────────────
    //
    // The constant is unused in-crate (`T::MIN_SIGNED_SIZE` is what
    // code uses) but is part of the public API. Pin its value with an
    // explicit assertion so it doesn't silently drift under a
    // refactor.

    /// `signed::MIN_SIGNED_SIZE` is the sum of the three layout
    /// constants.
    #[test]
    fn min_signed_size_constant_is_sum_of_layout_parts() {
        let expected = SCHEMA_SIZE + VERIFYING_KEY_SIZE + SIGNATURE_SIZE;
        assert_eq!(
            MIN_SIGNED_SIZE, expected,
            "MIN_SIGNED_SIZE must equal SCHEMA_SIZE + VERIFYING_KEY_SIZE + SIGNATURE_SIZE"
        );
        assert_eq!(
            MIN_SIGNED_SIZE, 100,
            "MIN_SIGNED_SIZE must be 4 + 32 + 64 = 100"
        );
    }
}
