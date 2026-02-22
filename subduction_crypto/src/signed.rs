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
//! ┌─────────────────────────── Payload ────────────────────────────┬─ Seal ─┐
//! ╔════════╦══════════╦═══════════════════════════════════════════╦════════╗
//! ║ Schema ║ IssuerVK ║           Type-Specific Fields            ║  Sig   ║
//! ║   4B   ║   32B    ║              (variable)                   ║  64B   ║
//! ╚════════╩══════════╩═══════════════════════════════════════════╩════════╝
//! ```
//!
//! - **Schema**: 4-byte header identifying type and version
//! - **`IssuerVK`**: `Ed25519` verifying key of the signer (32 bytes)
//! - **Fields**: Type-specific data encoded by the [`Encode`] implementation
//! - **Signature**: `Ed25519` signature over bytes `[0..len-64]`

use alloc::vec::Vec;
use core::{cmp::Ordering, marker::PhantomData};

use ed25519_dalek::{Signature, VerifyingKey};
use sedimentree_core::codec::{
    decode::Decode,
    encode::EncodeFields,
    error::{DecodeError, InvalidSchema},
    schema::Schema,
};
use thiserror::Error;

use crate::verified_signature::VerifiedSignature;

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
pub struct Signed<T: Schema + EncodeFields + Decode> {
    /// Cached issuer verifying key (also at bytes[4..36]).
    issuer: VerifyingKey,

    /// Cached signature (also at bytes[len-64..]).
    signature: Signature,

    /// Full wire bytes: schema(4) + issuer(32) + fields(N) + signature(64).
    bytes: Vec<u8>,

    _marker: PhantomData<T>,
}

impl<T: Schema + EncodeFields + Decode> Clone for Signed<T> {
    fn clone(&self) -> Self {
        Self {
            issuer: self.issuer,
            signature: self.signature,
            bytes: self.bytes.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: Schema + EncodeFields + Decode> Signed<T> {
    /// Get the issuer's verifying key.
    #[must_use]
    pub const fn issuer(&self) -> VerifyingKey {
        self.issuer
    }

    /// Get the full wire bytes.
    ///
    /// This includes the schema, issuer, fields, and signature.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Get the payload bytes (everything before the signature).
    ///
    /// This is the data that was signed: schema + issuer + fields.
    #[must_use]
    pub fn payload_bytes(&self) -> &[u8] {
        // SAFETY: Signed<T> is only constructed after validating MIN_SIZE >= SIGNATURE_SIZE
        self.bytes
            .get(..self.bytes.len().saturating_sub(SIGNATURE_SIZE))
            .unwrap_or(&[])
    }

    /// Get the fields bytes (after schema + issuer, before signature).
    #[must_use]
    pub fn fields_bytes(&self) -> &[u8] {
        let start = SCHEMA_SIZE + VERIFYING_KEY_SIZE;
        let end = self.bytes.len().saturating_sub(SIGNATURE_SIZE);
        // SAFETY: Signed<T> is only constructed after validating MIN_SIZE
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
        T::try_decode_fields(self.fields_bytes())
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
    pub fn try_decode(mut bytes: Vec<u8>) -> Result<Self, DecodeError> {
        // Check minimum size
        if bytes.len() < T::MIN_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: core::any::type_name::<T>(),
                need: T::MIN_SIZE,
                have: bytes.len(),
            });
        }

        // Validate schema - safe because MIN_SIZE >= SCHEMA_SIZE
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

        // Extract issuer - safe because MIN_SIZE >= SCHEMA_SIZE + VERIFYING_KEY_SIZE
        let issuer_bytes: [u8; VERIFYING_KEY_SIZE] = bytes
            .get(SCHEMA_SIZE..SCHEMA_SIZE + VERIFYING_KEY_SIZE)
            .and_then(|s| s.try_into().ok())
            .ok_or(DecodeError::MessageTooShort {
                type_name: core::any::type_name::<T>(),
                need: SCHEMA_SIZE + VERIFYING_KEY_SIZE,
                have: bytes.len(),
            })?;
        let issuer = VerifyingKey::from_bytes(&issuer_bytes)
            .map_err(|_| DecodeError::InvalidVerifyingKey)?;

        // Decode the payload to determine the actual message size.
        // The fields start after schema + issuer, and we need to parse them
        // to know where they end (since they have variable-length arrays).
        let fields_start = SCHEMA_SIZE + VERIFYING_KEY_SIZE;
        let fields_bytes = bytes
            .get(fields_start..)
            .ok_or(DecodeError::MessageTooShort {
                type_name: core::any::type_name::<T>(),
                need: fields_start + 1,
                have: bytes.len(),
            })?;
        let payload = T::try_decode_fields(fields_bytes)?;
        let fields_size = payload.fields_size();

        // Calculate the actual message size and validate we have enough bytes
        let actual_size = SCHEMA_SIZE + VERIFYING_KEY_SIZE + fields_size + SIGNATURE_SIZE;
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

        // Truncate to only include the actual signed message bytes
        bytes.truncate(actual_size);

        Ok(Self {
            issuer,
            signature,
            bytes,
            _marker: PhantomData,
        })
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
    pub async fn seal<K: future_form::FutureForm, S: crate::signer::Signer<K>>(
        signer: &S,
        payload: T,
    ) -> VerifiedSignature<T> {
        let issuer = signer.verifying_key();

        // Pre-allocate
        let total_size = payload.signed_size();
        let mut bytes = Vec::with_capacity(total_size);

        // Write schema
        bytes.extend_from_slice(&T::SCHEMA);

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

impl<T: Schema + EncodeFields + Decode> PartialEq for Signed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl<T: Schema + EncodeFields + Decode> Eq for Signed<T> {}

impl<T: Schema + EncodeFields + Decode> core::hash::Hash for Signed<T> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl<T: Schema + EncodeFields + Decode> PartialOrd for Signed<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Schema + EncodeFields + Decode> Ord for Signed<T> {
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
impl<'a, T: Schema + EncodeFields + Decode + arbitrary::Arbitrary<'a>> arbitrary::Arbitrary<'a>
    for Signed<T>
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
        let total_size = SCHEMA_SIZE + VERIFYING_KEY_SIZE + fields_size + SIGNATURE_SIZE;
        let mut bytes = Vec::with_capacity(total_size);

        bytes.extend_from_slice(&T::SCHEMA);
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
        decode::{self, Decode},
        encode::{self, EncodeFields},
        error::DecodeError,
        schema::{self, Schema},
    };
    use testresult::TestResult;

    use crate::signer::memory::MemorySigner;

    use super::*;

    /// Minimal test payload for testing Signed round-trips.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    impl Decode for TestPayload {
        const MIN_SIZE: usize = 4 + 32 + 8 + 64; // schema + issuer + value + signature

        fn try_decode_fields(buf: &[u8]) -> Result<Self, DecodeError> {
            let value = decode::u64(buf, 0)?;
            Ok(Self { value })
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
        let parsed = Signed::<TestPayload>::try_decode(bytes)?;

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

        let parsed = Signed::<TestPayload>::try_decode(bytes)?;
        let result = parsed.try_verify();

        assert!(result.is_err(), "tampered bytes should fail verification");
        Ok(())
    }
}
