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
//! - **IssuerVK**: Ed25519 verifying key of the signer (32 bytes)
//! - **Fields**: Type-specific data encoded by the [`Codec`] implementation
//! - **Signature**: Ed25519 signature over bytes `[0..len-64]`

use alloc::vec::Vec;
use core::{cmp::Ordering, marker::PhantomData};

use ed25519_dalek::{Signature, VerifyingKey};
use sedimentree_core::codec::{error::CodecError, Codec};
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
/// let verified = signed.try_verify(&ctx)?;
/// let payload: &T = verified.payload();
/// ```
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Signed<T: Codec> {
    /// Cached issuer verifying key (also at bytes[4..36]).
    issuer: VerifyingKey,

    /// Cached signature (also at bytes[len-64..]).
    signature: Signature,

    /// Full wire bytes: schema(4) + issuer(32) + fields(N) + signature(64).
    bytes: Vec<u8>,

    _marker: PhantomData<T>,
}

impl<T: Codec> Clone for Signed<T> {
    fn clone(&self) -> Self {
        Self {
            issuer: self.issuer,
            signature: self.signature,
            bytes: self.bytes.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: Codec> Signed<T> {
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
        &self.bytes[..self.bytes.len() - SIGNATURE_SIZE]
    }

    /// Get the fields bytes (after schema + issuer, before signature).
    #[must_use]
    pub fn fields_bytes(&self) -> &[u8] {
        &self.bytes[SCHEMA_SIZE + VERIFYING_KEY_SIZE..self.bytes.len() - SIGNATURE_SIZE]
    }

    /// Verify the signature and decode the payload.
    ///
    /// # Errors
    ///
    /// Returns an error if the signature is invalid or the payload cannot be decoded.
    pub fn try_verify(&self, ctx: &T::Context) -> Result<VerifiedSignature<T>, VerificationError> {
        // Verify signature over payload bytes
        self.issuer
            .verify_strict(self.payload_bytes(), &self.signature)
            .map_err(|_| VerificationError::InvalidSignature)?;

        // Decode payload from fields bytes
        let payload = T::try_decode_fields(self.fields_bytes(), ctx)?;

        Ok(VerifiedSignature::new(self.clone(), payload))
    }

    /// Decode the payload without signature verification.
    ///
    /// Use this only for data from trusted sources (e.g., local storage
    /// that was populated via a verified path).
    ///
    /// For untrusted data, use [`try_verify`](Self::try_verify) instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be decoded.
    pub fn try_decode_payload(&self, ctx: &T::Context) -> Result<T, CodecError> {
        T::try_decode_fields(self.fields_bytes(), ctx)
    }

    /// Decode from wire bytes.
    ///
    /// This validates the schema header and extracts the issuer and signature,
    /// but does NOT verify the signature. Use [`try_verify`](Self::try_verify)
    /// to verify.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The buffer is too short
    /// - The schema header doesn't match `T::SCHEMA`
    /// - The verifying key is invalid
    pub fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, CodecError> {
        // Check minimum size
        if bytes.len() < T::MIN_SIZE {
            return Err(CodecError::BufferTooShort {
                need: T::MIN_SIZE,
                have: bytes.len(),
            });
        }

        // Validate schema
        let schema: [u8; SCHEMA_SIZE] = bytes[0..SCHEMA_SIZE]
            .try_into()
            .expect("length checked above");
        if schema != T::SCHEMA {
            return Err(CodecError::InvalidSchema {
                expected: T::SCHEMA,
                got: schema,
            });
        }

        // Extract issuer
        let issuer_bytes: [u8; VERIFYING_KEY_SIZE] = bytes
            [SCHEMA_SIZE..SCHEMA_SIZE + VERIFYING_KEY_SIZE]
            .try_into()
            .expect("length checked above");
        let issuer =
            VerifyingKey::from_bytes(&issuer_bytes).map_err(|_| CodecError::InvalidVerifyingKey)?;

        // Extract signature
        let sig_start = bytes.len() - SIGNATURE_SIZE;
        let sig_bytes: [u8; SIGNATURE_SIZE] =
            bytes[sig_start..].try_into().expect("length checked above");
        let signature = Signature::from_bytes(&sig_bytes);

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
    /// * `ctx` - Context for encoding (e.g., `SedimentreeId` for commits)
    pub async fn seal<K: future_form::FutureForm, S: crate::signer::Signer<K>>(
        signer: &S,
        payload: T,
        ctx: &T::Context,
    ) -> VerifiedSignature<T> {
        let issuer = signer.verifying_key();

        // Calculate size and pre-allocate
        let fields_size = payload.fields_size(ctx);
        let total_size = SCHEMA_SIZE + VERIFYING_KEY_SIZE + fields_size + SIGNATURE_SIZE;
        let mut bytes = Vec::with_capacity(total_size);

        // Write schema
        bytes.extend_from_slice(&T::SCHEMA);

        // Write issuer
        bytes.extend_from_slice(issuer.as_bytes());

        // Write fields
        payload.encode_fields(ctx, &mut bytes);

        // Sign the payload (everything so far)
        let signature = signer.sign(&bytes).await;

        // Append signature
        bytes.extend_from_slice(&signature.to_bytes());

        debug_assert_eq!(bytes.len(), total_size);

        let signed = Self {
            issuer,
            signature,
            bytes,
            _marker: PhantomData,
        };

        // We just signed it, so we know it's valid — no need to verify
        VerifiedSignature::new(signed, payload)
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
    /// * `ctx` - Context for encoding
    #[must_use]
    pub fn from_parts(
        issuer: VerifyingKey,
        signature: Signature,
        payload: &T,
        ctx: &T::Context,
    ) -> Self {
        let fields_size = payload.fields_size(ctx);
        let total_size = SCHEMA_SIZE + VERIFYING_KEY_SIZE + fields_size + SIGNATURE_SIZE;
        let mut bytes = Vec::with_capacity(total_size);

        bytes.extend_from_slice(&T::SCHEMA);
        bytes.extend_from_slice(issuer.as_bytes());
        payload.encode_fields(ctx, &mut bytes);
        bytes.extend_from_slice(&signature.to_bytes());

        Self {
            issuer,
            signature,
            bytes,
            _marker: PhantomData,
        }
    }
}

impl<T: Codec> PartialEq for Signed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl<T: Codec> Eq for Signed<T> {}

impl<T: Codec> core::hash::Hash for Signed<T> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl<T: Codec> PartialOrd for Signed<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Codec> Ord for Signed<T> {
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
    Codec(#[from] CodecError),
}

#[cfg(feature = "arbitrary")]
impl<'a, T: Codec + arbitrary::Arbitrary<'a>> arbitrary::Arbitrary<'a> for Signed<T>
where
    T::Context: arbitrary::Arbitrary<'a>,
{
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        use ed25519_dalek::{Signer as _, SigningKey};

        // Generate arbitrary payload and context
        let payload: T = u.arbitrary()?;
        let ctx: T::Context = u.arbitrary()?;

        // Generate a random signing key from arbitrary bytes
        let key_bytes: [u8; 32] = u.arbitrary()?;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let issuer = signing_key.verifying_key();

        // Encode the payload
        let fields_size = payload.fields_size(&ctx);
        let total_size = SCHEMA_SIZE + VERIFYING_KEY_SIZE + fields_size + SIGNATURE_SIZE;
        let mut bytes = Vec::with_capacity(total_size);

        bytes.extend_from_slice(&T::SCHEMA);
        bytes.extend_from_slice(issuer.as_bytes());
        payload.encode_fields(&ctx, &mut bytes);

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
mod tests {
    // Tests will be added after we implement Codec for test types
}
