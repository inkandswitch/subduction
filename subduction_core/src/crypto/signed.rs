//! Signed payloads.

/// CBOR-encoded payload bytes with phantom type tracking.
pub mod encoded_payload;

/// Envelope wrapper with magic bytes and protocol version.
pub mod envelope;

/// Magic bytes for signed payload identification.
pub mod magic;

/// Protocol version for signed payload format evolution.
pub mod protocol_version;

use core::cmp::Ordering;

use futures_kind::FutureKind;
use thiserror::Error;

use self::{
    encoded_payload::EncodedPayload, envelope::Envelope, magic::Magic,
    protocol_version::ProtocolVersion,
};
use super::{signer::Signer, verified::Verified};

/// A signed payload with its issuer and signature.
#[derive(Clone, Debug, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Signed<T: for<'a> minicbor::Decode<'a, ()>> {
    #[n(0)]
    #[cbor(with = "crate::cbor::verifying_key")]
    issuer: ed25519_dalek::VerifyingKey,

    #[n(1)]
    #[cbor(with = "crate::cbor::signature")]
    signature: ed25519_dalek::Signature,

    #[n(2)]
    encoded_payload: EncodedPayload<T>,
}

impl<T: for<'a> minicbor::Decode<'a, ()>> Signed<T> {
    /// Create a new [`Signed`] instance.
    #[must_use]
    pub const fn new(
        issuer: ed25519_dalek::VerifyingKey,
        signature: ed25519_dalek::Signature,
        encoded_payload: EncodedPayload<T>,
    ) -> Self {
        Self {
            issuer,
            signature,
            encoded_payload,
        }
    }

    /// Verify the signature and decode the payload.
    ///
    /// # Errors
    ///
    /// Returns an error if the signature is invalid or the payload cannot be decoded.
    pub fn try_verify(&self) -> Result<Verified<T>, VerificationError> {
        self.issuer
            .verify_strict(self.encoded_payload.as_slice(), &self.signature)?;
        let envelope = minicbor::decode::<Envelope<T>>(self.encoded_payload.as_slice())?;
        Ok(Verified {
            issuer: self.issuer,
            payload: envelope.into_payload(),
        })
    }

    /// Get the issuer's verifying key.
    #[must_use]
    pub const fn issuer(&self) -> ed25519_dalek::VerifyingKey {
        self.issuer
    }

    /// Get the encoded payload bytes.
    #[must_use]
    pub const fn encoded_payload(&self) -> &EncodedPayload<T> {
        &self.encoded_payload
    }
}

impl<T: for<'a> minicbor::Decode<'a, ()> + minicbor::Encode<()>> Signed<T> {
    /// Seal a payload with the given signer's cryptographic signature.
    ///
    /// # Panics
    ///
    /// Panics if CBOR encoding fails (should never happen for well-formed types).
    #[allow(clippy::expect_used)]
    pub async fn seal<K: FutureKind, S: Signer<K>>(signer: &S, payload: T) -> Self {
        let envelope = Envelope::new(Magic, ProtocolVersion::V0_1, payload);
        let encoded = minicbor::to_vec(&envelope).expect("envelope encoding should not fail");
        let signature = signer.sign(&encoded).await;

        Self {
            issuer: signer.verifying_key(),
            signature,
            encoded_payload: EncodedPayload::new(encoded),
        }
    }
}

impl<T: for<'a> minicbor::Decode<'a, ()>> PartialEq for Signed<T> {
    fn eq(&self, other: &Self) -> bool {
        let Signed {
            issuer: a_issuer,
            signature: a_sig,
            encoded_payload: a_bytes,
        } = self;
        let Signed {
            issuer: b_issuer,
            signature: b_sig,
            encoded_payload: b_bytes,
        } = other;

        a_issuer.as_bytes() == b_issuer.as_bytes()
            && a_sig.to_bytes() == b_sig.to_bytes()
            && a_bytes.as_slice() == b_bytes.as_slice()
    }
}

impl<T: for<'a> minicbor::Decode<'a, ()>> Eq for Signed<T> {}

impl<T: for<'a> minicbor::Decode<'a, ()>> PartialOrd for Signed<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: for<'a> minicbor::Decode<'a, ()>> Ord for Signed<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        let Signed {
            issuer: a_issuer,
            signature: a_sig,
            encoded_payload: a_bytes,
        } = self;
        let Signed {
            issuer: b_issuer,
            signature: b_sig,
            encoded_payload: b_bytes,
        } = other;

        match a_issuer.as_bytes().cmp(b_issuer.as_bytes()) {
            Ordering::Equal => match a_sig.to_bytes().cmp(&b_sig.to_bytes()) {
                Ordering::Equal => a_bytes.as_slice().cmp(b_bytes.as_slice()),
                ord @ (Ordering::Less | Ordering::Greater) => ord,
            },
            ord @ (Ordering::Less | Ordering::Greater) => ord,
        }
    }
}

/// Errors that can occur during signature verification.
#[derive(Debug, Error)]
pub enum VerificationError {
    /// Invalid signature error.
    #[error("invalid signature")]
    InvalidSignature,

    /// CBOR decoding error.
    #[error("CBOR decode error: {0}")]
    DecodeError(#[from] minicbor::decode::Error),
}

impl From<ed25519_dalek::SignatureError> for VerificationError {
    fn from(_: ed25519_dalek::SignatureError) -> Self {
        Self::InvalidSignature
    }
}
