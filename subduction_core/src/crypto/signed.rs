//! Signed payloads.

pub mod encoded_payload;
pub mod envelope;
pub mod magic;
pub mod protocol_version;

use core::cmp::Ordering;
use thiserror::Error;

use self::{
    encoded_payload::EncodedPayload,
    envelope::Envelope,
    magic::Magic,
    protocol_version::ProtocolVersion,
};
use super::{signer::Signer, verified::Verified};

/// A signed payload with its issuer and signature.
#[derive(Clone, Debug, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Signed<T: for<'de> minicbor::Decode<'de, ()>> {
    #[n(0)]
    #[cbor(with = "crate::cbor::verifying_key")]
    issuer: ed25519_dalek::VerifyingKey,

    #[n(1)]
    #[cbor(with = "crate::cbor::signature")]
    signature: ed25519_dalek::Signature,

    #[n(2)]
    encoded_payload: EncodedPayload<T>,
}

impl<T: for<'de> minicbor::Decode<'de, ()>> Signed<T> {
    /// Create a new [`Signed`] instance.
    #[must_use]
    pub fn new(
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
    #[must_use]
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
    pub fn issuer(&self) -> ed25519_dalek::VerifyingKey {
        self.issuer
    }

    /// Get the encoded payload bytes.
    #[must_use]
    pub fn encoded_payload(&self) -> &EncodedPayload<T> {
        &self.encoded_payload
    }
}

impl<T: for<'de> minicbor::Decode<'de, ()> + minicbor::Encode<()>> Signed<T> {
    /// Sign a payload using the provided signer.
    ///
    /// This wraps the payload in an [`Envelope`] with magic bytes and protocol
    /// version, encodes it to CBOR, signs the bytes, and returns a [`Signed<T>`].
    #[must_use]
    pub fn sign(signer: &impl Signer, payload: T) -> Self {
        let envelope = Envelope::new(Magic, ProtocolVersion::V0_1, payload);
        let encoded = minicbor::to_vec(&envelope).expect("envelope encoding should not fail");
        let signature = signer.sign(&encoded);

        Self {
            issuer: signer.verifying_key(),
            signature,
            encoded_payload: EncodedPayload::new(encoded),
        }
    }
}

impl<T: for<'de> minicbor::Decode<'de, ()>> PartialEq for Signed<T> {
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

impl<T: for<'de> minicbor::Decode<'de, ()>> Eq for Signed<T> {}

impl<T: for<'de> minicbor::Decode<'de, ()>> PartialOrd for Signed<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
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

        match a_issuer.as_bytes().partial_cmp(b_issuer.as_bytes()) {
            Some(Ordering::Equal) => match a_sig.to_bytes().partial_cmp(&b_sig.to_bytes()) {
                Some(Ordering::Equal) => a_bytes.as_slice().partial_cmp(b_bytes.as_slice()),
                non_eq => non_eq,
            },
            non_eq => non_eq,
        }
    }
}

impl<T: for<'de> minicbor::Decode<'de, ()>> Ord for Signed<T> {
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
                non_eq => non_eq,
            },
            non_eq => non_eq,
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
