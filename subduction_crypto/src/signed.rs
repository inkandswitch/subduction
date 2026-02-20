//! Signed payloads.

mod encoded_payload;
mod envelope;
mod magic;
mod protocol_version;

pub use encoded_payload::EncodedPayload;
pub use envelope::Envelope;
pub use magic::Magic;
pub use protocol_version::ProtocolVersion;

use core::cmp::Ordering;

use thiserror::Error;

use crate::verified_signature::VerifiedSignature;

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
/// let verified = signed.try_verify()?;
/// let payload: &T = verified.payload();
/// ```
#[derive(Debug, minicbor::Encode, minicbor::Decode)]
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

impl<T: for<'a> minicbor::Decode<'a, ()>> Clone for Signed<T> {
    fn clone(&self) -> Self {
        Self {
            issuer: self.issuer,
            signature: self.signature,
            encoded_payload: self.encoded_payload.clone(),
        }
    }
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
    pub fn try_verify(&self) -> Result<VerifiedSignature<T>, VerificationError> {
        self.issuer
            .verify_strict(self.encoded_payload.as_slice(), &self.signature)?;
        let envelope = minicbor::decode::<Envelope<T>>(self.encoded_payload.as_slice())?;
        Ok(VerifiedSignature::new(
            self.clone(),
            envelope.into_payload(),
        ))
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
    pub fn decode_payload(&self) -> Result<T, minicbor::decode::Error> {
        let envelope = minicbor::decode::<Envelope<T>>(self.encoded_payload.as_slice())?;
        Ok(envelope.into_payload())
    }
}

impl<T: for<'a> minicbor::Decode<'a, ()> + minicbor::Encode<()>> Signed<T> {
    /// Seal a payload with the given signer's cryptographic signature.
    ///
    /// Returns a [`VerifiedSignature<T>`] since we know our own signature is valid.
    /// Use [`.into_signed()`](VerifiedSignature::into_signed) to get the [`Signed<T>`]
    /// for wire transmission.
    ///
    /// The `K` type parameter determines whether the future is `Send` (`Sendable`)
    /// or `!Send` (`Local`). Specify it with turbofish syntax:
    ///
    /// ```ignore
    /// Signed::seal::<Sendable, _>(&signer, payload).await
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if CBOR encoding fails (should never happen for well-formed types).
    #[allow(clippy::expect_used)]
    pub async fn seal<K, S>(signer: &S, payload: T) -> VerifiedSignature<T>
    where
        K: future_form::FutureForm,
        S: crate::signer::Signer<K>,
    {
        let envelope = Envelope::new(Magic, ProtocolVersion::V0_1, payload);
        let encoded = minicbor::to_vec(&envelope).expect("envelope encoding should not fail");
        let signature = signer.sign(&encoded).await;

        // Decode payload back from encoded bytes (avoids Clone bound on T)
        let decoded_envelope =
            minicbor::decode::<Envelope<T>>(&encoded).expect("just-encoded envelope should decode");

        let signed = Self {
            issuer: signer.verifying_key(),
            signature,
            encoded_payload: EncodedPayload::new(encoded),
        };

        // We just signed it, so we know it's valid — no need to verify
        VerifiedSignature::new(signed, decoded_envelope.into_payload())
    }

    /// Create a signed payload from raw components.
    ///
    /// This is a low-level constructor. Most callers should use
    /// [`seal`](Self::seal) instead.
    ///
    /// # Panics
    ///
    /// Panics if CBOR encoding fails (should never happen for well-formed types).
    #[allow(clippy::expect_used)]
    #[must_use]
    pub fn from_parts(
        issuer: ed25519_dalek::VerifyingKey,
        signature: ed25519_dalek::Signature,
        payload: T,
    ) -> (Self, T) {
        let envelope = Envelope::new(Magic, ProtocolVersion::V0_1, payload);
        let encoded = minicbor::to_vec(&envelope).expect("envelope encoding should not fail");

        // Decode payload back from encoded bytes (avoids Clone bound)
        let decoded_envelope =
            minicbor::decode::<Envelope<T>>(&encoded).expect("just-encoded envelope should decode");

        let signed = Self {
            issuer,
            signature,
            encoded_payload: EncodedPayload::new(encoded),
        };

        (signed, decoded_envelope.into_payload())
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

impl<T: for<'a> minicbor::Decode<'a, ()>> core::hash::Hash for Signed<T> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.issuer.as_bytes().hash(state);
        self.signature.to_bytes().hash(state);
        self.encoded_payload.as_slice().hash(state);
    }
}

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

#[cfg(feature = "arbitrary")]
impl<'a, T> arbitrary::Arbitrary<'a> for Signed<T>
where
    T: for<'b> minicbor::Decode<'b, ()> + minicbor::Encode<()> + arbitrary::Arbitrary<'a>,
{
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        use ed25519_dalek::{Signer as _, SigningKey};

        // Generate arbitrary payload
        let payload: T = u.arbitrary()?;

        // Generate a random signing key from arbitrary bytes
        let key_bytes: [u8; 32] = u.arbitrary()?;
        let signing_key = SigningKey::from_bytes(&key_bytes);

        // Wrap in envelope and encode
        let envelope = Envelope::new(Magic, ProtocolVersion::V0_1, payload);
        let encoded = minicbor::to_vec(&envelope).map_err(|_| arbitrary::Error::IncorrectFormat)?;

        // Sign the encoded payload
        let signature = signing_key.sign(&encoded);

        Ok(Self {
            issuer: signing_key.verifying_key(),
            signature,
            encoded_payload: EncodedPayload::new(encoded),
        })
    }
}
