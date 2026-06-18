//! A payload whose signature has been verified.

use core::cmp::Ordering;

use sedimentree_core::codec::{
    decode::DecodeFields, encode::EncodeFields, error::DecodeError, schema::Schema,
};

use crate::{
    signed::{Signed, VerificationError},
    verified_author::VerifiedAuthor,
};

/// A payload whose signature has been verified.
///
/// This type is a **witness** that the signature is valid. It can only be
/// created through controlled entry points:
///
/// - [`try_from_signed`](Self::try_from_signed) — verify untrusted data from the wire
/// - [`Signed::seal`] — sign new data (we know our own signature is valid)
/// - [`try_from_trusted`](Self::try_from_trusted) — load from trusted storage
///
/// It provides access to the payload that [`Signed<T>`] intentionally withholds,
/// ensuring callers cannot skip verification.
///
/// ```text
/// Local:    T  ──seal──►  VerifiedSignature<T>  ──putter──►  Storage
/// Remote:   Signed<T>  ──try_from_signed──►  VerifiedSignature<T>  ──putter──►  Storage
/// Reload:   Signed<T>  ──try_from_trusted──►  VerifiedSignature<T>  (no re-verify)
/// ```
///
/// # Storage Pattern
///
/// `VerifiedSignature<T>` retains the original [`Signed<T>`] so you can store the
/// signed version after verification. For loading from storage, use
/// [`try_from_trusted`](Self::try_from_trusted) which skips re-verification.
///
/// # Wire Format
///
/// This type should NEVER be sent over the wire directly. Always transmit
/// [`Signed<T>`] and have the recipient verify via [`try_from_signed`](Self::try_from_signed).
#[derive(Clone, Debug)]
pub struct VerifiedSignature<T: Schema + EncodeFields + DecodeFields> {
    signed: Signed<T>,
    payload: T,
}

impl<T: Schema + EncodeFields + DecodeFields> VerifiedSignature<T> {
    /// Verify the signature and decode the payload.
    ///
    /// This is the primary way to create a `VerifiedSignature` from untrusted data.
    ///
    /// # Errors
    ///
    /// Returns an error if the signature is invalid or the payload cannot be decoded.
    #[must_use = "verification result should not be discarded"]
    pub fn try_from_signed(signed: &Signed<T>) -> Result<Self, VerificationError> {
        // Verify signature over payload bytes
        signed
            .issuer()
            .verify_strict(signed.payload_bytes(), signed.signature())
            .map_err(|_| VerificationError::InvalidSignature)?;

        // Decode payload from fields bytes
        let (payload, _) = T::try_decode_fields(signed.fields_bytes())?;

        Ok(Self {
            signed: signed.clone(),
            payload,
        })
    }

    /// Create a new `VerifiedSignature` from parts.
    ///
    /// This is `pub(crate)` because it bypasses verification. Only use for:
    /// - `Signed::seal()` where we just created the signature ourselves
    /// - Internal construction after verification has already happened
    pub(crate) const fn from_parts(signed: Signed<T>, payload: T) -> Self {
        Self { signed, payload }
    }

    /// Returns the verifying key of the issuer who signed this payload.
    #[must_use]
    pub const fn issuer(&self) -> ed25519_dalek::VerifyingKey {
        self.signed.issuer()
    }

    /// Extract the author identity as a [`VerifiedAuthor`] witness.
    ///
    /// This is the primary way to obtain a [`VerifiedAuthor`] — it
    /// proves the author's signing key has been cryptographically verified.
    #[must_use]
    pub const fn verified_author(&self) -> VerifiedAuthor {
        VerifiedAuthor::new(self.signed.issuer())
    }

    /// Returns a reference to the verified payload.
    #[must_use]
    pub const fn payload(&self) -> &T {
        &self.payload
    }

    /// Consumes the `VerifiedSignature` and returns the payload.
    #[must_use]
    pub fn into_payload(self) -> T {
        self.payload
    }

    /// Returns a reference to the original signed value.
    ///
    /// Use this when you need to store the signed version after verification.
    #[must_use]
    pub const fn signed(&self) -> &Signed<T> {
        &self.signed
    }

    /// Consumes the `VerifiedSignature` and returns the original signed value.
    #[must_use]
    pub fn into_signed(self) -> Signed<T> {
        self.signed
    }

    /// Consumes the `VerifiedSignature` and returns both the signed value and payload.
    #[must_use]
    pub fn into_parts(self) -> (Signed<T>, T) {
        (self.signed, self.payload)
    }

    /// Reconstruct from trusted storage without signature verification.
    ///
    /// Use this only for data loaded from trusted storage that was
    /// previously verified before being stored.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] if the payload cannot be decoded.
    #[must_use = "decoded payload should not be discarded"]
    pub fn try_from_trusted(signed: Signed<T>) -> Result<Self, DecodeError> {
        let payload = signed.try_decode_trusted_payload()?;
        Ok(Self { signed, payload })
    }
}

impl<T: Schema + EncodeFields + DecodeFields + PartialEq> PartialEq for VerifiedSignature<T> {
    fn eq(&self, other: &Self) -> bool {
        // Compare by signed value (includes issuer, signature, encoded payload)
        self.signed == other.signed
    }
}

impl<T: Schema + EncodeFields + DecodeFields + Eq> Eq for VerifiedSignature<T> {}

impl<T: Schema + EncodeFields + DecodeFields + core::hash::Hash> core::hash::Hash
    for VerifiedSignature<T>
{
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.signed.hash(state);
    }
}

impl<T: Schema + EncodeFields + DecodeFields + PartialOrd> PartialOrd for VerifiedSignature<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.payload.partial_cmp(&other.payload) {
            Some(Ordering::Equal) => self
                .signed
                .issuer()
                .as_bytes()
                .partial_cmp(other.signed.issuer().as_bytes()),
            ord => ord,
        }
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl<T: Schema + EncodeFields + DecodeFields + Ord> Ord for VerifiedSignature<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.payload.cmp(&other.payload) {
            Ordering::Equal => self
                .signed
                .issuer()
                .as_bytes()
                .cmp(other.signed.issuer().as_bytes()),
            ord @ (Ordering::Less | Ordering::Greater) => ord,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    //! Deterministic regression tests for [`VerifiedSignature<T>`]
    //! trait impls.

    use alloc::vec::Vec;

    use ed25519_dalek::{Signer as _, SigningKey};
    use sedimentree_core::codec::{
        decode::{self, DecodeFields},
        encode::{self, EncodeFields},
        error::DecodeError,
        schema::{self, Schema},
    };

    use super::VerifiedSignature;
    use crate::signed::{SCHEMA_SIZE, SIGNATURE_SIZE, Signed, VERIFYING_KEY_SIZE};

    /// Minimal payload for these tests. Uses a schema byte distinct
    /// from `signed::regression::Payload` so each module's test
    /// fixtures stay self-contained.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct Payload {
        value: u64,
    }

    impl Schema for Payload {
        const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
        const TYPE_BYTE: u8 = b'V';
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

    fn seal_payload(key_bytes: [u8; 32], value: u64) -> Vec<u8> {
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

    fn verified(key_bytes: [u8; 32], value: u64) -> VerifiedSignature<Payload> {
        Signed::<Payload>::try_decode(&seal_payload(key_bytes, value))
            .expect("decode")
            .try_verify()
            .expect("verify")
    }

    /// `VerifiedSignature<T>::eq` delegates to the underlying
    /// `Signed<T>` and must distinguish different signed values.
    #[test]
    fn partial_eq_distinguishes_different_values() {
        let a = verified([3u8; 32], 555);
        let a_again = verified([3u8; 32], 555);
        let b = verified([3u8; 32], 999);

        assert_eq!(a, a, "self-equality must hold for VerifiedSignature");
        assert_eq!(
            a, a_again,
            "two verified seals of identical content must be equal"
        );
        assert_ne!(a, b, "different payloads must verify-equal-distinctly");
    }

    /// `VerifiedSignature<T>::hash` delegates to the inner `Signed<T>`
    /// and must distinguish different signed values.
    #[test]
    fn hash_distinguishes_different_values() {
        use core::hash::{BuildHasher as _, Hasher as _};
        use std::collections::hash_map::RandomState;

        let a = verified([7u8; 32], 100);
        let b = verified([7u8; 32], 200);

        let builder = RandomState::new();
        let mut ha = builder.build_hasher();
        let mut hb = builder.build_hasher();
        core::hash::Hash::hash(&a, &mut ha);
        core::hash::Hash::hash(&b, &mut hb);

        assert_ne!(
            ha.finish(),
            hb.finish(),
            "VerifiedSignature::hash must distinguish different signed values"
        );
    }

    /// `VerifiedSignature<T>::partial_cmp` must produce `Some(_)` for
    /// any two values, since `Ord` defines a total order.
    #[test]
    fn partial_cmp_is_total() {
        let a = verified([8u8; 32], 1);
        let b = verified([8u8; 32], 2);

        assert!(a.partial_cmp(&a).is_some());
        assert!(a.partial_cmp(&b).is_some());
        assert!(b.partial_cmp(&a).is_some());
        assert_ne!(a.partial_cmp(&b), b.partial_cmp(&a));
    }
}
