//! A payload whose signature has been verified.

use core::cmp::Ordering;

use sedimentree_core::codec::{
    decode::Decode, encode::EncodeFields, error::DecodeError, schema::Schema,
};

use crate::signed::{Signed, VerificationError};

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
pub struct VerifiedSignature<T: Schema + EncodeFields + Decode> {
    signed: Signed<T>,
    payload: T,
}

impl<T: Schema + EncodeFields + Decode> VerifiedSignature<T> {
    /// Verify the signature and decode the payload.
    ///
    /// This is the primary way to create a `VerifiedSignature` from untrusted data.
    ///
    /// # Errors
    ///
    /// Returns an error if the signature is invalid or the payload cannot be decoded.
    pub fn try_from_signed(signed: &Signed<T>) -> Result<Self, VerificationError> {
        // Verify signature over payload bytes
        signed
            .issuer()
            .verify_strict(signed.payload_bytes(), signed.signature())
            .map_err(|_| VerificationError::InvalidSignature)?;

        // Decode payload from fields bytes
        let payload = T::try_decode_fields(signed.fields_bytes())?;

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
    pub fn try_from_trusted(signed: Signed<T>) -> Result<Self, DecodeError> {
        let payload = signed.try_decode_trusted_payload()?;
        Ok(Self { signed, payload })
    }
}

impl<T: Schema + EncodeFields + Decode + PartialEq> PartialEq for VerifiedSignature<T> {
    fn eq(&self, other: &Self) -> bool {
        // Compare by signed value (includes issuer, signature, encoded payload)
        self.signed == other.signed
    }
}

impl<T: Schema + EncodeFields + Decode + Eq> Eq for VerifiedSignature<T> {}

impl<T: Schema + EncodeFields + Decode + core::hash::Hash> core::hash::Hash
    for VerifiedSignature<T>
{
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.signed.hash(state);
    }
}

impl<T: Schema + EncodeFields + Decode + PartialOrd> PartialOrd for VerifiedSignature<T> {
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
impl<T: Schema + EncodeFields + Decode + Ord> Ord for VerifiedSignature<T> {
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
