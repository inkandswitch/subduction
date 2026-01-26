//! A payload that has been verified.

use core::cmp::Ordering;

use super::signed::Signed;

/// A payload whose signature has been verified.
///
/// This type is a **witness** that the signature is valid, either because:
/// - [`Signed::try_verify`](super::signed::Signed::try_verify) succeeded (remote data)
/// - [`Signed::seal`](super::signed::Signed::seal) created it (local data, self-signed)
///
/// It provides access to the payload that [`Signed<T>`](super::signed::Signed)
/// intentionally withholds, ensuring callers cannot skip verification.
///
/// ```text
/// Local:    T  ──seal──►  Verified<T>  ──putter──►  Storage
/// Remote:   Signed<T>  ──try_verify──►  Verified<T>  ──putter──►  Storage
/// ```
///
/// # Storage Pattern
///
/// `Verified<T>` retains the original [`Signed<T>`] so you can store the signed
/// version after verification. The [`Putter`] accepts `Verified<T>` to enforce
/// that only verified data enters storage.
///
/// For loading from storage, use [`Signed::decode_payload`](super::signed::Signed::decode_payload)
/// to extract the payload directly (storage is trusted).
///
/// # Wire Format
///
/// This type should NEVER be sent over the wire directly. Always transmit
/// [`Signed<T>`](super::signed::Signed) and have the recipient verify.
///
/// [`Putter`]: crate::storage::putter::Putter
#[derive(Clone, Debug)]
pub struct Verified<T: for<'a> minicbor::Decode<'a, ()>> {
    pub(super) signed: Signed<T>,
    pub(super) payload: T,
}

impl<T: for<'a> minicbor::Decode<'a, ()>> Verified<T> {
    /// Returns the verifying key of the issuer who signed this payload.
    #[must_use]
    pub fn issuer(&self) -> ed25519_dalek::VerifyingKey {
        self.signed.issuer()
    }

    /// Returns a reference to the verified payload.
    #[must_use]
    pub const fn payload(&self) -> &T {
        &self.payload
    }

    /// Consumes the `Verified` and returns the payload.
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

    /// Consumes the `Verified` and returns the original signed value.
    #[must_use]
    pub fn into_signed(self) -> Signed<T> {
        self.signed
    }
}

impl<T: for<'a> minicbor::Decode<'a, ()> + PartialEq> PartialEq for Verified<T> {
    fn eq(&self, other: &Self) -> bool {
        // Compare by signed value (includes issuer, signature, encoded payload)
        self.signed == other.signed
    }
}

impl<T: for<'a> minicbor::Decode<'a, ()> + Eq> Eq for Verified<T> {}

impl<T: for<'a> minicbor::Decode<'a, ()> + core::hash::Hash> core::hash::Hash for Verified<T> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.signed.issuer().as_bytes().hash(state);
        self.signed.encoded_payload().as_slice().hash(state);
    }
}

impl<T: for<'a> minicbor::Decode<'a, ()> + PartialOrd> PartialOrd for Verified<T> {
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
impl<T: for<'a> minicbor::Decode<'a, ()> + Ord> Ord for Verified<T> {
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
