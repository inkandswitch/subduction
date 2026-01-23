use core::cmp::Ordering;

/// A payload that has been verified against a signing key.
///
/// This type is used internally after signature verification.
/// It should not be sent over the wire directly.
#[derive(Clone, Debug, PartialEq, Eq, Hash, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Verified<T> {
    #[cbor(with = "crate::cbor::verifying_key")]
    #[n(0)]
    pub(super) issuer: ed25519_dalek::VerifyingKey,

    #[n(1)]
    pub(super) payload: T,
}

impl<T> Verified<T> {
    /// Returns the verifying key of the issuer who signed this payload.
    #[must_use]
    pub fn issuer(&self) -> &ed25519_dalek::VerifyingKey {
        &self.issuer
    }

    /// Returns a reference to the verified payload.
    #[must_use]
    pub fn payload(&self) -> &T {
        &self.payload
    }
}

impl<T: PartialOrd> PartialOrd for Verified<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.payload.partial_cmp(&other.payload) {
            Some(Ordering::Equal) => self.issuer.as_bytes().partial_cmp(other.issuer.as_bytes()),
            non_eq => non_eq,
        }
    }
}

impl<T: Ord> Ord for Verified<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.payload.cmp(&other.payload) {
            Ordering::Equal => self.issuer.as_bytes().cmp(other.issuer.as_bytes()),
            non_eq => non_eq,
        }
    }
}
