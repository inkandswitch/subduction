//! Witness type proving that an author identity was extracted from a
//! cryptographically verified signature.
//!
//! [`VerifiedAuthor`] can only be constructed via
//! [`VerifiedSignature::verified_author`] or [`VerifiedMeta::verified_author`],
//! ensuring that the author's signing key has been verified before being
//! used for authorization decisions.
//!
//! [`VerifiedSignature::verified_author`]: crate::verified_signature::VerifiedSignature::verified_author
//! [`VerifiedMeta::verified_author`]: crate::verified_meta::VerifiedMeta::verified_author

use ed25519_dalek::VerifyingKey;

/// Proof that an author identity was extracted from a cryptographically
/// verified signature.
///
/// [`VerifiedSignature::verified_author`]: crate::verified_signature::VerifiedSignature::verified_author
/// [`VerifiedMeta::verified_author`]: crate::verified_meta::VerifiedMeta::verified_author
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedAuthor(VerifyingKey);

impl VerifiedAuthor {
    /// Create a new `VerifiedAuthor` from a verified verifying key.
    ///
    /// This is `pub(crate)` — only verification witnesses within
    /// `subduction_crypto` can construct this type.
    pub(crate) const fn new(verifying_key: VerifyingKey) -> Self {
        Self(verifying_key)
    }

    /// The Ed25519 verifying key of the verified author.
    #[must_use]
    pub const fn verifying_key(&self) -> &VerifyingKey {
        &self.0
    }
}

impl core::fmt::Display for VerifiedAuthor {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let bytes = self.0.as_bytes();
        write!(
            f,
            "VerifiedAuthor({:02x}{:02x}{:02x}{:02x}…)",
            bytes[0], bytes[1], bytes[2], bytes[3]
        )
    }
}
