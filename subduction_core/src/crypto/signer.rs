//! Signing trait for cryptographic key management abstraction.
//!
//! This module provides the [`Signer`] trait which abstracts over signing
//! operations, allowing different key management strategies (in-memory keys,
//! hardware security modules, remote signing services, etc.).

use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
use future_form::{FutureForm, Local, Sendable, future_form};
use subduction_crypto::{
    Signed, VerifiedSignature,
    signed::{EncodedPayload, Envelope, Magic, ProtocolVersion},
};

use crate::peer::id::PeerId;

/// A trait for signing data with an ed25519 key.
///
/// This abstraction allows different key management strategies:
/// - In-memory keys via [`MemorySigner`]
/// - Hardware security modules
/// - Remote signing services
/// - Key derivation schemes
///
/// For synchronous signers like [`MemorySigner`], the async overhead is negligible.
pub trait Signer<K: FutureForm> {
    /// Sign the given message bytes.
    fn sign(&self, message: &[u8]) -> K::Future<'_, Signature>;

    /// Get the verifying (public) key corresponding to this signer.
    fn verifying_key(&self) -> VerifyingKey;

    /// Get the peer ID derived from the verifying key.
    fn peer_id(&self) -> PeerId {
        PeerId::from(self.verifying_key())
    }

    /// Seal a payload with this signer's cryptographic signature.
    ///
    /// Returns a [`VerifiedSignature<T>`] since we know our own signature is valid.
    /// Use [`.into_signed()`](VerifiedSignature::into_signed) to get the [`Signed<T>`]
    /// for wire transmission.
    ///
    /// # Panics
    ///
    /// Panics if CBOR encoding fails (should never happen for well-formed types).
    #[allow(clippy::expect_used)]
    async fn seal<T>(&self, payload: T) -> VerifiedSignature<T>
    where
        T: minicbor::Encode<()> + for<'a> minicbor::Decode<'a, ()>,
    {
        let envelope = Envelope::new(Magic, ProtocolVersion::V0_1, payload);
        let encoded = minicbor::to_vec(&envelope).expect("envelope encoding should not fail");
        let signature = self.sign(&encoded).await;

        let signed = Signed::new(
            self.verifying_key(),
            signature,
            EncodedPayload::new(encoded),
        );

        // Since we just signed it, verification is guaranteed to succeed
        signed
            .try_verify()
            .expect("self-signed payload should verify")
    }
}

/// An in-memory signer that holds an ed25519 signing key.
#[derive(Clone)]
pub struct MemorySigner {
    signing_key: SigningKey,
}

impl MemorySigner {
    /// Create a new local signer from a signing key.
    #[must_use]
    pub const fn new(signing_key: SigningKey) -> Self {
        Self { signing_key }
    }

    /// Create a new local signer with a randomly generated key.
    ///
    /// # Panics
    ///
    /// Panics if the system random number generator fails.
    #[allow(clippy::expect_used)]
    #[must_use]
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        getrandom::getrandom(&mut bytes).expect("getrandom failed");
        Self::new(SigningKey::from_bytes(&bytes))
    }

    /// Create a local signer from raw key bytes.
    #[must_use]
    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Self::new(SigningKey::from_bytes(bytes))
    }

    /// Get the verifying (public) key.
    #[must_use]
    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// Get the peer ID derived from the verifying key.
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        PeerId::from(self.verifying_key())
    }
}

#[future_form(Sendable, Local)]
impl<K: FutureForm> Signer<K> for MemorySigner {
    fn sign(&self, message: &[u8]) -> K::Future<'_, Signature> {
        use ed25519_dalek::Signer as _;
        let signature = self.signing_key.sign(message);
        K::from_future(async move { signature })
    }

    fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }
}

impl core::fmt::Debug for MemorySigner {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MemorySigner")
            .field("peer_id", &self.peer_id())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;
    use ed25519_dalek::Verifier;
    use future_form::Sendable;

    #[tokio::test]
    async fn local_signer_sign_and_verify() {
        let key_bytes = [42u8; 32];
        let signer = MemorySigner::from_bytes(&key_bytes);

        let message = b"hello world";
        let signature = <MemorySigner as Signer<Sendable>>::sign(&signer, message).await;

        assert!(signer.verifying_key().verify(message, &signature).is_ok());
    }

    #[test]
    fn peer_id_matches_verifying_key() {
        let key_bytes = [42u8; 32];
        let signer = MemorySigner::from_bytes(&key_bytes);

        let expected_peer_id = PeerId::from(signer.verifying_key());
        assert_eq!(signer.peer_id(), expected_peer_id);
    }

    #[test]
    fn debug_does_not_leak_private_key() {
        let key_bytes = [42u8; 32];
        let signer = MemorySigner::from_bytes(&key_bytes);

        let debug_str = format!("{signer:?}");

        // Should contain peer_id but not the raw key bytes
        assert!(debug_str.contains("MemorySigner"));
        assert!(debug_str.contains("peer_id"));
        // Raw key bytes as hex would be "2a2a2a..." - should not appear
        assert!(!debug_str.contains("2a2a2a"));
    }
}
