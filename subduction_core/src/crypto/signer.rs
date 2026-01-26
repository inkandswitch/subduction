//! Signing trait for cryptographic key management abstraction.
//!
//! This module provides the [`Signer`] trait which abstracts over signing
//! operations, allowing different key management strategies (in-memory keys,
//! hardware security modules, remote signing services, etc.).

use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
use future_form::{FutureForm, Local, Sendable, future_form};

use crate::peer::id::PeerId;

/// A trait for signing data with an ed25519 key.
///
/// This abstraction allows different key management strategies:
/// - In-memory keys via [`LocalSigner`]
/// - Hardware security modules
/// - Remote signing services
/// - Key derivation schemes
///
/// The trait is generic over [`FutureForm`] to support both:
/// - `Sendable`: Thread-safe futures for multi-threaded runtimes like Tokio
/// - `Local`: Single-threaded futures for Wasm and local executors
///
/// For synchronous signers like [`LocalSigner`], the async overhead is negligible.
pub trait Signer<K: FutureForm> {
    /// Sign the given message bytes.
    fn sign(&self, message: &[u8]) -> K::Future<'_, Signature>;

    /// Get the verifying (public) key corresponding to this signer.
    fn verifying_key(&self) -> VerifyingKey;

    /// Get the peer ID derived from the verifying key.
    fn peer_id(&self) -> PeerId {
        PeerId::from(self.verifying_key())
    }
}

/// A local signer that holds an ed25519 signing key in memory.
#[derive(Clone)]
pub struct LocalSigner {
    signing_key: SigningKey,
}

impl LocalSigner {
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
    #[cfg(feature = "getrandom")]
    #[cfg_attr(docsrs, doc(cfg(feature = "getrandom")))]
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
impl<K: FutureForm> Signer<K> for LocalSigner {
    fn sign(&self, message: &[u8]) -> K::Future<'_, Signature> {
        use ed25519_dalek::Signer as _;
        let signature = self.signing_key.sign(message);
        K::from_future(async move { signature })
    }

    fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }
}

impl core::fmt::Debug for LocalSigner {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LocalSigner")
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
        let signer = LocalSigner::from_bytes(&key_bytes);

        let message = b"hello world";
        let signature = <LocalSigner as Signer<Sendable>>::sign(&signer, message).await;

        assert!(signer.verifying_key().verify(message, &signature).is_ok());
    }

    #[test]
    fn peer_id_matches_verifying_key() {
        let key_bytes = [42u8; 32];
        let signer = LocalSigner::from_bytes(&key_bytes);

        let expected_peer_id = PeerId::from(signer.verifying_key());
        assert_eq!(signer.peer_id(), expected_peer_id);
    }

    #[test]
    fn debug_does_not_leak_private_key() {
        let key_bytes = [42u8; 32];
        let signer = LocalSigner::from_bytes(&key_bytes);

        let debug_str = format!("{signer:?}");

        // Should contain peer_id but not the raw key bytes
        assert!(debug_str.contains("LocalSigner"));
        assert!(debug_str.contains("peer_id"));
        // Raw key bytes as hex would be "2a2a2a..." - should not appear
        assert!(!debug_str.contains("2a2a2a"));
    }
}
