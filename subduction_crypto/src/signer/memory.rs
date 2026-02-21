//! In-memory Ed25519 signer for development and testing.

use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
use future_form::{FutureForm, Local, Sendable, future_form};

use super::Signer;

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
            .field("verifying_key", &self.verifying_key())
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
    async fn sign_and_verify() {
        let key_bytes = [42u8; 32];
        let signer = MemorySigner::from_bytes(&key_bytes);

        let message = b"hello world";
        let signature = <MemorySigner as Signer<Sendable>>::sign(&signer, message).await;

        assert!(signer.verifying_key().verify(message, &signature).is_ok());
    }

    #[test]
    fn debug_does_not_leak_private_key() {
        let key_bytes = [42u8; 32];
        let signer = MemorySigner::from_bytes(&key_bytes);

        let debug_str = format!("{signer:?}");

        // Should contain verifying_key but not the raw signing key bytes
        assert!(debug_str.contains("MemorySigner"));
        assert!(debug_str.contains("verifying_key"));
        // Raw key bytes as hex would be "2a2a2a..." - should not appear
        assert!(!debug_str.contains("2a2a2a"));
    }
}
