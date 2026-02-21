//! Signing trait for cryptographic key management abstraction.
//!
//! This module provides the [`Signer`] trait which abstracts over signing
//! operations, allowing different key management strategies (in-memory keys,
//! hardware security modules, remote signing services, etc.).

pub mod memory;

use ed25519_dalek::{Signature, VerifyingKey};
use future_form::FutureForm;

/// A trait for signing data with an ed25519 key.
///
/// This abstraction allows different key management strategies:
/// - In-memory keys via [`memory::MemorySigner`]
/// - Hardware security modules
/// - Remote signing services
/// - Key derivation schemes
///
/// For synchronous signers, the async overhead is negligible.
///
/// # Sealing Payloads
///
/// Use [`Signed::seal`](crate::signed::Signed::seal) to sign a payload:
///
/// ```ignore
/// Signed::seal::<Sendable, _>(&signer, payload).await
/// ```
pub trait Signer<K: FutureForm> {
    /// Sign the given message bytes.
    fn sign(&self, message: &[u8]) -> K::Future<'_, Signature>;

    /// Get the verifying (public) key corresponding to this signer.
    fn verifying_key(&self) -> VerifyingKey;
}
