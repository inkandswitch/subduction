//! Duck-typed signer interface for JavaScript.
//!
//! This module provides a JavaScript-compatible wrapper for the [`Signer`] trait,
//! allowing JavaScript code to provide signing implementations (e.g., using
//! Web Crypto API, hardware keys, or remote signing services).

use alloc::vec::Vec;

use ed25519_dalek::{Signature, VerifyingKey};
use futures_kind::{FutureKind, Local};
use js_sys::Uint8Array;
use subduction_core::crypto::signer::Signer;
use wasm_bindgen::prelude::*;

use crate::peer_id::WasmPeerId;

#[wasm_bindgen]
extern "C" {
    /// Cryptographic signer.
    #[wasm_bindgen(js_name = Signer)]
    pub type JsSigner;

    /// Sign a message and return the 64-byte Ed25519 signature.
    #[wasm_bindgen(method, js_name = sign)]
    fn js_sign(this: &JsSigner, message: &[u8]) -> Uint8Array;

    /// Get the 32-byte Ed25519 verifying (public) key.
    #[wasm_bindgen(method, js_name = verifyingKey)]
    fn js_verifying_key(this: &JsSigner) -> Uint8Array;
}

impl JsSigner {
    /// Get the peer ID derived from this signer's verifying key.
    #[must_use]
    pub fn peer_id(&self) -> WasmPeerId {
        WasmPeerId::from(<Self as Signer<Local>>::peer_id(self))
    }
}

impl Signer<Local> for JsSigner {
    /// Sign the message using the JavaScript signer.
    ///
    /// # Panics
    ///
    /// Panics if the JavaScript signer returns an invalid signature (not 64 bytes).
    #[allow(clippy::expect_used)]
    fn sign(&self, message: &[u8]) -> <Local as FutureKind>::Future<'_, Signature> {
        let sig_bytes: Vec<u8> = self.js_sign(message).to_vec();
        let sig_array: [u8; 64] = sig_bytes
            .try_into()
            .expect("JsSigner.sign must return exactly 64 bytes");
        let signature = Signature::from_bytes(&sig_array);
        Local::into_kind(async move { signature })
    }

    /// Get the verifying key from the JavaScript signer.
    ///
    /// # Panics
    ///
    /// Panics if the JavaScript signer returns an invalid public key.
    #[allow(clippy::expect_used)]
    fn verifying_key(&self) -> VerifyingKey {
        let vk_bytes: Vec<u8> = self.js_verifying_key().to_vec();
        let vk_array: [u8; 32] = vk_bytes
            .try_into()
            .expect("JsSigner.verifyingKey must return exactly 32 bytes");
        VerifyingKey::from_bytes(&vk_array)
            .expect("JsSigner.verifyingKey must return a valid Ed25519 public key")
    }
}
