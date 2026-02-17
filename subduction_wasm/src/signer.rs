//! Signer implementations for Wasm.
//!
//! This module provides two signer implementations:
//!
//! - [`JsSigner`]: An interface for JavaScript-provided signers
//! - [`webcrypto::WebCryptoSigner`]: A ready-to-use Ed25519 signer using the browser's `WebCrypto` API

pub mod webcrypto;

use alloc::vec::Vec;

use ed25519_dalek::{Signature, VerifyingKey};
use future_form::{FutureForm, Local};
use js_sys::{Promise, Uint8Array};
use keyhive_core::crypto::{
    signed::SigningError as KeyhiveSigningError, signer::sync_signer::SyncSigner,
    verifiable::Verifiable,
};
use subduction_core::crypto::signer::Signer;
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::JsFuture;

use crate::peer_id::WasmPeerId;

#[wasm_bindgen]
extern "C" {
    /// Cryptographic signer interface.
    ///
    /// This allows JavaScript code to provide signing implementations
    /// (e.g., hardware keys or remote signing services).
    #[wasm_bindgen(js_name = Signer)]
    #[derive(Clone)]
    pub type JsSigner;

    /// Sign a message and return the 64-byte Ed25519 signature.
    /// Can return either a Uint8Array directly or a Promise<Uint8Array>.
    #[wasm_bindgen(method, js_name = sign)]
    fn js_sign(this: &JsSigner, message: &[u8]) -> JsValue;

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
    /// Handles both sync (returns `Uint8Array`) and async (returns `Promise<Uint8Array>`) signers.
    ///
    /// # Panics
    ///
    /// Panics if the JavaScript signer returns an invalid signature (not 64 bytes).
    #[allow(clippy::expect_used)]
    fn sign(&self, message: &[u8]) -> <Local as FutureForm>::Future<'_, Signature> {
        let result = self.js_sign(message);

        Local::from_future(async move {
            // Check if result is a Promise and await it if so
            let sig_array: Uint8Array = if result.has_type::<Promise>() {
                let promise: Promise = result.unchecked_into();
                JsFuture::from(promise)
                    .await
                    .expect("JsSigner.sign promise rejected")
                    .unchecked_into()
            } else {
                result.unchecked_into()
            };

            let sig_bytes: Vec<u8> = sig_array.to_vec();
            let sig_array: [u8; 64] = sig_bytes
                .try_into()
                .expect("JsSigner.sign must return exactly 64 bytes");
            Signature::from_bytes(&sig_array)
        })
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

// Keyhive trait implementations for JsSigner.
// Note: JsSigner is fundamentally async (JavaScript Promises), so SyncSigner
// cannot be properly implemented. These impls allow Subduction to compile,
// but keyhive sync functionality is not supported with JsSigner.

impl Verifiable for JsSigner {
    fn verifying_key(&self) -> VerifyingKey {
        <Self as Signer<Local>>::verifying_key(self)
    }
}

impl SyncSigner for JsSigner {
    fn try_sign_bytes_sync(&self, _payload_bytes: &[u8]) -> Result<Signature, KeyhiveSigningError> {
        // JsSigner is async-only (JavaScript Promises). Keyhive sync is not
        // supported in Wasm with JsSigner. Use MemorySigner if keyhive sync
        // is needed.
        unimplemented!(
            "JsSigner does not support synchronous signing. \
             Keyhive sync requires a signer that implements SyncSigner. \
             Use MemorySigner or WebCryptoSigner for keyhive sync support."
        )
    }
}
