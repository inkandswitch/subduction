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
use subduction_core::peer::id::PeerId;
use subduction_crypto::signer::Signer;
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

use crate::peer_id::WasmPeerId;

#[wasm_bindgen]
extern "C" {
    /// Cryptographic signer interface.
    ///
    /// This allows JavaScript code to provide signing implementations
    /// (e.g., hardware keys or remote signing services).
    #[wasm_bindgen(js_name = Signer)]
    pub type JsSigner;

    /// Sign a message and return the 64-byte Ed25519 signature.
    /// Can return either a Uint8Array directly or a Promise<Uint8Array>.
    ///
    /// Accepts a `Uint8Array` (JS-heap owned) rather than `&[u8]` (Wasm memory view)
    /// to prevent detached ArrayBuffer errors when the JS signer re-enters Wasm
    /// and malloc grows memory.
    #[wasm_bindgen(method, js_name = sign)]
    fn js_sign(this: &JsSigner, message: Uint8Array) -> JsValue;

    /// Get the 32-byte Ed25519 verifying (public) key.
    #[wasm_bindgen(method, js_name = verifyingKey)]
    fn js_verifying_key(this: &JsSigner) -> Uint8Array;
}

impl JsSigner {
    /// Get the peer ID derived from this signer's verifying key.
    #[must_use]
    pub fn peer_id(&self) -> WasmPeerId {
        WasmPeerId::from(PeerId::from(Signer::<Local>::verifying_key(self)))
    }
}

impl Signer<Local> for JsSigner {
    /// Sign the message using the JavaScript signer.
    ///
    /// Handles both sync (returns `Uint8Array`) and async (returns `Promise<Uint8Array>`) signers.
    ///
    /// # Panics
    ///
    /// Because the [`Signer`] trait does not return `Result`, any
    /// failure in the JS-supplied signer becomes a panic that traps
    /// the entire Wasm module. Specifically, this method panics if:
    ///
    /// - `signer.sign()` returns a value that is not a `Uint8Array`
    ///   or a `Promise<Uint8Array>` (configuration bug in JS code)
    /// - the returned promise rejects (transient or permanent JS error)
    /// - the resolved value is not exactly 64 bytes (Ed25519 signature
    ///   size — likely indicates a wrong-curve signer)
    ///
    /// JS callers must therefore implement `Signer.sign` defensively.
    /// A future revision is expected to convert these to typed errors
    /// so that auth failures are recoverable rather than fatal — see
    /// the Phase 2 wasm-hardening plan.
    #[allow(clippy::expect_used)]
    fn sign(&self, message: &[u8]) -> <Local as FutureForm>::Future<'_, Signature> {
        // Copy message to a JS-heap Uint8Array before crossing the Wasm/JS boundary.
        // Passing &[u8] creates a view into Wasm linear memory; if the JS signer
        // re-enters Wasm (e.g. WebCryptoSigner.sign → passArray8ToWasm0 → malloc)
        // and malloc grows memory, the view's backing ArrayBuffer is detached.
        let js_message = Uint8Array::from(message);
        let result = self.js_sign(js_message);

        Local::from_future(async move {
            // Check if result is a Promise and await it if so
            #[allow(clippy::expect_used)]
            let sig_array: Uint8Array = if result.has_type::<Promise>() {
                let promise: Promise = result.dyn_into().expect(
                    "[Subduction/JsSigner] BUG: js_sign returned a value that is_promise() \
                     true but failed dyn_into::<Promise>(). This is an internal \
                     wasm-bindgen invariant violation.",
                );
                JsFuture::from(promise)
                    .await
                    .expect(
                        "[Subduction/JsSigner] Your `Signer.sign(message)` returned a Promise \
                     that rejected. Subduction cannot recover from a rejected sign call \
                     today; ensure your signer's promise resolves with a 64-byte \
                     Uint8Array (Ed25519 signature) or fix the underlying error \
                     before calling Subduction APIs.",
                    )
                    .dyn_into()
                    .expect(
                        "[Subduction/JsSigner] Your `Signer.sign(message)` returned a Promise \
                     that resolved to something other than a Uint8Array. The contract is: \
                     `sign(message: Uint8Array) => Uint8Array | Promise<Uint8Array>` where \
                     the result must be a 64-byte Ed25519 signature.",
                    )
            } else {
                result.dyn_into().expect(
                    "[Subduction/JsSigner] Your `Signer.sign(message)` returned something \
                     other than a Uint8Array or Promise. The contract is: \
                     `sign(message: Uint8Array) => Uint8Array | Promise<Uint8Array>` where \
                     the result must be a 64-byte Ed25519 signature.",
                )
            };

            let sig_bytes: Vec<u8> = sig_array.to_vec();
            #[allow(clippy::expect_used)]
            let sig_array: [u8; 64] = sig_bytes.try_into().unwrap_or_else(|v: Vec<u8>| {
                panic!(
                    "[Subduction/JsSigner] Your `Signer.sign(message)` returned a Uint8Array \
                     of length {} bytes; an Ed25519 signature must be exactly 64 bytes. \
                     If you are using a non-Ed25519 signer, Subduction does not yet support \
                     it.",
                    v.len()
                )
            });
            Signature::from_bytes(&sig_array)
        })
    }

    /// Get the verifying key from the JavaScript signer.
    ///
    /// # Panics
    ///
    /// As with [`sign`](Self::sign), failures in the JS-supplied signer
    /// become panics. Specifically, this panics if `verifyingKey()`
    /// returns a `Uint8Array` that is not exactly 32 bytes, or whose
    /// bytes are not a valid Ed25519 public-key encoding (e.g. a
    /// torsion-subgroup point or all zeros).
    ///
    /// This runs on _every_ authentication handshake — a misconfigured
    /// JS signer crashes the tab on the first connection attempt.
    #[allow(clippy::expect_used)]
    fn verifying_key(&self) -> VerifyingKey {
        let vk_bytes: Vec<u8> = self.js_verifying_key().to_vec();
        let vk_array: [u8; 32] = vk_bytes.try_into().unwrap_or_else(|v: Vec<u8>| {
            panic!(
                "[Subduction/JsSigner] Your `Signer.verifyingKey()` returned a Uint8Array \
                 of length {} bytes; an Ed25519 verifying key must be exactly 32 bytes. \
                 If you are using a non-Ed25519 signer, Subduction does not yet support it.",
                v.len()
            )
        });
        VerifyingKey::from_bytes(&vk_array).expect(
            "[Subduction/JsSigner] Your `Signer.verifyingKey()` returned 32 bytes that \
             do not encode a valid Ed25519 verifying key (likely a torsion-subgroup point \
             or an uninitialised buffer of zeros). Generate the key via \
             `ed25519_dalek::SigningKey::generate(...).verifying_key()` or equivalent.",
        )
    }
}
