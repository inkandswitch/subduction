//! In-memory Ed25519 signer for Wasm.
//!
//! This wraps the core [`MemorySigner`] with `wasm_bindgen` bindings,
//! exposing the [`JsSigner`](crate::signer::JsSigner) interface so it
//! can be used from JavaScript as an in-memory signing key (useful for
//! development, testing, and ephemeral sessions).

use future_form::Local;
use js_sys::Uint8Array;
use subduction_core::peer::id::PeerId;
use subduction_crypto::signer::{Signer, memory::MemorySigner};
use wasm_bindgen::prelude::*;

use crate::peer_id::WasmPeerId;

/// An in-memory Ed25519 signer exposed to JavaScript.
///
/// Implements the `Signer` interface (`sign` / `verifyingKey`) so it can be
/// passed anywhere a [`JsSigner`](crate::signer::JsSigner) is expected.
///
/// # Example
///
/// ```javascript
/// import { MemorySigner } from "@automerge/subduction";
///
/// const signer = MemorySigner.generate();
/// console.log("Peer ID:", signer.peerId().toString());
/// ```
#[wasm_bindgen(js_name = MemorySigner)]
#[derive(Debug)]
pub struct JsMemorySigner(MemorySigner);

impl Default for JsMemorySigner {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen(js_class = MemorySigner)]
impl JsMemorySigner {
    /// Create a new signer with a randomly generated Ed25519 key.
    ///
    /// This is the JS constructor (`new MemorySigner()`), equivalent to
    /// [`generate`](Self::generate).
    ///
    /// # Panics
    ///
    /// Panics if the system random number generator fails.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new() -> Self {
        Self::generate()
    }

    /// Create a new signer with a randomly generated Ed25519 key.
    ///
    /// # Panics
    ///
    /// Panics if the system random number generator fails.
    #[wasm_bindgen]
    #[must_use]
    pub fn generate() -> Self {
        Self(MemorySigner::generate())
    }

    /// Create a signer from raw 32-byte Ed25519 secret key bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` is not exactly 32 bytes.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> Result<JsMemorySigner, JsValue> {
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| JsValue::from_str("secret key must be exactly 32 bytes"))?;
        Ok(Self(MemorySigner::from_bytes(&arr)))
    }

    /// Sign a message and return the 64-byte Ed25519 signature as a `Uint8Array`.
    ///
    /// This fulfils the `Signer.sign(message)` interface contract.
    /// Returns a `Promise<Uint8Array>` for consistency with the async signer interface.
    pub async fn sign(&self, message: &[u8]) -> Uint8Array {
        let signature = Signer::<Local>::sign(&self.0, message).await;
        Uint8Array::from(signature.to_bytes().as_slice())
    }

    /// Get the 32-byte Ed25519 verifying (public) key as a `Uint8Array`.
    ///
    /// This fulfils the `Signer.verifyingKey()` interface contract.
    #[wasm_bindgen(js_name = verifyingKey)]
    #[must_use]
    pub fn verifying_key(&self) -> Uint8Array {
        let vk = Signer::<Local>::verifying_key(&self.0);
        Uint8Array::from(vk.to_bytes().as_slice())
    }

    /// Get the peer ID derived from this signer's verifying key.
    #[wasm_bindgen(js_name = peerId)]
    #[must_use]
    pub fn peer_id(&self) -> WasmPeerId {
        let vk = Signer::<Local>::verifying_key(&self.0);
        WasmPeerId::from(PeerId::from(vk))
    }
}
