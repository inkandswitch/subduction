//! Structural adapters around private, stateful implementation drivers.
use crate::{
    memory_signer::JsMemorySigner,
    signer::{JsSigner, webcrypto::WebCryptoSigner},
    transport::{JsTransport, message_port::WasmMessagePortTransport},
};
use sedimentree_wasm::pojo;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(inline_js = r#"
export function signerObject(driver) {
    return {
        sign(message) { return driver.sign(message); },
        verifyingKey() { return driver.verifyingKey(); },
        peerId() { return Array.from(driver.verifyingKey(), b => b.toString(16).padStart(2, '0')).join(''); },
    };
}
export function transportObject(driver) {
    return {
        sendBytes(bytes) { return driver.sendBytes(bytes); },
        recvBytes() { return driver.recvBytes(); },
        disconnect() { return driver.disconnect(); },
        onDisconnect(callback) { return driver.onDisconnect(callback); },
    };
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = signerObject)]
    fn signer_object(driver: JsValue) -> JsSigner;
    #[wasm_bindgen(js_name = transportObject)]
    pub(crate) fn transport_object(driver: JsValue) -> JsTransport;
}

#[wasm_bindgen(typescript_custom_section)]
const TYPES: &str =
    "export function createMemorySigner(seed?: Uint8Array): Signer & { peerId(): PeerId };";

/// Generate an in-memory identity, or import a 32-byte Ed25519 secret seed.
/// The returned service is caller-owned and may be shared between nodes.
///
/// # Errors
/// Rejects seeds that are not an ordinary 32-byte `Uint8Array`.
#[wasm_bindgen(js_name = createMemorySigner, skip_typescript)]
pub fn create_memory_signer(seed: &JsValue) -> Result<JsSigner, JsValue> {
    let signer = if seed.is_undefined() {
        JsMemorySigner::generate()
    } else {
        let bytes = pojo::bytes(seed, "seed")?;
        if bytes.len() != 32 {
            return Err(pojo::invalid("seed", "expected 32 bytes"));
        }
        JsMemorySigner::from_bytes(&bytes)?
    };
    Ok(signer_object(signer.into()))
}

/// Open the existing persisted `WebCrypto` identity as a structural signer.
///
/// # Errors
/// Propagates key-generation and identity-storage failures.
#[wasm_bindgen(js_name = createWebCryptoSigner, unchecked_return_type = "Signer & { peerId(): PeerId }")]
pub async fn create_web_crypto_signer() -> Result<JsSigner, JsValue> {
    Ok(signer_object(WebCryptoSigner::setup().await?.into()))
}

/// Construct a structural transport. The admitting node takes responsibility for
/// disconnecting it; the implementation driver remains private.
pub(crate) fn message_port(port: JsValue) -> JsTransport {
    transport_object(WasmMessagePortTransport::new(port).into())
}
