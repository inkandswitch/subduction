//! Ed25519 signer using the browser's `WebCrypto` API.
//!
//! This module provides a ready-to-use signer that uses `crypto.subtle`
//! for secure key generation and signing operations. Keys are persisted
//! to `IndexedDB` so they survive page reloads.
//!
//! Works in both window and Web Worker contexts by accessing APIs through
//! `globalThis` rather than `window`.

use ed25519_dalek::{Signature, VerifyingKey};
use future_form::{FutureForm, Local};
use js_sys::Uint8Array;
use subduction_crypto::signer::Signer;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::peer_id::WasmPeerId;

/// Get the `SubtleCrypto` object from the global scope.
///
/// Works in both window and Web Worker contexts.
fn get_subtle_crypto() -> Result<web_sys::SubtleCrypto, JsValue> {
    let global = js_sys::global();

    let crypto = js_sys::Reflect::get(&global, &JsValue::from_str("crypto"))
        .map_err(|_| JsValue::from_str("crypto not available in this context"))?;

    if crypto.is_undefined() {
        return Err(JsValue::from_str("crypto not available in this context"));
    }

    let crypto: web_sys::Crypto = crypto
        .dyn_into()
        .map_err(|_| JsValue::from_str("crypto is not a Crypto object"))?;

    Ok(crypto.subtle())
}

/// Get the `IDBFactory` from the global scope.
///
/// Works in both window and Web Worker contexts.
fn get_idb_factory() -> Result<web_sys::IdbFactory, JsValue> {
    let global = js_sys::global();

    let idb = js_sys::Reflect::get(&global, &JsValue::from_str("indexedDB"))
        .map_err(|_| JsValue::from_str("indexedDB not available in this context"))?;

    if idb.is_undefined() {
        return Err(JsValue::from_str("indexedDB not available in this context"));
    }

    idb.dyn_into()
        .map_err(|_| JsValue::from_str("indexedDB is not an IDBFactory object"))
}

const DB_NAME: &str = "subduction-signer";
const STORE_NAME: &str = "keys";
const KEY_ID: &str = "default";

/// An Ed25519 signer using the browser's `WebCrypto` API.
///
/// This signer generates and stores Ed25519 keys using `crypto.subtle`,
/// providing secure key generation and signing operations. The key is
/// persisted to `IndexedDB` so it survives page reloads.
///
/// # Example
///
/// ```javascript
/// import { WebCryptoSigner } from "@anthropic/subduction";
///
/// const signer = await WebCryptoSigner.setup();
/// console.log("Peer ID:", signer.peerId().toString());
/// ```
#[wasm_bindgen(js_name = WebCryptoSigner)]
#[derive(Debug)]
pub struct WebCryptoSigner {
    private_key: web_sys::CryptoKey,
    public_key_bytes: [u8; 32],
}

#[wasm_bindgen(js_class = WebCryptoSigner)]
impl WebCryptoSigner {
    /// Set up the signer, loading an existing key from `IndexedDB` or generating a new one.
    ///
    /// # Errors
    ///
    /// Returns an error if `WebCrypto` or `IndexedDB` operations fail.
    pub async fn setup() -> Result<WebCryptoSigner, JsValue> {
        // Try to load existing key from IndexedDB
        if let Some(signer) = Self::load_from_idb().await? {
            return Ok(signer);
        }

        // Generate new key and persist it
        let signer = Self::generate_new().await?;
        signer.save_to_idb().await?;
        Ok(signer)
    }

    /// Generate a new signer without persisting (for testing or ephemeral use).
    async fn generate_new() -> Result<WebCryptoSigner, JsValue> {
        let subtle = get_subtle_crypto()?;

        // Generate Ed25519 keypair
        let algorithm = js_sys::Object::new();
        js_sys::Reflect::set(&algorithm, &"name".into(), &"Ed25519".into())?;

        let key_pair = JsFuture::from(subtle.generate_key_with_object(
            &algorithm,
            true, // extractable (needed for persistence)
            &js_sys::Array::of2(&"sign".into(), &"verify".into()),
        )?)
        .await?;

        let key_pair = js_sys::Object::from(key_pair);
        let private_key: web_sys::CryptoKey =
            js_sys::Reflect::get(&key_pair, &"privateKey".into())?.into();
        let public_key: web_sys::CryptoKey =
            js_sys::Reflect::get(&key_pair, &"publicKey".into())?.into();

        // Export public key to raw bytes
        let public_key_buffer = JsFuture::from(subtle.export_key("raw", &public_key)?).await?;
        let public_key_array = js_sys::Uint8Array::new(&public_key_buffer);
        let mut public_key_bytes = [0u8; 32];
        public_key_array.copy_to(&mut public_key_bytes);

        Ok(Self {
            private_key,
            public_key_bytes,
        })
    }

    /// Load the signer from `IndexedDB` if it exists.
    async fn load_from_idb() -> Result<Option<WebCryptoSigner>, JsValue> {
        let idb_factory = get_idb_factory()?;

        // Open database with version and upgrade handler to ensure store exists.
        // This prevents a race where open() without version creates an empty DB,
        // then save_to_idb()'s upgrade handler never runs because version matches.
        let open_request = idb_factory.open_with_u32(DB_NAME, 1)?;

        let store_name = STORE_NAME;
        #[allow(clippy::expect_used)]
        let onupgradeneeded = Closure::once(move |event: web_sys::IdbVersionChangeEvent| {
            let db: web_sys::IdbDatabase = event
                .target()
                .and_then(|t| t.dyn_into::<web_sys::IdbOpenDbRequest>().ok())
                .and_then(|r| r.result().ok())
                .and_then(|r| r.dyn_into().ok())
                .expect("database from upgrade event");
            if !db.object_store_names().contains(store_name) {
                db.create_object_store(store_name)
                    .expect("create object store");
            }
        });
        open_request.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));

        let db = Self::await_idb_request(&open_request).await?;
        drop(onupgradeneeded);
        let db: web_sys::IdbDatabase = db.into();

        // Read from store
        let tx = db.transaction_with_str(STORE_NAME)?;
        let store = tx.object_store(STORE_NAME)?;
        let get_request = store.get(&KEY_ID.into())?;
        let result = Self::await_idb_request(&get_request).await?;
        db.close();

        if result.is_undefined() || result.is_null() {
            return Ok(None);
        }

        // Parse stored data
        let stored = js_sys::Object::from(result);
        let jwk = js_sys::Reflect::get(&stored, &"privateKeyJwk".into())?;
        let public_key_array: Uint8Array =
            js_sys::Reflect::get(&stored, &"publicKeyBytes".into())?.into();

        let mut public_key_bytes = [0u8; 32];
        public_key_array.copy_to(&mut public_key_bytes);

        // Import private key from JWK
        let subtle = get_subtle_crypto()?;

        let algorithm = js_sys::Object::new();
        js_sys::Reflect::set(&algorithm, &"name".into(), &"Ed25519".into())?;

        let private_key = JsFuture::from(subtle.import_key_with_object(
            "jwk",
            &jwk.into(),
            &algorithm,
            true,
            &js_sys::Array::of1(&"sign".into()),
        )?)
        .await?;

        Ok(Some(Self {
            private_key: private_key.into(),
            public_key_bytes,
        }))
    }

    /// Save the signer to `IndexedDB`.
    async fn save_to_idb(&self) -> Result<(), JsValue> {
        let subtle = get_subtle_crypto()?;
        let idb_factory = get_idb_factory()?;

        // Export private key as JWK
        let jwk = JsFuture::from(subtle.export_key("jwk", &self.private_key)?).await?;

        // Open database with upgrade if needed
        let open_request = idb_factory.open_with_u32(DB_NAME, 1)?;

        // Set up upgrade handler to create store
        let store_name = STORE_NAME;
        #[allow(clippy::expect_used)]
        let onupgradeneeded = Closure::once(move |event: web_sys::IdbVersionChangeEvent| {
            let db: web_sys::IdbDatabase = event
                .target()
                .and_then(|t| t.dyn_into::<web_sys::IdbOpenDbRequest>().ok())
                .and_then(|r| r.result().ok())
                .and_then(|r| r.dyn_into().ok())
                .expect("database from upgrade event");
            if !db.object_store_names().contains(store_name) {
                db.create_object_store(store_name)
                    .expect("create object store");
            }
        });
        open_request.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));

        let db = Self::await_idb_request(&open_request).await?;
        drop(onupgradeneeded);
        let db: web_sys::IdbDatabase = db.into();

        // Store the key data
        let tx =
            db.transaction_with_str_and_mode(STORE_NAME, web_sys::IdbTransactionMode::Readwrite)?;
        let store = tx.object_store(STORE_NAME)?;

        let data = js_sys::Object::new();
        js_sys::Reflect::set(&data, &"privateKeyJwk".into(), &jwk)?;
        js_sys::Reflect::set(
            &data,
            &"publicKeyBytes".into(),
            &Uint8Array::from(self.public_key_bytes.as_slice()),
        )?;

        let put_request = store.put_with_key(&data, &KEY_ID.into())?;
        Self::await_idb_request(&put_request).await?;
        db.close();

        Ok(())
    }

    /// Helper to await an IDB request.
    async fn await_idb_request(request: &web_sys::IdbRequest) -> Result<JsValue, JsValue> {
        let (tx, rx) = futures::channel::oneshot::channel::<Result<(), JsValue>>();
        let tx = core::cell::RefCell::new(Some(tx));

        let onsuccess = Closure::once(move |_event: web_sys::Event| {
            if let Some(tx) = tx.borrow_mut().take() {
                drop(tx.send(Ok(())));
            }
        });

        let tx_err = core::cell::RefCell::new(Some(()));
        let onerror = Closure::once(move |_event: web_sys::Event| {
            tx_err.borrow_mut().take();
        });

        request.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
        request.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        rx.await
            .map_err(|_| JsValue::from_str("IDB request canceled"))??;

        request.result()
    }

    /// Sign a message and return the 64-byte Ed25519 signature.
    ///
    /// # Errors
    ///
    /// Returns an error if `WebCrypto` signing fails.
    pub async fn sign(&self, message: &[u8]) -> Result<Uint8Array, JsValue> {
        let subtle = get_subtle_crypto()?;

        let algorithm = js_sys::Object::new();
        js_sys::Reflect::set(&algorithm, &"name".into(), &"Ed25519".into())?;

        let signature_buffer = JsFuture::from(subtle.sign_with_object_and_u8_array(
            &algorithm,
            &self.private_key,
            message,
        )?)
        .await?;

        Ok(js_sys::Uint8Array::new(&signature_buffer))
    }

    /// Get the 32-byte Ed25519 verifying (public) key.
    #[wasm_bindgen(js_name = verifyingKey)]
    #[must_use]
    pub fn verifying_key(&self) -> Uint8Array {
        Uint8Array::from(self.public_key_bytes.as_slice())
    }

    /// Get the peer ID derived from this signer's verifying key.
    ///
    /// # Panics
    ///
    /// Panics if the stored public key bytes are invalid (should never happen).
    #[wasm_bindgen(js_name = peerId)]
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn peer_id(&self) -> WasmPeerId {
        let vk = VerifyingKey::from_bytes(&self.public_key_bytes)
            .expect("public key bytes should be valid");
        WasmPeerId::from(subduction_core::peer::id::PeerId::from(vk))
    }
}

impl Signer<Local> for WebCryptoSigner {
    #[allow(clippy::expect_used)]
    fn sign(&self, message: &[u8]) -> <Local as FutureForm>::Future<'_, Signature> {
        let message = message.to_vec();
        Local::from_future(async move {
            let sig_array = WebCryptoSigner::sign(self, &message)
                .await
                .expect("WebCrypto signing failed");

            let mut sig_bytes = [0u8; 64];
            sig_array.copy_to(&mut sig_bytes);
            Signature::from_bytes(&sig_bytes)
        })
    }

    fn verifying_key(&self) -> VerifyingKey {
        #[allow(clippy::expect_used)]
        VerifyingKey::from_bytes(&self.public_key_bytes).expect("public key bytes should be valid")
    }
}
