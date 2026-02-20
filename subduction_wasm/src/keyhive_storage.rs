//! JavaScript-backed keyhive storage.
//!
//! Implements [`KeyhiveArchiveStorage`] and [`KeyhiveEventStorage`] by calling
//! methods on the JavaScript storage bridge.

use alloc::{format, string::String, vec::Vec};
use core::fmt;

use future_form::Local;
use futures::future::LocalBoxFuture;
use futures::FutureExt;
use js_sys::{Array, Promise, Reflect, Uint8Array};
use subduction_keyhive::storage::{KeyhiveArchiveStorage, KeyhiveEventStorage, StorageHash};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(typescript_custom_section)]
const TS_KEYHIVE_STORAGE: &str = r#"
/**
 * Interface for keyhive storage operations.
 * The SubductionStorageBridge implements this interface.
 */
export interface KeyhiveStorageInterface {
    saveKeyhiveArchive(hash: Uint8Array, data: Uint8Array): Promise<void>;
    loadKeyhiveArchives(): Promise<Array<{hash: Uint8Array, data: Uint8Array}>>;
    deleteKeyhiveArchive(hash: Uint8Array): Promise<void>;

    saveKeyhiveEvent(hash: Uint8Array, data: Uint8Array): Promise<void>;
    loadKeyhiveEvents(): Promise<Array<{hash: Uint8Array, data: Uint8Array}>>;
    deleteKeyhiveEvent(hash: Uint8Array): Promise<void>;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// Duck-typed storage interface for keyhive data.
    /// The JavaScript SubductionStorageBridge implements this interface.
    #[wasm_bindgen(typescript_type = "KeyhiveStorageInterface")]
    pub type JsKeyhiveStorage;

    #[wasm_bindgen(method, js_name = saveKeyhiveArchive)]
    fn js_save_archive(this: &JsKeyhiveStorage, hash: &[u8], data: &[u8]) -> Promise;

    #[wasm_bindgen(method, js_name = loadKeyhiveArchives)]
    fn js_load_archives(this: &JsKeyhiveStorage) -> Promise;

    #[wasm_bindgen(method, js_name = deleteKeyhiveArchive)]
    fn js_delete_archive(this: &JsKeyhiveStorage, hash: &[u8]) -> Promise;

    #[wasm_bindgen(method, js_name = saveKeyhiveEvent)]
    fn js_save_event(this: &JsKeyhiveStorage, hash: &[u8], data: &[u8]) -> Promise;

    #[wasm_bindgen(method, js_name = loadKeyhiveEvents)]
    fn js_load_events(this: &JsKeyhiveStorage) -> Promise;

    #[wasm_bindgen(method, js_name = deleteKeyhiveEvent)]
    fn js_delete_event(this: &JsKeyhiveStorage, hash: &[u8]) -> Promise;
}

impl fmt::Debug for JsKeyhiveStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsKeyhiveStorage").finish()
    }
}

/// Errors from JavaScript keyhive storage operations.
#[derive(Debug, Error)]
pub enum JsKeyhiveStorageError {
    /// A JavaScript error occurred.
    #[error("JavaScript error: {0}")]
    JsError(String),

    /// Failed to parse hash from JS object.
    #[error("invalid hash length: expected 32 bytes, got {0}")]
    InvalidHashLength(usize),

    /// Failed to reflect a property from JS object.
    #[error("failed to get property '{0}' from JS object")]
    ReflectError(String),
}

/// Parse an array of {hash, data} objects from JavaScript.
fn parse_hash_data_array(
    js_value: &JsValue,
) -> Result<Vec<(StorageHash, Vec<u8>)>, JsKeyhiveStorageError> {
    let array = Array::from(js_value);
    let mut result = Vec::with_capacity(array.length() as usize);

    for i in 0..array.length() {
        let item = array.get(i);

        let hash_val = Reflect::get(&item, &JsValue::from_str("hash"))
            .map_err(|_| JsKeyhiveStorageError::ReflectError("hash".into()))?;
        let data_val = Reflect::get(&item, &JsValue::from_str("data"))
            .map_err(|_| JsKeyhiveStorageError::ReflectError("data".into()))?;

        let hash_bytes = Uint8Array::new(&hash_val).to_vec();
        let data_bytes = Uint8Array::new(&data_val).to_vec();

        if hash_bytes.len() != 32 {
            return Err(JsKeyhiveStorageError::InvalidHashLength(hash_bytes.len()));
        }

        let mut hash_arr = [0u8; 32];
        hash_arr.copy_from_slice(&hash_bytes);
        result.push((StorageHash::new(hash_arr), data_bytes));
    }

    Ok(result)
}

impl KeyhiveArchiveStorage<Local> for JsKeyhiveStorage {
    type SaveError = JsKeyhiveStorageError;
    type LoadError = JsKeyhiveStorageError;
    type DeleteError = JsKeyhiveStorageError;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> LocalBoxFuture<'_, Result<(), Self::SaveError>> {
        async move {
            let promise = self.js_save_archive(hash.as_bytes(), &data);
            JsFuture::from(promise)
                .await
                .map_err(|e| JsKeyhiveStorageError::JsError(format!("{e:?}")))?;
            Ok(())
        }
        .boxed_local()
    }

    fn load_archives(
        &self,
    ) -> LocalBoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::LoadError>> {
        async move {
            let promise = self.js_load_archives();
            let js_value = JsFuture::from(promise)
                .await
                .map_err(|e| JsKeyhiveStorageError::JsError(format!("{e:?}")))?;

            parse_hash_data_array(&js_value)
        }
        .boxed_local()
    }

    fn delete_archive(
        &self,
        hash: StorageHash,
    ) -> LocalBoxFuture<'_, Result<(), Self::DeleteError>> {
        async move {
            let promise = self.js_delete_archive(hash.as_bytes());
            JsFuture::from(promise)
                .await
                .map_err(|e| JsKeyhiveStorageError::JsError(format!("{e:?}")))?;
            Ok(())
        }
        .boxed_local()
    }
}

impl KeyhiveEventStorage<Local> for JsKeyhiveStorage {
    type SaveError = JsKeyhiveStorageError;
    type LoadError = JsKeyhiveStorageError;
    type DeleteError = JsKeyhiveStorageError;

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> LocalBoxFuture<'_, Result<(), Self::SaveError>> {
        async move {
            let promise = self.js_save_event(hash.as_bytes(), &data);
            JsFuture::from(promise)
                .await
                .map_err(|e| JsKeyhiveStorageError::JsError(format!("{e:?}")))?;
            Ok(())
        }
        .boxed_local()
    }

    fn load_events(
        &self,
    ) -> LocalBoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::LoadError>> {
        async move {
            let promise = self.js_load_events();
            let js_value = JsFuture::from(promise)
                .await
                .map_err(|e| JsKeyhiveStorageError::JsError(format!("{e:?}")))?;

            parse_hash_data_array(&js_value)
        }
        .boxed_local()
    }

    fn delete_event(&self, hash: StorageHash) -> LocalBoxFuture<'_, Result<(), Self::DeleteError>> {
        async move {
            let promise = self.js_delete_event(hash.as_bytes());
            JsFuture::from(promise)
                .await
                .map_err(|e| JsKeyhiveStorageError::JsError(format!("{e:?}")))?;
            Ok(())
        }
        .boxed_local()
    }
}
