use sedimentree_sync_core::{
    chunk::Chunk,
    storage::{adapter::StorageAdapter, key::StorageKey},
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use super::key::JsStorageKey;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "StorageAdapterInterface")]
    #[derive(Debug, Clone)]
    pub type StorageAdapterInterface;

    #[wasm_bindgen(method, js_name = "load")] // FIXME TS type for Promise
    pub fn js_load(this: &StorageAdapterInterface, key: JsStorageKey) -> js_sys::Promise;

    #[wasm_bindgen(method, js_name = "save")] // FIXME TS type for Promise
    pub fn js_save(
        this: &StorageAdapterInterface,
        key: JsStorageKey,
        data: Vec<u8>,
    ) -> js_sys::Promise;

    #[wasm_bindgen(method, js_name = "remove")] // FIXME TS type for Promise
    pub fn js_remove(this: &StorageAdapterInterface, key: JsStorageKey) -> js_sys::Promise;

    #[wasm_bindgen(method, js_name = "loadRange")] // FIXME TS type for Promise
    pub fn js_load_range(this: &StorageAdapterInterface, prefix: JsStorageKey) -> js_sys::Promise;

    #[wasm_bindgen(method, js_name = "removeRange")] // FIXME TS type for Promise
    pub fn js_remove_range(this: &StorageAdapterInterface, key: JsStorageKey) -> js_sys::Promise;
}

impl StorageAdapter for StorageAdapterInterface {
    async fn load(&self, key: &StorageKey) -> Result<Option<Vec<u8>>, String> {
        let value = JsFuture::from(self.js_load(key.clone().into()))
            .await
            .map_err(|err| err.as_string().expect("Error converting JsValue to String"))?;
        if value.is_undefined() {
            Ok(None)
        } else if let Some(bytes) = value.dyn_into::<js_sys::Uint8Array>().ok() {
            Ok(Some(bytes.to_vec()))
        } else {
            Err("Expected Uint8Array".to_string())
        }
    }

    async fn save(&self, key: &StorageKey, data: &[u8]) -> Result<(), String> {
        let promise = self.js_save(key.clone().into(), data.to_vec());

        if let Err(err) = JsFuture::from(promise).await {
            Err(err.as_string().expect("Error converting JsValue to String"))?
        }

        Ok(())
    }

    async fn remove(&self, key: &StorageKey) -> Result<(), String> {
        let promise = self.js_remove(key.clone().into());

        if let Err(err) = JsFuture::from(promise).await {
            Err(err.as_string().expect("Error converting JsValue to String"))?
        }

        Ok(())
    }

    async fn load_range(&self, prefix: &StorageKey) -> Result<Vec<Chunk>, String> {
        let promise = self.js_load_range(prefix.clone().into());
        let js_val = JsFuture::from(promise)
            .await
            .map_err(|err| err.as_string().expect("Error converting JsValue to String"))?;

        let js_array = js_val
            .dyn_into::<js_sys::Array>()
            .map_err(|_| "Expected an array".to_string())?;

        let mut chunks = Vec::new();
        for js_value in js_array.iter() {
            let js_obj = js_value
                .dyn_into::<js_sys::Object>()
                .map_err(|_| "Expected an object".to_string())?;

            let key: StorageKey = js_sys::Reflect::get(&js_obj, &"key".into())
                .map_err(|_| "Expected key field".to_string())?
                .dyn_into::<js_sys::Array>()
                .map_err(|_| "Key should be an array".to_string())?
                .iter()
                .map(|v| v.as_string().expect("Key should be a string"))
                .collect::<Vec<String>>()
                .into();

            let data = js_sys::Reflect::get(&js_obj, &"data".into())
                .map_err(|_| "Expected data field".to_string())?
                .dyn_into::<js_sys::Uint8Array>()
                .map_err(|_| "Data should be a Uint8Array".to_string())?
                .to_vec();

            chunks.push(Chunk { key, data });
        }

        Ok(chunks)
    }

    async fn remove_range(&self, prefix: &StorageKey) -> Result<(), String> {
        let promise = self.js_remove_range(prefix.clone().into());

        if let Err(err) = JsFuture::from(promise).await {
            return Err(err.as_string().expect("Error converting JsValue to String"));
        }

        Ok(())
    }
}
