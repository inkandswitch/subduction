use std::{collections::HashMap, str::FromStr};

use js_sys::Function;
use sedimentree_core::DocumentId;
use sedimentree_sync_core::SedimentreeSync;
use wasm_bindgen::prelude::*;

use super::{
    document::JsDocumentId, network::adapter::NetworkAdapterInterface,
    storage::adapter::StorageAdapterInterface,
};

#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = "SedimentreeSync")]
pub struct JsSedimentreeSync(SedimentreeSync<NetworkAdapterInterface, StorageAdapterInterface>);

#[wasm_bindgen(js_class = "SedimentreeSync")]
impl JsSedimentreeSync {
    #[wasm_bindgen(constructor)]
    pub fn new(
        js_docs: js_sys::Map,
        js_network_adapters: Vec<NetworkAdapterInterface>,
        js_storage_adapters: Vec<StorageAdapterInterface>,
    ) -> Self {
        let mut docs = HashMap::new();
        let doc_entries = js_docs.entries();
        let entry_array = js_sys::Array::from(&doc_entries);

        for entry in entry_array.iter() {
            let pair = js_sys::Array::from(&entry); // NOTE [doc_id, doc_content]
            if pair.length() != 2 {
                continue;
            }

            let key = pair.get(0);
            let value = pair.get(1);

            let bytes: Vec<u8> = if value.is_instance_of::<js_sys::Uint8Array>() {
                value.unchecked_into::<js_sys::Uint8Array>().to_vec()
            } else {
                todo!("FIXME error")
            };

            let doc = automerge::Automerge::load(&bytes).expect("FIXME");

            if let Some(k) = key.as_string() {
                docs.insert(FromStr::from_str(k.as_str()).expect("FIXME"), doc);
            } else {
                web_sys::console::error_1(&"Non-string key or value".into());
            }
        }

        let mut network_adapters = HashMap::new();
        for (n, js_net_adapter) in js_network_adapters.iter().enumerate() {
            // FIXME should reallt be pased in or random, not iterated
            let peer_id: String = NetworkAdapterInterface::peer_id(&js_net_adapter)
                .as_string()
                .expect("FIXME");
            network_adapters.insert(
                format!("network-adapter-{n}-{peer_id}"),
                js_net_adapter.clone(),
            );
        }

        let mut storage_adapters = HashMap::new();
        for (n, js_storage_adapter) in js_storage_adapters.iter().enumerate() {
            // FIXME should reallt be pased in or random, not iterated
            storage_adapters.insert(format!("storage-adapter-{n}"), js_storage_adapter.clone());
        }

        Self(SedimentreeSync::new(
            docs,
            network_adapters,
            storage_adapters,
        ))
    }

    pub fn start(&mut self) {
        self.0.start();
    }

    pub async fn find(&self, js_doc_id: String) -> Option<js_sys::Array> {
        let doc_id = DocumentId::from_str(&js_doc_id).expect("FIXME");

        self.0.find(doc_id.into()).await.map(|vec_doc_bytes| {
            vec_doc_bytes
                .into_iter()
                .map(|am_bytes| {
                    let doc_uint8array = js_sys::Uint8Array::new_with_length(am_bytes.len() as u32);
                    doc_uint8array.copy_from(&am_bytes);
                    tracing::warn!("doc_uint8array: {:?}", doc_uint8array);
                    doc_uint8array
                })
                .collect::<js_sys::Array>()
        })
    }

    pub fn on(&mut self, event: String, js_callback: &Function) -> Result<(), JsValue> {
        let callback = || {
            js_callback.call0(&JsValue::NULL).expect("FIXME");
            ()
        };

        self.0
            .on(event, &callback)
            .map_err(|e| JsValue::from_str(&e))
    }

    pub fn off(&mut self, js_doc_id: &JsDocumentId, js_callback: &Function) -> Result<(), JsValue> {
        let callback = || {
            js_callback.call0(&JsValue::NULL).expect("FIXME");
            ()
        };

        self.0
            .off(js_doc_id.clone().into(), &callback)
            .map_err(|e| JsValue::from_str(&e))
    }

    pub async fn stop(&mut self) {
        self.0.stop().await;
    }

    #[wasm_bindgen(js_name = "whenReady")]
    pub async fn when_ready(&self) -> bool {
        true
    }

    #[wasm_bindgen(js_name = "newCommit")]
    pub async fn new_commit(&mut self, js_document_id: &JsDocumentId, hash: String, data: &[u8]) {
        self.0
            .new_commit(js_document_id.clone().into(), hash, data.to_vec())
            .await;
    }
}
