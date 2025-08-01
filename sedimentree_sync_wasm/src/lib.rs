#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(
    clippy::dbg_macro,
    clippy::expect_used,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::todo,
    clippy::unwrap_used,
    future_incompatible,
    let_underscore,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    rust_2021_compatibility
)]
#![deny(
    clippy::all,
    clippy::cargo,
    clippy::pedantic,
    rust_2018_idioms,
    unreachable_pub,
    unused_extern_crates
)]
#![forbid(unsafe_code)]

use std::{collections::HashMap, hash::{DefaultHasher, Hash, Hasher}};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use js_sys::Function;
use sedimentree_sync_core::{PeerId, Message, SedimentreeSync, NetworkAdapter, PeerMetadata, StorageId};

pub(crate) type JsDocumentId = String;
pub(crate) type JsPeerId = String;
pub(crate) type JsStorageId = String;

#[wasm_bindgen(js_name = "Message")]
pub struct JsMessage(Message);

#[wasm_bindgen(js_class = "Message")]
impl JsMessage {
    #[wasm_bindgen(constructor)]
    pub fn new(action: &str, sender_id: JsPeerId, target_id: JsPeerId) -> Self {
        web_sys::console::log_1(&format!("Creating Message with action: {}, senderId: {}, targetId: {}", action, sender_id, target_id).into());
        Self(
            Message {
                action: action.to_string(),
                sender_id: PeerId::new(sender_id),
                target_id: PeerId::new(target_id),
            }
        )
    }

    #[wasm_bindgen(getter, js_name = "type")]
    pub fn action(&self) -> String {
        self.0.action.clone()
    }

    #[wasm_bindgen(getter, js_name = "senderId")]
    pub fn sender_id(&self) -> String {
        self.0.sender_id.to_string()
    }

    #[wasm_bindgen(getter, js_name = "targetId")]
    pub fn target_id(&self) -> String {
        self.0.target_id.to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Callback {
    function: Function,
    source_digest: u64
}

impl From<Function> for Callback {
    fn from(function: Function) -> Self {
        let fn_str: String = function.to_string().into();
        web_sys::console::log_1(&format!("Creating Callback from function: {}", fn_str).into());

        let mut hasher = DefaultHasher::new();
        fn_str.hash(&mut hasher);
        let source_digest = hasher.finish();

        Self { function, source_digest }
    }
}

impl Hash for Callback {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.source_digest.hash(state);
    }
}


#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "PeerMetadata")]
     #[derive(Debug, Clone)]
    pub type JsPeerMetadata;

    #[wasm_bindgen(method, getter, js_name = storageId)]
    pub fn storage_id(this: &JsPeerMetadata) -> Option<JsStorageId>;

    #[wasm_bindgen(method, getter, js_name = isEphemeral)]
    pub fn is_ephemeral(this: &JsPeerMetadata) -> Option<bool>;
}


#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "NetworkAdapterInterface")]
     #[derive(Debug, Clone)]
    pub type NetworkAdapterInterface;

    #[wasm_bindgen(method, getter, js_name = peerId)]
    pub fn peer_id(this: &NetworkAdapterInterface) -> JsValue; // replace JsValue with concrete PeerId wrapper if needed

    #[wasm_bindgen(method, getter, js_name = peerMetadata)]
    pub fn peer_metadata(this: &NetworkAdapterInterface) -> JsPeerMetadata;

    #[wasm_bindgen(method, js_name = isReady)]
    pub fn is_ready(this: &NetworkAdapterInterface) -> bool;

    #[wasm_bindgen(method, js_name = whenReady)]
    pub fn when_ready(this: &NetworkAdapterInterface) -> js_sys::Promise;

    #[wasm_bindgen(method)]
    pub fn connect(
        this: &NetworkAdapterInterface,
        peer_id: &JsValue,
        peer_metadata: &JsValue, // NOTE may be undefined
    );

    #[wasm_bindgen(method)]
    pub fn send(this: &NetworkAdapterInterface, message: JsMessage);

    #[wasm_bindgen(method)]
    pub fn disconnect(this: &NetworkAdapterInterface);
}

impl NetworkAdapter for NetworkAdapterInterface {
    fn peer_id(&self) -> PeerId {
        NetworkAdapterInterface::peer_id(self)
            .as_string()
            .map(PeerId::new)
            .expect("PeerId should be a string")
    }

    fn peer_metadata(&self) -> Option<PeerMetadata> {
        let adapter = NetworkAdapterInterface::peer_metadata(self);
        let storage_id = adapter.storage_id().map(StorageId::new);
        let is_ephemeral = adapter.is_ephemeral();
        let metadata: JsValue =  adapter.into();
        if metadata.is_undefined() {
            None
        } else {
            Some(PeerMetadata {
                storage_id, is_ephemeral
            })
        }
    }

    fn is_ready(&self) -> bool {
        NetworkAdapterInterface::is_ready(self)
    }

    async fn when_ready(&self) -> Result<(), String> {
        let result = JsFuture::from(NetworkAdapterInterface::when_ready(self)).await;
        match result {
            Ok(_) => {
                Ok(())
            },
            Err(_err) => {
                Err("FIXME Error".to_string())
            }
        }
    }

    fn connect(&self, peer_id: &PeerId, peer_metadata: &Option<PeerMetadata>) {
        let js_peer_id = JsValue::from(peer_id.to_string());
        let js_peer_metadata = match peer_metadata {
            Some(metadata) => {
                let obj = js_sys::Object::new();
                if let Some(storage_id) = &metadata.storage_id {
                    js_sys::Reflect::set(&obj, &"storageId".into(), &JsValue::from(storage_id.to_string())).unwrap();
                }
                if let Some(is_ephemeral) = metadata.is_ephemeral {
                    js_sys::Reflect::set(&obj, &"isEphemeral".into(), &JsValue::from(is_ephemeral)).unwrap();
                }
                JsValue::from(obj)
            },
            None => JsValue::undefined(),
        };
        NetworkAdapterInterface::connect(self, &js_peer_id, &js_peer_metadata);
    }
    fn send(&self, message: Message) {
        let js_message = JsMessage::new(
            &message.action,
            message.sender_id.to_string(),
            message.target_id.to_string(),
        );
        NetworkAdapterInterface::send(self, js_message);
    }
    fn disconnect(&mut self) {
        NetworkAdapterInterface::disconnect(self);
    }
}


#[derive(Debug, Clone)]
#[wasm_bindgen]
pub struct SedimentreeSyncWasm(SedimentreeSync<NetworkAdapterInterface>);

#[wasm_bindgen]
impl SedimentreeSyncWasm {
    #[wasm_bindgen(constructor)]
    pub fn new(js_docs: js_sys::Map, js_network_adapters: Vec<NetworkAdapterInterface>) -> Self {
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
                docs.insert(k.into(), doc);
            } else {
                web_sys::console::error_1(&"Non-string key or value".into());
            }
        }

        let mut network_adapters = HashMap::new();
        for (n, js_adapter) in js_network_adapters.iter().enumerate() {
            // FIXME should reallt be pased in or random, not iterated
            let peer_id: String = NetworkAdapterInterface::peer_id(&js_adapter).as_string().expect("FIXME");
            network_adapters.insert(format!("{n}-{peer_id}"), js_adapter.clone());
        }

        Self(SedimentreeSync::new(
            docs,
            network_adapters,
        ))
    }

    pub fn start(&mut self) {
        self.0.start();
    }

    pub async fn find(&self, doc_id: JsDocumentId) -> Option<js_sys::Array> {
        self.0.find(doc_id.into()).await.map(|vec_doc_bytes| {
            vec_doc_bytes.into_iter().map(|am_bytes| {
                let doc_uint8array = js_sys::Uint8Array::new_with_length(am_bytes.len() as u32);
                doc_uint8array.copy_from(&am_bytes);
                web_sys::console::log_1(&format!("doc_uint8array: {:?}", doc_uint8array).into());
                doc_uint8array
            }).collect::<js_sys::Array>()
        })
    }

    pub fn on(&mut self, event: String, js_callback: &Function) -> Result<(), JsValue> {
        // web_sys::console::log_1(&format!("Registering callback for doc_id: {:?}", doc_id).into());
        // self.entry(doc_id)
        //     .and_modify(|set| {
        //         set.insert(js_callback.clone().into());
        //     })
        //     .or_insert_with(|| {
        //         HashSet::from_iter([js_callback.clone().into()])
        //     });

        Ok(())
    }



    pub fn off(&mut self, doc_id: JsDocumentId, js_callback: &Function) -> Result<(), JsValue> {
        // self.entry(doc_id.clone())
        //     .and_modify(|set| {
        //         set.remove(&js_callback.clone().into());
        //     });

        // if let Some(doc_adapters) = self.adapters.get(&doc_id)  {
        //     if doc_adapters.is_empty() {
        //         self.adapters.remove(&doc_id);
        //     }
        // }

        Ok(())
    }

    // fn entry(&mut self, doc_id: DocumentId) -> Entry<'_, DocumentId, HashSet<Callback>> {
    //     self.adapters.entry(doc_id)
    // }

    pub async fn stop(&mut self) {
        todo!()
    }

    #[wasm_bindgen(js_name = "whenReady")]
    pub async fn when_ready(&self) -> bool {
        true
    }


    #[wasm_bindgen(js_name = "newCommit")]
    pub async fn new_commit(&mut self, _document_id: JsDocumentId, hash: String, data: js_sys::Uint8Array) {
        todo!()
    }
}
