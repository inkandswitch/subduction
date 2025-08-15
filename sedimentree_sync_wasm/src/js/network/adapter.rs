use sedimentree_sync_core::{
    message::Message,
    network::adapter::NetworkAdapter,
    peer::{id::PeerId, metadata::PeerMetadata},
    storage::id::StorageId,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::js::{message::JsMessage, peer::metadata::JsPeerMetadata};

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
        let storage_id = adapter.storage_id().map(Into::into);
        let is_ephemeral = adapter.is_ephemeral();
        let metadata: JsValue = adapter.into();
        if metadata.is_undefined() {
            None
        } else {
            Some(PeerMetadata {
                storage_id,
                is_ephemeral,
            })
        }
    }

    fn is_ready(&self) -> bool {
        NetworkAdapterInterface::is_ready(self)
    }

    async fn when_ready(&self) -> Result<(), String> {
        let result = JsFuture::from(NetworkAdapterInterface::when_ready(self)).await;
        match result {
            Ok(_) => Ok(()),
            Err(_err) => Err("FIXME Error".to_string()),
        }
    }

    fn connect(&self, peer_id: &PeerId, peer_metadata: &Option<PeerMetadata>) {
        let js_peer_id = JsValue::from(peer_id.to_string());
        let js_peer_metadata = match peer_metadata {
            Some(metadata) => {
                let obj = js_sys::Object::new();
                if let Some(storage_id) = &metadata.storage_id {
                    js_sys::Reflect::set(
                        &obj,
                        &"storageId".into(),
                        &JsValue::from(storage_id.to_string()),
                    )
                    .unwrap();
                }
                if let Some(is_ephemeral) = metadata.is_ephemeral {
                    js_sys::Reflect::set(&obj, &"isEphemeral".into(), &JsValue::from(is_ephemeral))
                        .unwrap();
                }
                JsValue::from(obj)
            }
            None => JsValue::undefined(),
        };
        NetworkAdapterInterface::connect(self, &js_peer_id, &js_peer_metadata);
    }

    fn send(&self, message: Message) {
        NetworkAdapterInterface::send(
            self,
            JsMessage::new(
                &message.action,
                message.sender_id.into(),
                message.target_id.into(),
            ),
        )
    }

    fn disconnect(&mut self) {
        NetworkAdapterInterface::disconnect(self);
    }
}
