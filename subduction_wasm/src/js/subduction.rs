use std::{collections::HashMap, time::Duration};

use sedimentree_core::{future::Local, storage::MemoryStorage};
use subduction_core::{connection::Connection, Subduction};
use wasm_bindgen::prelude::*;

use super::{
    blob::JsBlob, chunk::JsChunk, chunk_requested::JsChunkRequested, connection_id::JsConnectionId,
    digest::JsDigest, error::JsIoError, loose_commit::JsLooseCommit, peer_id::JsPeerId,
    sedimentree_id::JsSedimentreeId, websocket::JsWebSocket,
};

/// Wasm bindings for [`Subduction`](subduction_core::Subduction)
#[wasm_bindgen(js_name = Subduction)]
#[derive(Debug, Clone)]
pub struct JsSubduction(Subduction<Local, MemoryStorage, JsWebSocket>);

#[wasm_bindgen(js_class = Subduction)]
impl JsSubduction {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self(Subduction::new(
            HashMap::new(),
            MemoryStorage::default(), // FIXME use lcoalstorage or indexeddb
            HashMap::new(),
        ))
    }

    pub async fn run(&self) -> Result<(), JsValue> {
        self.0
            .run()
            .await
            .map_err(|e| JsValue::from_str(&e.to_string())) // FIXME
    }

    pub async fn attach(&self, conn: JsWebSocket) -> Result<Registered, JsIoError> {
        let (is_new, conn_id) = self.0.attach(conn).await.map_err(JsIoError::from)?;
        Ok(Registered {
            is_new,
            conn_id: conn_id.into(),
        })
    }

    pub async fn disconnect(&self, js_conn_id: JsConnectionId) -> Result<bool, String> {
        let new_disconnect = self.0.disconnect(&js_conn_id.into()).await.expect("FIXME");
        Ok(new_disconnect)
    }

    #[wasm_bindgen(js_name = disconnectFromPeer)]
    pub async fn disconnect_from_peer(&self, peer_id: JsPeerId) -> Result<bool, String> {
        let new_disconnect = self
            .0
            .disconnect_from_peer(&peer_id.into())
            .await
            .expect("FIXME");
        Ok(new_disconnect)
    }

    pub async fn register(&self, conn: JsWebSocket) -> Result<Registered, String> {
        let (is_new, conn_id) = self.0.register(conn).await.expect("FIXME");
        Ok(Registered {
            is_new,
            conn_id: conn_id.into(),
        })
    }

    pub async fn unregister(&self, conn_id: JsConnectionId) -> bool {
        self.0.unregister(&conn_id.into()).await
    }

    #[wasm_bindgen(js_name = getLocalBlob)]
    pub async fn get_local_blob(&self, digest: JsDigest) -> Result<Option<JsBlob>, String> {
        let maybe_blob = self.0.get_local_blob(digest.into()).await.expect("FIXME");
        Ok(maybe_blob.map(JsBlob::from))
    }

    #[wasm_bindgen(js_name = getLocalBlobs)]
    pub async fn get_local_blobs(&self, id: JsSedimentreeId) -> Result<Vec<JsBlob>, String> {
        if let Some(blobs) = self.0.get_local_blobs(id.into()).await.expect("FIXME") {
            Ok(blobs.into_iter().map(JsBlob::from).collect())
        } else {
            Ok(vec![])
        }
    }

    #[wasm_bindgen(js_name = fetchBlobs)]
    pub async fn fetch_blobs(
        &self,
        id: JsSedimentreeId,
        timeout_milliseconds: Option<u64>,
    ) -> Result<Option<Vec<JsBlob>>, String> {
        let timeout = timeout_milliseconds.map(|ms| Duration::from_millis(ms));
        if let Some(blobs) = self.0.fetch_blobs(id.into(), timeout).await.expect("FIXME") {
            Ok(Some(blobs.into_iter().map(JsBlob::from).collect()))
        } else {
            Ok(None)
        }
    }

    #[wasm_bindgen(js_name = addCommit)]
    pub async fn add_commit(
        &self,
        id: JsSedimentreeId,
        commit: &JsLooseCommit,
        blob: &JsBlob,
    ) -> Result<Option<JsChunkRequested>, String> {
        let maybe_chunk_requested = self
            .0
            .add_commit(id.into(), &commit.clone().into(), blob.clone().into())
            .await
            .expect("FIXME");

        Ok(maybe_chunk_requested.map(JsChunkRequested::from))
    }

    #[wasm_bindgen(js_name = addChunk)]
    pub async fn add_chunk(
        &self,
        id: JsSedimentreeId,
        chunk: &JsChunk,
        blob: &JsBlob,
    ) -> Result<(), String> {
        self.0
            .add_chunk(id.into(), &chunk.clone().into(), blob.clone().into())
            .await
            .expect("FIXME");
        Ok(())
    }

    #[wasm_bindgen(js_name = requestBlobs)]
    pub async fn request_blobs(&self, digests: Vec<JsDigest>) {
        let digests: Vec<_> = digests.into_iter().map(Into::into).collect();
        self.0.request_blobs(digests).await
    }

    // pub async fn request_peer_batch_sync(
    //     &self,
    //     to_ask: JsPeerId,
    //     id: JsSedimentreeId,
    //     timeout_milliseconds: Option<u64>,
    // ) -> Result<
    //     (
    //         bool,
    //         Vec<(JsWebSocket, <JsWebSocket as Connection<Local>>::CallError)>,
    //     ),
    //     JsIoError,
    // > {
    //     let timeout = timeout_milliseconds.map(Duration::from_millis);
    //     self.0
    //         .request_peer_batch_sync(&to_ask.into(), id.into(), timeout)
    //         .await
    //         .expect("FIXME")
    // }
}

#[wasm_bindgen(js_name = Registered)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Registered {
    is_new: bool,
    conn_id: JsConnectionId,
}

#[wasm_bindgen(js_class = Registered)]
impl Registered {
    #[wasm_bindgen(getter)]
    pub fn is_new(&self) -> bool {
        self.is_new
    }

    #[wasm_bindgen(getter)]
    pub fn conn_id(&self) -> JsConnectionId {
        self.conn_id
    }
}
