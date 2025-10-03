use std::{sync::Arc, time::Duration};

use futures::{
    future::{FutureExt, LocalBoxFuture},
    lock::Mutex,
};
use js_sys::Uint8Array;
use sedimentree_core::future::Local;
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId, SyncDiff},
        Connection,
    },
    peer::id::PeerId,
};
use wasm_bindgen::JsValue;

use crate::js::{chunk::JsChunk, loose_commit::JsLooseCommit, sedimentree_id::JsSedimentreeId};

#[derive(Debug, Clone)]
pub(crate) struct JsConnectionCallbackReader<T: Connection<Local>> {
    pub(crate) conn: T,
    pub(crate) commit_callbacks: Arc<Mutex<Vec<js_sys::Function>>>,
    pub(crate) chunk_callbacks: Arc<Mutex<Vec<js_sys::Function>>>,
    pub(crate) blob_callbacks: Arc<Mutex<Vec<js_sys::Function>>>,
}

impl<T: PartialEq + Connection<Local>> PartialEq for JsConnectionCallbackReader<T> {
    fn eq(&self, other: &Self) -> bool {
        self.conn == other.conn
    }
}

impl<T: Connection<Local>> Connection<Local> for JsConnectionCallbackReader<T> {
    type DisconnectionError = T::DisconnectionError;
    type SendError = T::SendError;
    type RecvError = T::RecvError;
    type CallError = T::CallError;

    fn peer_id(&self) -> PeerId {
        self.conn.peer_id()
    }

    fn disconnect(&mut self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        self.conn.disconnect()
    }

    fn send(&self, message: Message) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        self.conn.send(message)
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<Message, Self::RecvError>> {
        async {
            let msg = self.conn.recv().await?;

            match &msg {
                Message::LooseCommit { id, commit, blob } => {
                    let callbacks_lock = self.commit_callbacks.lock().await;
                    for callback in callbacks_lock.iter() {
                        let _fixme = callback
                            .call3(
                                &JsValue::NULL,
                                &JsValue::from(JsSedimentreeId::from(id.clone())),
                                &JsValue::from(JsLooseCommit::from(commit.clone())),
                                &JsValue::from(Uint8Array::from(blob.as_slice())),
                            )
                            .expect("FIXME");
                    }
                    todo!()
                }
                Message::Chunk { id, chunk, blob } => {
                    let callbacks_lock = self.chunk_callbacks.lock().await;
                    for callback in callbacks_lock.iter() {
                        let _fixme = callback
                            .call3(
                                &JsValue::NULL,
                                &JsValue::from(JsSedimentreeId::from(id.clone())),
                                &JsValue::from(JsChunk::from(chunk.clone())),
                                &JsValue::from(Uint8Array::from(blob.as_slice())),
                            )
                            .expect("FIXME");
                    }
                    todo!()
                }
                Message::BlobsResponse(blobs) => {
                    for blob in blobs {
                        let lock = self.blob_callbacks.lock().await;
                        for callback in lock.iter() {
                            let _fixme = callback
                                .call1(
                                    &JsValue::NULL,
                                    &JsValue::from(Uint8Array::from(blob.as_slice())),
                                )
                                // FIXME add sedimentreeID?
                                .expect("FIXME");
                        }
                    }
                }
                Message::BatchSyncResponse(BatchSyncResponse {
                    id,
                    diff:
                        SyncDiff {
                            missing_commits,
                            missing_chunks,
                        },
                    ..
                }) => {
                    let lock = self.commit_callbacks.lock().await;

                    for (commit, blob) in missing_commits {
                        for callback in lock.iter() {
                            let this = JsValue::NULL;
                            let _fixme = callback
                                .call3(
                                    &this,
                                    &JsValue::from(JsSedimentreeId::from(id.clone())),
                                    &JsValue::from(JsLooseCommit::from(commit.clone())),
                                    &JsValue::from(blob.clone().into_contents()),
                                )
                                .expect("FIXME");
                        }
                    }

                    let lock = self.chunk_callbacks.lock().await;

                    for (chunk, blob) in missing_chunks {
                        for callback in lock.iter() {
                            let this = JsValue::NULL;
                            let _fixme = callback
                                .call3(
                                    &this,
                                    &JsValue::from(JsSedimentreeId::from(id.clone())),
                                    &JsValue::from(JsChunk::from(chunk.clone())),
                                    &JsValue::from(blob.clone().into_contents()),
                                )
                                .expect("FIXME");
                        }
                    }
                }
                _otherwise => { /* Noop */ }
            }

            Ok(msg)
        }
        .boxed_local()
    }

    fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
        self.conn.next_request_id()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        self.conn.call(req, timeout)
    }
}
