use std::{rc::Rc, time::Duration};

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
use thiserror::Error;
use wasm_bindgen::JsValue;

use crate::{
    fragment::WasmFragment, loose_commit::WasmLooseCommit, sedimentree_id::WasmSedimentreeId,
};

#[derive(Debug, Clone)]
pub(crate) struct WasmConnectionCallbackReader<T: Connection<Local>> {
    pub(crate) conn: T,
    pub(crate) commit_callbacks: Rc<Mutex<Vec<js_sys::Function>>>,
    pub(crate) fragment_callbacks: Rc<Mutex<Vec<js_sys::Function>>>,
    pub(crate) blob_callbacks: Rc<Mutex<Vec<js_sys::Function>>>,
}

impl<T: PartialEq + Connection<Local>> PartialEq for WasmConnectionCallbackReader<T> {
    fn eq(&self, other: &Self) -> bool {
        self.conn == other.conn
    }
}

impl<T: Connection<Local>> Connection<Local> for WasmConnectionCallbackReader<T> {
    type DisconnectionError = T::DisconnectionError;
    type SendError = T::SendError;
    type RecvError = RecvOrCallbackErr<T>;
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
            let msg = self.conn.recv().await.map_err(RecvOrCallbackErr::Recv)?;

            match &msg {
                Message::LooseCommit { id, commit, blob } => {
                    let callbacks_lock = self.commit_callbacks.lock().await;
                    for callback in callbacks_lock.iter() {
                        callback
                            .call3(
                                &JsValue::NULL,
                                &JsValue::from(WasmSedimentreeId::from(*id)),
                                &JsValue::from(WasmLooseCommit::from(commit.clone())),
                                &JsValue::from(Uint8Array::from(blob.as_slice())),
                            )
                            .map_err(RecvOrCallbackErr::CommitCallback)?;
                    }
                }
                Message::Fragment { id, fragment, blob } => {
                    let callbacks_lock = self.fragment_callbacks.lock().await;
                    for callback in callbacks_lock.iter() {
                        callback
                            .call3(
                                &JsValue::NULL,
                                &JsValue::from(WasmSedimentreeId::from(*id)),
                                &JsValue::from(WasmFragment::from(fragment.clone())),
                                &JsValue::from(Uint8Array::from(blob.as_slice())),
                            )
                            .map_err(RecvOrCallbackErr::FragmentCallback)?;
                    }
                }
                Message::BlobsResponse(blobs) => {
                    for blob in blobs {
                        let lock = self.blob_callbacks.lock().await;
                        for callback in lock.iter() {
                            callback
                                .call1(
                                    &JsValue::NULL,
                                    &JsValue::from(Uint8Array::from(blob.as_slice())),
                                )
                                .map_err(RecvOrCallbackErr::BlobCallback)?;
                        }
                    }
                }
                Message::BatchSyncResponse(BatchSyncResponse {
                    id,
                    diff:
                        SyncDiff {
                            missing_commits,
                            missing_fragments,
                        },
                    ..
                }) => {
                    let lock = self.commit_callbacks.lock().await;

                    for (commit, blob) in missing_commits {
                        for callback in lock.iter() {
                            let this = JsValue::NULL;
                            callback
                                .call3(
                                    &this,
                                    &JsValue::from(WasmSedimentreeId::from(*id)),
                                    &JsValue::from(WasmLooseCommit::from(commit.clone())),
                                    &JsValue::from(blob.clone().into_contents()),
                                )
                                .map_err(RecvOrCallbackErr::CommitCallback)?;
                        }
                    }

                    let lock = self.fragment_callbacks.lock().await;

                    for (fragment, blob) in missing_fragments {
                        for callback in lock.iter() {
                            let this = JsValue::NULL;
                            callback
                                .call3(
                                    &this,
                                    &JsValue::from(WasmSedimentreeId::from(*id)),
                                    &JsValue::from(WasmFragment::from(fragment.clone())),
                                    &JsValue::from(blob.clone().into_contents()),
                                )
                                .map_err(RecvOrCallbackErr::FragmentCallback)?;
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

#[derive(Error)]
pub(crate) enum RecvOrCallbackErr<T: Connection<Local>> {
    /// An error occurred while sending a message.
    #[error(transparent)]
    Recv(T::RecvError),

    /// An error occurred while invoking a blob callback.
    #[error("Blob callback error: {0:?}")]
    BlobCallback(JsValue),

    /// An error occurred while invoking a commit callback.
    #[error("Commit callback error: {0:?}")]
    CommitCallback(JsValue),

    /// An error occurred while invoking a fragment callback.
    #[error("Fragment callback error: {0:?}")]
    FragmentCallback(JsValue),
}

impl<T: Connection<Local>> std::fmt::Debug for RecvOrCallbackErr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
