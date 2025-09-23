use std::{sync::Arc, time::Duration};

use futures::{future::LocalBoxFuture, lock::Mutex, FutureExt};
use sedimentree_core::future::Local;
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection,
    },
    peer::id::PeerId,
};
use thiserror::Error;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = JsWebSocket)]
#[derive(Debug, Clone)]
pub struct JsWebSocket {
    peer_id: PeerId,
    request_id_counter: Arc<Mutex<u128>>,
    socket: web_sys::WebSocket,
}

impl PartialEq for JsWebSocket {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id && self.socket == other.socket
    }
}

#[derive(Debug, Error)]
#[error("Not implemented")]
pub struct Fixme;

impl Connection<Local> for JsWebSocket {
    type DisconnectionError = Fixme;
    type SendError = Fixme;
    type RecvError = Fixme;
    type CallError = Fixme;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
        let counter = self.request_id_counter.clone();
        async move {
            let mut counter = counter.lock().await;
            *counter += 1;
            tracing::debug!("generated message id {:?}", *counter);
            RequestId {
                requestor: self.peer_id,
                nonce: *counter,
            }
        }
        .boxed_local()
    }

    fn disconnect(&mut self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        // Implement disconnection logic
        unimplemented!()
    }

    fn send(&self, message: Message) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        async move {
            tracing::debug!("sending outbound message id {:?}", message.request_id());
            let msg = bincode::serde::encode_to_vec(&message, bincode::config::standard())
                .expect("FIXME");
            self.socket
                .send_with_u8_array(msg.as_slice())
                .expect("FIXME");
            Ok(())
        }
        .boxed_local()
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<Message, Self::RecvError>> {
        async {
            todo!("Implement sending logic using WebSocket");
        }
        .boxed_local()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async {
            todo!("Implement sending logic using WebSocket");
        }
        .boxed_local()
    }
}
