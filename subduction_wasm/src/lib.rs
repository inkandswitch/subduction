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

use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::{future::LocalBoxFuture, lock::Mutex, FutureExt};
use sedimentree_core::{future::Local, storage::MemoryStorage};
use subduction_core::{
    connection::{
        id::ConnectionId,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection,
    },
    peer::id::PeerId,
    Subduction,
};
use thiserror::Error;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = Subduction)]
pub struct JsSubduction(Subduction<Local, MemoryStorage, JsWebSocket>);

#[wasm_bindgen]
impl JsSubduction {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self(Subduction::new(
            HashMap::new(),
            MemoryStorage::default(),
            HashMap::new(),
        ))
    }

    pub async fn run(&self) -> Result<(), JsValue> {
        self.0
            .run()
            .await
            .map_err(|e| JsValue::from_str(&e.to_string())) // FIXME
    }
}

#[wasm_bindgen(js_name = JsWebSocket)]
#[derive(Debug, Clone)]
pub struct JsWebSocket {
    connection_id: ConnectionId,
    peer_id: PeerId,

    request_id_counter: Arc<Mutex<u128>>,
    socket: web_sys::WebSocket,
}

#[derive(Debug, Error)]
#[error("Not implemented")]
pub struct Fixme;

impl Connection<Local> for JsWebSocket {
    type DisconnectionError = Fixme;
    type SendError = Fixme;
    type RecvError = Fixme;
    type CallError = Fixme;

    fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

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
            self.socket.send_with_u8_array(msg.as_slice());
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
