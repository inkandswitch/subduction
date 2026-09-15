//! An axum extractor that yields an [`async_tungstenite::WebSocketStream`]
//! ready for `subduction_websocket`.
//!
//! Name [`TungsteniteUpgrade`] as a handler argument and finish with
//! [`on_upgrade`](TungsteniteUpgrade::on_upgrade), which returns the `101`
//! response and hands the upgraded stream to a callback on a spawned task.
//!
//! The callback runs the Subduction handshake and gives the node the resulting
//! connection. [`handshake::respond`] is the accepting side of that exchange;
//! `handshake::initiate` is for sockets this node dialed. The node records
//! which side it was, so the choice outlives the handshake — and because a
//! mutual `initiate` still authenticates, getting it backwards here fails
//! silently rather than loudly.
//!
//! [`handshake::respond`]: subduction_core::handshake::respond
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use axum::{Router, response::Response, routing::get};
//! # use future_form::Sendable;
//! # use subduction_core::{
//! #     handshake, nonce_cache::NonceCache, peer::id::PeerId, spawn::Spawn,
//! #     timestamp::TimestampSeconds, transport::message::MessageTransport,
//! # };
//! # use subduction_crypto::signer::memory::MemorySigner;
//! # use subduction_hyper::{axum::TungsteniteUpgrade, upgrade::HyperIo};
//! # use subduction_websocket::{
//! #     handshake::WebSocketHandshake, sleep::TokioSleeper, tokio::TokioSpawn,
//! #     websocket::{KeepAlive, WebSocket},
//! # };
//! # use tungstenite::protocol::WebSocketConfig;
//! # const MAX_DRIFT: core::time::Duration = core::time::Duration::from_secs(60);
//! async fn ws(upgrade: TungsteniteUpgrade) -> Response {
//!     // Any `Spawn<Sendable>` works here; `TokioSpawn` is the stock one.
//!     // The callback wants it too, so hand it a copy rather than a borrow.
//!     let spawner = TokioSpawn;
//!     let inner = spawner;
//!
//!     upgrade.on_upgrade(&spawner, WebSocketConfig::default(), move |ws| async move {
//!         # let signer: MemorySigner = unimplemented!();
//!         # let nonce_cache: NonceCache = unimplemented!();
//!         # let my_peer_id: PeerId = unimplemented!();
//!         let Ok((authenticated, ())) = handshake::respond::<Sendable, _, _, _, _>(
//!             WebSocketHandshake::new(ws),
//!             |hs, peer_id| {
//!                 // Framing the socket yields two futures alongside it. Nothing
//!                 // moves until all three are running.
//!                 let (socket, sender_fut, keepalive) = WebSocket::new_with_keepalive(
//!                     hs.into_inner(),
//!                     peer_id,
//!                     KeepAlive::balanced(),
//!                     TokioSleeper,
//!                 );
//!
//!                 let listener = socket.clone();
//!                 inner.spawn(Box::pin(async move {
//!                     let _ = listener.listen().await;
//!                 }));
//!                 inner.spawn(Box::pin(async move {
//!                     let _ = sender_fut.await;
//!                 }));
//!                 inner.spawn(Box::pin(async move {
//!                     let _ = keepalive.await;
//!                 }));
//!
//!                 (MessageTransport::new(socket), ())
//!             },
//!             &signer,
//!             &nonce_cache,
//!             my_peer_id,
//!             None,
//!             TimestampSeconds::now(),
//!             MAX_DRIFT,
//!         )
//!         .await
//!         else {
//!             return;
//!         };
//!
//!         # let subduction: Arc<()> = unimplemented!();
//!         // subduction.add_connection(authenticated).await;
//!         let _ = authenticated;
//!     })
//! }
//!
//! let app: Router = Router::new().route("/ws", get(ws));
//! ```
//!
//! Extracting removes hyper's [`OnUpgrade`] from the request, so this cannot be
//! combined with `axum::extract::ws::WebSocketUpgrade` on the same request.
//! For request headers (e.g. `X-Forwarded-For`) or the peer address, add
//! axum's `HeaderMap` / `ConnectInfo` extractors alongside this one.
//!
//! See the [`crate::upgrade`] module docs for limitations.

use core::future::Future;

use async_tungstenite::WebSocketStream;
use axum::{
    body::Body,
    extract::FromRequestParts,
    http::request::Parts,
    response::{IntoResponse, Response},
};
use future_form::Sendable;
use hyper::upgrade::OnUpgrade;
use subduction_core::spawn::Spawn;
use tungstenite::protocol::WebSocketConfig;

use crate::upgrade::{self, AcceptKey, HyperIo, Rejection};

/// A validated WebSocket upgrade request.
///
/// Name it as a handler argument; finish with [`on_upgrade`](Self::on_upgrade).
#[derive(Debug)]
#[must_use = "call `on_upgrade` and return the response, or the client hangs"]
pub struct TungsteniteUpgrade {
    key: AcceptKey,
    on_upgrade: OnUpgrade,
}

impl TungsteniteUpgrade {
    /// Finish the upgrade.
    ///
    /// Returns the `101 Switching Protocols` response, which the handler must
    /// return for the upgrade to complete. Once hyper has written it, the
    /// connection is wrapped with `config` and passed to `f` on a spawned task.
    ///
    /// Nothing between this handler and the socket may replace that response.
    /// See [`upgrade::spawn_upgrade`] for the contract, including the absence
    /// of any deadline.
    ///
    /// # Arguments
    ///
    /// * `spawner` — where the upgrade task runs. `&TokioSpawn` spawns on the
    ///   current runtime and holds no state; `&TrackedTokioSpawn` additionally
    ///   joins its tasks at shutdown. Pass the spawner the node was built with,
    ///   so upgrade tasks share its lifecycle.
    /// * `config` — tungstenite framing limits for the resulting stream.
    ///   [`WebSocketConfig::default`] unless message sizes need raising.
    /// * `f` — runs on the upgraded stream once the client completes the
    ///   handshake. This is where [`handshake::respond`] and
    ///   `Subduction::add_connection` go; see the [module docs](self).
    ///
    /// [`handshake::respond`]: subduction_core::handshake::respond
    ///
    /// To drive the upgrade yourself instead, use
    /// [`into_parts`](Self::into_parts) with [`upgrade::upgrade`].
    #[must_use = "this response must be returned from the handler"]
    pub fn on_upgrade<Sp, F, Fut>(self, spawner: &Sp, config: WebSocketConfig, f: F) -> Response
    where
        Sp: Spawn<Sendable>,
        F: FnOnce(WebSocketStream<HyperIo>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        drop(upgrade::spawn_upgrade(spawner, self.on_upgrade, config, f));
        upgrade::accept_response(&self.key).map(|()| Body::empty())
    }

    /// Take the pieces apart to drive the upgrade yourself.
    ///
    /// Return [`upgrade::accept_response`] for the key from the handler — the
    /// client hangs otherwise — and pass `on_upgrade` to [`upgrade::upgrade`]
    /// on a task of your choosing.
    #[must_use]
    pub fn into_parts(self) -> (AcceptKey, OnUpgrade) {
        (self.key, self.on_upgrade)
    }
}

impl<S: Send + Sync> FromRequestParts<S> for TungsteniteUpgrade {
    type Rejection = Rejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let key = upgrade::validate(parts)?;

        let on_upgrade = parts
            .extensions
            .remove::<OnUpgrade>()
            .ok_or(Rejection::NotUpgradable)?;

        Ok(Self { key, on_upgrade })
    }
}

impl IntoResponse for Rejection {
    fn into_response(self) -> Response {
        self.response().map(Body::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Method, Request, header};

    /// A well-formed request built outside a live hyper connection has no
    /// `OnUpgrade` extension: header validation passes and we reach the final
    /// check. Pins that validation runs before the extension lookup.
    #[tokio::test]
    async fn valid_headers_without_hyper_are_not_upgradable() {
        let (mut parts, ()) = Request::builder()
            .method(Method::GET)
            .uri("/ws")
            .header(header::CONNECTION, "Upgrade")
            .header(header::UPGRADE, "websocket")
            .header(header::SEC_WEBSOCKET_VERSION, "13")
            .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .body(())
            .unwrap_or_else(|_| unreachable!("static request is valid"))
            .into_parts();

        let result = TungsteniteUpgrade::from_request_parts(&mut parts, &()).await;
        assert_eq!(result.err(), Some(Rejection::NotUpgradable));
    }
}
