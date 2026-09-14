//! Replacement for `axum::extract::ws::WebSocketUpgrade` that yields an
//! [`async_tungstenite::WebSocketStream`].
//!
//! It stands where axum's extractor stands: same handler position, same
//! routing, middleware, state, and rejection-into-response. The handler body
//! differs, because the point is to get a different stream out —
//! [`on_upgrade`](TungsteniteUpgrade::on_upgrade) takes a [`WebSocketConfig`]
//! rather than builder methods, and the callback receives a tungstenite
//! stream rather than axum's sealed `WebSocket`.
//!
//! ```no_run
//! use axum::{Router, extract::State, response::Response, routing::get};
//! use subduction_hyper::axum::TungsteniteUpgrade;
//! use subduction_websocket::tokio::TokioSpawn;
//! use tungstenite::protocol::WebSocketConfig;
//!
//! // Hold the spawner the node was built with, so upgrade tasks share its
//! // lifecycle. `TrackedTokioSpawn` additionally joins them at shutdown.
//! #[derive(Clone)]
//! struct App {
//!     spawner: TokioSpawn,
//! }
//!
//! async fn ws(upgrade: TungsteniteUpgrade, State(app): State<App>) -> Response {
//!     upgrade.on_upgrade(&app.spawner, WebSocketConfig::default(), |ws| async move {
//!         // `ws` is an `async_tungstenite::WebSocketStream`; run the
//!         // Subduction handshake and hand it to `WebSocket::new_with_keepalive`.
//!         let _ = ws;
//!     })
//! }
//!
//! let app: Router = Router::new()
//!     .route("/ws", get(ws))
//!     .with_state(App { spawner: TokioSpawn });
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
    /// See [`upgrade::spawn_upgrade`] for the contract, including the spawner
    /// to pass and the absence of any deadline.
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
