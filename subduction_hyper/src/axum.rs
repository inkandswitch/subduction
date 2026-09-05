//! Drop-in replacement for `axum::extract::ws::WebSocketUpgrade` that yields
//! an [`async_tungstenite::WebSocketStream`].
//!
//! Swap the extractor type in the handler signature; routing, middleware, and
//! state are otherwise unchanged:
//!
//! ```no_run
//! use axum::{Router, response::Response, routing::get};
//! use subduction_hyper::axum::TungsteniteUpgrade;
//! use tungstenite::protocol::WebSocketConfig;
//!
//! async fn ws(upgrade: TungsteniteUpgrade) -> Response {
//!     upgrade.on_upgrade(WebSocketConfig::default(), |ws| async move {
//!         // `ws` is an `async_tungstenite::WebSocketStream`; run the
//!         // Subduction handshake and hand it to `WebSocket::new_with_keepalive`.
//!         let _ = ws;
//!     })
//! }
//!
//! let app: Router = Router::new().route("/ws", get(ws));
//! ```
//!
//! Extracting removes hyper's [`OnUpgrade`] from the request, so this cannot be
//! combined with `axum::extract::ws::WebSocketUpgrade` on the same request.

use core::future::Future;

use async_tungstenite::WebSocketStream;
use axum::{
    body::Body,
    extract::FromRequestParts,
    http::{request::Parts, HeaderMap},
    response::{IntoResponse, Response},
};
use hyper::upgrade::OnUpgrade;
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
    headers: HeaderMap,
}

impl TungsteniteUpgrade {
    /// The upgrade request's headers, e.g. for reading `X-Forwarded-For`.
    #[must_use]
    pub const fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Finish the upgrade.
    ///
    /// Returns the `101 Switching Protocols` response, which the handler must
    /// return for the upgrade to complete. Once hyper has written it, the
    /// connection is wrapped with `config` and passed to `f` on a spawned task.
    #[must_use = "this response must be returned from the handler"]
    pub fn on_upgrade<F, Fut>(self, config: WebSocketConfig, f: F) -> Response
    where
        F: FnOnce(WebSocketStream<HyperIo>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        upgrade::spawn_upgrade(self.on_upgrade, config, f);
        upgrade::accept_response(&self.key).map(|()| Body::empty())
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

        Ok(Self {
            key,
            on_upgrade,
            headers: parts.headers.clone(),
        })
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
    use axum::http::{header, Method, Request};

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
