//! # Subduction over hyper
//!
//! Accept Subduction WebSocket connections from any HTTP server built on
//! [`hyper`] 1.x — axum, poem, salvo, or hyper itself — and hand them to
//! `subduction_websocket` as an [`async_tungstenite::WebSocketStream`].
//!
//! Framework WebSocket modules (e.g. `axum::extract::ws`) wrap their own copy
//! of tungstenite and seal the stream, so nothing from them can reach
//! `WebSocket::new_with_keepalive`. This crate goes one layer down instead:
//! hyper exposes the raw post-`101` connection through
//! [`hyper::upgrade::OnUpgrade`], and that is all a WebSocket framer needs.
//!
//! ```text
//! HTTP request ─▶ upgrade::validate(parts)        ─▶ AcceptKey     (http types only)
//!              ─▶ upgrade::accept_response(key)   ─▶ 101 response  (send it)
//!              ─▶ OnUpgrade.await                 ─▶ Upgraded      (hyper hands over the pipe)
//!              ─▶ upgrade::from_upgraded(io, cfg) ─▶ WebSocketStream<HyperIo>
//! ```
//!
//! The [`upgrade`] module is framework-neutral. With the `axum` feature, the
//! `axum::TungsteniteUpgrade` extractor packages those steps into a drop-in
//! replacement for `axum::extract::ws::WebSocketUpgrade`.
//!
//! The result is an `async_tungstenite::WebSocketStream<HyperIo>`, which
//! `subduction_websocket::websocket::WebSocket::new_with_keepalive` accepts
//! directly. It is *not* a `TokioWebSocketServer` connection: that server's
//! accepted type is fixed to plain TCP, so embedders own the listen / sender /
//! keepalive task spawning and `Subduction::add_connection` themselves, as the
//! CLI does in `subduction_cli/src/server.rs`.
//!
//! See [`upgrade`] for limitations (HTTP/1.1 only, no subprotocol negotiation,
//! no `Origin` policy).

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "axum")]
#[cfg_attr(docsrs, doc(cfg(feature = "axum")))]
pub mod axum;

pub mod upgrade;
