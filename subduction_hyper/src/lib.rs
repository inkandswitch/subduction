//! # Subduction over hyper
//!
//! Accept Subduction WebSocket connections from any HTTP server built on
//! [`hyper`] 1.x — axum, poem, salvo, or hyper itself — and hand them to
//! `subduction_websocket` as an [`async_tungstenite::WebSocketStream`].
//!
//! Many framework WebSocket modules wrap their own copy of tungstenite and seal the
//! stream, so nothing from them can reach `WebSocket::new_with_keepalive`.
//! This crate goes one layer down instead: hyper exposes the raw post-`101`
//! connection through [`hyper::upgrade::OnUpgrade`], and that is all a
//! WebSocket framer needs.
//!
//! ```text
//! HTTP request ─▶ upgrade::validate(parts)        ─▶ AcceptKey     (http types only)
//!              ─▶ upgrade::accept_response(key)   ─▶ 101 response  (send it)
//!              ─▶ OnUpgrade.await                 ─▶ Upgraded      (hyper hands over the pipe)
//!              ─▶ upgrade::from_upgraded(io, cfg) ─▶ WebSocketStream<HyperIo>
//! ```
//!
//! The [`upgrade`] module is framework-neutral. With the `axum` feature, the
//! [`axum::TungsteniteUpgrade`] extractor packages those steps into a single
//! handler argument.
//!
//! The result is an `async_tungstenite::WebSocketStream<HyperIo>`, which
//! `subduction_websocket::websocket::WebSocket::new_with_keepalive` accepts
//! directly. It is *not* a `TokioWebSocketServer` connection: that server's
//! accepted type is fixed to plain TCP. Embedders therefore spawn the listen,
//! sender, and keepalive tasks, wrap the socket in a `MessageTransport`, and
//! call `Subduction::add_connection` themselves — the [`axum`] module docs
//! carry that callback in full, and `tests/axum.rs` runs it end to end.
//!
//! Task spawning is generic: [`spawn_upgrade`](upgrade::spawn_upgrade) takes
//! any [`Spawn<Sendable>`](subduction_core::spawn::Spawn), so upgrade tasks can
//! run wherever the node's own tasks run.
//!
//! The I/O underneath is not. [`HyperIo`](upgrade::HyperIo) adapts hyper's
//! [`Upgraded`](hyper::upgrade::Upgraded) through tokio's traits to the
//! `futures` traits `async-tungstenite` wants, so a tokio reactor has to be
//! driving the socket. That is an adapter choice rather than a design one, but
//! it does mean there is no `no_std` build and no runtime-agnostic path today.
//!
//! See [`upgrade`] for limitations.

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "axum")]
#[cfg_attr(docsrs, doc(cfg(feature = "axum")))]
pub mod axum;

pub mod upgrade;
