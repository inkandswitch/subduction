//! Tokio implementations for [`WebSocket`][crate::websocket::WebSocket]s.
//!
//! Runtime plumbing that is not WebSocket-specific — spawners, the Tokio
//! [`Timeout`](subduction_core::timeout::Timeout), and node lifecycle — lives
//! in the `subduction_tokio` crate.

#[cfg(feature = "tokio_client_any")]
pub mod client;

#[cfg(feature = "tokio_server_any")]
pub mod server;

#[cfg(feature = "tokio_server_any")]
pub mod unified;
