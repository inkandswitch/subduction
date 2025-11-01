//! Tokio implementations for [`WebSocket`][crate::websocket::WebSocket]s.

#[cfg(feature = "tokio_client")]
pub mod client;

#[cfg(feature = "tokio_server")]
pub mod server;
