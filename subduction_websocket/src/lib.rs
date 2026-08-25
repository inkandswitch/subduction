//! # Subduction WebSocket Transport
//!
//! A WebSocket implementation of the driver's
//! [`Transport`](subduction_runtime::transport::Transport) capability:
//! one complete Subduction wire message per binary WebSocket frame.
//!
//! Like the rest of the driver, scheduling stays with the caller: both
//! [`client::connect`] and [`server::accept`] hand back the transport
//! _and_ its socket-pump future, which the application spawns on its own
//! runtime.

pub mod client;
pub mod server;
pub mod transport;
