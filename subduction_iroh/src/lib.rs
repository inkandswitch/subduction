//! # Subduction Iroh Transport
//!
//! A QUIC transport layer for the Subduction sync protocol using [iroh].
//!
//! Iroh provides peer-to-peer QUIC connections with NAT traversal via relay
//! servers and hole punching. This crate bridges iroh's connection model to
//! Subduction's [`Connection`](subduction_core::connection::Connection) trait.
//!
//! # Authentication
//!
//! This crate runs the full Subduction handshake (`Signed<Challenge>` /
//! `Signed<Response>`) over the QUIC bi-directional stream. While iroh
//! already provides mutual TLS authentication at the transport layer, the
//! Subduction handshake proves that the peer holds the expected _Subduction_
//! signing key (which may differ from the iroh node identity).
//!
//! # Architecture
//!
//! Each connection uses a single QUIC bi-directional stream with
//! length-prefixed framing:
//!
//! ```text
//! [handshake: Challenge/Response exchange]
//!       |
//!       v
//! send()  --> outbound_tx --> [sender task] --> QUIC SendStream
//! QUIC RecvStream --> [listener task] --> inbound_writer --> recv()
//! ```
//!
//! The `call()` method uses the same pending-map + oneshot pattern as
//! the WebSocket and HTTP transports for request-response correlation.
//!
//! # Example
//!
//! ```ignore
//! use iroh::{Endpoint, EndpointAddr};
//! use subduction_iroh::client;
//! use subduction_core::connection::handshake::Audience;
//! use std::time::Duration;
//!
//! // Connect to a peer
//! let ep = Endpoint::bind().await?;
//! let result = client::connect(
//!     &ep, addr, Duration::from_secs(30), timeout,
//!     &signer, Audience::known(peer_id),
//! ).await?;
//! tokio::spawn(result.listener_task);
//! tokio::spawn(result.sender_task);
//! subduction.register(result.authenticated).await?;
//! ```
//!
//! [iroh]: https://iroh.computer/

extern crate alloc;

pub mod client;
pub mod connection;
pub mod error;
pub mod handshake;
pub mod tasks;

#[cfg(feature = "server")]
pub mod server;

/// ALPN protocol identifier for Subduction over Iroh.
///
/// Both sides must agree on this during the QUIC/TLS handshake.
pub const ALPN: &[u8] = b"subduction/0";
