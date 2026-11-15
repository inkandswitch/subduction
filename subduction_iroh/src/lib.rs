//! # Subduction Iroh Transport
//!
//! A QUIC transport layer for the Subduction sync protocol using [iroh].
//!
//! Iroh provides peer-to-peer QUIC connections with NAT traversal via relay
//! servers and hole punching. This crate bridges iroh's connection model to
//! Subduction's [`Transport`](subduction_core::transport::Transport) trait.
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
//! use subduction_iroh::{DEFAULT_MAX_FRAME_SIZE, client};
//! use subduction_core::handshake::audience::Audience;
//!
//! // Connect to a peer
//! let ep = Endpoint::bind().await?;
//! let result = client::connect(
//!     &ep, addr, &signer, Audience::known(peer_id), DEFAULT_MAX_FRAME_SIZE,
//! ).await?;
//! tokio::spawn(result.listener_task);
//! tokio::spawn(result.sender_task);
//! subduction.add_connection(result.authenticated).await?;
//! ```
//!
//! [iroh]: https://iroh.computer/

extern crate alloc;

pub mod client;
pub mod error;
pub mod handshake;
pub mod tasks;
pub mod transport;

#[cfg(feature = "server")]
pub mod server;

/// ALPN protocol identifier for Subduction over Iroh.
///
/// Both sides must agree on this during the QUIC/TLS handshake.
pub const ALPN: &[u8] = b"subduction/0";

/// The default maximum frame size (50 MiB).
///
/// Matches `DEFAULT_MAX_MESSAGE_SIZE` / `DEFAULT_MAX_BODY_SIZE` in the
/// WebSocket and HTTP long-poll transports. Each frame carries one message,
/// so this caps message size too. It applies to inbound frames only; peers do
/// not negotiate it, and a peer with a smaller limit closes the connection on
/// a frame larger than its own.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 50 * 1024 * 1024;
