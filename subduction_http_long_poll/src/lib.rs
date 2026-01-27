//! # Subduction HTTP Long Poll
//!
//! HTTP long-polling transport for the Subduction sync protocol.
//!
//! This crate provides both client and server implementations for syncing
//! over HTTP using long-polling for server-to-client message delivery.
//!
//! ## Features
//!
//! - `client` — HTTP client using [`reqwest`]
//! - `server` — HTTP server using [`axum`]
//! - `std` — Standard library support (required by client/server)

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod error;
pub mod session;

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub mod client;

#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub mod server;

/// Default long-poll timeout (30 seconds).
pub const DEFAULT_POLL_TIMEOUT_SECS: u64 = 30;

/// Default session timeout (5 minutes).
pub const DEFAULT_SESSION_TIMEOUT_SECS: u64 = 5 * 60;

/// Default call timeout (30 seconds).
pub const DEFAULT_CALL_TIMEOUT_SECS: u64 = 30;

/// Session ID header name.
pub const SESSION_ID_HEADER: &str = "X-Session-Id";
