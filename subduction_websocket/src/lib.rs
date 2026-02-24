//! # Suduction WebSocket

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(windows), allow(clippy::multiple_crate_versions))] // windows-sys

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod error;
pub mod handshake;
pub mod timeout;
pub mod websocket;

#[cfg(feature = "tokio_base")]
pub mod tokio;

/// Default maximum WebSocket message size (50 MB).
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 50 * 1024 * 1024;
