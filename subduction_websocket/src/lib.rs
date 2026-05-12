//! # Suduction WebSocket

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(windows), allow(clippy::multiple_crate_versions))] // windows-sys

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod error;
pub mod handshake;
#[cfg(feature = "futures-timer")]
pub mod timeout;
pub mod websocket;

#[cfg(feature = "tokio_base")]
pub mod tokio;

/// Default maximum WebSocket message size (50 MB).
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 50 * 1024 * 1024;

/// Default interval between server-side keepalive Pings.
///
/// Chosen to comfortably under-shoot common proxy idle timeouts
/// (Caddy / nginx / cloud load balancers tend to allow ≥ 60 s of idle
/// time on hijacked / upgraded connections). 30 s gives two
/// keepalive opportunities before a 60 s timeout fires, which keeps
/// idle WS connections alive without meaningful bandwidth overhead.
pub const DEFAULT_KEEPALIVE_INTERVAL: core::time::Duration = core::time::Duration::from_secs(30);
