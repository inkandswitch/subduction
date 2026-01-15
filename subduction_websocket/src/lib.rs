//! # Suduction WebSocket

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(windows), allow(clippy::multiple_crate_versions))] // windows-sys

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod error;
pub mod timeout;
pub mod websocket;

#[cfg(any(feature = "tokio", feature = "tokio-rustls"))]
pub mod tokio;
