//! # Suduction WebSocket

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod error;
pub mod timeout;
pub mod websocket;

#[cfg(feature = "tokio")]
pub mod tokio;
