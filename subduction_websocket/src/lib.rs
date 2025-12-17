//! # Suduction WebSocket

#![no_std]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

pub mod error;
pub mod websocket;

#[cfg(feature = "tokio")]
pub mod tokio;
