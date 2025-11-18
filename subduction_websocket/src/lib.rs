//! # Suduction WebSocket

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod error;
pub mod websocket;

#[cfg(feature = "tokio")]
pub mod tokio;
