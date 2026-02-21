//! Canonical binary codec for subduction protocol types.
//!
//! The [`Codec`] implementations for handshake types (`Challenge`, `Response`)
//! are located in [`connection::handshake`](crate::connection::handshake).
//!
//! See [`sedimentree_core::codec`] for the trait definition and format overview.

// Re-export core codec types for convenience
pub use sedimentree_core::codec::{Codec, CodecError};
