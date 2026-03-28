//! Ephemeral (non-persisted) messaging for Subduction.
//!
//! Provides authenticated, fire-and-forget messaging scoped to
//! [`Topic`] identifiers. Primary use cases: cursor positions,
//! selections, presence-in-document, typing indicators, and other
//! transient application-level signals.
//!
//! # Architecture
//!
//! ```text
//! subduction_core                      subduction_ephemeral
//! ┌──────────────────────┐            ┌────────────────────────┐
//! │ Connection<K, M>     │            │ EphemeralMessage       │
//! │ Handler<K, C>        │◄───────────│ EphemeralHandler       │
//! │ SyncMessage          │            │ EphemeralPolicy        │
//! │ SyncHandler          │            └────────────────────────┘
//! └──────────────────────┘
//! ```
//!
//! The core crate knows nothing about ephemeral behavior. This crate
//! defines its own message type ([`EphemeralMessage`]), policy trait
//! ([`EphemeralPolicy`]), and handler ([`EphemeralHandler`]).
//!
//! Wire multiplexing (combining sync, ephemeral, and keyhive messages
//! into a single envelope type) is the application's responsibility.
//! See `subduction_wasm` for an example `WireMessage` enum.
//!
//! [`Topic`]: topic::Topic

#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod clock;
pub mod composed;
pub mod config;
pub mod handler;
pub mod message;
pub mod nonce_cache;
pub mod policy;
pub mod topic;
