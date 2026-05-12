//! Keyhive sync protocol.

#![cfg_attr(not(feature = "std"), no_std)]
// keyhive_core uses thiserror 1.x and derivative (syn 1.x)
#![allow(clippy::multiple_crate_versions)]

extern crate alloc;

mod collections;

pub mod connection;
pub mod error;
#[cfg(feature = "handler")]
pub mod handler;
#[cfg(feature = "handler")]
pub mod policy;
#[cfg(feature = "runtime")]
pub mod runtime;
pub mod message;
pub mod peer_id;
pub mod signed_message;
pub mod storage;
pub mod wire;

#[cfg(feature = "serde")]
pub mod all_agent_events;
#[cfg(feature = "serde")]
mod cache;
#[cfg(feature = "serde")]
pub mod protocol;
#[cfg(feature = "serde")]
mod serde_compat;
#[cfg(feature = "serde")]
pub mod storage_ops;
#[cfg(feature = "serde")]
mod syncpoints;

#[cfg(any(test, feature = "test-utils"))]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    missing_debug_implementations,
    missing_docs
)]
pub mod test_utils;

pub use connection::KeyhiveConnection;
pub use error::StorageError;
#[cfg(feature = "std")]
pub use error::{ProtocolError, SigningError, VerificationError};
pub use message::Message;
pub use peer_id::KeyhivePeerId;
pub use signed_message::{SignedMessage, VerifiedMessage};
pub use storage::{KeyhiveStorage, MemoryKeyhiveStorage, StorageHash};
pub use wire::{KEYHIVE_SCHEMA, KeyhiveMessage};

#[cfg(feature = "serde")]
pub use all_agent_events::AllAgentEvents;
#[cfg(feature = "serde")]
pub use protocol::{KeyhiveProtocol, SyncOutcome};
#[cfg(feature = "serde")]
pub use storage_ops::{
    compact, hash_event_bytes, ingest_from_storage, load_archives, load_event_bytes, load_events,
    save_event, save_event_bytes, save_keyhive_archive,
};
