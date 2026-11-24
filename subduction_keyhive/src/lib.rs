//! Keyhive sync protocol.

#![cfg_attr(not(feature = "std"), no_std)]
// keyhive_core uses thiserror 1.x and derivative (syn 1.x)
#![allow(clippy::multiple_crate_versions)]

extern crate alloc;

// Heap-profiling global allocator for the in-crate periodic-cache membench
// (`cache::tests::serving_membench`). Only installed under the `dhat-heap`
// feature so normal builds and `cargo test` are unaffected.
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static DHAT_ALLOC: dhat::Alloc = dhat::Alloc;

mod awaiting_confirmation;
mod cache;
mod codec;
mod collections;
mod syncpoints;

pub mod access;
pub mod all_agent_events;
pub mod change_id;
pub mod connection;
pub mod error;
pub mod handler;
pub mod message;
pub mod native;
pub mod peer_id;
#[cfg(feature = "handler")]
pub mod policy;
pub mod protocol;
#[cfg(feature = "runtime")]
pub mod runtime;
pub mod signed_message;
pub mod storage;
pub mod storage_ops;
pub mod wire;

// Tests that build `keyhive_core` keyhives need `--features test-utils`.
#[cfg(feature = "test-utils")]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    missing_debug_implementations,
    missing_docs
)]
pub mod test_utils;

pub use access::{AgentEventIndex, AgentId, ArchiveBytes, KeyhiveAccess, NotIngested, Undecodable};
pub use all_agent_events::AllAgentEvents;
pub use connection::KeyhiveConnection;
pub use error::{ProtocolError, SigningError, StorageError, VerificationError, WireDecodeError};
pub use message::Message;
pub use native::NativeKeyhiveError;
pub use peer_id::KeyhivePeerId;
pub use protocol::{KeyhiveProtocol, KeyhiveProtocolError, MessageOutcome, SyncMode};
pub use signed_message::{SignedMessage, VerifiedMessage};
pub use storage::{KeyhiveStorage, MemoryKeyhiveStorage, StorageHash};
pub use storage_ops::{
    compact, hash_event_bytes, ingest_from_storage, load_archives, load_event_bytes,
    save_event_bytes, save_keyhive_archive,
};
pub use wire::{KEYHIVE_SCHEMA, KeyhiveMessage};
