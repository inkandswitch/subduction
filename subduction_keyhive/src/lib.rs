//! Keyhive protocol implementation for subduction.
//!
//! This crate implements the keyhive sync protocol, which enables peers to
//! synchronize keyhive operations (delegations, revocations, prekey operations)
//! in a decentralized manner.

#![cfg_attr(not(feature = "std"), no_std)]
// keyhive_core uses thiserror 1.x and derivative (syn 1.x)
#![allow(clippy::multiple_crate_versions)]

extern crate alloc;

mod collections;

pub mod connection;
pub mod error;
pub mod message;
pub mod peer_id;
pub mod signed_message;
pub mod storage;

#[cfg(feature = "serde")]
pub mod storage_ops;

pub use connection::KeyhiveConnection;
pub use error::{ProtocolError, SigningError, StorageError, VerificationError};
pub use message::Message;
pub use peer_id::KeyhivePeerId;
pub use signed_message::SignedMessage;
pub use storage::{KeyhiveStorage, MemoryKeyhiveStorage, StorageHash};

#[cfg(feature = "serde")]
pub use storage_ops::{
    compact, hash_event_bytes, ingest_from_storage, load_archives, load_event_bytes, load_events,
    save_event, save_event_bytes, save_keyhive_archive, StorageEventHandler,
};
