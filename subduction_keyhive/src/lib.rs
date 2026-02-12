//! Keyhive protocol implementation for subduction.
//!
//! This crate implements the keyhive sync protocol, which enables peers to
//! synchronize keyhive operations (delegations, revocations, prekey operations)
//! in a decentralized manner.

#![cfg_attr(not(feature = "std"), no_std)]
// keyhive_core uses thiserror 1.x and derivative (syn 1.x)
#![allow(clippy::multiple_crate_versions)]

extern crate alloc;

pub mod connection;
pub mod error;
pub mod message;
pub mod peer_id;
pub mod signed_message;

pub use connection::KeyhiveConnection;
pub use error::{ProtocolError, SigningError, StorageError, VerificationError};
pub use message::Message;
pub use peer_id::KeyhivePeerId;
pub use signed_message::SignedMessage;
