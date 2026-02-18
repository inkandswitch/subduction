//! Keyhive sync protocol.

#![cfg_attr(not(feature = "std"), no_std)]
// keyhive_core uses thiserror 1.x and derivative (syn 1.x)
#![allow(clippy::multiple_crate_versions)]

extern crate alloc;

mod collections;

#[cfg(feature = "fs")]
pub mod fs_storage;

#[cfg(feature = "serde")]
pub mod storage_ops;

#[cfg(feature = "serde")]
pub mod protocol;

pub mod connection;
pub mod error;
pub mod message;
pub mod peer_id;
pub mod signed_message;
pub mod storage;

#[cfg(all(test, feature = "serde"))]
#[allow(clippy::expect_used, clippy::indexing_slicing)]
mod test_utils;
