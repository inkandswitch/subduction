//! Keyhive sync protocol.

#![cfg_attr(not(feature = "std"), no_std)]
// keyhive_core uses thiserror 1.x and derivative (syn 1.x)
#![allow(clippy::multiple_crate_versions)]

extern crate alloc;

mod collections;

pub mod connection;
pub mod error;
#[cfg(feature = "std")]
pub mod fs_storage;
pub mod message;
pub mod peer_id;
#[cfg(feature = "serde")]
pub mod protocol;
pub mod signed_message;
pub mod storage;
#[cfg(feature = "serde")]
pub mod storage_ops;

#[cfg(all(test, feature = "serde"))]
#[allow(clippy::expect_used, clippy::indexing_slicing)]
mod test_utils;
