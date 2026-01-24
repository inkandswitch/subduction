//! Policies for controlling access in Subduction.
//!
//! This module provides:
//! - [`ConnectionPolicy`] - Controls which peers can connect
//! - [`StoragePolicy`] - Controls read/write access to sedimentrees
//! - [`OpenPolicy`] - A policy that allows all operations
//! - [`CachingPolicy`] - A wrapper that caches successful authorization results
//!
//! For capabilities that bundle authorization with storage access,
//! see [`crate::storage::fetcher`] and [`crate::storage::putter`].

pub mod cached;
pub mod connection;
pub mod error;
pub mod open;
pub mod storage;

pub use cached::CachedPolicy;
pub use connection::ConnectionPolicy;
pub use error::{CapabilityRevoked, Generation};
pub use open::OpenPolicy;
pub use storage::StoragePolicy;
