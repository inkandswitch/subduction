//! Policies for controlling access in Subduction.
//!
//! This module provides:
//! - [`ConnectionPolicy`] - Controls which peers can connect
//! - [`StoragePolicy`] - Controls read/write access to sedimentrees
//!
//! For capabilities that bundle authorization with storage access,
//! see [`crate::storage::fetcher`] and [`crate::storage::putter`].

pub mod connection;
pub mod error;
pub mod storage;

pub use connection::ConnectionPolicy;
pub use error::{CapabilityRevoked, Generation};
pub use storage::StoragePolicy;
