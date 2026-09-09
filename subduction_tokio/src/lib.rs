//! Tokio runtime integration for Subduction.
//!
//! `subduction_core` has no runtime of its own: construction hands back the
//! listener and manager futures and expects the caller to drive them, and
//! the node is generic over a [`Spawn`](subduction_core::spawn::Spawn)
//! implementation for its own fan-out. This crate supplies the Tokio side of
//! that contract.
//!
//! - [`spawn::TokioSpawn`] / [`spawn::TrackedTokioSpawn`] — spawners.
//! - [`timeout::TimeoutTokio`] — `tokio::time::timeout` as a
//!   [`Timeout`](subduction_core::timeout::Timeout).
//! - [`node::TokioSubduction`] — owns a node: spawns and supervises its
//!   loops, scopes your background tasks to its lifetime, and tears
//!   everything down in the right order. This is the recommended way to hold
//!   a `Subduction` on Tokio; the lower-level pieces exist for callers with
//!   their own supervision.

pub mod node;
pub mod spawn;
pub mod timeout;
