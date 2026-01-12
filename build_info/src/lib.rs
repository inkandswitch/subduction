//! Git commit hash of the current build.

#![no_std]

/// The current git hash.
pub const GIT_HASH: &str = env!("GIT_HASH");
