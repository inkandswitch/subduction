//! # Subduction Tokio Glue
//!
//! Tokio implementations of the driver's platform capabilities, shared
//! by the tokio-based transport and storage crates. Currently:
//! [`clock::TokioClock`].
//!
//! Keeping this out of `subduction_runtime` keeps the driver honest:
//! L2 commits to no executor, and everything runtime-specific lives in
//! platform crates like this one.

pub mod clock;
