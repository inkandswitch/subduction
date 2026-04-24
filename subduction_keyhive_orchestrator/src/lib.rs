//! Orchestration layer above the keyhive sync protocol.
//!
//! This crate exposes [`SubductionKeyhiveOrchestrator`], a passive
//! coordinator that wraps [`subduction_keyhive::KeyhiveProtocol`] and
//! owns syncpoint tracking, the periodic event cache, rate
//! limiting, and storage-policy config for keyhive sync. Drivers are
//! responsible for all concurrency primitives.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod cache;
pub mod config;
pub mod orchestrator;
pub mod syncpoints;

pub use config::OrchestratorConfig;
pub use orchestrator::{OrchestratorError, SubductionKeyhiveOrchestrator};
