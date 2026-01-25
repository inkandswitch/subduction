//! Storage-related types and capabilities.
//!
//! This module provides:
//! - [`Fetcher`] / [`Putter`] / [`Destroyer`] - Capabilities for storage access
//! - Storage key and ID utilities
//!
//! # Capability Model
//!
//! This module uses a powerbox pattern to mint reference capabilities
//! from certificate capabilities:
//!
//! ```text
//! ┌─────────────────┐      ┌────────────┐      ┌─────────────────────┐
//! │   Certificate   │      │            │      │      Reference      │
//! │   Capability    │      │ Subduction │      │     Capability      │
//! │                 │ ──▶  │ (Powerbox) │ ──▶  │                     │
//! │ "peer X may     │      │            │      │  Fetcher / Putter   │
//! │  fetch doc Y"   │      │            │      │  (storage + scope)  │
//! └─────────────────┘      └────────────┘      └─────────────────────┘
//! ```
//!
//! - **Certificate capability**: Authorization proof from Keyhive delivered
//!   over the wire. Verified by policy checks (`authorize_fetch`, `authorize_put`).
//!
//! - **Powerbox**: [`Subduction`] acts as the powerbox, combining the
//!   authorization proof with the storage backend to mint reference capabilities.
//!
//! - **Reference capability**: [`Fetcher`] and [`Putter`] bundle the storage
//!   handle with the sedimentree scope. Holding one of these types is both
//!   necessary and sufficient to perform the corresponding storage operations.
//!
//! - **Local-only capability**: [`Destroyer`] is for local cleanup operations
//!   (compaction, GC) and is never handed out to peers.
//!
//! [`Subduction`]: crate::subduction::Subduction
//! [`Destroyer`]: destroyer::Destroyer

pub mod destroyer;
pub mod fetcher;
pub mod id;
pub mod key;
pub mod powerbox;
pub mod putter;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub mod metrics;

#[cfg(feature = "metrics")]
pub use metrics::{MetricsStorage, RefreshMetrics};
