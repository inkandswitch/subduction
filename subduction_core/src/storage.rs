//! Storage-related types and capabilities.
//!
//! This module provides:
//! - [`Storage`] - The storage trait for persisting signed commits and fragments
//! - [`MemoryStorage`] - In-memory storage backend
//! - [`Fetcher`] / [`Putter`] / [`Destroyer`] - Capabilities for storage access
//! - Storage key and ID utilities
//!
//! # Signed Storage
//!
//! All commits and fragments are stored as [`Signed`] values. This ensures:
//!
//! - **Provenance**: Every stored commit/fragment has a verified author
//! - **Integrity**: Signatures prevent tampering with stored data
//! - **Trust on load**: Data loaded from storage was verified before storage
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
//! [`Signed`]: subduction_crypto::signed::Signed

pub mod blob_access;
pub mod destroyer;
pub mod fetcher;
pub mod id;
pub mod key;
pub mod local_access;
pub mod memory;
pub mod powerbox;
pub mod putter;
pub mod traits;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub mod metrics;
