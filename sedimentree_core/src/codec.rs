//! Canonical binary codec for signed payloads.
//!
//! This module provides traits and helpers for encoding and decoding types
//! in a deterministic binary format suitable for signing, network transmission,
//! and persistent storage.
//!
//! # Traits
//!
//! - [`Schema`] — Type identity (4-byte header)
//! - [`Encode`] — Serialize to bytes (any type)
//! - [`EncodeFields`] — Encode fields for schema-prefixed types
//! - [`Decode`] — Deserialize from bytes
//!
//! # Format Overview
//!
//! All signed types share a common structure:
//!
//! ```text
//! ┌─────────────────────── Payload ───────────────────────┬─ Seal ─┐
//! ╔════════╦══════════╦═══════════════════════════════════╦════════╗
//! ║ Schema ║ IssuerVK ║         Type-Specific Fields      ║  Sig   ║
//! ║   4B   ║   32B    ║            (variable)             ║  64B   ║
//! ╚════════╩══════════╩═══════════════════════════════════╩════════╝
//! ```
//!
//! - **Schema**: 4-byte header identifying type and version (e.g., `STC\x00`)
//! - **`IssuerVK`**: `Ed25519` verifying key of the signer (32 bytes)
//! - **Fields**: Type-specific data encoded by the [`Encode`] implementation
//! - **Signature**: `Ed25519` signature over the payload (64 bytes)
//!
//! # Encoding Conventions
//!
//! - All integers are **big-endian** (network byte order)
//! - All arrays are **sorted ascending** by byte order
//! - Sizes use fixed-width integers (not variable-length encoding)
//!
//! # Schema Headers
//!
//! | Prefix | Crate | Types |
//! |--------|-------|-------|
//! | `ST` | `sedimentree_core` | `LooseCommit`, `Fragment` |
//! | `SU` | `subduction_core` | `Challenge`, `Response`, `SyncMessage` |

pub mod decode;
pub mod encode;
pub mod error;
pub mod schema;
