//! Completion tokens: witnesses that pair driver-performed work with the
//! machine state it was issued against.
//!
//! Every effect that completes asynchronously (crypto, storage) carries a
//! token; the completion event echoes it back. The machine validates the
//! token before applying the result:
//!
//! ```text
//! machine ──effect { token: (scope, generation, seq) }──▶ driver worker
//!                                                              │
//! machine ◀──completion { token, result }─────────────────────┘
//!    │
//!    ├─ token.generation == entity's current generation → apply
//!    └─ otherwise → stale (entity was torn down / restarted) → drop
//! ```
//!
//! This replaces mutual exclusion: state that is mid-operation is an
//! explicit `Awaiting*` variant holding the expected token, so interleaved
//! or stale completions are rejectable by construction (ADR-006/007).
//! Tokens are plain data so they cross FFI unchanged; the compile-time
//! phantom-witness layer lives on the machine-internal pending entries,
//! not on the wire-crossing token itself.

use crate::id::{ConnId, Generation, Seq};

/// What machine entity an operation was issued against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Scope {
    /// Scoped to one connection (e.g. handshake verification): stale when
    /// the connection's generation moves on.
    Connection(ConnId),

    /// A local operation not tied to any connection (e.g. signing a
    /// locally created commit): stale only across machine restarts, which
    /// drivers must not span.
    Local,
}

/// A completion token for a driver-performed crypto operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CryptoToken {
    /// The entity this operation was issued against.
    pub scope: Scope,

    /// The entity's generation at issue time.
    pub generation: Generation,

    /// Distinguishes in-flight operations under the same generation.
    pub seq: Seq,
}

/// A completion token for a driver-performed storage operation.
///
/// Identical shape to [`CryptoToken`], kept as a distinct type so a storage
/// completion can never be fed to a crypto continuation (or vice versa).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StorageToken {
    /// The entity this operation was issued against.
    pub scope: Scope,

    /// The entity's generation at issue time.
    pub generation: Generation,

    /// Distinguishes in-flight operations under the same generation.
    pub seq: Seq,
}
