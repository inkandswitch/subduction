//! Completion tickets: witnesses that pair driver-performed work with the
//! machine state it was issued against.
//!
//! Every effect that completes asynchronously (crypto, storage) carries a
//! ticket; the completion event echoes it back. The machine validates the
//! ticket before applying the result:
//!
//! ```text
//! machine ──effect { ticket: (entity, generation, seq) }──▶ driver worker
//!                                                              │
//! machine ◀──completion { ticket, result }─────────────────────┘
//!    │
//!    ├─ ticket.generation == entity's current generation → apply
//!    └─ otherwise → stale (entity was torn down / restarted) → drop
//! ```
//!
//! This replaces mutual exclusion: state that is mid-operation is an
//! explicit `Awaiting*` variant holding the expected ticket, so interleaved
//! or stale completions are rejectable by construction.
//! Tickets are plain data so they cross FFI unchanged; the compile-time
//! phantom-witness layer lives on the machine-internal pending entries,
//! not on the wire-crossing ticket itself.

use crate::id::{ConnId, Generation, Seq};

/// A completion ticket for a driver-performed crypto operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CryptoTicket {
    /// The entity this operation was issued against.
    pub entity: Entity,

    /// The entity's generation at issue time.
    pub generation: Generation,

    /// Distinguishes in-flight operations under the same generation.
    pub seq: Seq,
}

/// A completion ticket for a driver-performed storage operation.
///
/// Identical shape to [`CryptoTicket`], kept as a distinct type so a storage
/// completion can never be fed to a crypto continuation (or vice versa).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StorageTicket {
    /// The entity this operation was issued against.
    pub entity: Entity,

    /// The entity's generation at issue time.
    pub generation: Generation,

    /// Distinguishes in-flight operations under the same generation.
    pub seq: Seq,
}

/// What machine entity an operation was issued against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Entity {
    /// Scoped to one connection (e.g. handshake verification): stale when
    /// the connection's generation moves on.
    Connection(ConnId),

    /// A local operation not tied to any connection (e.g. sealing and
    /// persisting a locally authored commit): stale only across machine
    /// restarts, which drivers must not span.
    Local,
}
