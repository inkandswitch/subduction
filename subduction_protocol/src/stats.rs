//! Tier-2 telemetry: pull-based internal counters (ADR-003).
//!
//! Plain `u64` fields, snapshotted on demand via
//! [`Machine::stats`](crate::machine::Machine::stats) — no allocation, no
//! effect traffic, crosses FFI as a plain struct (quinn-proto style).

/// A snapshot of the machine's internal counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Stats {
    /// Connections announced via `Connected`.
    pub connections_opened: u64,

    /// Connections confirmed gone via `Disconnected`.
    pub connections_closed: u64,

    /// Handshakes that reached `Authenticated`.
    pub handshakes_completed: u64,

    /// Handshakes that failed (fault, rejection, or timeout).
    pub handshakes_failed: u64,

    /// Handshake deadlines that expired.
    pub handshake_timeouts: u64,

    /// Completions dropped because the entity generation had moved on.
    pub stale_completions: u64,

    /// Completions dropped because no pending operation matched.
    pub unknown_tickets: u64,

    /// Wire messages received (well-formed or not).
    pub messages_received: u64,

    /// Received messages that failed to decode.
    pub malformed_messages: u64,
}
