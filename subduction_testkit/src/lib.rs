//! In-memory test harness for Subduction [`Node`]s.
//!
//! Wires N nodes through a fake point-to-point network with fake leaf
//! drivers (signing, storage, frame table), so full-pipeline behavior —
//! handshake, sync sessions, subscription fan-out — runs deterministically
//! in plain unit tests with no IO, no clocks, and no async runtime.
//!
//! ```text
//!   ┌ Net ──────────────────────────────────────────────┐
//!   │  TestDriver[0]      TestDriver[1]      TestDriver[n]
//!   │  ┌───────────┐      ┌───────────┐
//!   │  │ Node      │      │ Node      │   links: (i,conn) ↔ (j,conn)
//!   │  │ signer    │      │ signer    │   pump(): shuttle outboxes
//!   │  │ storage   │      │ storage   │           until quiescent
//!   │  │ frames ───┼──────┼─▶ invariants: no use-after-free,
//!   │  └───────────┘      └──── no leak at quiescence
//!   └───────────────────────────────────────────────────┘
//! ```
//!
//! [`driver::TestDriver`] is one node with its fake leaf drivers;
//! [`net::Net`] wires several together; [`sim`] adds a seeded
//! deterministic scheduler over a `Net`.
//!
//! [`Node`]: subduction_protocol::node::Node

pub mod driver;
pub mod net;
pub mod sim;

/// The harness error type: invariant violations carry a message and
/// surface through `?` in tests (rather than panicking mid-pipeline).
pub type TestError = Box<dyn std::error::Error>;

/// Fallible invariant check (the harness's assert).
///
/// # Errors
/// Returns `msg` as the error when `cond` is false.
pub fn ensure(cond: bool, msg: &str) -> Result<(), TestError> {
    if cond { Ok(()) } else { Err(msg.into()) }
}
