//! Driver-facing event vocabulary shared by the machines.
//!
//! The composed input alphabet lives on [`Node`](crate::node::NodeEvent);
//! the per-machine alphabets live with their machines. This module keeps
//! the shared pieces.

/// Who initiated a connection. Determines the handshake role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Direction {
    /// We dialed the peer (we begin the handshake).
    Outbound,

    /// The peer dialed us (we respond to the handshake).
    Inbound,
}
