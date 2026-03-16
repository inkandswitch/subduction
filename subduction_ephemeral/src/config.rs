//! Configuration and event types for the ephemeral system.

use alloc::vec::Vec;

use sedimentree_core::id::SedimentreeId;
use subduction_core::peer::id::PeerId;

/// A received ephemeral message delivered to the application.
///
/// Produced by [`EphemeralHandler`] and sent via the callback channel.
/// Only remote peers' messages appear here — self-delivery is suppressed.
///
/// [`EphemeralHandler`]: crate::handler::EphemeralHandler
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EphemeralEvent {
    /// The sedimentree topic this message was published to.
    pub id: SedimentreeId,
    /// The peer that sent the message.
    pub sender: PeerId,
    /// The opaque application payload.
    pub payload: Vec<u8>,
}

/// Configuration for the ephemeral handler.
#[derive(Debug, Clone, Copy)]
pub struct EphemeralConfig {
    /// Maximum payload size in bytes.
    ///
    /// Messages exceeding this limit are silently dropped.
    /// Default: `65_536` (64 KB).
    pub max_payload_size: usize,

    /// Capacity of the inbound event channel.
    ///
    /// The channel is bounded; senders block if the app doesn't drain
    /// fast enough.
    /// Default: 1024.
    pub channel_capacity: usize,
}

impl Default for EphemeralConfig {
    fn default() -> Self {
        Self {
            max_payload_size: 65_536,
            channel_capacity: 1024,
        }
    }
}
