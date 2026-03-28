//! Configuration and event types for the ephemeral system.

use alloc::vec::Vec;
use core::time::Duration;

use crate::topic::Topic;
use subduction_core::peer::id::PeerId;

/// A received ephemeral message delivered to the application.
///
/// Produced by [`EphemeralHandler`] and sent via the callback channel.
/// Only remote peers' messages appear here — self-delivery is suppressed.
///
/// The `sender` field is the _originator_ of the message (from the
/// signed wire format), not the relay that forwarded it.
///
/// [`EphemeralHandler`]: crate::handler::EphemeralHandler
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EphemeralEvent {
    /// The topic this message was published to.
    pub id: Topic,
    /// The originator peer that authored and signed the message.
    pub sender: PeerId,
    /// The dedup nonce (exposed for application-level ordering if desired).
    pub nonce: u64,
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
    /// The channel is bounded; events are dropped if the app doesn't
    /// drain fast enough.
    /// Default: 1024.
    pub channel_capacity: usize,

    /// Duration of each nonce cache window.
    ///
    /// Nonces are retained for 1-2 window periods. Longer windows
    /// provide stronger replay protection at the cost of more memory.
    /// Default: 30 seconds.
    pub nonce_window_duration: Duration,

    /// Maximum age of an ephemeral message.
    ///
    /// Messages whose timestamp is further from the receiver's wall
    /// clock than this duration are dropped (both past and future).
    ///
    /// This provides an absolute bound on replay: even if the nonce
    /// cache has evicted the nonce, the timestamp check rejects
    /// stale messages.
    ///
    /// Default: 30 seconds.
    pub max_message_age: Duration,
}

impl Default for EphemeralConfig {
    fn default() -> Self {
        Self {
            max_payload_size: 65_536,
            channel_capacity: 1024,
            nonce_window_duration: Duration::from_secs(30),
            max_message_age: Duration::from_secs(30),
        }
    }
}
