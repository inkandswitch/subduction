//! Events: everything the driver tells the machine.
//!
//! Every event is fed through `handle(now, event)` together with the
//! driver's current monotonic [`Timestamp`](crate::timestamp::Timestamp);
//! the machine also processes any due internal deadlines on every call.

use alloc::vec::Vec;

use crate::{effect::CryptoResult, id::ConnId, peer_id::PeerId, token::CryptoToken};

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

/// An input to the machine.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Event {
    /// A transport connection is up (pre-handshake). The driver allocated
    /// the [`ConnId`] and must never reuse it.
    Connected {
        /// The new connection.
        conn: ConnId,
        /// Who initiated it.
        direction: Direction,
        /// The expected peer identity, when dialing a known peer. The
        /// handshake fails the connection if the authenticated identity
        /// does not match.
        expected_peer: Option<PeerId>,
    },

    /// A transport connection is gone (peer close, transport error, or a
    /// requested [`Effect::Disconnect`](crate::effect::Effect::Disconnect)
    /// completing).
    Disconnected {
        /// The closed connection.
        conn: ConnId,
    },

    /// One complete wire message arrived on a connection.
    ///
    /// The driver must deliver exactly one whole encoded protocol message
    /// per event: transports that fragment (WebSocket splits one message
    /// across frames; QUIC/iroh chunk streams) reassemble *below* this
    /// boundary, and transports that batch must split. The machine never
    /// sees partial messages.
    MessageReceived {
        /// The receiving connection.
        conn: ConnId,
        /// One complete encoded wire message.
        bytes: Vec<u8>,
    },

    /// A crypto operation finished; `token` is echoed from the issuing
    /// [`Effect::Crypto`](crate::effect::Effect::Crypto). Stale tokens are
    /// dropped (see [`token`](crate::token)).
    CryptoDone {
        /// The witness from the issuing effect.
        token: CryptoToken,
        /// The operation's result.
        result: CryptoResult,
    },

    /// The driver's timer fired (or it simply wants deadlines processed).
    /// Carries no payload: the machine re-derives due work from `now`
    /// against its own deadline map, so spurious or late wakes are
    /// harmless by construction.
    Wake,
}
