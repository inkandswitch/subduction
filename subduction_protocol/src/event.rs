//! Events: everything the driver tells the machine.
//!
//! Every event is fed through `handle(now, event)` together with the
//! driver's current monotonic [`Timestamp`](crate::timestamp::Timestamp);
//! the machine also processes any due internal deadlines on every call.

use alloc::vec::Vec;

use crate::{
    command::Command,
    effect::CryptoResult,
    handshake::audience::Audience,
    id::ConnId,
    storage::StorageResult,
    ticket::{CryptoTicket, StorageTicket},
};

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
        /// Who we believe we are dialing. Required for
        /// [`Outbound`](Direction::Outbound) connections:
        /// [`Audience::Known`] additionally pins the authenticated identity
        /// (mismatch is a [`Fault::PeerMismatch`]). Ignored for inbound.
        ///
        /// [`Fault::PeerMismatch`]: crate::outcome::Fault::PeerMismatch
        audience: Option<Audience>,
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

    /// A crypto operation finished; `ticket` is echoed from the issuing
    /// [`Effect::Crypto`](crate::effect::Effect::Crypto). Stale tickets are
    /// dropped (see [`ticket`](crate::ticket)).
    CryptoDone {
        /// The witness from the issuing effect.
        ticket: CryptoTicket,
        /// The operation's result.
        result: CryptoResult,
    },

    /// A storage operation finished; `ticket` is echoed from the issuing
    /// [`Effect::Storage`](crate::effect::Effect::Storage). Stale tickets
    /// are dropped (see [`ticket`](crate::ticket)).
    StorageDone {
        /// The witness from the issuing effect.
        ticket: StorageTicket,
        /// The operation's result.
        result: StorageResult,
    },

    /// A local application request (see [`Command`]).
    Command(Command),

    /// The driver's timer fired (or it simply wants deadlines processed).
    /// Carries no payload: the machine re-derives due work from `now`
    /// against its own deadline map, so spurious or late wakes are
    /// harmless by construction.
    Wake,
}
