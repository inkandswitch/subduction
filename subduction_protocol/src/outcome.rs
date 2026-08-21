//! Structured transition outcomes — the machine's return channel.
//!
//! Every `handle(now, event)` call returns an [`Outcome`]. Outcomes are
//! tier-3 telemetry: drivers pattern-match them into
//! `metrics`/`tracing`, and tests assert on them directly. They grow
//! additively as sub-machines land; they must stay plain data.

use crate::{handshake::rejection::RejectionReason, id::ConnId};

/// The result of feeding one event to the machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "outcomes are the machine's telemetry and error channel"]
pub enum Outcome {
    /// State advanced; drain effects via `poll_effect`.
    Progressed,

    /// Nothing to do (e.g. a wake with no due deadlines).
    Idle,

    /// The event was dropped without touching state, for a benign reason.
    Ignored(IgnoreReason),

    /// The event revealed a protocol violation or a broken invariant on a
    /// connection; a disconnect effect has been queued.
    ConnectionFault {
        /// The offending connection.
        conn: ConnId,
        /// Why the connection was condemned.
        fault: Fault,
    },
}

/// Benign reasons for dropping an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgnoreReason {
    /// A completion arrived for an entity generation that has moved on
    /// (the entity was torn down or restarted). Expected under teardown
    /// races; safe to drop by construction.
    StaleTicket,

    /// A completion ticket was never issued or was already consumed.
    UnknownTicket,

    /// An event referenced a connection the machine does not know.
    UnknownConnection(ConnId),

    /// [`Connected`](crate::node::NodeEvent::Connected) arrived for a
    /// [`ConnId`] that already exists (driver bug — ids must be fresh).
    DuplicateConnection(ConnId),

    /// An event arrived for a connection that is already being torn down
    /// (a disconnect effect is in flight).
    ConnectionClosing(ConnId),

    /// A sync response arrived that matches no in-flight request
    /// (already answered, timed out, or never ours). Unsolicited but
    /// harmless — dropped without touching state.
    UnknownRequest,

    /// A command targeted a connection that has not completed the
    /// handshake (e.g. an extension send — extension traffic is gated on
    /// authentication).
    NotAuthenticated(ConnId),

    /// An inter-machine edge message failed the sequencing discipline
    /// (stale generation, replay, gap, or misrouting) — a router/driver
    /// bug made observable, never applied to state.
    Edge(crate::edge::EdgeViolation),
}

/// Protocol violations that condemn a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Received bytes failed to decode as a wire message.
    MalformedMessage,

    /// A message arrived that is not valid in the connection's current
    /// protocol state.
    UnexpectedMessage,

    /// A signature check failed during the handshake.
    HandshakeVerificationFailed,

    /// The authenticated peer did not match the pinned
    /// [`Audience::Known`](crate::handshake::audience::Audience::Known)
    /// identity.
    PeerMismatch,

    /// A handshake deadline expired.
    HandshakeTimeout,

    /// The peer rejected our handshake (we are the initiator).
    HandshakeRejected(RejectionReason),

    /// We rejected the peer's handshake (we are the responder); a
    /// [`Rejection`](crate::handshake::rejection::Rejection) was sent.
    ChallengeRejected(RejectionReason),

    /// Simultaneous open: the peer replayed our own challenge bytes back
    /// at us (reflection attack).
    ReflectedChallenge,

    /// Simultaneous open: the crossed challenge was signed by our own key
    /// (reflection attack or self-connection).
    ReflectionAttack,

    /// Simultaneous open: the challenge signer and the response signer
    /// are different peers (MITM splice or protocol violation).
    SimultaneousOpenPeerMismatch,

    /// An outbound connection was opened without an
    /// [`Audience`](crate::handshake::audience::Audience) — the machine
    /// cannot know who to challenge (driver bug).
    MissingAudience,
}
