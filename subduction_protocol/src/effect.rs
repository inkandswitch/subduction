//! Effects: everything the machine asks the driver to do.
//!
//! Effects are drained via `poll_effect` after feeding events. They are
//! plain data (bytes, ids, tokens) so the boundary crosses FFI unchanged.
//!
//! Timers are deliberately *not* effects: the machine keeps its own
//! deadline map and exposes only the next deadline via `poll_timeout`
//! (quinn-proto style). The driver arms a single timer and sends a bare
//! [`Event::Wake`](crate::event::Event::Wake) on expiry — no timer ids, no
//! cancellation races.

use alloc::vec::Vec;

use crate::{id::ConnId, peer_id::PeerId, token::CryptoToken};

/// An instruction from the machine to the driver.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Effect {
    /// Send one complete wire message on a connection.
    ///
    /// `bytes` is exactly one encoded protocol message. Transports that
    /// fragment (WebSocket frames, QUIC stream chunks, …) must deliver it
    /// atomically as a unit — fragmentation and reassembly live below this
    /// boundary, inside the transport.
    SendMessage {
        /// The connection to send on.
        conn: ConnId,
        /// One complete encoded wire message.
        bytes: Vec<u8>,
    },

    /// Close a connection. The driver must eventually answer with
    /// [`Event::Disconnected`](crate::event::Event::Disconnected).
    Disconnect {
        /// The connection to close.
        conn: ConnId,
    },

    /// Perform a crypto operation on a worker and answer with
    /// [`Event::CryptoDone`](crate::event::Event::CryptoDone) carrying the
    /// same token.
    Crypto {
        /// Witness pairing the completion with current machine state.
        token: CryptoToken,
        /// The operation to perform.
        op: CryptoOp,
    },

    /// Surface an application-facing event (subscriptions, auth, data).
    App(AppEvent),
}

/// A crypto operation for a driver worker (ADR-006a: sign/verify are
/// effects; small metadata hashing stays inline in the machine; blob
/// digests are fused into storage/ingest effects).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum CryptoOp {
    /// Sign `payload` with the machine's identity signing key (held by the
    /// driver; the machine never sees key material).
    Sign {
        /// The bytes to sign.
        payload: Vec<u8>,
    },

    /// Verify one ed25519 signature.
    Verify(VerifyItem),

    /// Verify many signatures; the driver may fan these across a worker
    /// pool and must answer with per-item results in the same order.
    VerifyBatch(Vec<VerifyItem>),
}

/// One signature to verify.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct VerifyItem {
    /// The claimed ed25519 verifying key.
    pub verifying_key: [u8; 32],

    /// The signed payload bytes.
    pub payload: Vec<u8>,

    /// The claimed ed25519 signature.
    pub signature: [u8; 64],
}

/// The result of one signature check.
///
/// A dedicated two-state enum rather than `bool` so completions read
/// unambiguously at call sites and across FFI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
pub enum SignatureCheck {
    /// The signature is valid for the payload and key.
    Valid,

    /// The signature is invalid.
    Invalid,
}

/// The result of a [`CryptoOp`], echoed back via
/// [`Event::CryptoDone`](crate::event::Event::CryptoDone).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum CryptoResult {
    /// The signature produced by [`CryptoOp::Sign`].
    Signed {
        /// The ed25519 signature bytes.
        signature: [u8; 64],
    },

    /// The outcome of [`CryptoOp::Verify`].
    Verified(SignatureCheck),

    /// The outcomes of [`CryptoOp::VerifyBatch`], in request order.
    BatchVerified(Vec<SignatureCheck>),
}

/// An application-facing event surfaced by the machine.
///
/// Drivers translate these into callbacks, streams, or platform-native
/// notifications; they also feed tier-1/tier-3 telemetry.
// Not `Copy`: Phase 2 adds data-carrying variants (ingested commits, heads
// updates), and removing a `Copy` impl later is a breaking change.
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AppEvent {
    /// A connection completed the handshake and is now authenticated.
    PeerAuthenticated {
        /// The authenticated connection.
        conn: ConnId,
        /// The peer's verified identity.
        peer: PeerId,
    },

    /// A connection is gone (closed by us, by the peer, or by transport
    /// failure), after any authenticated state was torn down.
    ConnectionClosed {
        /// The closed connection.
        conn: ConnId,
        /// The peer identity, if the handshake had completed.
        peer: Option<PeerId>,
    },

    /// A message for an extension protocol (not Subduction's own) arrived
    /// on an authenticated connection (ADR-010).
    ///
    /// Extension protocols (ephemeral, keyhive, application-defined)
    /// multiplex over the same connection, distinguished by their 4-byte
    /// schema prefix. The machine only gates them on authentication and
    /// passes the bytes through untouched — routing beyond that is the
    /// application's job.
    ExtensionMessage {
        /// The receiving connection.
        conn: ConnId,
        /// The authenticated peer.
        peer: PeerId,
        /// The complete extension message, schema prefix included.
        bytes: Vec<u8>,
    },
}
