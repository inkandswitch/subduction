//! The connection capability: conn-scoped authority, minted by a
//! completed handshake.
//!
//! See the [driver docs](super) for the authority split between the
//! ambient [`Handle`](super::handle::Handle) and these capabilities.

use async_channel::Sender;
use futures::channel::oneshot;
use sedimentree_core::id::SedimentreeId;
use subduction_protocol::{command::Command, id::ConnId, peer_id::PeerId};
use thiserror::Error;

use super::{AuthOutcome, DriverClosed, Input};

/// An authenticated-connection capability: the witness that a handshake
/// completed, and the only source of conn-scoped operations.
///
/// Unforgeable — minted solely by
/// [`PendingConnection::authenticated`] — and self-routing: it carries
/// its own channel to the driver that created it, so "wrong driver"
/// misuse is unrepresentable rather than merely tolerated. Cloning is
/// delegation: handing a clone to a subsystem grants it this connection.
#[derive(Debug)]
pub struct Connection<T> {
    id: ConnId,
    peer: PeerId,
    tx: Sender<Input<T>>,
}

impl<T> Connection<T> {
    /// The verified identity of the peer on the far end (pinned at
    /// handshake time).
    #[must_use]
    pub const fn peer(&self) -> PeerId {
        self.peer
    }

    /// The raw connection id — for correlating with
    /// [`AppEvent`](subduction_protocol::effect::AppEvent)s and
    /// telemetry, which speak the plain FFI vocabulary. Deliberately
    /// one-way: there is no `from_raw`.
    #[must_use]
    pub const fn id(&self) -> ConnId {
        self.id
    }

    /// Batch-sync `tree` with this peer. Concludes with
    /// [`SyncFinished`](subduction_protocol::effect::AppEvent::SyncFinished);
    /// incoming data additionally surfaces as
    /// [`TreeUpdated`](subduction_protocol::effect::AppEvent::TreeUpdated).
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn sync_tree(
        &self,
        tree: SedimentreeId,
        subscribe: bool,
    ) -> Result<(), DriverClosed> {
        self.command(Command::SyncTree {
            conn: self.id,
            tree,
            subscribe,
        })
        .await
    }

    /// Stop receiving pushes for `trees` from this peer.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn unsubscribe(&self, trees: Vec<SedimentreeId>) -> Result<(), DriverClosed> {
        self.command(Command::Unsubscribe {
            conn: self.id,
            trees,
        })
        .await
    }

    /// Send one extension-protocol message (schema prefix included).
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn send_extension(&self, bytes: Vec<u8>) -> Result<(), DriverClosed> {
        self.command(Command::SendExtension {
            conn: self.id,
            bytes,
        })
        .await
    }

    /// Close this connection. Consumes the capability: no operations on
    /// a connection you asked to tear down.
    pub async fn disconnect(self) {
        let _result = self.tx.send(Input::Disconnect { conn: self.id }).await;
    }

    /// Conn-bearing commands are built here and only here.
    async fn command(&self, command: Command) -> Result<(), DriverClosed> {
        self.tx
            .send(Input::Command(command))
            .await
            .map_err(|_| DriverClosed)
    }
}

impl<T> Clone for Connection<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            peer: self.peer,
            tx: self.tx.clone(),
        }
    }
}

/// A connection whose handshake is still in flight.
///
/// The only way to reach conn-scoped operations is
/// [`authenticated`](Self::authenticated): sync and extension traffic on
/// an unauthenticated connection is unrepresentable at this surface.
#[derive(Debug)]
pub struct PendingConnection<T> {
    id: ConnId,
    tx: Sender<Input<T>>,
    outcome: oneshot::Receiver<AuthOutcome>,
}

impl<T> PendingConnection<T> {
    /// Minted by [`Handle::connect`](super::handle::Handle::connect).
    pub(super) const fn new(
        id: ConnId,
        tx: Sender<Input<T>>,
        outcome: oneshot::Receiver<AuthOutcome>,
    ) -> Self {
        Self { id, tx, outcome }
    }

    /// Wait for the handshake to conclude, upgrading to an
    /// authenticated [`Connection`] capability.
    ///
    /// # Errors
    ///
    /// [`ConnectError::Closed`] if the connection died first;
    /// [`ConnectError::Driver`] if the driver stopped.
    pub async fn authenticated(self) -> Result<Connection<T>, ConnectError> {
        match self.outcome.await {
            Ok(AuthOutcome::Authenticated { peer }) => Ok(Connection {
                id: self.id,
                peer,
                tx: self.tx,
            }),
            Ok(AuthOutcome::Closed) => Err(ConnectError::Closed),
            Err(_canceled) => Err(ConnectError::Driver(DriverClosed)),
        }
    }
}

/// The handshake did not produce an authenticated connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ConnectError {
    /// The connection closed (transport death, handshake fault, or
    /// timeout) before authenticating.
    #[error("connection closed before authenticating")]
    Closed,

    /// The driver was shut down.
    #[error(transparent)]
    Driver(#[from] DriverClosed),
}
