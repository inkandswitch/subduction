//! The ambient handle: tree-local operations and app events.
//!
//! Conn-scoped authority is deliberately absent here — it lives on the
//! [`Connection`](super::connection::Connection) capability minted by
//! [`Handle::connect`]. See the [driver docs](super).

use async_channel::{Receiver, Sender};
use future_form::FutureForm;
use futures::channel::oneshot;
use sedimentree_core::{
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};
use subduction_protocol::{
    command::{Command, NewCommit, NewFragment},
    effect::AppEvent,
    event::Direction,
    handshake::audience::Audience,
    id::ConnId,
};

use super::{connection::PendingConnection, DriverClosed, Input};
use crate::transport::Transport;

/// A clonable handle for talking to a running [`Driver`](super::Driver).
///
/// Carries only _ambient_ (tree-local) authority; conn-scoped
/// operations require the [`Connection`](super::connection::Connection)
/// capability minted by [`connect`](Self::connect).
#[derive(Debug)]
pub struct Handle<T> {
    tx: Sender<Input<T>>,
    app_rx: Receiver<AppEvent>,
}

impl<T> Handle<T> {
    /// Constructed alongside its [`Driver`](super::Driver).
    pub(super) const fn new(tx: Sender<Input<T>>, app_rx: Receiver<AppEvent>) -> Self {
        Self { tx, app_rx }
    }

    /// Register `transport` as a new connection.
    ///
    /// Returns the [`PendingConnection`] (upgrade it via
    /// [`PendingConnection::authenticated`]) and the connection's
    /// read-loop future, which the caller must spawn (or otherwise poll)
    /// on its own runtime — the driver never schedules tasks. The read
    /// loop feeds inbound messages to the driver and reports transport
    /// death.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn connect<Async>(
        &self,
        transport: T,
        direction: Direction,
        audience: Option<Audience>,
    ) -> Result<
        (
            PendingConnection<T>,
            impl Future<Output = ()> + use<Async, T>,
        ),
        DriverClosed,
    >
    where
        Async: FutureForm,
        T: Transport<Async>,
    {
        let (reply, response) = oneshot::channel();
        let (auth, outcome) = oneshot::channel();
        self.tx
            .send(Input::Connect {
                transport: transport.clone(),
                direction,
                audience,
                reply,
                auth,
            })
            .await
            .map_err(|_| DriverClosed)?;
        let conn = response.await.map_err(|_| DriverClosed)?;
        let pump = read_loop::<Async, T>(transport, conn, self.tx.clone());
        Ok((PendingConnection::new(conn, self.tx.clone(), outcome), pump))
    }

    /// Author new commits locally (sealed + persisted by the driver;
    /// [`AppEvent::CommitsStored`] confirms durability).
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn add_commits(
        &self,
        tree: SedimentreeId,
        commits: Vec<NewCommit>,
    ) -> Result<(), DriverClosed> {
        self.command(Command::AddCommits { tree, commits }).await
    }

    /// Author new fragments locally — the fragment twin of
    /// [`add_commits`](Self::add_commits).
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn add_fragments(
        &self,
        tree: SedimentreeId,
        fragments: Vec<NewFragment>,
    ) -> Result<(), DriverClosed> {
        self.command(Command::AddFragments { tree, fragments })
            .await
    }

    /// Install a tree's metadata loaded from storage at startup.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn hydrate_tree(
        &self,
        tree: SedimentreeId,
        commits: Vec<LooseCommit>,
        fragments: Vec<Fragment>,
    ) -> Result<(), DriverClosed> {
        self.command(Command::HydrateTree {
            tree,
            commits,
            fragments,
        })
        .await
    }

    /// Remove a tree locally ([`AppEvent::TreeRemoved`] confirms).
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn remove_tree(&self, tree: SedimentreeId) -> Result<(), DriverClosed> {
        self.command(Command::RemoveTree { tree }).await
    }

    /// A resident tree's current heads, or `None` if the tree is unknown.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn tree_heads(
        &self,
        tree: SedimentreeId,
    ) -> Result<Option<Vec<CommitId>>, DriverClosed> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Input::TreeHeads { tree, reply })
            .await
            .map_err(|_| DriverClosed)?;
        response.await.map_err(|_| DriverClosed)
    }

    /// Receive the next application event.
    ///
    /// Events are distributed, not broadcast: with multiple handles
    /// receiving concurrently, each event goes to exactly one of them.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped and the event
    /// queue is drained.
    pub async fn next_app_event(&self) -> Result<AppEvent, DriverClosed> {
        self.app_rx.recv().await.map_err(|_| DriverClosed)
    }

    /// Stop the driver. Idempotent; pending operations are dropped.
    pub async fn shutdown(&self) {
        let _result = self.tx.send(Input::Shutdown).await;
    }

    /// Tree-local commands only: conn-bearing commands are minted by
    /// [`Connection`](super::connection::Connection), keeping connection
    /// authority capability-shaped.
    async fn command(&self, command: Command) -> Result<(), DriverClosed> {
        self.tx
            .send(Input::Command(command))
            .await
            .map_err(|_| DriverClosed)
    }
}

impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            app_rx: self.app_rx.clone(),
        }
    }
}

/// Pump one transport's inbound messages into the driver until the
/// transport or the driver goes away.
async fn read_loop<Async, T>(transport: T, conn: ConnId, tx: Sender<Input<T>>)
where
    Async: FutureForm,
    T: Transport<Async>,
{
    while let Ok(Some(bytes)) = transport.recv_bytes().await {
        if tx.send(Input::Inbound { conn, bytes }).await.is_err() {
            return;
        }
    }
    let _result = tx.send(Input::ConnClosed { conn }).await;
}
