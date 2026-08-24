//! Extension-protocol composition: route schema-prefixed traffic to
//! sans-io extension machines over authenticated connections.
//!
//! Extension protocols (ephemeral messages, capability sync,
//! application-defined) share the node's authenticated connections,
//! distinguished by their 4-byte schema prefix. The node surfaces
//! unknown-schema messages as
//! [`AppEvent::ExtensionMessage`];
//! this module hosts the machines those messages belong to:
//!
//! ```text
//!          ┌─ Composite ──────────────────────────────────────────┐
//!  app     │ grant(Connection) ──▶ PeerUp to every machine        │
//!  events ─▶ dispatch(AppEvent):                                  │
//!          │   ExtensionMessage ── match bytes[0..4] ──▶ machine  │
//!          │   ConnectionClosed ──▶ PeerDown + revoke capability  │
//!          │ wake(now) ──▶ machines with due deadlines            │
//!          │        machines' sends ──▶ Connection::send_extension │
//!          └──────────────────────────────────────────────────────┘
//! ```
//!
//! Authentication gating is capability-shaped, not checked: machines can
//! only reach peers through the [`Connection`]s the application
//! [`grant`](Composite::grant)ed, and a `Connection` only exists for a
//! completed handshake. Revocation on death is handled here
//! ([`dispatch`](Composite::dispatch) drops the capability on
//! `ConnectionClosed`).
//!
//! Extension machines follow the same sans-io discipline as the node:
//! pure state + queued sends, time injected, no IO. See
//! [`ExtensionMachine`].

use sedimentree_core::collections::Map;
use subduction_protocol::{
    effect::AppEvent,
    id::ConnId,
    peer_id::PeerId,
    timestamp::{Now, Timestamp},
};

use crate::driver::{connection::Connection, DriverClosed};

/// The schema-prefix length shared with the wire format.
pub const SCHEMA_PREFIX_LEN: usize = 4;

/// Hosts extension machines and routes traffic between them and the
/// node's authenticated connections. See the [module docs](self).
pub struct Composite<T> {
    machines: Vec<Box<dyn ExtensionMachine>>,
    conns: Map<ConnId, Connection<T>>,
}

impl<T> Composite<T> {
    /// An empty composite.
    #[must_use]
    pub fn new() -> Self {
        Self {
            machines: Vec::new(),
            conns: Map::new(),
        }
    }

    /// Host `machine`, routing messages bearing its schema prefix to it.
    ///
    /// Machines registered while connections are live receive `PeerUp`
    /// for each of them immediately.
    pub fn register(&mut self, mut machine: Box<dyn ExtensionMachine>) {
        for conn in self.conns.values() {
            machine.peer_up(conn.id(), conn.peer());
        }
        self.machines.push(machine);
    }

    /// Delegate an authenticated connection to the hosted machines
    /// (each sees `PeerUp`). The capability is revoked automatically
    /// when [`dispatch`](Self::dispatch) sees its `ConnectionClosed`.
    pub fn grant(&mut self, conn: Connection<T>) {
        for machine in &mut self.machines {
            machine.peer_up(conn.id(), conn.peer());
        }
        let _previous = self.conns.insert(conn.id(), conn);
    }

    /// Route one app event; returns whether it was consumed by an
    /// extension (`ExtensionMessage` with a hosted prefix). Un-consumed
    /// events (all sync/storage events, unknown prefixes) are the
    /// caller's to handle.
    ///
    /// Machines may queue sends in response; flush them by awaiting the
    /// returned future's completion — this method drains them before
    /// returning.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if a send could not reach the driver.
    // Deliberate wildcard arm: the composite consumes exactly two event
    // kinds; everything else is the caller's, including future variants.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub async fn dispatch(&mut self, now: Now, event: &AppEvent) -> Result<bool, DriverClosed> {
        let consumed = match event {
            AppEvent::ExtensionMessage { conn, peer, bytes } => {
                let Some(prefix) = bytes.get(..SCHEMA_PREFIX_LEN) else {
                    return Ok(false);
                };
                let mut consumed = false;
                for machine in &mut self.machines {
                    if machine.schema() == prefix {
                        machine.on_message(*conn, *peer, bytes);
                        consumed = true;
                    }
                }
                consumed
            }
            AppEvent::ConnectionClosed { conn, peer } => {
                if let (Some(_capability), Some(peer)) = (self.conns.remove(conn), peer) {
                    for machine in &mut self.machines {
                        machine.peer_down(*conn, *peer);
                    }
                }
                false // lifecycle events stay visible to the caller
            }
            _ => false,
        };
        self.flush(now).await?;
        Ok(consumed)
    }

    /// Deliver a wake (deadlines may be due) and flush resulting sends.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if a send could not reach the driver.
    pub async fn wake(&mut self, now: Now) -> Result<(), DriverClosed> {
        for machine in &mut self.machines {
            machine.wake(now);
        }
        self.flush(now).await
    }

    /// The earliest deadline across hosted machines.
    #[must_use]
    pub fn poll_timeout(&self) -> Option<Timestamp> {
        self.machines
            .iter()
            .filter_map(|machine| machine.poll_timeout())
            .min()
    }

    /// Drain queued sends through the connection capabilities. Sends to
    /// revoked (dead) connections are dropped — the machine already saw
    /// `PeerDown` or is about to.
    async fn flush(&mut self, _now: Now) -> Result<(), DriverClosed> {
        for machine in &mut self.machines {
            while let Some((conn, bytes)) = machine.poll_send() {
                debug_assert_eq!(
                    bytes.get(..SCHEMA_PREFIX_LEN),
                    Some(machine.schema().as_slice()),
                    "extension sends must carry the machine's own schema prefix"
                );
                if let Some(capability) = self.conns.get(&conn) {
                    capability.send_extension(bytes).await?;
                }
            }
        }
        Ok(())
    }
}

impl<T> Default for Composite<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> core::fmt::Debug for Composite<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Composite")
            .field("machines", &self.machines.len())
            .field("conns", &self.conns.len())
            .finish()
    }
}

/// One extension protocol as a sans-io machine.
///
/// Same discipline as the node: pure state, injected time, sends queued
/// and drained by the host. Machines never see pre-handshake traffic
/// and never manage connection lifecycle — `PeerUp`/`PeerDown` arrive
/// from the [`Composite`], scoped to the connections the application
/// granted it.
pub trait ExtensionMachine {
    /// The 4-byte schema prefix this machine owns. Must not collide with
    /// the sync (`SUM\0`) or handshake (`SUH\0`) schemas — those never
    /// reach extensions.
    fn schema(&self) -> [u8; SCHEMA_PREFIX_LEN];

    /// An authenticated connection is available.
    fn peer_up(&mut self, conn: ConnId, peer: PeerId);

    /// It is gone; drop all state for it.
    fn peer_down(&mut self, conn: ConnId, peer: PeerId);

    /// A complete message bearing this machine's schema prefix.
    fn on_message(&mut self, conn: ConnId, peer: PeerId, bytes: &[u8]);

    /// Deadlines may be due.
    fn wake(&mut self, now: Now);

    /// Next queued outbound message (schema prefix included).
    fn poll_send(&mut self) -> Option<(ConnId, Vec<u8>)>;

    /// The earliest deadline this machine needs a wake at.
    fn poll_timeout(&self) -> Option<Timestamp> {
        None
    }
}
