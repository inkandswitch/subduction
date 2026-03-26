//! Ephemeral message handler.
//!
//! [`EphemeralHandler`] implements the [`Handler`] trait from `subduction_core`,
//! processing [`EphemeralMessage`]s independently of the sync protocol.
//! It manages its own subscription map and fans out ephemeral payloads
//! to authorized subscribers.
//!
//! # Publish API
//!
//! The application holds `Arc<EphemeralHandler>` and calls
//! [`publish()`](EphemeralHandler::publish) directly to send ephemeral
//! messages to subscribers. Inbound messages arrive via the callback
//! channel returned from [`new()`](EphemeralHandler::new).
//!
//! [`Handler`]: subduction_core::handler::Handler
//! [`EphemeralMessage`]: crate::message::EphemeralMessage

use alloc::{sync::Arc, vec::Vec};

use async_channel::Sender;
use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable};
use nonempty::NonEmpty;
use sedimentree_core::{
    collections::{Map, Set},
    id::SedimentreeId,
};
use subduction_core::{
    authenticated::Authenticated, connection::Connection, handler::Handler, peer::id::PeerId,
};
use thiserror::Error;
use tracing::{debug, warn};

use crate::{
    config::{EphemeralConfig, EphemeralEvent},
    message::EphemeralMessage,
    policy::EphemeralPolicy,
};

/// Handler for ephemeral (non-persisted) messages.
///
/// Manages ephemeral subscriptions, performs authorization via
/// [`EphemeralPolicy`], and fans out payloads to subscribers.
///
/// Construct via [`new()`](Self::new), which returns both the handler
/// and a receiver for inbound [`EphemeralEvent`]s.
#[allow(clippy::type_complexity)]
pub struct EphemeralHandler<F: FutureForm, C: Clone + 'static, E: EphemeralPolicy<F>> {
    /// Inbound subscriptions: which peers are subscribed to receive ephemeral messages from us.
    ephemeral_subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
    /// Outbound subscriptions: sedimentree IDs we want to receive ephemeral messages for.
    outgoing_subscriptions: Arc<Mutex<Set<SedimentreeId>>>,
    connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>,
    policy: E,
    callback_tx: Sender<EphemeralEvent>,
    max_payload_size: usize,
}

impl<F: FutureForm, C: Clone + 'static, E: EphemeralPolicy<F> + Clone> Clone
    for EphemeralHandler<F, C, E>
{
    fn clone(&self) -> Self {
        Self {
            ephemeral_subscriptions: self.ephemeral_subscriptions.clone(),
            outgoing_subscriptions: self.outgoing_subscriptions.clone(),
            connections: self.connections.clone(),
            policy: self.policy.clone(),
            callback_tx: self.callback_tx.clone(),
            max_payload_size: self.max_payload_size,
        }
    }
}

impl<F: FutureForm, C: Clone + 'static, E: EphemeralPolicy<F>> core::fmt::Debug
    for EphemeralHandler<F, C, E>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("EphemeralHandler").finish_non_exhaustive()
    }
}

impl<F: FutureForm, C: Clone + 'static, E: EphemeralPolicy<F>> EphemeralHandler<F, C, E> {
    /// Create a new ephemeral handler.
    ///
    /// Returns the handler and a receiver for inbound [`EphemeralEvent`]s.
    /// The `connections` map is shared with `Subduction` / `SyncHandler`.
    #[allow(clippy::type_complexity)]
    pub fn new(
        connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>,
        policy: E,
        config: EphemeralConfig,
    ) -> (Self, async_channel::Receiver<EphemeralEvent>) {
        let (tx, rx) = async_channel::bounded(config.channel_capacity);

        let handler = Self {
            ephemeral_subscriptions: Arc::new(Mutex::new(Map::new())),
            outgoing_subscriptions: Arc::new(Mutex::new(Set::new())),
            connections,
            policy,
            callback_tx: tx,
            max_payload_size: config.max_payload_size,
        };

        (handler, rx)
    }

    /// Publish an ephemeral message to all subscribers of `id`.
    ///
    /// The message is sent to all authorized subscribers (excluding
    /// the local node). Errors on individual sends are logged but
    /// not propagated — fire-and-forget semantics.
    pub async fn publish(&self, id: SedimentreeId, payload: Vec<u8>)
    where
        C: Connection<F, EphemeralMessage>,
    {
        if payload.len() > self.max_payload_size {
            warn!(
                id = %id,
                size = payload.len(),
                max = self.max_payload_size,
                "ephemeral publish payload too large, dropping"
            );
            return;
        }

        // Peers subscribed to us (inbound) — we relay to them directly.
        let mut target_peers: Set<PeerId> = {
            let subs = self.ephemeral_subscriptions.lock().await;
            subs.get(&id)
                .map(|peers| peers.iter().copied().collect())
                .unwrap_or_default()
        };

        // If we have an outgoing subscription for this ID, also send to
        // all connected peers — they're the relays we subscribed to.
        let is_outgoing = self.outgoing_subscriptions.lock().await.contains(&id);
        if is_outgoing {
            let conns = self.connections.lock().await;
            for peer in conns.keys() {
                target_peers.insert(*peer);
            }
        }

        if target_peers.is_empty() {
            return;
        }

        let authorized_peers = self
            .policy
            .filter_authorized_subscribers(id, target_peers.into_iter().collect())
            .await;

        if authorized_peers.is_empty() {
            return;
        }

        let msg = EphemeralMessage::Ephemeral { id, payload };

        // Collect target connections while holding the lock, then drop it
        // before awaiting sends to avoid holding the mutex across .await.
        let targets: Vec<Authenticated<C, F>> = {
            let conns = self.connections.lock().await;
            authorized_peers
                .iter()
                .flat_map(|peer| {
                    conns
                        .get(peer)
                        .into_iter()
                        .flat_map(|peer_conns| peer_conns.iter().cloned())
                })
                .collect()
        };

        for conn in &targets {
            if let Err(e) = conn.send(&msg).await {
                debug!(
                    peer = %conn.peer_id(),
                    error = %e,
                    "ephemeral fan-out send failed"
                );
            }
        }
    }

    /// Subscribe to ephemeral messages for the given sedimentree IDs.
    ///
    /// Sends `Subscribe` to all connected peers and tracks the IDs so
    /// that newly connected peers (via [`subscribe_peer`](Self::subscribe_peer))
    /// also receive the subscription request.
    pub async fn subscribe(&self, ids: Vec<SedimentreeId>)
    where
        C: Connection<F, EphemeralMessage>,
    {
        if ids.is_empty() {
            return;
        }

        {
            let mut outgoing = self.outgoing_subscriptions.lock().await;
            for id in &ids {
                outgoing.insert(*id);
            }
        }

        let msg = EphemeralMessage::Subscribe { ids };
        self.send_to_all_peers(&msg).await;
    }

    /// Unsubscribe from ephemeral messages for the given sedimentree IDs.
    ///
    /// Sends `Unsubscribe` to all connected peers and removes the IDs
    /// from outgoing subscription tracking.
    pub async fn unsubscribe(&self, ids: Vec<SedimentreeId>)
    where
        C: Connection<F, EphemeralMessage>,
    {
        if ids.is_empty() {
            return;
        }

        {
            let mut outgoing = self.outgoing_subscriptions.lock().await;
            for id in &ids {
                outgoing.remove(id);
            }
        }

        let msg = EphemeralMessage::Unsubscribe { ids };
        self.send_to_all_peers(&msg).await;
    }

    /// Send current outgoing ephemeral subscriptions to a specific peer.
    ///
    /// Call this after a new peer connects so they know to send us
    /// ephemeral messages for our subscribed sedimentree IDs.
    pub async fn subscribe_peer(&self, peer_id: PeerId)
    where
        C: Connection<F, EphemeralMessage>,
    {
        let ids: Vec<SedimentreeId> = {
            let outgoing = self.outgoing_subscriptions.lock().await;
            if outgoing.is_empty() {
                return;
            }
            outgoing.iter().copied().collect()
        };

        let msg = EphemeralMessage::Subscribe { ids };

        let targets: Vec<Authenticated<C, F>> = {
            let conns = self.connections.lock().await;
            conns
                .get(&peer_id)
                .into_iter()
                .flat_map(|peer_conns| peer_conns.iter().cloned())
                .collect()
        };

        for conn in &targets {
            if let Err(e) = conn.send(&msg).await {
                debug!(
                    peer = %conn.peer_id(),
                    error = %e,
                    "ephemeral subscribe_peer send failed"
                );
            }
        }
    }

    async fn send_to_all_peers(&self, msg: &EphemeralMessage)
    where
        C: Connection<F, EphemeralMessage>,
    {
        let targets: Vec<Authenticated<C, F>> = {
            let conns = self.connections.lock().await;
            conns
                .values()
                .flat_map(|peer_conns| peer_conns.iter().cloned())
                .collect()
        };

        for conn in &targets {
            if let Err(e) = conn.send(msg).await {
                debug!(
                    peer = %conn.peer_id(),
                    error = %e,
                    "ephemeral send failed"
                );
            }
        }
    }
}

// ── Handler impl ────────────────────────────────────────────────────────

/// Errors from the ephemeral handler.
#[derive(Debug, Error)]
pub enum EphemeralHandlerError<SendErr: core::error::Error> {
    /// A send to a peer failed.
    #[error("ephemeral send failed: {0}")]
    Send(SendErr),
}

#[future_form::future_form(
    Sendable where
        C: Connection<Sendable, EphemeralMessage>
            + Clone + Send + Sync + 'static,
        E: EphemeralPolicy<Sendable> + Send + Sync,
        E::SubscribeDisallowed: Send + 'static,
        E::PublishDisallowed: Send + 'static,
        C::SendError: Send + 'static,
    Local where
        C: Connection<Local, EphemeralMessage>
            + Clone + 'static,
        E: EphemeralPolicy<Local>
)]
impl<K: FutureForm, C, E> Handler<K, C> for EphemeralHandler<K, C, E> {
    type Message = EphemeralMessage;
    type HandlerError = EphemeralHandlerError<C::SendError>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<C, K>,
        message: EphemeralMessage,
    ) -> K::Future<'a, Result<(), Self::HandlerError>> {
        K::from_future(async move { self.dispatch(conn, message).await })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> K::Future<'_, ()> {
        K::from_future(async move {
            let mut subs = self.ephemeral_subscriptions.lock().await;
            subs.retain(|_id, peers| {
                peers.remove(&peer);
                !peers.is_empty()
            });
            debug!(peer = %peer, "cleaned ephemeral subscriptions on disconnect");
        })
    }
}

impl<F: FutureForm, C: Connection<F, EphemeralMessage> + Clone + 'static, E: EphemeralPolicy<F>>
    EphemeralHandler<F, C, E>
{
    async fn dispatch(
        &self,
        conn: &Authenticated<C, F>,
        message: EphemeralMessage,
    ) -> Result<(), EphemeralHandlerError<C::SendError>> {
        match message {
            EphemeralMessage::Ephemeral { id, payload } => {
                self.recv_ephemeral(conn, id, payload).await;
            }
            EphemeralMessage::Subscribe { ids } => {
                self.recv_subscribe(conn, ids).await;
            }
            EphemeralMessage::Unsubscribe { ids } => {
                self.recv_unsubscribe(conn, ids).await;
            }
            EphemeralMessage::SubscribeRejected { .. } => {
                // Informational — nothing to do on the handler side.
                debug!("received SubscribeRejected (informational)");
            }
        }
        Ok(())
    }

    /// Handle an inbound ephemeral payload from a peer.
    async fn recv_ephemeral(
        &self,
        conn: &Authenticated<C, F>,
        id: SedimentreeId,
        payload: Vec<u8>,
    ) {
        let sender = conn.peer_id();

        if payload.len() > self.max_payload_size {
            warn!(
                peer = %sender,
                id = %id,
                size = payload.len(),
                max = self.max_payload_size,
                "ephemeral payload too large, dropping"
            );
            return;
        }

        // Check publish authorization.
        if let Err(e) = self.policy.authorize_publish(sender, id).await {
            debug!(
                peer = %sender,
                id = %id,
                error = %e,
                "ephemeral publish unauthorized"
            );
            return;
        }

        // Deliver to local callback channel.
        let event = EphemeralEvent {
            id,
            sender,
            payload: payload.clone(),
        };
        if self.callback_tx.try_send(event).is_err() {
            warn!("ephemeral callback channel full, dropping event");
        }

        // Fan-out to other subscribers.
        let subscriber_peers: Vec<PeerId> = {
            let subs = self.ephemeral_subscriptions.lock().await;
            subs.get(&id)
                .map(|peers| peers.iter().copied().filter(|p| *p != sender).collect())
                .unwrap_or_default()
        };

        if subscriber_peers.is_empty() {
            return;
        }

        let authorized_peers = self
            .policy
            .filter_authorized_subscribers(id, subscriber_peers)
            .await;

        let msg = EphemeralMessage::Ephemeral { id, payload };

        // Collect target connections while holding the lock, then drop it
        // before awaiting sends to avoid holding the mutex across .await.
        let targets: Vec<Authenticated<C, F>> = {
            let conns = self.connections.lock().await;
            authorized_peers
                .iter()
                .flat_map(|peer| {
                    conns
                        .get(peer)
                        .into_iter()
                        .flat_map(|peer_conns| peer_conns.iter().cloned())
                })
                .collect()
        };

        for conn in &targets {
            if let Err(e) = conn.send(&msg).await {
                debug!(
                    peer = %conn.peer_id(),
                    error = %e,
                    "ephemeral fan-out send failed"
                );
            }
        }
    }

    /// Handle a subscribe request from a peer.
    async fn recv_subscribe(&self, conn: &Authenticated<C, F>, ids: Vec<SedimentreeId>) {
        let peer = conn.peer_id();
        let mut rejected = Vec::new();

        for id in &ids {
            if let Err(e) = self.policy.authorize_subscribe(peer, *id).await {
                debug!(
                    peer = %peer,
                    id = %id,
                    error = %e,
                    "ephemeral subscribe rejected"
                );
                rejected.push(*id);
            } else {
                let mut subs = self.ephemeral_subscriptions.lock().await;
                subs.entry(*id).or_default().insert(peer);
            }
        }

        if !rejected.is_empty() {
            let msg = EphemeralMessage::SubscribeRejected { ids: rejected };
            if let Err(e) = conn.send(&msg).await {
                debug!(
                    peer = %peer,
                    error = %e,
                    "failed to send SubscribeRejected"
                );
            }
        }
    }

    /// Handle an unsubscribe request from a peer.
    async fn recv_unsubscribe(&self, conn: &Authenticated<C, F>, ids: Vec<SedimentreeId>) {
        let peer = conn.peer_id();
        let mut subs = self.ephemeral_subscriptions.lock().await;

        for id in &ids {
            if let Some(peers) = subs.get_mut(id) {
                peers.remove(&peer);
                if peers.is_empty() {
                    subs.remove(id);
                }
            }
        }
    }
}
