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
use sedimentree_core::collections::{Map, Set};
use subduction_core::{
    authenticated::Authenticated, connection::Connection, handler::Handler, peer::id::PeerId,
};
use thiserror::Error;
use tracing::{debug, warn};

use crate::{
    clock::Clock,
    config::{EphemeralConfig, EphemeralEvent},
    message::EphemeralMessage,
    nonce_cache::EphemeralNonceCache,
    policy::EphemeralPolicy,
    topic::Topic,
};

/// Handler for ephemeral (non-persisted) messages.
///
/// Manages ephemeral subscriptions, performs authorization via
/// [`EphemeralPolicy`], verifies signatures on inbound messages,
/// deduplicates by nonce, and fans out payloads to subscribers.
///
/// Construct via [`new()`](Self::new), which returns both the handler
/// and a receiver for inbound [`EphemeralEvent`]s.
#[allow(clippy::type_complexity)]
pub struct EphemeralHandler<F: FutureForm, C: Clone + 'static, E: EphemeralPolicy<F>, Clk: Clock> {
    /// Inbound subscriptions: which peers are subscribed to receive ephemeral messages from us.
    ephemeral_subscriptions: Arc<Mutex<Map<Topic, Set<PeerId>>>>,
    /// Outbound subscriptions: sedimentree IDs we want to receive ephemeral messages for.
    outgoing_subscriptions: Arc<Mutex<Set<Topic>>>,
    connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>,
    policy: E,
    callback_tx: Sender<EphemeralEvent>,
    max_payload_size: usize,
    max_message_age: core::time::Duration,
    clock: Clk,
    nonce_cache: Arc<Mutex<EphemeralNonceCache>>,
}

impl<F: FutureForm, C: Clone + 'static, E: EphemeralPolicy<F> + Clone, Clk: Clock> Clone
    for EphemeralHandler<F, C, E, Clk>
{
    fn clone(&self) -> Self {
        Self {
            ephemeral_subscriptions: self.ephemeral_subscriptions.clone(),
            outgoing_subscriptions: self.outgoing_subscriptions.clone(),
            connections: self.connections.clone(),
            policy: self.policy.clone(),
            callback_tx: self.callback_tx.clone(),
            max_payload_size: self.max_payload_size,
            max_message_age: self.max_message_age,
            clock: self.clock.clone(),
            nonce_cache: self.nonce_cache.clone(),
        }
    }
}

impl<F: FutureForm, C: Clone + 'static, E: EphemeralPolicy<F>, Clk: Clock> core::fmt::Debug
    for EphemeralHandler<F, C, E, Clk>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("EphemeralHandler").finish_non_exhaustive()
    }
}

impl<F: FutureForm, C: Clone + 'static, E: EphemeralPolicy<F>, Clk: Clock>
    EphemeralHandler<F, C, E, Clk>
{
    /// Create a new ephemeral handler.
    ///
    /// Returns the handler and a receiver for inbound [`EphemeralEvent`]s.
    /// The `connections` map is shared with `Subduction` / `SyncHandler`.
    #[allow(clippy::type_complexity)]
    pub fn new(
        connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>,
        policy: E,
        config: EphemeralConfig,
        clock: Clk,
    ) -> (Self, async_channel::Receiver<EphemeralEvent>) {
        let (tx, rx) = async_channel::bounded(config.channel_capacity);

        let handler = Self {
            ephemeral_subscriptions: Arc::new(Mutex::new(Map::new())),
            outgoing_subscriptions: Arc::new(Mutex::new(Set::new())),
            connections,
            policy,
            callback_tx: tx,
            max_payload_size: config.max_payload_size,
            max_message_age: config.max_message_age,
            clock,
            nonce_cache: Arc::new(Mutex::new(EphemeralNonceCache::new(
                config.nonce_window_duration,
            ))),
        };

        (handler, rx)
    }

    /// Publish a pre-signed ephemeral message to all subscribers.
    ///
    /// The caller is responsible for constructing a signed message via
    /// [`EphemeralMessage::new_signed`]. This method checks the payload
    /// size limit, gathers subscribers, filters by policy, and fans out.
    ///
    /// Errors on individual sends are logged but not propagated —
    /// fire-and-forget semantics.
    pub async fn publish(&self, msg: EphemeralMessage)
    where
        C: Connection<F, EphemeralMessage>,
    {
        let EphemeralMessage::Ephemeral(ref signed) = msg else {
            warn!("publish called with non-Ephemeral message, ignoring");
            return;
        };
        let Ok(payload) = signed.try_decode_trusted_payload() else {
            warn!("publish called with undecodable Signed<EphemeralPayload>, ignoring");
            return;
        };
        let id = payload.id;
        let payload_len = payload.payload.len();

        let max_payload = self.max_payload_size;
        if payload_len > max_payload {
            warn!(
                id = %id,
                size = payload_len,
                max = max_payload,
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

    /// Subscribe to ephemeral messages for the given topics.
    ///
    /// Sends `Subscribe` to all connected peers and tracks the topics so
    /// that newly connected peers (via [`subscribe_peer`](Self::subscribe_peer))
    /// also receive the subscription request.
    pub async fn subscribe(&self, topics: NonEmpty<Topic>)
    where
        C: Connection<F, EphemeralMessage>,
    {
        {
            let mut outgoing = self.outgoing_subscriptions.lock().await;
            for topic in &topics {
                outgoing.insert(*topic);
            }
        }

        let msg = EphemeralMessage::Subscribe { topics };
        self.send_to_all_peers(&msg).await;
    }

    /// Unsubscribe from ephemeral messages for the given topics.
    ///
    /// Sends `Unsubscribe` to all connected peers and removes the topics
    /// from outgoing subscription tracking.
    pub async fn unsubscribe(&self, topics: NonEmpty<Topic>)
    where
        C: Connection<F, EphemeralMessage>,
    {
        {
            let mut outgoing = self.outgoing_subscriptions.lock().await;
            for topic in &topics {
                outgoing.remove(topic);
            }
        }

        let msg = EphemeralMessage::Unsubscribe { topics };
        self.send_to_all_peers(&msg).await;
    }

    /// Send current outgoing ephemeral subscriptions to a specific peer.
    ///
    /// Call this after a new peer connects so they know to send us
    /// ephemeral messages for our subscribed topics.
    pub async fn subscribe_peer(&self, peer_id: PeerId)
    where
        C: Connection<F, EphemeralMessage>,
    {
        let topics: NonEmpty<Topic> = {
            let outgoing = self.outgoing_subscriptions.lock().await;
            let topics: Vec<Topic> = outgoing.iter().copied().collect();
            let Some(topics) = NonEmpty::from_vec(topics) else {
                return;
            };
            topics
        };

        let msg = EphemeralMessage::Subscribe { topics };

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
        Clk: Clock + Send + Sync,
    Local where
        C: Connection<Local, EphemeralMessage>
            + Clone + 'static,
        E: EphemeralPolicy<Local>,
        Clk: Clock
)]
impl<K: FutureForm, C, E, Clk> Handler<K, C> for EphemeralHandler<K, C, E, Clk> {
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

            self.nonce_cache.lock().await.remove_peer(peer);

            debug!(peer = %peer, "cleaned ephemeral subscriptions and nonce cache on disconnect");
        })
    }
}

impl<
    F: FutureForm,
    C: Connection<F, EphemeralMessage> + Clone + 'static,
    E: EphemeralPolicy<F>,
    Clk: Clock,
> EphemeralHandler<F, C, E, Clk>
{
    async fn dispatch(
        &self,
        conn: &Authenticated<C, F>,
        message: EphemeralMessage,
    ) -> Result<(), EphemeralHandlerError<C::SendError>> {
        match message {
            EphemeralMessage::Ephemeral { .. } => {
                self.recv_ephemeral(conn, message).await;
            }
            EphemeralMessage::Subscribe { topics } => {
                self.recv_subscribe(conn, topics).await;
            }
            EphemeralMessage::Unsubscribe { topics } => {
                self.recv_unsubscribe(conn, topics).await;
            }
            EphemeralMessage::SubscribeRejected { .. } => {
                // Informational — nothing to do on the handler side.
                debug!("received SubscribeRejected (informational)");
            }
        }
        Ok(())
    }

    /// Handle an inbound signed ephemeral message from a peer.
    ///
    /// Verifies the signature via [`Signed::try_verify`], checks the
    /// timestamp age, checks the nonce cache for duplicates, authorizes
    /// the originator via policy, delivers to the callback channel, and
    /// fans out to other subscribers.
    ///
    /// [`Signed::try_verify`]: subduction_crypto::signed::Signed::try_verify
    #[allow(clippy::too_many_lines)]
    async fn recv_ephemeral(&self, conn: &Authenticated<C, F>, message: EphemeralMessage) {
        let EphemeralMessage::Ephemeral(ref signed) = message else {
            return;
        };

        let relay = conn.peer_id();
        let sender = PeerId::from(signed.issuer());

        // 1. Verify signature and decode payload.
        let verified = match signed.try_verify() {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    originator = %sender,
                    relay = %relay,
                    error = %e,
                    "ephemeral signature verification failed, dropping"
                );
                return;
            }
        };

        let ep = verified.payload();
        let id = ep.id;
        let nonce = ep.nonce;
        let timestamp = ep.timestamp;

        // 2. Check payload size.
        let max_payload = self.max_payload_size;
        if ep.payload.len() > max_payload {
            warn!(
                originator = %sender,
                relay = %relay,
                id = %id,
                size = ep.payload.len(),
                max = max_payload,
                "ephemeral payload too large, dropping"
            );
            return;
        }

        // 3. Check message age — reject stale or future-dated messages.
        let now = self.clock.now();
        let max_age = self.max_message_age;
        {
            let age = now.abs_diff(timestamp);
            if age > max_age {
                debug!(
                    originator = %sender,
                    relay = %relay,
                    id = %id,
                    timestamp_secs = timestamp.as_secs(),
                    now_secs = now.as_secs(),
                    age_secs = age.as_secs(),
                    max_age_secs = max_age.as_secs(),
                    "ephemeral message too old or too far in the future, dropping"
                );
                return;
            }
        }

        // 4. Check nonce (dedup).
        {
            let mut cache = self.nonce_cache.lock().await;
            if !cache.check_and_insert(sender, id, nonce, now) {
                debug!(
                    originator = %sender,
                    relay = %relay,
                    id = %id,
                    nonce = nonce,
                    "duplicate ephemeral nonce, dropping"
                );
                return;
            }
        }

        // 5. Check publish authorization (using originator, not relay).
        if let Err(e) = self.policy.authorize_publish(sender, id).await {
            debug!(
                originator = %sender,
                relay = %relay,
                id = %id,
                error = %e,
                "ephemeral publish unauthorized"
            );
            return;
        }

        // 6. Deliver to local callback channel.
        let event = EphemeralEvent {
            id,
            sender,
            nonce,
            payload: ep.payload.clone(),
        };
        if self.callback_tx.try_send(event).is_err() {
            warn!("ephemeral callback channel full, dropping event");
        }

        // 7. Fan out to other subscribers, excluding:
        //    - the relay that forwarded the message to us
        //    - the originator (they already have it — they wrote it)
        let subscriber_peers: Vec<PeerId> = {
            let subs = self.ephemeral_subscriptions.lock().await;
            subs.get(&id)
                .map(|peers| {
                    peers
                        .iter()
                        .copied()
                        .filter(|p| *p != relay && *p != sender)
                        .collect()
                })
                .unwrap_or_default()
        };

        if subscriber_peers.is_empty() {
            return;
        }

        let authorized_peers = self
            .policy
            .filter_authorized_subscribers(id, subscriber_peers)
            .await;

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

        // Forward the original signed message as-is (preserving sender + signature).
        for target_conn in &targets {
            if let Err(e) = target_conn.send(&message).await {
                debug!(
                    peer = %target_conn.peer_id(),
                    error = %e,
                    "ephemeral fan-out send failed"
                );
            }
        }
    }

    /// Handle a subscribe request from a peer.
    ///
    /// Policy checks are batched first (no lock held), then all
    /// authorized topics are inserted under a single lock acquisition.
    async fn recv_subscribe(&self, conn: &Authenticated<C, F>, topics: NonEmpty<Topic>) {
        let peer = conn.peer_id();
        let mut authorized = Vec::new();
        let mut rejected = Vec::new();

        // 1. Batch policy checks (no subscription lock held).
        for topic in &topics {
            if let Err(e) = self.policy.authorize_subscribe(peer, *topic).await {
                debug!(
                    peer = %peer,
                    topic = %topic,
                    error = %e,
                    "ephemeral subscribe rejected"
                );
                rejected.push(*topic);
            } else {
                authorized.push(*topic);
            }
        }

        // 2. Insert authorized topics under a single lock acquisition.
        if !authorized.is_empty() {
            let mut subs = self.ephemeral_subscriptions.lock().await;
            for topic in &authorized {
                subs.entry(*topic).or_default().insert(peer);
            }
        }

        if let Some(rejected) = NonEmpty::from_vec(rejected) {
            let msg = EphemeralMessage::SubscribeRejected { topics: rejected };
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
    async fn recv_unsubscribe(&self, conn: &Authenticated<C, F>, topics: NonEmpty<Topic>) {
        let peer = conn.peer_id();
        let mut subs = self.ephemeral_subscriptions.lock().await;

        for topic in &topics {
            if let Some(peers) = subs.get_mut(topic) {
                peers.remove(&peer);
                if peers.is_empty() {
                    subs.remove(topic);
                }
            }
        }
    }
}
