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
use futures::{StreamExt, stream::FuturesUnordered};
use nonempty::NonEmpty;
use sedimentree_core::collections::{Map, Set};
use subduction_core::{
    authenticated::Authenticated, connection::Connection, handler::Handler, peer::id::PeerId,
    spawn::Spawn,
};
use thiserror::Error;
use tracing::{debug, warn};

use crate::{
    clock::Clock,
    config::{EphemeralConfig, EphemeralEvent},
    message::EphemeralMessage,
    nonce_cache::EphemeralNonceCache,
    payload_header::EphemeralPayloadHeader,
    policy::EphemeralPolicy,
    topic::Topic,
};

/// Maximum unfinished fan-out sends per peer before further ephemeral
/// payloads to that peer are dropped.
///
/// Sends to a healthy peer complete as soon as the detached fan-out task is
/// polled, so the in-flight count stays near zero; only a genuinely
/// backpressured connection (e.g. a peer that stopped reading its socket)
/// accumulates toward the cap. Dropping is safe — ephemeral messages are
/// fire-and-forget by design, and a subscriber dozens of messages behind
/// has no use for stale payloads anyway.
///
/// This bounds how many detached fan-out sends can park against a wedged
/// connection between keepalive reaps. Sizing: large enough that a burst
/// dispatched before the executor polls the fan-out tasks doesn't shed
/// messages for healthy peers, small enough that a wedged peer holds at
/// most `64 × max_payload_size` (4 MiB at the 64 KiB default) in parked
/// sends.
pub const MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER: usize = 64;

/// Handler for ephemeral (non-persisted) messages.
///
/// Manages ephemeral subscriptions, performs authorization via
/// [`EphemeralPolicy`], verifies signatures on inbound messages,
/// deduplicates by nonce, and fans out payloads to subscribers.
///
/// Inbound relay fan-out runs *off* the per-peer dispatch permit: `handle`
/// spawns the sends via `Sp` so a backpressured subscriber parks a detached
/// task instead of stalling inbound dispatch (see
/// [`MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER`] for the parking bound).
///
/// Construct via [`new()`](Self::new), which returns both the handler
/// and a receiver for inbound [`EphemeralEvent`]s.
#[allow(clippy::type_complexity)]
pub struct EphemeralHandler<
    Async: FutureForm,
    Conn: Clone + 'static,
    E: EphemeralPolicy<Async>,
    Clk: Clock,
    Sp,
> {
    /// Inbound subscriptions: which peers are subscribed to receive ephemeral messages from us.
    ephemeral_subscriptions: Arc<Mutex<Map<Topic, Set<PeerId>>>>,
    /// Outbound subscriptions: sedimentree IDs we want to receive ephemeral messages for.
    outgoing_subscriptions: Arc<Mutex<Set<Topic>>>,
    connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>>,
    policy: E,
    callback_tx: Sender<EphemeralEvent>,
    max_payload_size: usize,
    max_message_age: core::time::Duration,
    clock: Clk,
    nonce_cache: Arc<Mutex<EphemeralNonceCache>>,
    spawner: Sp,
    /// Unfinished fan-out sends per peer; see
    /// [`MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER`].
    inflight_sends: Arc<Mutex<Map<PeerId, usize>>>,
}

impl<
    Async: FutureForm,
    Conn: Clone + 'static,
    E: EphemeralPolicy<Async> + Clone,
    Clk: Clock,
    Sp: Clone,
> Clone for EphemeralHandler<Async, Conn, E, Clk, Sp>
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
            spawner: self.spawner.clone(),
            inflight_sends: self.inflight_sends.clone(),
        }
    }
}

impl<Async: FutureForm, Conn: Clone + 'static, E: EphemeralPolicy<Async>, Clk: Clock, Sp>
    core::fmt::Debug for EphemeralHandler<Async, Conn, E, Clk, Sp>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("EphemeralHandler").finish_non_exhaustive()
    }
}

impl<Async: FutureForm, Conn: Clone + 'static, E: EphemeralPolicy<Async>, Clk: Clock, Sp>
    EphemeralHandler<Async, Conn, E, Clk, Sp>
{
    /// Create a new ephemeral handler.
    ///
    /// Returns the handler and a receiver for inbound [`EphemeralEvent`]s.
    /// The `connections` map is shared with `Subduction` / `SyncHandler`.
    /// `spawner` runs relay fan-out sends detached from inbound dispatch.
    #[allow(clippy::type_complexity)]
    pub fn new(
        connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>>,
        policy: E,
        config: EphemeralConfig,
        clock: Clk,
        spawner: Sp,
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
            spawner,
            inflight_sends: Arc::new(Mutex::new(Map::new())),
        };

        (handler, rx)
    }

    /// Publish a pre-signed ephemeral message to all subscribers.
    ///
    /// Checks payload size, seeds the nonce cache (so bounce-backs via
    /// gossip cycles are detected as duplicates on receive), gathers
    /// subscribers, filters by policy, and fans out.
    ///
    /// See [`design/ephemeral.md#bounce-back-amplification`] for the
    /// rationale behind the cache seed.
    ///
    /// [`design/ephemeral.md#bounce-back-amplification`]: https://github.com/inkandswitch/subduction/blob/main/design/ephemeral.md#bounce-back-amplification
    /// [`Signed::seal`]: subduction_crypto::signed::Signed::seal
    /// [`Handler::handle`]: subduction_core::handler::Handler::handle
    ///
    /// Errors on individual sends are logged but not propagated —
    /// fire-and-forget semantics.
    pub async fn publish(&self, msg: EphemeralMessage)
    where
        Conn: Connection<Async, EphemeralMessage>,
    {
        let EphemeralMessage::Ephemeral(ref signed) = msg else {
            warn!("publish called with non-Ephemeral message, ignoring");
            return;
        };
        // Decode just the header (id / nonce / timestamp / payload_len)
        // — no copy of the payload bytes, since we only need the
        // sizing info and the nonce-cache key.
        let Ok(header) = EphemeralPayloadHeader::try_decode(signed.fields_bytes()) else {
            warn!("publish called with undecodable Signed<EphemeralPayload>, ignoring");
            return;
        };
        let id = header.id;
        let nonce = header.nonce;
        let issuer = PeerId::from(signed.issuer());
        let payload_len = header.payload_len;

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

        // Seed the nonce cache so any bounce-back via gossip is treated
        // as a duplicate at recv. See design/ephemeral.md#bounce-back-amplification.
        // A `false` return here means this exact triple is already
        // present (replay of our own publish); silent no-op.
        let now = self.clock.now();
        {
            let mut cache = self.nonce_cache.lock().await;
            if !cache.check_and_insert(issuer, id, nonce, now) {
                debug!(
                    issuer = %issuer,
                    id = %id,
                    nonce = nonce,
                    "publish called with already-seen (issuer, topic, nonce); skipping fan-out"
                );
                return;
            }
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
        let targets: Vec<Authenticated<Conn, Async>> = {
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

        // Fan out concurrently so one backpressured peer can't head-of-line
        // block delivery to the others. Peers already at their in-flight cap
        // are dropped (fire-and-forget semantics) instead of parked on, so a
        // wedged subscriber can stall `publish` for at most
        // MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER sends.
        if let Some(fan_out) = self.admit_fan_out(msg, targets).await {
            fan_out.run().await;
        }
    }

    /// Subscribe to ephemeral messages for the given topics.
    ///
    /// Sends `Subscribe` to all connected peers and tracks the topics so
    /// that newly connected peers (via [`subscribe_peer`](Self::subscribe_peer))
    /// also receive the subscription request.
    pub async fn subscribe(&self, topics: NonEmpty<Topic>)
    where
        Conn: Connection<Async, EphemeralMessage>,
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
        Conn: Connection<Async, EphemeralMessage>,
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
        Conn: Connection<Async, EphemeralMessage>,
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

        let targets: Vec<Authenticated<Conn, Async>> = {
            let conns = self.connections.lock().await;
            conns
                .get(&peer_id)
                .into_iter()
                .flat_map(|peer_conns| peer_conns.iter().cloned())
                .collect()
        };

        let msg_ref = &msg;
        let mut sends: FuturesUnordered<_> = targets
            .iter()
            .map(|conn| async move { (conn.peer_id(), conn.send(msg_ref).await) })
            .collect();
        while let Some((peer, result)) = sends.next().await {
            if let Err(e) = result {
                debug!(
                    %peer,
                    error = %e,
                    "ephemeral subscribe_peer send failed"
                );
            }
        }
    }

    async fn send_to_all_peers(&self, msg: &EphemeralMessage)
    where
        Conn: Connection<Async, EphemeralMessage>,
    {
        let targets: Vec<Authenticated<Conn, Async>> = {
            let conns = self.connections.lock().await;
            conns
                .values()
                .flat_map(|peer_conns| peer_conns.iter().cloned())
                .collect()
        };

        let mut sends: FuturesUnordered<_> = targets
            .iter()
            .map(|conn| async move { (conn.peer_id(), conn.send(msg).await) })
            .collect();
        while let Some((peer, result)) = sends.next().await {
            if let Err(e) = result {
                debug!(
                    %peer,
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
        Conn: Connection<Sendable, EphemeralMessage>
            + Clone + Send + Sync + 'static,
        E: EphemeralPolicy<Sendable> + Send + Sync,
        E::SubscribeDisallowed: Send + 'static,
        E::PublishDisallowed: Send + 'static,
        Conn::SendError: Send + 'static,
        Clk: Clock + Send + Sync,
        Sp: Spawn<Sendable> + Send + Sync + 'static,
    Local where
        Conn: Connection<Local, EphemeralMessage>
            + Clone + 'static,
        E: EphemeralPolicy<Local>,
        Clk: Clock,
        Sp: Spawn<Local> + 'static
)]
impl<Async: FutureForm, Conn, E, Clk, Sp> Handler<Async, Conn>
    for EphemeralHandler<Async, Conn, E, Clk, Sp>
{
    type Message = EphemeralMessage;
    type HandlerError = EphemeralHandlerError<Conn::SendError>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<Conn, Async>,
        message: EphemeralMessage,
    ) -> Async::Future<'a, Result<(), Self::HandlerError>> {
        Async::from_future(async move {
            // The relay fan-out is spawned OFF the per-peer dispatch permit
            // so a backpressured subscriber parks a detached task instead of
            // stalling inbound dispatch for this peer (and, transitively,
            // every publisher sharing a topic with the slow subscriber).
            if let Some(fan_out) = self.dispatch(conn, message).await? {
                self.spawner.spawn(Async::from_future(fan_out.run()));
            }
            Ok(())
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> Async::Future<'_, ()> {
        Async::from_future(async move {
            let mut subs = self.ephemeral_subscriptions.lock().await;
            subs.retain(|_id, peers| {
                peers.remove(&peer);
                !peers.is_empty()
            });

            self.nonce_cache.lock().await.remove_peer(peer);
            self.inflight_sends.lock().await.remove(&peer);

            debug!(peer = %peer, "cleaned ephemeral subscriptions and nonce cache on disconnect");
        })
    }
}

impl<
    Async: FutureForm,
    Conn: Connection<Async, EphemeralMessage> + Clone + 'static,
    E: EphemeralPolicy<Async>,
    Clk: Clock,
    Sp,
> EphemeralHandler<Async, Conn, E, Clk, Sp>
{
    async fn dispatch(
        &self,
        conn: &Authenticated<Conn, Async>,
        message: EphemeralMessage,
    ) -> Result<Option<EphemeralFanOut<Conn, Async>>, EphemeralHandlerError<Conn::SendError>> {
        match message {
            EphemeralMessage::Ephemeral { .. } => {
                return Ok(self.recv_ephemeral(conn, message).await);
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
        Ok(None)
    }

    /// Admit `targets` against the per-peer in-flight cap, incrementing the
    /// counter for each admitted connection.
    ///
    /// Connections whose peer is already at
    /// [`MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER`] are dropped from the
    /// fan-out — fire-and-forget semantics make dropping safe, and it stops
    /// a wedged connection from accumulating parked send tasks. Returns
    /// `None` when nothing was admitted.
    async fn admit_fan_out(
        &self,
        message: EphemeralMessage,
        targets: Vec<Authenticated<Conn, Async>>,
    ) -> Option<EphemeralFanOut<Conn, Async>> {
        let admitted: Vec<Authenticated<Conn, Async>> = {
            let mut inflight = self.inflight_sends.lock().await;
            targets
                .into_iter()
                .filter(|conn| {
                    let peer = conn.peer_id();
                    let n = inflight.get(&peer).copied().unwrap_or(0);
                    if n >= MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER {
                        debug!(
                            peer = %peer,
                            inflight = n,
                            "ephemeral send dropped: peer at in-flight cap"
                        );
                        false
                    } else {
                        inflight.insert(peer, n + 1);
                        true
                    }
                })
                .collect()
        };

        if admitted.is_empty() {
            return None;
        }

        Some(EphemeralFanOut {
            message,
            targets: admitted,
            inflight_sends: Arc::clone(&self.inflight_sends),
        })
    }

    /// Handle an inbound signed ephemeral message from a peer.
    ///
    /// Step order: decode-unverified → size → age → cache `contains`
    /// (read-only) → verify → cache `check_and_insert` → authorise →
    /// deliver → fan out. The cache probe before verify is the
    /// cross-edge fast path; the post-verify `check_and_insert` is the
    /// only place cache state is written and is gated behind a
    /// successful signature check.
    ///
    /// See [`design/ephemeral.md`] for the full rationale and threat
    /// model.
    ///
    /// [`design/ephemeral.md`]: https://github.com/inkandswitch/subduction/blob/main/design/ephemeral.md#recv-ephemeralhandlerrecv_ephemeral
    #[allow(clippy::too_many_lines)]
    async fn recv_ephemeral(
        &self,
        conn: &Authenticated<Conn, Async>,
        message: EphemeralMessage,
    ) -> Option<EphemeralFanOut<Conn, Async>> {
        let EphemeralMessage::Ephemeral(ref signed) = message else {
            return None;
        };

        let relay = conn.peer_id();
        let sender = PeerId::from(signed.issuer());

        // 1. Decode the header fields (id / nonce / timestamp /
        //    payload_len) without verifying and without copying the
        //    payload bytes. These values are UNTRUSTED until step 3
        //    succeeds — used only for read-only checks below. The full
        //    payload is materialised once, post-verify, via
        //    `try_verify`.
        let header = match EphemeralPayloadHeader::try_decode(signed.fields_bytes()) {
            Ok(h) => h,
            Err(e) => {
                warn!(
                    relay = %relay,
                    error = %e,
                    "ephemeral payload undecodable, dropping"
                );
                return None;
            }
        };

        let untrusted_id = header.id;
        let untrusted_nonce = header.nonce;
        let untrusted_timestamp = header.timestamp;
        let payload_len = header.payload_len;

        // 2a. Payload size (signature-independent).
        let max_payload = self.max_payload_size;
        if payload_len > max_payload {
            warn!(
                originator = %sender,
                relay = %relay,
                id = %untrusted_id,
                size = payload_len,
                max = max_payload,
                "ephemeral payload too large, dropping"
            );
            return None;
        }

        // 2b. Message age (signature-independent).
        let now = self.clock.now();
        let max_age = self.max_message_age;
        {
            let age = now.abs_diff(untrusted_timestamp);
            if age > max_age {
                debug!(
                    originator = %sender,
                    relay = %relay,
                    id = %untrusted_id,
                    timestamp_secs = untrusted_timestamp.as_secs(),
                    now_secs = now.as_secs(),
                    age_secs = age.as_secs(),
                    max_age_secs = max_age.as_secs(),
                    "ephemeral message too old or too far in the future, dropping"
                );
                return None;
            }
        }

        // 2c. Read-only cache probe. Hit ⇒ drop before paying for Ed25519 verify.
        {
            let cache = self.nonce_cache.lock().await;
            if cache.contains(sender, untrusted_id, untrusted_nonce) {
                debug!(
                    originator = %sender,
                    relay = %relay,
                    id = %untrusted_id,
                    nonce = untrusted_nonce,
                    "duplicate ephemeral nonce (pre-verify fast path), dropping"
                );
                return None;
            }
        }

        // 3. Verify signature. `sender` is trusted from here on.
        let verified = match signed.try_verify() {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    originator = %sender,
                    relay = %relay,
                    error = %e,
                    "ephemeral signature verification failed, dropping"
                );
                return None;
            }
        };

        let ep = verified.payload();
        let id = ep.id;
        let nonce = ep.nonce;

        // 4. Post-verify insert. The only place cache state is written.
        //    `false` here means a concurrent duplicate raced past 2c
        //    and inserted first; drop.
        {
            let mut cache = self.nonce_cache.lock().await;
            if !cache.check_and_insert(sender, id, nonce, now) {
                debug!(
                    originator = %sender,
                    relay = %relay,
                    id = %id,
                    nonce = nonce,
                    "duplicate ephemeral nonce (post-verify race), dropping"
                );
                return None;
            }
        }

        // 5. Check publish authorization (using verified originator).
        if let Err(e) = self.policy.authorize_publish(sender, id).await {
            debug!(
                originator = %sender,
                relay = %relay,
                id = %id,
                error = %e,
                "ephemeral publish unauthorized"
            );
            return None;
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
            return None;
        }

        let authorized_peers = self
            .policy
            .filter_authorized_subscribers(id, subscriber_peers)
            .await;

        // Collect target connections while holding the lock, then drop it
        // before awaiting sends to avoid holding the mutex across .await.
        let targets: Vec<Authenticated<Conn, Async>> = {
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

        // Forward the original signed message as-is (preserving sender +
        // signature). The returned fan-out is spawned by `handle` OFF this
        // peer's dispatch permit; peers at their in-flight cap were dropped
        // by `admit_fan_out`.
        self.admit_fan_out(message, targets).await
    }

    /// Handle a subscribe request from a peer.
    ///
    /// Policy checks are batched first (no lock held), then all
    /// authorized topics are inserted under a single lock acquisition.
    async fn recv_subscribe(&self, conn: &Authenticated<Conn, Async>, topics: NonEmpty<Topic>) {
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
    async fn recv_unsubscribe(&self, conn: &Authenticated<Conn, Async>, topics: NonEmpty<Topic>) {
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

/// Deferred fan-out of an ephemeral payload to subscriber connections.
///
/// Built on the dispatch path but **run off it**: [`EphemeralHandler`]'s
/// `handle` spawns [`run`](Self::run) via the handler's spawner, so a
/// backpressured subscriber parks a detached task instead of holding the
/// originating peer's dispatch permit. `publish` runs it inline (the
/// application controls its own concurrency there).
///
/// Each target was admitted against the per-peer in-flight cap
/// ([`MAX_INFLIGHT_EPHEMERAL_SENDS_PER_PEER`]) with its counter already
/// incremented; [`run`](Self::run) decrements as each send completes.
struct EphemeralFanOut<Conn: Clone + 'static, Async: FutureForm> {
    message: EphemeralMessage,
    targets: Vec<Authenticated<Conn, Async>>,
    inflight_sends: Arc<Mutex<Map<PeerId, usize>>>,
}

impl<Conn, Async> EphemeralFanOut<Conn, Async>
where
    Conn: Connection<Async, EphemeralMessage> + Clone + 'static,
    Async: FutureForm,
{
    /// Send the payload to every admitted target concurrently.
    ///
    /// Sends run via `FuturesUnordered` so one slow target can't
    /// head-of-line block the others. Failures are logged and dropped
    /// (fire-and-forget). Per-peer in-flight counters are decremented as
    /// sends complete, re-admitting the peer for future fan-outs.
    async fn run(self) {
        let msg = &self.message;
        let mut sends: FuturesUnordered<_> = self
            .targets
            .iter()
            .map(|conn| async move { (conn.peer_id(), conn.send(msg).await) })
            .collect();

        while let Some((peer, result)) = sends.next().await {
            if let Err(e) = result {
                debug!(
                    %peer,
                    error = %e,
                    "ephemeral fan-out send failed"
                );
            }

            let mut inflight = self.inflight_sends.lock().await;
            if let Some(n) = inflight.get_mut(&peer) {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    inflight.remove(&peer);
                }
            }
        }
    }
}
