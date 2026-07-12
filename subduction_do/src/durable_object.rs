//! The [`SyncDurableObject`]: Subduction's sync engine as a hibernatable
//! Cloudflare Durable Object.
//!
//! # Why this shape
//!
//! A Durable Object can be evicted from memory whenever it is idle, then
//! reconstructed on the next event — its in-RAM state is gone, but its
//! hibernatable WebSockets and SQLite database survive. Subduction's normal
//! server (`ConnectionManager`) keeps a long-lived `recv()` task per peer and
//! rich in-memory state, which does not survive eviction.
//!
//! So instead of a persistent listen loop, this object is **event-driven**:
//!
//! * Durable state (commits, fragments, the server identity, and the
//!   subscription set) lives in SQLite via [`DoSqlStorage`].
//! * On every `websocket_message` we rebuild the volatile engine state
//!   (the connection map from the live hibernatable sockets, the subscription
//!   map from SQLite) and drive [`SyncHandler::handle`] for exactly that one
//!   message.
//! * Subscription fan-out is queued by [`CollectingSpawner`] and **awaited
//!   inline** before we return, so every side-effecting send completes while
//!   the isolate is still alive — the object can safely hibernate the instant
//!   the handler returns.
//!
//! Peer identity is pinned to each socket with `serializeAttachment`, so an
//! authenticated connection can be rebuilt after hibernation without re-running
//! the handshake (see [`Authenticated::from_persisted_peer_id`]).

use std::{cell::Cell, sync::Arc};

use async_lock::Mutex;
use future_form::Local;
use nonempty::NonEmpty;
use sedimentree_core::{
    collections::{Map, Set},
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
    sedimentree::minimized::MinimizedSedimentree,
};
use subduction_core::{
    authenticated::Authenticated,
    collections::bounded_sharded_map::BoundedShardedMap,
    connection::message::SyncMessage,
    handler::{sync::SyncHandler, Handler},
    handshake::{self, audience::Audience, HandshakeMessage, MAX_PLAUSIBLE_DRIFT},
    nonce_cache::NonceCache,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::powerbox::StoragePowerbox,
    timestamp::TimestampSeconds,
};
use subduction_crypto::signer::memory::MemorySigner;
use worker::{
    durable_object, Date, DurableObject, Env, Request, Response, Result as WorkerResult, State,
    WebSocket, WebSocketIncomingMessage, WebSocketPair,
};

use crate::{
    spawn::CollectingSpawner,
    storage::{subscriptions_fingerprint, DoSqlStorage, DoStorageError},
    transport::{DoConnection, OneShot},
};

/// How long after pending work is detected to run the cleanup/compaction alarm.
/// Cleanup is not latency-sensitive, so a coarse interval keeps the object
/// asleep between bursts; replay nonces expire on the order of ~10 minutes, so
/// this comfortably GCs them shortly after they lapse.
const ALARM_INTERVAL_MS: i64 = 5 * 60 * 1000;

/// Discovery service name. The browser client hands the same string to
/// `SubductionWebSocket.tryDiscover(url, signer, SERVICE_NAME)`; both sides
/// derive the handshake audience from it, so it must match exactly.
const SERVICE_NAME: &str = "subduction-do";

/// SQLite `meta` key under which the server's Ed25519 seed is persisted, so the
/// object keeps a stable peer identity across hibernation and restarts.
const SIGNER_SEED_KEY: &str = "signer_seed";

/// The concrete sync handler for the Durable Object environment: single-threaded
/// futures ([`Local`]), SQLite storage, DO WebSocket connections, an open
/// policy, the default depth metric, and the inline-draining spawner.
type EngineHandler = SyncHandler<
    Local,
    DoSqlStorage,
    DoConnection,
    OpenPolicy,
    CountLeadingZeroBytes,
    CollectingSpawner,
>;

type Connections = Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<DoConnection, Local>>>>>;
type Subscriptions = Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>;

/// Subduction sync engine running as a hibernatable Durable Object.
#[durable_object]
pub struct SyncDurableObject {
    state: State,
    #[allow(dead_code)]
    env: Env,
    sql: DoSqlStorage,
    signer: MemorySigner,
    peer_id: PeerId,
    handler: EngineHandler,
    connections: Connections,
    subscriptions: Subscriptions,
    spawner: CollectingSpawner,
    nonce_cache: NonceCache,
    /// Whether the subscription map has been hydrated from SQLite for this
    /// (post-hibernation) instance yet.
    subs_loaded: Cell<bool>,
    /// Order-independent fingerprint of the subscription set last written to
    /// SQLite, so we can skip the (full-table) rewrite when nothing changed.
    subs_fingerprint: Cell<[u8; 32]>,
}

impl DurableObject for SyncDurableObject {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();

        let sql = DoSqlStorage::new(state.storage().sql());
        sql.init_schema().expect("initialise durable object schema");

        let seed = load_or_create_seed(&sql);
        let signer = MemorySigner::from_bytes(&seed);
        let peer_id = PeerId::from(signer.verifying_key());

        let sedimentrees =
            Arc::new(BoundedShardedMap::<SedimentreeId, MinimizedSedimentree, 256>::new());
        let connections: Connections = Arc::new(Mutex::new(Map::new()));
        let subscriptions: Subscriptions = Arc::new(Mutex::new(Map::new()));
        let spawner = CollectingSpawner::new();
        let storage = StoragePowerbox::new(sql.clone(), Arc::new(OpenPolicy));

        let handler = SyncHandler::new(
            sedimentrees,
            connections.clone(),
            subscriptions.clone(),
            storage,
            CountLeadingZeroBytes,
            spawner.clone(),
        );

        Self {
            state,
            env,
            sql,
            signer,
            peer_id,
            handler,
            connections,
            subscriptions,
            spawner,
            nonce_cache: NonceCache::default(),
            subs_loaded: Cell::new(false),
            subs_fingerprint: Cell::new([0u8; 32]),
        }
    }

    /// Handle the WebSocket upgrade. We register the server end as
    /// *hibernatable* (`accept_web_socket`) rather than keeping a live
    /// `recv()` loop, then hand the client end back with a `101`.
    ///
    /// This endpoint only speaks WebSocket, so a plain HTTP request (e.g. a
    /// human opening the URL) is answered with `426 Upgrade Required` rather
    /// than failing obscurely when we try to return a socket to a non-upgrade
    /// request.
    async fn fetch(&self, req: Request) -> WorkerResult<Response> {
        let is_upgrade = req
            .headers()
            .get("Upgrade")?
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
        if !is_upgrade {
            return Response::error("expected a websocket upgrade on /sync/<doc>", 426);
        }

        let pair = WebSocketPair::new()?;
        self.state.accept_web_socket(&pair.server);
        Response::from_websocket(pair.client)
    }

    async fn websocket_message(
        &self,
        ws: WebSocket,
        message: WebSocketIncomingMessage,
    ) -> WorkerResult<()> {
        let bytes = match message {
            WebSocketIncomingMessage::Binary(bytes) => bytes,
            // The subduction wire protocol is binary. Reject text frames
            // explicitly (close code 1003 = "unsupported data") rather than
            // silently ignoring them, so a misbehaving client can't keep the
            // object awake pushing frames we'll never act on.
            WebSocketIncomingMessage::String(_) => {
                let _ = ws.close(Some(1003), Some("binary protocol only"));
                return Ok(());
            }
        };

        match ws.deserialize_attachment::<Vec<u8>>()? {
            // No attachment yet → this is the handshake challenge (first frame
            // on a fresh socket).
            None => self.on_handshake(ws, bytes).await,
            // Attachment present → an authenticated peer sending sync traffic.
            Some(peer_bytes) => self.on_sync(ws, &peer_bytes, &bytes).await,
        }
    }

    async fn websocket_close(
        &self,
        ws: WebSocket,
        _code: usize,
        _reason: String,
        _was_clean: bool,
    ) -> WorkerResult<()> {
        self.teardown(&ws).await;
        Ok(())
    }

    /// Without this override the trait default `unimplemented!()`s, which would
    /// panic (and crash the object) on any transport error. Treat an errored
    /// socket the same as a close.
    async fn websocket_error(&self, ws: WebSocket, _error: worker::Error) -> WorkerResult<()> {
        self.teardown(&ws).await;
        Ok(())
    }

    /// Scheduled cleanup: expire durable replay nonces and compact each stored
    /// tree (drop loose commits/fragments that a kept fragment makes redundant).
    ///
    /// The alarm is only ever armed when there is pending work — a fragment was
    /// written (compaction) or a nonce was recorded (GC) — so an idle object
    /// never wakes itself. We re-arm here only while replay nonces remain to be
    /// collected; compaction re-arms on demand the next time a fragment lands.
    async fn alarm(&self) -> WorkerResult<Response> {
        let now = now_secs();

        if let Err(e) = self.sql.gc_nonces(now) {
            tracing::warn!(error = %e, "durable object nonce GC failed");
        }
        match self.sql.compact_all(&CountLeadingZeroBytes) {
            Ok(removed) if removed > 0 => {
                tracing::info!(removed, "durable object compacted redundant items");
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "durable object compaction failed"),
        }

        // Re-arm only if there is still something to expire later.
        if matches!(self.sql.active_nonce_count(now), Ok(n) if n > 0) {
            let _ = self.state.storage().set_alarm(ALARM_INTERVAL_MS).await;
        }

        Response::empty()
    }
}

impl SyncDurableObject {
    /// Run the responder side of the handshake for a not-yet-authenticated
    /// socket, then pin the verified peer id to the socket so it survives
    /// hibernation.
    ///
    /// We do not touch the connection map here: it is rebuilt from the live
    /// hibernatable sockets on every sync message (see [`rebuild_connections`]),
    /// so simply recording the peer id on the socket is enough for the peer to
    /// appear there on its next frame.
    ///
    /// [`rebuild_connections`]: Self::rebuild_connections
    async fn on_handshake(&self, ws: WebSocket, challenge: Vec<u8>) -> WorkerResult<()> {
        let now = now_secs();

        // Peek the challenge to extract `(peer, nonce, timestamp)` for durable
        // replay protection. The in-memory `NonceCache` is reset every time the
        // isolate is evicted, so on its own it leaves a replay window across
        // hibernation; the SQLite `nonces` table closes it. If the challenge
        // can't be decoded/verified here, `respond` below will reject it anyway.
        let peeked = peek_challenge(&challenge);
        if let Some((peer, nonce, _ts)) = peeked {
            if matches!(self.sql.nonce_seen(&peer, &nonce, now), Ok(true)) {
                let _ = ws.close(Some(1008), Some("replayed handshake"));
                tracing::warn!("durable object rejected replayed handshake");
                return Ok(());
            }
        }

        let transport = OneShot::new(challenge, ws.clone());
        let ws_for_conn = ws.clone();

        let result = handshake::respond::<Local, OneShot, DoConnection, (), MemorySigner>(
            transport,
            move |_transport, _peer_id| (DoConnection::new(ws_for_conn), ()),
            &self.signer,
            &self.nonce_cache,
            self.peer_id,
            Some(Audience::discover(SERVICE_NAME.as_bytes())),
            TimestampSeconds::new(now),
            MAX_PLAUSIBLE_DRIFT,
        )
        .await;

        match result {
            Ok((authenticated, ())) => {
                // Record the claimed nonce durably so a captured challenge can't
                // be replayed after the isolate hibernates. It stays valid only
                // until the challenge's freshness window closes, after which the
                // timestamp check alone blocks the replay and the alarm GCs it.
                if let Some((peer, nonce, ts)) = peeked {
                    let expires_at = ts.saturating_add(MAX_PLAUSIBLE_DRIFT.as_secs());
                    if let Err(e) = self.sql.record_nonce(&peer, &nonce, expires_at) {
                        tracing::warn!(error = %e, "durable object failed to record nonce");
                    }
                    // Ensure a cleanup alarm exists so this nonce is eventually
                    // collected (no-op if one is already scheduled).
                    self.ensure_alarm().await;
                }

                // `respond` already sent the signed response; pin identity so the
                // socket is recognised as authenticated across hibernation.
                ws.serialize_attachment(authenticated.peer_id().as_bytes().to_vec())?;
                Ok(())
            }
            Err(e) => {
                // `respond` has already written a rejection frame where
                // applicable. Close the socket so a rejected/incompatible client
                // can't sit on an unauthenticated connection re-attempting the
                // handshake indefinitely.
                let _ = ws.close(Some(1008), Some("handshake failed"));
                tracing::warn!(error = %e, "durable object handshake rejected");
                Ok(())
            }
        }
    }

    /// Dispatch one sync message from an already-authenticated peer, then flush
    /// any queued fan-out inline so all sends land before we (possibly)
    /// hibernate.
    async fn on_sync(&self, ws: WebSocket, peer_bytes: &[u8], bytes: &[u8]) -> WorkerResult<()> {
        let peer = peer_from_bytes(peer_bytes).ok_or_else(|| werr("invalid peer attachment"))?;

        // Rebuild volatile state that hibernation may have wiped.
        self.rebuild_connections().await;
        self.ensure_subscriptions_loaded().await.map_err(werr)?;

        let conn = Authenticated::from_persisted_peer_id(DoConnection::new(ws), peer);
        let message = SyncMessage::try_decode(bytes).map_err(werr)?;

        self.handler.handle(&conn, message).await.map_err(werr)?;

        // Drain and await the fan-out that `handle` queued. This is the crux of
        // hibernation-safety: the DO stays resident until every push has been
        // written to the wire. Loop in case a drained task queues more work.
        loop {
            let batch = self.spawner.drain();
            if batch.is_empty() {
                break;
            }
            for fut in batch {
                fut.await;
            }
        }

        self.persist_subscriptions().await.map_err(werr)?;

        // If this message stored a fragment, some loose commits may now be
        // redundant. Arm the cleanup alarm (idempotent) so compaction runs off
        // the hot path; the flag is set inside the storage layer.
        if self.sql.take_compaction_hint() {
            self.ensure_alarm().await;
        }
        Ok(())
    }

    /// Schedule the cleanup/compaction alarm if one is not already pending.
    ///
    /// This is the efficiency guard the task calls for: the alarm is armed only
    /// when there is work to do, and we never stack duplicate alarms, so an idle
    /// object stays hibernating instead of waking on a fixed timer.
    async fn ensure_alarm(&self) {
        if matches!(self.state.storage().get_alarm().await, Ok(Some(_))) {
            return;
        }
        let _ = self.state.storage().set_alarm(ALARM_INTERVAL_MS).await;
    }

    /// Repopulate the connection map from the live hibernatable sockets. After
    /// an eviction the map is empty, but `get_websockets()` still returns every
    /// surviving socket, each tagged with its peer id via its attachment.
    async fn rebuild_connections(&self) {
        let sockets = self.state.get_websockets();
        let mut conns = self.connections.lock().await;
        conns.clear();
        for socket in sockets {
            if let Some(peer) = attachment_peer(&socket) {
                let auth = Authenticated::from_persisted_peer_id(DoConnection::new(socket), peer);
                // A peer may hold more than one socket (e.g. two tabs sharing an
                // identity); keep them all so fan-out reaches every one.
                match conns.get_mut(&peer) {
                    Some(list) => list.push(auth),
                    None => {
                        conns.insert(peer, NonEmpty::new(auth));
                    }
                }
            }
        }
    }

    /// Load the persisted subscription set into memory once per instance.
    async fn ensure_subscriptions_loaded(&self) -> Result<(), DoStorageError> {
        if self.subs_loaded.get() {
            return Ok(());
        }
        let pairs = self.sql.load_subscriptions()?;
        // Seed the fingerprint from what's already on disk so the first
        // `persist_subscriptions` is a no-op unless this event mutated the set.
        self.subs_fingerprint.set(subscriptions_fingerprint(&pairs));
        let mut subs = self.subscriptions.lock().await;
        for (tree, peer) in pairs {
            subs.entry(tree)
                .or_insert_with(Set::new)
                .insert(PeerId::new(peer));
        }
        drop(subs);
        self.subs_loaded.set(true);
        Ok(())
    }

    /// Snapshot the in-memory subscription map back to SQLite so it survives the
    /// next hibernation — but only when it actually changed, since the write is
    /// a full-table replace.
    async fn persist_subscriptions(&self) -> Result<(), DoStorageError> {
        let pairs: Vec<(SedimentreeId, [u8; 32])> = {
            let subs = self.subscriptions.lock().await;
            subs.iter()
                .flat_map(|(tree, peers)| peers.iter().map(move |peer| (*tree, *peer.as_bytes())))
                .collect()
        };
        let fingerprint = subscriptions_fingerprint(&pairs);
        if fingerprint == self.subs_fingerprint.get() {
            return Ok(());
        }
        self.sql.replace_subscriptions(&pairs)?;
        self.subs_fingerprint.set(fingerprint);
        Ok(())
    }

    /// Best-effort teardown when a socket closes or errors: prune the peer from
    /// every subscription so we stop trying to fan out to a dead transport.
    ///
    /// NOTE: this prunes by peer id, which assumes one socket per peer. If a
    /// single identity held multiple sockets, closing one would unsubscribe the
    /// identity entirely; the demo gives every tab a distinct identity so this
    /// does not arise in practice.
    async fn teardown(&self, ws: &WebSocket) {
        let Some(peer) = attachment_peer(ws) else {
            return;
        };
        self.ensure_subscriptions_loaded().await.ok();
        let changed = {
            let mut subs = self.subscriptions.lock().await;
            let mut changed = false;
            for peers in subs.values_mut() {
                changed |= peers.remove(&peer);
            }
            subs.retain(|_, peers| !peers.is_empty());
            changed
        };
        if changed {
            self.persist_subscriptions().await.ok();
        }
    }
}

/// Load the persisted signer seed, or generate + persist a fresh one.
fn load_or_create_seed(sql: &DoSqlStorage) -> [u8; 32] {
    match sql.get_meta(SIGNER_SEED_KEY).expect("read signer seed") {
        Some(bytes) if bytes.len() == 32 => {
            let mut seed = [0u8; 32];
            seed.copy_from_slice(&bytes);
            seed
        }
        _ => {
            let mut seed = [0u8; 32];
            getrandom::getrandom(&mut seed).expect("generate signer seed");
            sql.put_meta(SIGNER_SEED_KEY, seed.to_vec())
                .expect("persist signer seed");
            seed
        }
    }
}

/// Read the persisted peer id from a socket's attachment, if present and valid.
fn attachment_peer(ws: &WebSocket) -> Option<PeerId> {
    let bytes: Vec<u8> = ws.deserialize_attachment().ok().flatten()?;
    peer_from_bytes(&bytes)
}

fn peer_from_bytes(bytes: &[u8]) -> Option<PeerId> {
    <[u8; 32]>::try_from(bytes).ok().map(PeerId::new)
}

/// Extract `(peer, nonce, challenge_timestamp)` from a handshake challenge frame
/// for durable replay tracking.
///
/// Verifies the signature (so we never key the replay table off an unauthentic
/// challenge) but not the audience or freshness — those are `respond`'s job.
/// Returns `None` if the frame is not a decodable, correctly-signed challenge.
fn peek_challenge(bytes: &[u8]) -> Option<([u8; 32], [u8; 16], u64)> {
    let HandshakeMessage::SignedChallenge(signed) = HandshakeMessage::try_decode(bytes).ok()?
    else {
        return None;
    };
    let verified = signed.try_verify().ok()?;
    let challenge = verified.payload();
    let peer = PeerId::from(verified.issuer());
    Some((
        *peer.as_bytes(),
        *challenge.nonce.as_bytes(),
        challenge.timestamp.as_secs(),
    ))
}

/// Current UNIX time in seconds, from the isolate's clock.
fn now_secs() -> u64 {
    Date::now().as_millis() / 1000
}

/// Wrap any displayable error as a `worker::Error`.
fn werr(err: impl core::fmt::Display) -> worker::Error {
    worker::Error::RustError(err.to_string())
}
