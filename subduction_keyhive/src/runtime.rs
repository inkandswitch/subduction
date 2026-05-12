//! Keyhive thread runtime.
//!
//! Spawns a dedicated OS thread with a single-threaded tokio runtime and
//! `LocalSet` to host the `!Send` keyhive. Returns after initialization
//! completes so the caller can proceed with the protocol handle.

use alloc::{format, string::ToString, sync::Arc, vec::Vec};
use core::time::Duration;

use async_lock::Mutex as AsyncMutex;
use keyhive_core::{
    keyhive::Keyhive, listener::no_listener::NoListener, principal::identifier::Identifier,
    store::ciphertext::memory::MemoryCiphertextStore,
};
use keyhive_crypto::signer::memory::MemorySigner;
use tokio_util::sync::CancellationToken;

use crate::{
    KeyhivePeerId, KeyhiveProtocol,
    connection::KeyhiveConnection,
    handler::{KeyhiveCommand, run_actor},
    storage::KeyhiveStorage,
};

/// Concrete keyhive type used by the runtime.
pub type RuntimeKeyhive = Keyhive<
    future_form::Local,
    MemorySigner,
    Vec<u8>,
    Vec<u8>,
    MemoryCiphertextStore<Vec<u8>, Vec<u8>>,
    NoListener,
    rand::rngs::OsRng,
>;

/// Configuration for the keyhive runtime.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeConfig {
    /// Interval between periodic cache refreshes.
    pub cache_refresh_interval: Duration,
    // /// Minimum interval between outbound sync requests to the same peer.
    // ///
    // TODO: Implement
    // pub min_sync_request_interval: Duration,

    // /// Minimum interval between outbound sync responses to the same peer.
    // ///
    // TODO: Implement
    // pub min_sync_response_interval: Duration,

    // /// Optional batching interval for inbound messages.
    // ///
    // TODO: Implement
    // pub batch_interval: Option<Duration>,

    // /// Whether to attempt storage recovery when events remain pending
    // /// after ingestion.
    // ///
    // TODO: Implement
    // pub attempt_storage_recovery: bool,

    // /// Whether to suppress individual event writes during bulk ingest.
    // ///
    // TODO: Implement
    // pub suppress_writes_during_ingest: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            cache_refresh_interval: Duration::from_secs(2),
            // min_sync_request_interval: Duration::from_secs(1),
            // min_sync_response_interval: Duration::from_secs(1),
            // batch_interval: None,
            // attempt_storage_recovery: true,
            // suppress_writes_during_ingest: false,
        }
    }
}

/// Spawn the dedicated keyhive thread.
///
/// Creates a keyhive instance, protocol, message actor, cache refresh
/// loop, and calls `policy_setup` so the caller can wire policy actors
/// on the same `LocalSet`.
///
/// Awaits keyhive initialization and propagates errors so the caller
/// never proceeds with dead channels.
///
/// # Type parameters
///
/// - `C`: Handler connection payload (forwarded through `KeyhiveCommand`)
/// - `Conn`: Keyhive connection type (wraps the transport)
/// - `Store`: Keyhive storage backend
/// - `PolicySetup`: Closure called on the `LocalSet` after keyhive
///   init, receiving the shared keyhive mutex for policy actors
///
/// # Errors
///
/// Returns an error if keyhive generation fails or the thread panics
/// during initialization.
pub async fn spawn_keyhive_thread<C, Conn, Store, ConnAdapter, PolicySetup>(
    msg_rx: async_channel::Receiver<KeyhiveCommand<C, Conn>>,
    storage: Store,
    signer: MemorySigner,
    config: RuntimeConfig,
    conn_adapter: ConnAdapter,
    policy_setup: PolicySetup,
    cancel: CancellationToken,
) -> Result<(), std::string::String>
where
    C: Send + 'static,
    Conn: KeyhiveConnection<future_form::Local> + Send + 'static,
    Conn::SendError: 'static,
    Conn::DisconnectError: 'static,
    Store: KeyhiveStorage<future_form::Local> + Send + 'static,
    ConnAdapter: Fn(KeyhivePeerId, C) -> Conn + Send + 'static,
    PolicySetup: FnOnce(Arc<AsyncMutex<RuntimeKeyhive>>) + Send + 'static,
{
    let (init_tx, init_rx) = tokio::sync::oneshot::channel::<Result<(), std::string::String>>();

    std::thread::Builder::new()
        .name("keyhive".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let msg = format!("failed to build keyhive runtime: {e}");
                    tracing::error!("{msg}");
                    drop(init_tx.send(Err(msg)));
                    return;
                }
            };
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(run_keyhive(
                msg_rx,
                storage,
                signer,
                config,
                conn_adapter,
                policy_setup,
                cancel,
                init_tx,
            )));
        })
        .map_err(|e| format!("failed to spawn keyhive thread: {e}"))?;

    match init_rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err("keyhive thread panicked during initialization".into()),
    }
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn run_keyhive<C, Conn, Store, ConnAdapter, PolicySetup>(
    msg_rx: async_channel::Receiver<KeyhiveCommand<C, Conn>>,
    storage: Store,
    signer: MemorySigner,
    config: RuntimeConfig,
    conn_adapter: ConnAdapter,
    policy_setup: PolicySetup,
    cancel: CancellationToken,
    init_tx: tokio::sync::oneshot::Sender<Result<(), std::string::String>>,
) where
    C: 'static,
    Conn: KeyhiveConnection<future_form::Local> + 'static,
    Conn::SendError: 'static,
    Conn::DisconnectError: 'static,
    Store: KeyhiveStorage<future_form::Local> + 'static,
    ConnAdapter: Fn(KeyhivePeerId, C) -> Conn + 'static,
    PolicySetup: FnOnce(Arc<AsyncMutex<RuntimeKeyhive>>),
{
    let keyhive = match Keyhive::generate(
        signer,
        MemoryCiphertextStore::<Vec<u8>, Vec<u8>>::new(),
        NoListener,
        rand::rngs::OsRng,
    )
    .await
    {
        Ok(kh) => kh,
        Err(e) => {
            let msg = format!("failed to generate keyhive: {e}");
            tracing::error!("{msg}");
            drop(init_tx.send(Err(msg)));
            return;
        }
    };

    let contact_card = match keyhive.contact_card().await {
        Ok(cc) => cc,
        Err(e) => {
            let msg = format!("failed to generate keyhive contact card: {e}");
            tracing::error!("{msg}");
            drop(init_tx.send(Err(msg)));
            return;
        }
    };

    let kh_id: Identifier = keyhive.id().into();
    let peer_id = KeyhivePeerId::from_bytes(kh_id.to_bytes());

    let shared = Arc::new(AsyncMutex::new(keyhive));
    let policy_keyhive = Arc::clone(&shared);
    let protocol = Arc::new(KeyhiveProtocol::<
        _,
        Vec<u8>,
        Vec<u8>,
        _,
        _,
        _,
        Conn,
        Store,
        future_form::Local,
    >::new(shared, storage, peer_id, contact_card));

    if let Err(e) = protocol.ingest_from_storage().await {
        tracing::warn!("keyhive ingest_from_storage failed: {e}");
    }

    drop(init_tx.send(Ok(())));

    let process_proto = Arc::clone(&protocol);
    let connect_proto = Arc::clone(&protocol);
    let disconnect_proto = Arc::clone(&protocol);
    tokio::task::spawn_local(run_actor(
        msg_rx,
        move |peer, c, msg| {
            let proto = Arc::clone(&process_proto);
            let conn = conn_adapter(KeyhivePeerId::from_bytes(*peer.as_bytes()), c);
            async move {
                let keyhive_peer = conn.peer_id();
                proto
                    .handle_message(&keyhive_peer, msg, Some(conn))
                    .await
                    .map_err(|e| e.to_string())
            }
        },
        move |_peer, conn| {
            let proto = Arc::clone(&connect_proto);
            async move {
                let peer_id = conn.peer_id();
                proto.add_peer(peer_id, conn).await;
            }
        },
        move |peer| {
            let proto = Arc::clone(&disconnect_proto);
            async move {
                let keyhive_peer = KeyhivePeerId::from_bytes(*peer.as_bytes());
                proto.remove_peer(&keyhive_peer).await;
            }
        },
    ));

    policy_setup(policy_keyhive);

    let refresh_proto = Arc::clone(&protocol);
    let refresh_cancel = cancel.clone();
    let refresh_interval = config.cache_refresh_interval;
    tokio::task::spawn_local(async move {
        let mut tick = tokio::time::interval(refresh_interval);
        tick.tick().await;
        loop {
            tokio::select! {
                () = refresh_cancel.cancelled() => break,
                _ = tick.tick() => {
                    if let Err(e) = refresh_proto.refresh_cache().await {
                        tracing::warn!(error = %e, "refresh_cache failed");
                    }
                }
            }
        }
        tracing::debug!("keyhive cache refresh task shutting down");
    });

    tracing::info!("keyhive actor + policy actor + cache refresh started");

    cancel.cancelled().await;
    tracing::debug!("keyhive actors shutting down");
}
