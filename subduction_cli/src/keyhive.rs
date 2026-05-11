//! Keyhive setup for the CLI.
//!
//! `Keyhive` is `!Send`, so it can't live on the multi-threaded tokio runtime
//! with the rest of the server. This module spawns a dedicated OS thread with a
//! current-thread runtime and `LocalSet` hosting all keyhive state. The main
//! runtime communicates via actor bridges (message and policy channels) defined in
//! `subduction_keyhive_policy::handler`.

use core::convert::Infallible;
use std::{io, path::Path, sync::Arc};

use async_channel::Receiver;
use async_lock::Mutex;
use future_form::{Local, Sendable};
use futures::{FutureExt, future::LocalBoxFuture};
use tokio_util::sync::CancellationToken;

use subduction_core::connection::Connection;
use subduction_keyhive::{
    KeyhiveMessage, KeyhivePeerId, KeyhiveProtocol, SignedMessage,
    connection::KeyhiveConnection,
    signed_message::CborError,
    storage::{KeyhiveStorage, StorageHash},
};
use subduction_keyhive_orchestrator::{OrchestratorConfig, SubductionKeyhiveOrchestrator};
use subduction_keyhive_policy::handler::{KeyhiveCommand, KeyhiveProtocolHandle, run_actor};

use crate::{
    handler::CliConn,
    policy::{PolicyCommand, run_policy_actor},
    transport::TransportSendError,
    wire::CliWireMessage,
};

const ARCHIVES_SUBDIR: &str = "archives";
const OPS_SUBDIR: &str = "ops";

/// Filesystem-backed [`KeyhiveStorage<Local>`].
///
/// Layout:
/// ```text
/// <root>/archives/<hex>.bin
/// <root>/ops/<hex>.bin
/// ```
#[derive(Debug, Clone)]
pub(crate) struct FsKeyhiveStorage {
    root: std::path::PathBuf,
}

/// Error type returned by [`FsKeyhiveStorage`] operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FsKeyhiveStorageError {
    /// Underlying filesystem I/O failed.
    #[error("keyhive fs storage io error: {0}")]
    Io(#[from] io::Error),
}

impl FsKeyhiveStorage {
    /// Create the storage root, its `archives/` and `ops/` subdirs.
    pub(crate) fn new(root: std::path::PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(root.join(ARCHIVES_SUBDIR))?;
        std::fs::create_dir_all(root.join(OPS_SUBDIR))?;
        Ok(Self { root })
    }

    fn archive_path(&self, hash: StorageHash) -> std::path::PathBuf {
        self.root
            .join(ARCHIVES_SUBDIR)
            .join(format!("{}.bin", hash.to_hex()))
    }

    fn event_path(&self, hash: StorageHash) -> std::path::PathBuf {
        self.root
            .join(OPS_SUBDIR)
            .join(format!("{}.bin", hash.to_hex()))
    }

    async fn save_file(path: std::path::PathBuf, data: Vec<u8>) -> io::Result<()> {
        let tmp = path.with_extension("tmp");
        tokio::fs::write(&tmp, data).await?;
        tokio::fs::rename(&tmp, &path).await
    }

    async fn load_dir(dir: std::path::PathBuf) -> io::Result<Vec<(StorageHash, Vec<u8>)>> {
        let mut out = Vec::new();
        let mut rd = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let Some(hash) = StorageHash::from_hex(stem) else {
                continue;
            };
            let bytes = tokio::fs::read(&path).await?;
            out.push((hash, bytes));
        }
        Ok(out)
    }

    async fn delete_file(path: std::path::PathBuf) -> io::Result<()> {
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl KeyhiveStorage<Local> for FsKeyhiveStorage {
    type Error = FsKeyhiveStorageError;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        let path = self.archive_path(hash);
        async move { Self::save_file(path, data).await.map_err(Into::into) }.boxed_local()
    }

    fn load_archives(
        &self,
    ) -> LocalBoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        let dir = self.root.join(ARCHIVES_SUBDIR);
        async move { Self::load_dir(dir).await.map_err(Into::into) }.boxed_local()
    }

    fn delete_archive(&self, hash: StorageHash) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        let path = self.archive_path(hash);
        async move { Self::delete_file(path).await.map_err(Into::into) }.boxed_local()
    }

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        let path = self.event_path(hash);
        async move { Self::save_file(path, data).await.map_err(Into::into) }.boxed_local()
    }

    fn load_events(&self) -> LocalBoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        let dir = self.root.join(OPS_SUBDIR);
        async move { Self::load_dir(dir).await.map_err(Into::into) }.boxed_local()
    }

    fn delete_event(&self, hash: StorageHash) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        let path = self.event_path(hash);
        async move { Self::delete_file(path).await.map_err(Into::into) }.boxed_local()
    }
}

/// Errors from [`CliConnKeyhiveAdapter::send`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum CliConnKeyhiveSendError {
    /// Serializing the [`SignedMessage`] to CBOR failed.
    #[error("serialize signed message: {0}")]
    Serialize(#[from] CborError),
    /// The underlying subduction transport failed to send.
    #[error("send via cli conn: {0}")]
    Transport(#[from] TransportSendError),
}

/// Wraps a [`CliConn`] as a [`KeyhiveConnection`], framing outbound keyhive
/// messages as [`CliWireMessage::Keyhive`].
#[derive(Debug, Clone)]
pub(crate) struct CliConnKeyhiveAdapter {
    peer_id: KeyhivePeerId,
    conn: CliConn,
}

impl CliConnKeyhiveAdapter {
    pub(crate) const fn new(peer_id: KeyhivePeerId, conn: CliConn) -> Self {
        Self { peer_id, conn }
    }
}

impl KeyhiveConnection<Local> for CliConnKeyhiveAdapter {
    type SendError = CliConnKeyhiveSendError;
    type RecvError = Infallible;
    type DisconnectError = Infallible;

    fn peer_id(&self) -> KeyhivePeerId {
        self.peer_id.clone()
    }

    fn send(&self, message: SignedMessage) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        async move {
            let msg = CliWireMessage::Keyhive(KeyhiveMessage::from_signed(&message)?);
            <CliConn as Connection<Sendable, CliWireMessage>>::send(&self.conn, &msg).await?;
            Ok(())
        }
        .boxed_local()
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<SignedMessage, Self::RecvError>> {
        futures::future::pending().boxed_local()
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectError>> {
        futures::future::ready(Ok(())).boxed_local()
    }
}

pub(crate) type CliKeyhiveHandle = KeyhiveProtocolHandle<CliConn, CliConnKeyhiveAdapter>;

pub(crate) type CliKeyhive = keyhive_core::keyhive::Keyhive<
    future_form::Local,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    Vec<u8>,
    keyhive_core::store::ciphertext::memory::MemoryCiphertextStore<Vec<u8>, Vec<u8>>,
    keyhive_core::listener::no_listener::NoListener,
    rand::rngs::OsRng,
>;

/// Spawn the dedicated keyhive thread with message and policy actors.
///
/// Awaits keyhive init, propagating errors so the caller never proceeds
/// with dead channels.
pub(crate) async fn spawn_keyhive_thread(
    msg_rx: Receiver<KeyhiveCommand<CliConn, CliConnKeyhiveAdapter>>,
    policy_rx: Receiver<PolicyCommand>,
    data_dir: &Path,
    keyhive_signer: keyhive_crypto::signer::memory::MemorySigner,
    config: OrchestratorConfig,
    cancel: CancellationToken,
) -> eyre::Result<()> {
    let keyhive_root = data_dir.join(".keyhive");
    tracing::info!("Initializing keyhive storage at {:?}", keyhive_root);
    let storage = FsKeyhiveStorage::new(keyhive_root)?;

    let (init_tx, init_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

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
                policy_rx,
                storage,
                keyhive_signer,
                config,
                cancel,
                init_tx,
            )));
        })?;

    // Wait until keyhive init completes.
    match init_rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(eyre::eyre!(e)),
        Err(_) => Err(eyre::eyre!("keyhive thread panicked during initialization")),
    }
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn run_keyhive(
    msg_rx: Receiver<KeyhiveCommand<CliConn, CliConnKeyhiveAdapter>>,
    policy_rx: Receiver<PolicyCommand>,
    storage: FsKeyhiveStorage,
    signer: keyhive_crypto::signer::memory::MemorySigner,
    config: OrchestratorConfig,
    cancel: CancellationToken,
    init_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
) {
    use keyhive_core::{
        keyhive::Keyhive, listener::no_listener::NoListener, principal::identifier::Identifier,
        store::ciphertext::memory::MemoryCiphertextStore,
    };

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

    let shared = Arc::new(Mutex::new(keyhive));
    let policy_keyhive = Arc::clone(&shared);
    let protocol =
        KeyhiveProtocol::<_, Vec<u8>, Vec<u8>, _, _, _, CliConnKeyhiveAdapter, _, Local>::new(
            shared,
            storage,
            peer_id,
            contact_card,
        );
    let orchestrator = Arc::new(SubductionKeyhiveOrchestrator::new(
        Arc::new(protocol),
        config,
    ));

    if let Err(e) = orchestrator.ingest_from_storage().await {
        tracing::warn!("keyhive ingest_from_storage failed: {e}");
    }

    // Signal init success.
    drop(init_tx.send(Ok(())));

    let process_orch = Arc::clone(&orchestrator);
    let connect_orch = Arc::clone(&orchestrator);
    let disconnect_orch = Arc::clone(&orchestrator);
    tokio::task::spawn_local(run_actor(
        msg_rx,
        move |peer, c, msg| {
            let orch = Arc::clone(&process_orch);
            async move {
                let keyhive_peer = KeyhivePeerId::from_bytes(*peer.as_bytes());
                let adapter = CliConnKeyhiveAdapter::new(keyhive_peer, c);
                orch.handle_inbound(peer, adapter, msg)
                    .await
                    .map_err(|e| e.to_string())
            }
        },
        move |peer, conn| {
            let orch = Arc::clone(&connect_orch);
            async move {
                orch.on_peer_connect(peer, conn).await;
            }
        },
        move |peer| {
            let orch = Arc::clone(&disconnect_orch);
            async move {
                orch.on_peer_disconnect(peer).await;
            }
        },
    ));

    tokio::task::spawn_local(run_policy_actor(policy_rx, policy_keyhive));

    let refresh_orch = Arc::clone(&orchestrator);
    let refresh_cancel = cancel.clone();
    let refresh_interval = refresh_orch.config().cache_refresh_interval;
    tokio::task::spawn_local(async move {
        let mut tick = tokio::time::interval(refresh_interval);
        // Discard the immediate first tick so the first refresh runs
        // one interval after startup, not at t=0.
        tick.tick().await;
        loop {
            tokio::select! {
                () = refresh_cancel.cancelled() => break,
                _ = tick.tick() => {
                    if let Err(e) = refresh_orch.refresh_cache().await {
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
