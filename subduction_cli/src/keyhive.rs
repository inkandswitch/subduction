//! Keyhive setup for the CLI.
//!
//! CLI-specific types (filesystem storage, connection adapter) and the
//! thin `spawn_keyhive_thread` wrapper that delegates to
//! `subduction_keyhive::runtime`.

use core::convert::Infallible;
use std::{io, path::Path};

use future_form::{Local, Sendable};
use futures::{FutureExt, future::LocalBoxFuture};
use tokio_util::sync::CancellationToken;

use subduction_core::connection::Connection;
use subduction_keyhive::{
    KeyhiveMessage, KeyhivePeerId, SignedMessage,
    connection::KeyhiveConnection,
    handler::{KeyhiveCommand, KeyhiveProtocolHandle},
    runtime::RuntimeConfig,
    signed_message::CborError,
    storage::{KeyhiveStorage, StorageHash},
};

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

/// Spawn the dedicated keyhive thread with message and policy actors.
pub(crate) async fn spawn_keyhive_thread(
    msg_rx: async_channel::Receiver<KeyhiveCommand<CliConn, CliConnKeyhiveAdapter>>,
    policy_rx: async_channel::Receiver<PolicyCommand>,
    data_dir: &Path,
    keyhive_signer: keyhive_crypto::signer::memory::MemorySigner,
    cancel: CancellationToken,
) -> eyre::Result<()> {
    let keyhive_root = data_dir.join(".keyhive");
    tracing::info!("Initializing keyhive storage at {:?}", keyhive_root);
    let storage = FsKeyhiveStorage::new(keyhive_root)?;

    subduction_keyhive::runtime::spawn_keyhive_thread(
        msg_rx,
        storage,
        keyhive_signer,
        RuntimeConfig::default(),
        CliConnKeyhiveAdapter::new,
        move |keyhive| {
            tokio::task::spawn_local(run_policy_actor(policy_rx, keyhive));
        },
        cancel,
    )
    .await
    .map_err(|e| eyre::eyre!(e))
}
