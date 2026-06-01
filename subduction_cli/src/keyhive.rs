//! Keyhive setup for the CLI.
//!
//! CLI-specific types: filesystem storage ([`FsKeyhiveStorage`]) and
//! connection adapter ([`CliConnKeyhiveAdapter`]).

use core::{
    convert::Infallible,
    sync::atomic::{AtomicU64, Ordering},
};
use std::{io, path::PathBuf};

use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};

use subduction_core::{authenticated::Authenticated, connection::Connection};
use subduction_keyhive::{
    KeyhiveMessage, KeyhivePeerId, SignedMessage,
    connection::KeyhiveConnection,
    signed_message::CborError,
    storage::{KeyhiveStorage, StorageHash},
};

use crate::{handler::CliConn, transport::TransportSendError, wire::CliWireMessage};

const ARCHIVES_SUBDIR: &str = "archives";
const OPS_SUBDIR: &str = "ops";
const TMP_SUBDIR: &str = "tmp";

/// Monotonic per-process counter for temp filenames. A unique temp path per
/// save lets concurrent saves of the same hash proceed without sharing a
/// temp file.
static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(0);

/// Filesystem-backed [`KeyhiveStorage`].
///
/// Layout:
/// ```text
/// <root>/archives/<hex>.bin
/// <root>/ops/<hex>.bin
/// ```
#[derive(Debug, Clone)]
pub(crate) struct FsKeyhiveStorage {
    root: PathBuf,
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
    pub(crate) fn new(root: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(root.join(ARCHIVES_SUBDIR))?;
        std::fs::create_dir_all(root.join(OPS_SUBDIR))?;
        std::fs::create_dir_all(root.join(TMP_SUBDIR))?;
        Ok(Self { root })
    }

    fn archive_dir(&self) -> PathBuf {
        self.root.join(ARCHIVES_SUBDIR)
    }

    fn event_dir(&self) -> PathBuf {
        self.root.join(OPS_SUBDIR)
    }

    fn tmp_dir(&self) -> PathBuf {
        self.root.join(TMP_SUBDIR)
    }

    async fn save_file(
        &self,
        parent_dir: PathBuf,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> io::Result<()> {
        let filename = format!("{}.bin", hash.to_hex());
        let dest = parent_dir.join(&filename);

        let tmp_id = NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed);
        let tmp = self.tmp_dir().join(format!(
            "{}.{}.{tmp_id}.tmp",
            hash.to_hex(),
            std::process::id()
        ));

        tokio::fs::write(&tmp, data).await?;
        match tokio::fs::rename(&tmp, &dest).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Best-effort cleanup of the temp file after a failed rename.
                let _ = tokio::fs::remove_file(&tmp).await;
                // Content is hash-addressed. If `dest` already exists, an
                // equivalent save already stored it, so treat this as success.
                if tokio::fs::try_exists(&dest).await.unwrap_or(false) {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn load_dir(dir: PathBuf) -> io::Result<Vec<(StorageHash, Vec<u8>)>> {
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

    async fn delete_file(parent_dir: PathBuf, hash: StorageHash) -> io::Result<()> {
        let path = parent_dir.join(format!("{}.bin", hash.to_hex()));
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl KeyhiveStorage<Sendable> for FsKeyhiveStorage {
    type Error = FsKeyhiveStorageError;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        let parent_dir = self.archive_dir();
        async move {
            self.save_file(parent_dir, hash, data)
                .await
                .map_err(Into::into)
        }
        .boxed()
    }

    fn load_archives(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        let dir = self.archive_dir();
        async move { Self::load_dir(dir).await.map_err(Into::into) }.boxed()
    }

    fn delete_archive(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        let dir = self.archive_dir();
        async move { Self::delete_file(dir, hash).await.map_err(Into::into) }.boxed()
    }

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        let parent_dir = self.event_dir();
        async move {
            self.save_file(parent_dir, hash, data)
                .await
                .map_err(Into::into)
        }
        .boxed()
    }

    fn load_events(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        let dir = self.event_dir();
        async move { Self::load_dir(dir).await.map_err(Into::into) }.boxed()
    }

    fn delete_event(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        let dir = self.event_dir();
        async move { Self::delete_file(dir, hash).await.map_err(Into::into) }.boxed()
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

/// Wraps an [`Authenticated`] [`CliConn`] as a [`KeyhiveConnection`], framing
/// outbound keyhive messages as [`CliWireMessage::Keyhive`].
#[derive(Debug, Clone)]
pub(crate) struct CliConnKeyhiveAdapter {
    auth: Authenticated<CliConn, Sendable>,
}

impl CliConnKeyhiveAdapter {
    pub(crate) const fn new(auth: Authenticated<CliConn, Sendable>) -> Self {
        Self { auth }
    }
}

impl KeyhiveConnection<Sendable> for CliConnKeyhiveAdapter {
    type SendError = CliConnKeyhiveSendError;
    type RecvError = Infallible;
    type DisconnectError = Infallible;

    fn peer_id(&self) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes(*self.auth.peer_id().as_bytes())
    }

    fn send(&self, message: SignedMessage) -> BoxFuture<'_, Result<(), Self::SendError>> {
        async move {
            let msg = CliWireMessage::Keyhive(KeyhiveMessage::from_signed(&message)?);
            <CliConn as Connection<Sendable, CliWireMessage>>::send(self.auth.inner(), &msg)
                .await?;
            Ok(())
        }
        .boxed()
    }

    fn recv(&self) -> BoxFuture<'_, Result<SignedMessage, Self::RecvError>> {
        futures::future::pending().boxed()
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectError>> {
        futures::future::ready(Ok(())).boxed()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    trait SendableStorage: KeyhiveStorage<Sendable> {}
    impl SendableStorage for FsKeyhiveStorage {}

    fn test_hash(byte: u8) -> StorageHash {
        StorageHash::new([byte; 32])
    }

    fn make_storage(dir: &std::path::Path) -> impl SendableStorage {
        FsKeyhiveStorage::new(dir.to_path_buf()).unwrap()
    }

    #[tokio::test]
    async fn save_and_load_archive() {
        let dir = tempfile::tempdir().unwrap();
        let storage = make_storage(dir.path());
        let hash = test_hash(0xaa);
        let data = b"archive-data".to_vec();

        storage.save_archive(hash, data.clone()).await.unwrap();
        let loaded = storage.load_archives().await.unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0], (hash, data));
    }

    #[tokio::test]
    async fn save_and_load_event() {
        let dir = tempfile::tempdir().unwrap();
        let storage = make_storage(dir.path());
        let hash = test_hash(0xbb);
        let data = b"event-data".to_vec();

        storage.save_event(hash, data.clone()).await.unwrap();
        let loaded = storage.load_events().await.unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0], (hash, data));
    }

    #[tokio::test]
    async fn delete_archive() {
        let dir = tempfile::tempdir().unwrap();
        let storage = make_storage(dir.path());
        let hash = test_hash(0xcc);

        storage.save_archive(hash, b"data".to_vec()).await.unwrap();
        storage.delete_archive(hash).await.unwrap();
        let loaded = storage.load_archives().await.unwrap();

        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn delete_event() {
        let dir = tempfile::tempdir().unwrap();
        let storage = make_storage(dir.path());
        let hash = test_hash(0xdd);

        storage.save_event(hash, b"data".to_vec()).await.unwrap();
        storage.delete_event(hash).await.unwrap();
        let loaded = storage.load_events().await.unwrap();

        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn delete_nonexistent_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let storage = make_storage(dir.path());

        storage.delete_archive(test_hash(0x01)).await.unwrap();
        storage.delete_event(test_hash(0x02)).await.unwrap();
    }

    #[tokio::test]
    async fn overwrite_existing() {
        let dir = tempfile::tempdir().unwrap();
        let storage = make_storage(dir.path());
        let hash = test_hash(0xee);

        storage.save_archive(hash, b"old".to_vec()).await.unwrap();
        storage.save_archive(hash, b"new".to_vec()).await.unwrap();
        let loaded = storage.load_archives().await.unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].1, b"new");
    }

    #[tokio::test]
    async fn archives_and_events_are_independent() {
        let dir = tempfile::tempdir().unwrap();
        let storage = make_storage(dir.path());
        let hash = test_hash(0xff);

        storage
            .save_archive(hash, b"archive".to_vec())
            .await
            .unwrap();
        storage.save_event(hash, b"event".to_vec()).await.unwrap();

        let archives = storage.load_archives().await.unwrap();
        let events = storage.load_events().await.unwrap();

        assert_eq!(archives.len(), 1);
        assert_eq!(archives[0].1, b"archive");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1, b"event");
    }
}
