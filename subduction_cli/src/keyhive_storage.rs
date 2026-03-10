//! Filesystem-based keyhive storage.
//!
//! Persists keyhive archives and events as files under a root directory:
//!
//! ```text
//! root/
//! └── keyhive/
//!     ├── archives/
//!     │   └── {hex_hash}.bin
//!     └── events/
//!         └── {hex_hash}.bin
//! ```

use std::path::{Path, PathBuf};

use future_form::{Local, Sendable};
use futures::{
    FutureExt,
    future::{BoxFuture, LocalBoxFuture},
};
use subduction_keyhive::storage::{KeyhiveStorage, StorageHash};

/// Errors from [`FsKeyhiveStorage`] operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FsKeyhiveStorageError {
    /// Filesystem I/O error.
    #[error("keyhive storage I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// Persistent keyhive storage backed by the local filesystem.
///
/// Archives and events are stored as individual files under
/// `{root}/keyhive/archives/` and `{root}/keyhive/events/` respectively.
/// Each file is named `{hex_hash}.bin`.
///
/// Writes use an atomic rename pattern (write to `.tmp`, then rename)
/// to avoid partial-write corruption.
#[derive(Debug, Clone)]
pub(crate) struct FsKeyhiveStorage {
    archives_dir: PathBuf,
    events_dir: PathBuf,
}

impl FsKeyhiveStorage {
    /// Create a new filesystem keyhive storage rooted at `root`.
    ///
    /// Creates the `keyhive/archives/` and `keyhive/events/`
    /// subdirectories if they don't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created.
    pub(crate) fn new(root: &Path) -> Result<Self, FsKeyhiveStorageError> {
        let keyhive_dir = root.join("keyhive");
        let archives_dir = keyhive_dir.join("archives");
        let events_dir = keyhive_dir.join("events");

        std::fs::create_dir_all(&archives_dir)?;
        std::fs::create_dir_all(&events_dir)?;

        Ok(Self {
            archives_dir,
            events_dir,
        })
    }

    fn archive_path(&self, hash: StorageHash) -> PathBuf {
        self.archives_dir.join(format!("{}.bin", hash.to_hex()))
    }

    fn event_path(&self, hash: StorageHash) -> PathBuf {
        self.events_dir.join(format!("{}.bin", hash.to_hex()))
    }

    /// Read all `(StorageHash, Vec<u8>)` pairs from a directory.
    async fn load_all_from_dir(
        dir: &Path,
    ) -> Result<Vec<(StorageHash, Vec<u8>)>, FsKeyhiveStorageError> {
        let mut results = Vec::new();

        let mut entries = match tokio::fs::read_dir(dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(results),
            Err(e) => return Err(e.into()),
        };

        while let Some(entry) = entries.next_entry().await? {
            if let Ok(name) = entry.file_name().into_string()
                && let Some(hex_str) = name.strip_suffix(".bin")
                && let Some(hash) = StorageHash::from_hex(hex_str)
            {
                let data = tokio::fs::read(entry.path()).await?;
                results.push((hash, data));
            }
        }

        Ok(results)
    }

    /// Atomically write `data` to `path` (write `.tmp`, then rename).
    async fn atomic_write(path: &Path, data: &[u8]) -> Result<(), FsKeyhiveStorageError> {
        let tmp = path.with_extension("tmp");
        tokio::fs::write(&tmp, data).await?;
        tokio::fs::rename(&tmp, path).await?;
        Ok(())
    }

    /// Remove a file, ignoring "not found" errors.
    async fn remove_if_exists(path: &Path) -> Result<(), FsKeyhiveStorageError> {
        if let Err(e) = tokio::fs::remove_file(path).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(e.into());
        }
        Ok(())
    }
}

// ── KeyhiveStorage<Local> ──────────────────────────────────────────────

impl KeyhiveStorage<Local> for FsKeyhiveStorage {
    type Error = FsKeyhiveStorageError;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let path = self.archive_path(hash);
            Self::atomic_write(&path, &data).await
        }
        .boxed_local()
    }

    fn load_archives(
        &self,
    ) -> LocalBoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move { Self::load_all_from_dir(&self.archives_dir).await }.boxed_local()
    }

    fn delete_archive(&self, hash: StorageHash) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let path = self.archive_path(hash);
            Self::remove_if_exists(&path).await
        }
        .boxed_local()
    }

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let path = self.event_path(hash);
            Self::atomic_write(&path, &data).await
        }
        .boxed_local()
    }

    fn load_events(&self) -> LocalBoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move { Self::load_all_from_dir(&self.events_dir).await }.boxed_local()
    }

    fn delete_event(&self, hash: StorageHash) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let path = self.event_path(hash);
            Self::remove_if_exists(&path).await
        }
        .boxed_local()
    }
}

// ── KeyhiveStorage<Sendable> ───────────────────────────────────────────

impl KeyhiveStorage<Sendable> for FsKeyhiveStorage {
    type Error = FsKeyhiveStorageError;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let path = self.archive_path(hash);
            Self::atomic_write(&path, &data).await
        }
        .boxed()
    }

    fn load_archives(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move { Self::load_all_from_dir(&self.archives_dir).await }.boxed()
    }

    fn delete_archive(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let path = self.archive_path(hash);
            Self::remove_if_exists(&path).await
        }
        .boxed()
    }

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let path = self.event_path(hash);
            Self::atomic_write(&path, &data).await
        }
        .boxed()
    }

    fn load_events(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move { Self::load_all_from_dir(&self.events_dir).await }.boxed()
    }

    fn delete_event(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let path = self.event_path(hash);
            Self::remove_if_exists(&path).await
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn save_and_load_archive() -> testresult::TestResult {
        let tmp = tempfile::tempdir()?;
        let storage = FsKeyhiveStorage::new(tmp.path())?;

        let hash = StorageHash::new([1u8; 32]);
        let data = b"archive-data".to_vec();

        KeyhiveStorage::<Sendable>::save_archive(&storage, hash, data.clone()).await?;

        let loaded = KeyhiveStorage::<Sendable>::load_archives(&storage).await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, hash);
        assert_eq!(loaded[0].1, data);

        Ok(())
    }

    #[tokio::test]
    async fn save_and_load_event() -> testresult::TestResult {
        let tmp = tempfile::tempdir()?;
        let storage = FsKeyhiveStorage::new(tmp.path())?;

        let hash = StorageHash::new([2u8; 32]);
        let data = b"event-data".to_vec();

        KeyhiveStorage::<Sendable>::save_event(&storage, hash, data.clone()).await?;

        let loaded = KeyhiveStorage::<Sendable>::load_events(&storage).await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, hash);
        assert_eq!(loaded[0].1, data);

        Ok(())
    }

    #[tokio::test]
    async fn delete_archive() -> testresult::TestResult {
        let tmp = tempfile::tempdir()?;
        let storage = FsKeyhiveStorage::new(tmp.path())?;

        let hash = StorageHash::new([3u8; 32]);
        KeyhiveStorage::<Sendable>::save_archive(&storage, hash, b"data".to_vec()).await?;
        KeyhiveStorage::<Sendable>::delete_archive(&storage, hash).await?;

        let loaded = KeyhiveStorage::<Sendable>::load_archives(&storage).await?;
        assert!(loaded.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn delete_event() -> testresult::TestResult {
        let tmp = tempfile::tempdir()?;
        let storage = FsKeyhiveStorage::new(tmp.path())?;

        let hash = StorageHash::new([4u8; 32]);
        KeyhiveStorage::<Sendable>::save_event(&storage, hash, b"data".to_vec()).await?;
        KeyhiveStorage::<Sendable>::delete_event(&storage, hash).await?;

        let loaded = KeyhiveStorage::<Sendable>::load_events(&storage).await?;
        assert!(loaded.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn delete_nonexistent_is_ok() -> testresult::TestResult {
        let tmp = tempfile::tempdir()?;
        let storage = FsKeyhiveStorage::new(tmp.path())?;

        let hash = StorageHash::new([5u8; 32]);
        KeyhiveStorage::<Sendable>::delete_archive(&storage, hash).await?;
        KeyhiveStorage::<Sendable>::delete_event(&storage, hash).await?;

        Ok(())
    }

    #[tokio::test]
    async fn multiple_archives() -> testresult::TestResult {
        let tmp = tempfile::tempdir()?;
        let storage = FsKeyhiveStorage::new(tmp.path())?;

        let h1 = StorageHash::new([10u8; 32]);
        let h2 = StorageHash::new([11u8; 32]);

        KeyhiveStorage::<Sendable>::save_archive(&storage, h1, b"one".to_vec()).await?;
        KeyhiveStorage::<Sendable>::save_archive(&storage, h2, b"two".to_vec()).await?;

        let loaded = KeyhiveStorage::<Sendable>::load_archives(&storage).await?;
        assert_eq!(loaded.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn local_impl_works() -> testresult::TestResult {
        let tmp = tempfile::tempdir()?;
        let storage = FsKeyhiveStorage::new(tmp.path())?;

        let hash = StorageHash::new([20u8; 32]);
        KeyhiveStorage::<Local>::save_event(&storage, hash, b"local-data".to_vec()).await?;

        let loaded = KeyhiveStorage::<Local>::load_events(&storage).await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].1, b"local-data");

        Ok(())
    }
}
