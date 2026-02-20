//! Filesystem-based storage for keyhive data.
//!
//! This module provides [`FsKeyhiveStorage`], a filesystem storage backend
//! that implements the [`KeyhiveArchiveStorage`] and [`KeyhiveEventStorage`] traits.
//!
//! # Storage Layout
//!
//! ```text
//! root/
//! ├── archives/
//! │   └── {hash_hex}.cbor     ← Serialized Archive
//! └── events/
//!     └── {hash_hex}.cbor     ← Serialized StaticEvent bytes
//! ```

use std::path::{Path, PathBuf};

use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};
use thiserror::Error;

use crate::storage::{KeyhiveArchiveStorage, KeyhiveEventStorage, StorageHash};

/// Error during [`FsKeyhiveStorage`] initialization.
#[derive(Debug, Error)]
#[error("failed to create storage directory")]
pub struct FsStorageInitError(#[source] pub std::io::Error);

/// Filesystem-based storage backend for keyhive data.
///
/// Uses a content-addressed layout:
/// ```text
/// root/
/// ├── archives/
/// │   └── {hash_hex}.cbor
/// └── events/
///     └── {hash_hex}.cbor
/// ```
#[derive(Debug, Clone)]
pub struct FsKeyhiveStorage {
    root: PathBuf,
}

impl FsKeyhiveStorage {
    /// Create a new filesystem keyhive storage backend at the given root directory.
    ///
    /// Creates the directory structure if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created.
    pub fn new(root: PathBuf) -> Result<Self, FsStorageInitError> {
        std::fs::create_dir_all(&root).map_err(FsStorageInitError)?;
        std::fs::create_dir_all(root.join("archives")).map_err(FsStorageInitError)?;
        std::fs::create_dir_all(root.join("events")).map_err(FsStorageInitError)?;

        Ok(Self { root })
    }

    /// Returns the root directory of the storage.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn archives_dir(&self) -> PathBuf {
        self.root.join("archives")
    }

    fn events_dir(&self) -> PathBuf {
        self.root.join("events")
    }

    fn archive_path(&self, hash: StorageHash) -> PathBuf {
        self.archives_dir().join(format!("{}.cbor", hash.to_hex()))
    }

    fn event_path(&self, hash: StorageHash) -> PathBuf {
        self.events_dir().join(format!("{}.cbor", hash.to_hex()))
    }

    fn parse_hash_from_filename(name: &str) -> Option<StorageHash> {
        let hex_str = name.strip_suffix(".cbor")?;
        StorageHash::from_hex(hex_str)
    }
}

impl KeyhiveArchiveStorage<Sendable> for FsKeyhiveStorage {
    type SaveError = std::io::Error;
    type LoadError = std::io::Error;
    type DeleteError = std::io::Error;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::SaveError>> {
        async move {
            let path = self.archive_path(hash);

            // Skip if already exists (content-addressed)
            if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                return Ok(());
            }

            // Atomic write: write to temp file, then rename
            let temp_path = path.with_extension("tmp");
            tokio::fs::write(&temp_path, &data).await?;
            tokio::fs::rename(&temp_path, &path).await?;

            tracing::debug!(hash = %hash.to_hex(), bytes = data.len(), "saved keyhive archive");
            Ok(())
        }
        .boxed()
    }

    fn load_archives(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::LoadError>> {
        async move {
            let archives_dir = self.archives_dir();
            let mut result = Vec::new();

            let mut entries = match tokio::fs::read_dir(&archives_dir).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(result),
                Err(e) => return Err(e),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str()
                    && let Some(hash) = Self::parse_hash_from_filename(name)
                {
                    let data = tokio::fs::read(entry.path()).await?;
                    result.push((hash, data));
                }
            }

            tracing::debug!(count = result.len(), "loaded keyhive archives");
            Ok(result)
        }
        .boxed()
    }

    fn delete_archive(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::DeleteError>> {
        async move {
            let path = self.archive_path(hash);

            if let Err(e) = tokio::fs::remove_file(&path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e);
            }

            tracing::debug!(hash = %hash.to_hex(), "deleted keyhive archive");
            Ok(())
        }
        .boxed()
    }
}

impl KeyhiveEventStorage<Sendable> for FsKeyhiveStorage {
    type SaveError = std::io::Error;
    type LoadError = std::io::Error;
    type DeleteError = std::io::Error;

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::SaveError>> {
        async move {
            let path = self.event_path(hash);

            // Skip if already exists (content-addressed)
            if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                return Ok(());
            }

            // Atomic write: write to temp file, then rename
            let temp_path = path.with_extension("tmp");
            tokio::fs::write(&temp_path, &data).await?;
            tokio::fs::rename(&temp_path, &path).await?;

            tracing::debug!(hash = %hash.to_hex(), bytes = data.len(), "saved keyhive event");
            Ok(())
        }
        .boxed()
    }

    fn load_events(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::LoadError>> {
        async move {
            let events_dir = self.events_dir();
            let mut result = Vec::new();

            let mut entries = match tokio::fs::read_dir(&events_dir).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(result),
                Err(e) => return Err(e),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str()
                    && let Some(hash) = Self::parse_hash_from_filename(name)
                {
                    let data = tokio::fs::read(entry.path()).await?;
                    result.push((hash, data));
                }
            }

            tracing::debug!(count = result.len(), "loaded keyhive events");
            Ok(result)
        }
        .boxed()
    }

    fn delete_event(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::DeleteError>> {
        async move {
            let path = self.event_path(hash);

            if let Err(e) = tokio::fs::remove_file(&path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e);
            }

            tracing::debug!(hash = %hash.to_hex(), "deleted keyhive event");
            Ok(())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "keyhive_fs_test_{}_{}_{}",
            std::process::id(),
            name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[tokio::test]
    async fn test_save_and_load_archive() {
        let temp_dir = unique_temp_dir("save_load_archive");
        let storage = FsKeyhiveStorage::new(temp_dir.clone()).unwrap();

        let hash = StorageHash::new([1u8; 32]);
        let data = vec![1, 2, 3, 4, 5];

        // Save
        storage.save_archive(hash, data.clone()).await.unwrap();

        // Load
        let archives = storage.load_archives().await.unwrap();
        assert_eq!(archives.len(), 1);
        assert_eq!(archives[0].0, hash);
        assert_eq!(archives[0].1, data);

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[tokio::test]
    async fn test_save_and_load_event() {
        let temp_dir = unique_temp_dir("save_load_event");
        let storage = FsKeyhiveStorage::new(temp_dir.clone()).unwrap();

        let hash = StorageHash::new([2u8; 32]);
        let data = vec![10, 20, 30, 40, 50];

        // Save
        storage.save_event(hash, data.clone()).await.unwrap();

        // Load
        let events = storage.load_events().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, hash);
        assert_eq!(events[0].1, data);

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[tokio::test]
    async fn test_delete_archive() {
        let temp_dir = unique_temp_dir("delete_archive");
        let storage = FsKeyhiveStorage::new(temp_dir.clone()).unwrap();

        let hash = StorageHash::new([3u8; 32]);
        let data = vec![1, 2, 3];

        storage.save_archive(hash, data).await.unwrap();
        assert_eq!(storage.load_archives().await.unwrap().len(), 1);

        storage.delete_archive(hash).await.unwrap();
        assert_eq!(storage.load_archives().await.unwrap().len(), 0);

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[tokio::test]
    async fn test_delete_event() {
        let temp_dir = unique_temp_dir("delete_event");
        let storage = FsKeyhiveStorage::new(temp_dir.clone()).unwrap();

        let hash = StorageHash::new([4u8; 32]);
        let data = vec![1, 2, 3];

        storage.save_event(hash, data).await.unwrap();
        assert_eq!(storage.load_events().await.unwrap().len(), 1);

        storage.delete_event(hash).await.unwrap();
        assert_eq!(storage.load_events().await.unwrap().len(), 0);

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[tokio::test]
    async fn test_idempotent_save() {
        let temp_dir = unique_temp_dir("idempotent_save");
        let storage = FsKeyhiveStorage::new(temp_dir.clone()).unwrap();

        let hash = StorageHash::new([5u8; 32]);
        let data = vec![1, 2, 3, 4, 5];

        // Save twice - should be idempotent
        storage.save_event(hash, data.clone()).await.unwrap();
        storage.save_event(hash, data.clone()).await.unwrap();

        let events = storage.load_events().await.unwrap();
        assert_eq!(events.len(), 1);

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }
}
