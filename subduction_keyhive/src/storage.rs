//! Storage abstraction for keyhive data.
//!
//! This module provides the [`KeyhiveStorage`] trait for persisting keyhive archives
//! and events, along with an in-memory implementation for testing.
//!
//! # Storage Key Format
//!
//! Keyhive data is stored using the following key format
//! * Archives: `["keyhive-db", "/archives/", <hex-hash>]`
//! * Events: `["keyhive-db", "/ops/", <hex-hash>]`

use alloc::{string::String, sync::Arc, vec::Vec};
use core::convert::Infallible;

use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable};
use futures::{
    FutureExt,
    future::{BoxFuture, LocalBoxFuture},
};

use crate::collections::Map;

/// A 32-byte hash used as a storage key.
///
/// This is a BLAKE3 hash of the data being stored (for events) or
/// a keyhive storage identifier (for archives).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageHash(pub [u8; 32]);

impl StorageHash {
    /// Create a new storage hash from raw bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Get the raw bytes of the hash.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Convert to a hex string for display or storage key purposes.
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for byte in &self.0 {
            use core::fmt::Write;
            let _ = write!(s, "{byte:02x}");
        }
        s
    }

    /// Parse from a hex string.
    #[must_use]
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 64 {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
            let high = char::from(*chunk.first()?).to_digit(16)?;
            let low = char::from(*chunk.get(1)?).to_digit(16)?;
            #[allow(clippy::cast_possible_truncation)] // high and low are both < 16
            let byte = ((high << 4) | low) as u8;
            *bytes.get_mut(i)? = byte;
        }
        Some(Self(bytes))
    }
}

/// Abstraction over storage for keyhive data.
///
/// This trait provides methods for persisting keyhive archives and events.
/// Archives contain the full keyhive state, while events are individual
/// operations.
#[allow(clippy::type_complexity)]
pub trait KeyhiveStorage<K: FutureForm + ?Sized> {
    /// The error type for storage operations.
    type Error: core::error::Error;

    /// Save an archive to storage.
    ///
    /// Archives represent a snapshot of the keyhive state. The hash is
    /// a stable identifier derived from the peer ID.
    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Load all archives from storage.
    ///
    /// Returns a vector of (hash, data) pairs for all stored archives.
    #[allow(clippy::type_complexity)]
    fn load_archives(&self) -> K::Future<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>>;

    /// Delete an archive from storage.
    fn delete_archive(&self, hash: StorageHash) -> K::Future<'_, Result<(), Self::Error>>;

    /// Save an event to storage.
    ///
    /// Events are individual keyhive operations. The hash should be the BLAKE3
    /// hash of the event bytes.
    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Load all events from storage.
    ///
    /// Returns a vector of (hash, data) pairs for all stored events.
    #[allow(clippy::type_complexity)]
    fn load_events(&self) -> K::Future<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>>;

    /// Delete an event from storage.
    fn delete_event(&self, hash: StorageHash) -> K::Future<'_, Result<(), Self::Error>>;
}

/// An in-memory storage backend for testing.
#[derive(Debug, Clone, Default)]
pub struct MemoryKeyhiveStorage {
    archives: Arc<Mutex<Map<StorageHash, Vec<u8>>>>,
    events: Arc<Mutex<Map<StorageHash, Vec<u8>>>>,
}

impl MemoryKeyhiveStorage {
    /// Create a new in-memory storage backend.
    #[must_use]
    pub fn new() -> Self {
        Self {
            archives: Arc::new(Mutex::new(Map::new())),
            events: Arc::new(Mutex::new(Map::new())),
        }
    }
}

impl KeyhiveStorage<Local> for MemoryKeyhiveStorage {
    type Error = Infallible;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            self.archives.lock().await.insert(hash, data);
            Ok(())
        }
        .boxed_local()
    }

    fn load_archives(
        &self,
    ) -> LocalBoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move {
            let archives = self.archives.lock().await;
            Ok(archives.iter().map(|(k, v)| (*k, v.clone())).collect())
        }
        .boxed_local()
    }

    fn delete_archive(&self, hash: StorageHash) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            self.archives.lock().await.remove(&hash);
            Ok(())
        }
        .boxed_local()
    }

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            self.events.lock().await.insert(hash, data);
            Ok(())
        }
        .boxed_local()
    }

    fn load_events(&self) -> LocalBoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move {
            let events = self.events.lock().await;
            Ok(events.iter().map(|(k, v)| (*k, v.clone())).collect())
        }
        .boxed_local()
    }

    fn delete_event(&self, hash: StorageHash) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            self.events.lock().await.remove(&hash);
            Ok(())
        }
        .boxed_local()
    }
}

impl KeyhiveStorage<Sendable> for MemoryKeyhiveStorage {
    type Error = Infallible;

    fn save_archive(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            self.archives.lock().await.insert(hash, data);
            Ok(())
        }
        .boxed()
    }

    fn load_archives(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move {
            let archives = self.archives.lock().await;
            Ok(archives.iter().map(|(k, v)| (*k, v.clone())).collect())
        }
        .boxed()
    }

    fn delete_archive(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            self.archives.lock().await.remove(&hash);
            Ok(())
        }
        .boxed()
    }

    fn save_event(
        &self,
        hash: StorageHash,
        data: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            self.events.lock().await.insert(hash, data);
            Ok(())
        }
        .boxed()
    }

    fn load_events(&self) -> BoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, Self::Error>> {
        async move {
            let events = self.events.lock().await;
            Ok(events.iter().map(|(k, v)| (*k, v.clone())).collect())
        }
        .boxed()
    }

    fn delete_event(&self, hash: StorageHash) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            self.events.lock().await.remove(&hash);
            Ok(())
        }
        .boxed()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn storage_hash_hex_roundtrip() {
        let bytes = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67,
            0x89, 0xab, 0xcd, 0xef,
        ];
        let hash = StorageHash::new(bytes);
        let hex = hash.to_hex();
        assert_eq!(
            hex,
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        let parsed = StorageHash::from_hex(&hex).unwrap();
        assert_eq!(hash, parsed);
    }

    #[test]
    fn storage_hash_from_hex_invalid_length() {
        assert!(StorageHash::from_hex("0123").is_none());
        assert!(StorageHash::from_hex("").is_none());
    }

    #[test]
    fn storage_hash_from_hex_invalid_chars() {
        let invalid = "zzzz456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(StorageHash::from_hex(invalid).is_none());
    }
}
