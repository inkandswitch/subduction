//! Storage operations for keyhive data.
//!
//! This module provides high-level functions for persisting, loading, and compacting
//! keyhive state using the [`KeyhiveArchiveStorage`] and [`KeyhiveEventStorage`] traits.

use alloc::{sync::Arc, vec::Vec};
use core::convert::Infallible;

use keyhive_core::{
    archive::Archive, crypto::signer::async_signer::AsyncSigner, event::static_event::StaticEvent,
    keyhive::Keyhive,
};

use crate::storage::{KeyhiveArchiveStorage, KeyhiveEventStorage, StorageHash};

/// Errors that can occur during CBOR serialization/deserialization.
///
/// This is separate from storage errors since it doesn't depend on the storage backend.
#[derive(Debug, thiserror::Error)]
pub enum CborError {
    /// Failed to serialize data (minicbor).
    #[error("CBOR encode error")]
    CborEncode(#[from] minicbor::encode::Error<Infallible>),

    /// Failed to serialize data (serde).
    #[error("serde encode error")]
    SerdeEncode(#[from] minicbor_serde::error::EncodeError<Infallible>),

    /// Failed to deserialize data (minicbor).
    #[error("CBOR decode error")]
    CborDecode(#[from] minicbor::decode::Error),

    /// Failed to deserialize data (serde).
    #[error("serde decode error")]
    SerdeDecode(#[from] minicbor_serde::error::DecodeError),
}

/// Serialize a value to CBOR bytes.
fn cbor_serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, CborError> {
    Ok(minicbor_serde::to_vec(value)?)
}

/// Deserialize a value from CBOR bytes.
fn cbor_deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, CborError> {
    Ok(minicbor_serde::from_slice(bytes)?)
}

/// Hash event bytes using BLAKE3 to produce a storage key.
#[must_use]
pub fn hash_event_bytes(bytes: &[u8]) -> StorageHash {
    let hash = blake3::hash(bytes);
    StorageHash::new(*hash.as_bytes())
}

/// Error from saving an archive (CBOR serialization + storage).
#[derive(Debug, thiserror::Error)]
pub enum SaveArchiveError<SaveErr: core::error::Error> {
    /// CBOR serialization failed.
    #[error("serialization error")]
    Cbor(#[from] CborError),

    /// Storage save failed.
    #[error("storage save error")]
    Storage(#[source] SaveErr),
}

/// Serialize and save a keyhive archive to storage.
///
/// The archive is serialized using CBOR and stored with the provided storage ID
/// as the key. The storage ID should be a stable identifier derived from the peer ID.
///
/// # Errors
///
/// Returns an error if CBOR serialization or the storage write fails.
pub async fn save_keyhive_archive<T, S, K>(
    storage: &S,
    storage_id: StorageHash,
    archive: &Archive<T>,
) -> Result<(), SaveArchiveError<S::SaveError>>
where
    T: keyhive_core::content::reference::ContentRef,
    S: KeyhiveArchiveStorage<K>,
    K: future_form::FutureForm + ?Sized,
{
    let bytes = cbor_serialize(archive)?;

    tracing::debug!(
        hash = %storage_id.to_hex(),
        bytes = bytes.len(),
        "saving keyhive archive"
    );

    storage
        .save_archive(storage_id, bytes)
        .await
        .map_err(SaveArchiveError::Storage)
}

/// Error from saving an event (CBOR serialization + storage).
#[derive(Debug, thiserror::Error)]
pub enum SaveEventError<SaveErr: core::error::Error> {
    /// CBOR serialization failed.
    #[error("serialization error")]
    Cbor(#[from] CborError),

    /// Storage save failed.
    #[error("storage save error")]
    Storage(#[source] SaveErr),
}

/// Serialize and save an event to storage.
///
/// The event is serialized using CBOR and stored with its BLAKE3 hash as the key.
///
/// # Errors
///
/// Returns an error if CBOR serialization or the storage write fails.
pub async fn save_event<T, S, K>(
    storage: &S,
    event: &StaticEvent<T>,
) -> Result<StorageHash, SaveEventError<S::SaveError>>
where
    T: keyhive_core::content::reference::ContentRef,
    S: KeyhiveEventStorage<K>,
    K: future_form::FutureForm + ?Sized,
{
    let bytes = cbor_serialize(event)?;
    save_event_bytes(storage, bytes).await
}

/// Save raw event bytes to storage.
///
/// The bytes are stored with their BLAKE3 hash as the key.
///
/// # Errors
///
/// Returns an error if the storage write fails.
pub async fn save_event_bytes<S, K>(
    storage: &S,
    bytes: Vec<u8>,
) -> Result<StorageHash, SaveEventError<S::SaveError>>
where
    S: KeyhiveEventStorage<K>,
    K: future_form::FutureForm + ?Sized,
{
    let hash = hash_event_bytes(&bytes);

    tracing::debug!(
        hash = %hash.to_hex(),
        bytes = bytes.len(),
        "saving event"
    );

    storage
        .save_event(hash, bytes)
        .await
        .map_err(SaveEventError::Storage)?;

    Ok(hash)
}

/// Error from loading archives (storage + CBOR deserialization).
#[derive(Debug, thiserror::Error)]
pub enum LoadArchivesError<LoadErr: core::error::Error> {
    /// Storage load failed.
    #[error("storage load error")]
    Storage(#[source] LoadErr),

    /// CBOR deserialization failed.
    #[error("deserialization error")]
    Cbor(#[from] CborError),
}

/// Load and deserialize all archives from storage.
///
/// # Errors
///
/// Returns an error if the storage read or CBOR deserialization fails.
pub async fn load_archives<T, S, K>(
    storage: &S,
) -> Result<Vec<(StorageHash, Archive<T>)>, LoadArchivesError<S::LoadError>>
where
    T: keyhive_core::content::reference::ContentRef + serde::de::DeserializeOwned,
    S: KeyhiveArchiveStorage<K>,
    K: future_form::FutureForm + ?Sized,
{
    let raw_archives = storage
        .load_archives()
        .await
        .map_err(LoadArchivesError::Storage)?;

    let mut archives = Vec::with_capacity(raw_archives.len());
    for (hash, bytes) in raw_archives {
        let archive: Archive<T> = cbor_deserialize(&bytes)?;
        archives.push((hash, archive));
    }

    tracing::debug!(count = archives.len(), "loaded archives from storage");
    Ok(archives)
}

/// Error from loading events (storage + CBOR deserialization).
#[derive(Debug, thiserror::Error)]
pub enum LoadEventsError<LoadErr: core::error::Error> {
    /// Storage load failed.
    #[error("storage load error")]
    Storage(#[source] LoadErr),

    /// CBOR deserialization failed.
    #[error("deserialization error")]
    Cbor(#[from] CborError),
}

/// Load and deserialize all events from storage.
///
/// # Errors
///
/// Returns an error if the storage read or CBOR deserialization fails.
pub async fn load_events<T, S, K>(
    storage: &S,
) -> Result<Vec<(StorageHash, StaticEvent<T>)>, LoadEventsError<S::LoadError>>
where
    T: keyhive_core::content::reference::ContentRef + serde::de::DeserializeOwned,
    S: KeyhiveEventStorage<K>,
    K: future_form::FutureForm + ?Sized,
{
    let raw_events = storage
        .load_events()
        .await
        .map_err(LoadEventsError::Storage)?;

    let mut events = Vec::with_capacity(raw_events.len());
    for (hash, bytes) in raw_events {
        let event: StaticEvent<T> = cbor_deserialize(&bytes)?;
        events.push((hash, event));
    }

    tracing::debug!(count = events.len(), "loaded events from storage");
    Ok(events)
}

/// Load raw event bytes from storage.
///
/// This is useful when you need to track which events were stored without deserializing.
///
/// # Errors
///
/// Returns an error if the storage read fails.
pub async fn load_event_bytes<S, K>(
    storage: &S,
) -> Result<Vec<(StorageHash, Vec<u8>)>, S::LoadError>
where
    S: KeyhiveEventStorage<K>,
    K: future_form::FutureForm + ?Sized,
{
    storage.load_events().await
}

/// Error from ingesting archives and events from storage.
#[derive(Debug, thiserror::Error)]
pub enum IngestFromStorageError<ArchiveLoadErr, EventLoadErr>
where
    ArchiveLoadErr: core::error::Error,
    EventLoadErr: core::error::Error,
{
    /// Failed to load archives.
    #[error("failed to load archives")]
    LoadArchives(#[source] LoadArchivesError<ArchiveLoadErr>),

    /// Failed to load events.
    #[error("failed to load events")]
    LoadEvents(#[source] LoadEventsError<EventLoadErr>),

    /// Archive ingestion failed.
    ///
    /// Note: The underlying error is not captured because `ReceiveStaticEventError`
    /// has many generic parameters that would complicate this error type.
    #[error("archive ingestion failed")]
    ArchiveIngestion,
}

/// Ingest all stored archives and events into a keyhive instance.
///
/// This loads all archives and events from storage and ingests them into the
/// provided keyhive. Archives are ingested first, then events. Returns
/// any pending events.
///
/// # Errors
///
/// Returns an error if loading, deserialization, or archive ingestion fails.
pub async fn ingest_from_storage<K, Signer, T, P, C, L, R, S>(
    keyhive: &Keyhive<Signer, T, P, C, L, R>,
    storage: &S,
) -> Result<
    Vec<Arc<StaticEvent<T>>>,
    IngestFromStorageError<
        <S as KeyhiveArchiveStorage<K>>::LoadError,
        <S as KeyhiveEventStorage<K>>::LoadError,
    >,
>
where
    K: future_form::FutureForm,
    Signer: AsyncSigner<K, T> + Clone,
    T: keyhive_core::content::reference::ContentRef + serde::de::DeserializeOwned,
    P: for<'de> serde::Deserialize<'de>,
    C: keyhive_core::store::ciphertext::CiphertextStore<T, P> + Clone,
    L: keyhive_core::listener::membership::MembershipListener<K, Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    S: KeyhiveArchiveStorage<K> + KeyhiveEventStorage<K>,
{
    // Load archives
    let archives: Vec<(StorageHash, Archive<T>)> = load_archives(storage)
        .await
        .map_err(IngestFromStorageError::LoadArchives)?;

    tracing::debug!(count = archives.len(), "ingesting archives from storage");

    // Ingest each archive
    for (hash, archive) in archives {
        tracing::debug!(hash = %hash.to_hex(), "ingesting archive");
        keyhive.ingest_archive::<K>(archive).await.map_err(|e| {
            tracing::error!(error = ?e, "archive ingestion failed");
            IngestFromStorageError::ArchiveIngestion
        })?;
    }

    // Load and ingest events
    let events: Vec<(StorageHash, StaticEvent<T>)> = load_events(storage)
        .await
        .map_err(IngestFromStorageError::LoadEvents)?;

    tracing::debug!(count = events.len(), "ingesting events from storage");

    let event_list: Vec<StaticEvent<T>> = events.into_iter().map(|(_, e)| e).collect();
    let pending = keyhive.ingest_unsorted_static_events::<K>(event_list).await;

    tracing::debug!(
        pending_count = pending.len(),
        "finished ingesting from storage"
    );

    Ok(pending)
}

/// Error from keyhive persistence operations (compact, ingest, etc.).
#[derive(Debug, thiserror::Error)]
pub enum KeyhivePersistenceErrorRaw<
    ArchiveSaveErr: core::error::Error,
    ArchiveLoadErr: core::error::Error,
    ArchiveDeleteErr: core::error::Error,
    EventLoadErr: core::error::Error,
    EventDeleteErr: core::error::Error,
> {
    /// Failed to load archives.
    #[error("failed to load archives")]
    LoadArchives(#[source] ArchiveLoadErr),

    /// Failed to load events.
    #[error("failed to load events")]
    LoadEvents(#[source] EventLoadErr),

    /// Failed to save archive.
    #[error("failed to save archive")]
    SaveArchive(#[source] SaveArchiveError<ArchiveSaveErr>),

    /// Failed to delete archive.
    #[error("failed to delete archive")]
    DeleteArchive(#[source] ArchiveDeleteErr),

    /// Failed to delete event.
    #[error("failed to delete event")]
    DeleteEvent(#[source] EventDeleteErr),

    /// CBOR deserialization failed.
    #[error("deserialization error")]
    Cbor(#[from] CborError),

    /// Archive ingestion failed.
    ///
    /// Note: The underlying error is not captured because `ReceiveStaticEventError`
    /// has many generic parameters that would complicate this error type.
    #[error("archive ingestion failed")]
    ArchiveIngestion,
}

/// Keyhive persistence error parameterized by storage implementation.
///
/// This alias extracts the appropriate error types from the storage traits,
/// hiding the 5-parameter generic type from callers.
pub type KeyhivePersistenceError<S, K> = KeyhivePersistenceErrorRaw<
    <S as KeyhiveArchiveStorage<K>>::SaveError,
    <S as KeyhiveArchiveStorage<K>>::LoadError,
    <S as KeyhiveArchiveStorage<K>>::DeleteError,
    <S as KeyhiveEventStorage<K>>::LoadError,
    <S as KeyhiveEventStorage<K>>::DeleteError,
>;

/// Compact keyhive storage by consolidating archives and removing processed events.
///
/// This operation:
/// 1. Loads all archives and events from storage
/// 2. Ingests them into the keyhive
/// 3. Saves a new consolidated archive
/// 4. Deletes old archives
/// 5. Deletes events that have been processed (keeping only pending events)
///
/// # Errors
///
/// Returns an error if any storage operation, serialization, or deserialization fails.
pub async fn compact<K, Signer, T, P, C, L, R, S>(
    keyhive: &Keyhive<Signer, T, P, C, L, R>,
    storage: &S,
    storage_id: StorageHash,
) -> Result<(), KeyhivePersistenceError<S, K>>
where
    K: future_form::FutureForm,
    Signer: AsyncSigner<K, T> + Clone,
    T: keyhive_core::content::reference::ContentRef + serde::de::DeserializeOwned,
    P: for<'de> serde::Deserialize<'de>,
    C: keyhive_core::store::ciphertext::CiphertextStore<T, P> + Clone,
    L: keyhive_core::listener::membership::MembershipListener<K, Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    S: KeyhiveArchiveStorage<K> + KeyhiveEventStorage<K>,
    keyhive_core::principal::active::Active<Signer, T, L>:
        keyhive_core::principal::active::ActiveOps<K>,
{
    // Load raw data (we need hashes for cleanup)
    let raw_archives = storage
        .load_archives()
        .await
        .map_err(KeyhivePersistenceErrorRaw::LoadArchives)?;
    let raw_events = storage
        .load_events()
        .await
        .map_err(KeyhivePersistenceErrorRaw::LoadEvents)?;

    if raw_events.is_empty() && raw_archives.len() <= 1 {
        tracing::debug!("nothing to compact");
        return Ok(());
    }

    tracing::debug!(
        archives = raw_archives.len(),
        events = raw_events.len(),
        "starting compaction"
    );

    // Deserialize and ingest archives
    for (hash, bytes) in &raw_archives {
        let archive: Archive<T> = cbor_deserialize(bytes)?;
        tracing::debug!(hash = %hash.to_hex(), "ingesting archive for compaction");
        keyhive.ingest_archive::<K>(archive).await.map_err(|e| {
            tracing::error!(error = ?e, "archive ingestion failed during compaction");
            KeyhivePersistenceErrorRaw::ArchiveIngestion
        })?;
    }

    // Build a map from event hash to storage hash for tracking pending events
    let event_hash_to_storage: crate::collections::Map<[u8; 32], StorageHash> = raw_events
        .iter()
        .map(|(storage_hash, bytes)| {
            let event_hash = hash_event_bytes(bytes);
            (*event_hash.as_bytes(), *storage_hash)
        })
        .collect();

    // Deserialize and ingest events
    let events: Vec<StaticEvent<T>> = raw_events
        .iter()
        .map(|(_, bytes)| cbor_deserialize(bytes))
        .collect::<Result<_, _>>()?;

    let pending = keyhive.ingest_unsorted_static_events::<K>(events).await;

    // Get hashes of pending events
    let pending_hashes: crate::collections::Set<[u8; 32]> = pending
        .iter()
        .filter_map(|e| {
            let bytes = cbor_serialize(e.as_ref()).ok()?;
            Some(*hash_event_bytes(&bytes).as_bytes())
        })
        .collect();

    // Save the new consolidated archive
    let archive = keyhive.into_archive::<K>().await;
    save_keyhive_archive(storage, storage_id, &archive)
        .await
        .map_err(KeyhivePersistenceErrorRaw::SaveArchive)?;

    let mut deleted_archive_count = 0;
    let mut deleted_event_count = 0;

    // Delete old archives
    for (hash, _) in &raw_archives {
        if *hash != storage_id {
            storage
                .delete_archive(*hash)
                .await
                .map_err(KeyhivePersistenceErrorRaw::DeleteArchive)?;
            deleted_archive_count += 1;
        }
    }

    // Delete processed events (and keep pending ones)
    for (event_bytes_hash, storage_hash) in &event_hash_to_storage {
        if !pending_hashes.contains(event_bytes_hash) {
            storage
                .delete_event(*storage_hash)
                .await
                .map_err(KeyhivePersistenceErrorRaw::DeleteEvent)?;
            deleted_event_count += 1;
        }
    }

    tracing::debug!(
        pending_events = pending.len(),
        deleted_archives = deleted_archive_count,
        deleted_events = deleted_event_count,
        "compaction complete"
    );

    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::{
        storage::{KeyhiveArchiveStorage, KeyhiveEventStorage, MemoryKeyhiveStorage, StorageHash},
        test_utils::{keyhive_peer_id, make_keyhive},
    };
    use future_form::Sendable;

    #[tokio::test(flavor = "current_thread")]
    async fn save_and_load_archive_roundtrip() {
        let keyhive = make_keyhive().await;
        let storage = MemoryKeyhiveStorage::new();
        let storage_id = StorageHash::new([1u8; 32]);

        let archive = keyhive.into_archive::<Sendable>().await;
        save_keyhive_archive::<_, _, Sendable>(&storage, storage_id, &archive)
            .await
            .unwrap();

        let loaded = load_archives::<[u8; 32], _, Sendable>(&storage)
            .await
            .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, storage_id);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn compact_consolidates_archives_and_deletes_processed_events() {
        // Create two keyhives and exchange contact cards to generate real events
        let alice = make_keyhive().await;
        let bob = make_keyhive().await;

        let alice_cc = alice.contact_card::<Sendable>().await.unwrap();
        let bob_cc = bob.contact_card::<Sendable>().await.unwrap();
        alice.receive_contact_card(&bob_cc).await.unwrap();
        bob.receive_contact_card(&alice_cc).await.unwrap();

        let storage = MemoryKeyhiveStorage::new();

        // Save two separate archives (simulating multiple save points)
        let id1 = StorageHash::new([1u8; 32]);
        let archive1 = alice.into_archive::<Sendable>().await;
        save_keyhive_archive::<_, _, Sendable>(&storage, id1, &archive1)
            .await
            .unwrap();

        let id2 = StorageHash::new([2u8; 32]);
        let archive2 = alice.into_archive::<Sendable>().await;
        save_keyhive_archive::<_, _, Sendable>(&storage, id2, &archive2)
            .await
            .unwrap();

        // Save alice's real events to storage
        let alice_peer_id = keyhive_peer_id(&alice);
        let alice_id = alice_peer_id.to_identifier().unwrap();
        let alice_agent = alice
            .get_agent(alice_id)
            .await
            .expect("alice should have herself as agent");
        let events = alice.static_events_for_agent(&alice_agent).await.unwrap();
        assert!(
            !events.is_empty(),
            "contact card exchange should produce events"
        );

        for event in events.values() {
            save_event::<_, _, Sendable>(&storage, event).await.unwrap();
        }

        // Before compaction: 2 archives, N events
        let archives_before = KeyhiveArchiveStorage::<Sendable>::load_archives(&storage)
            .await
            .unwrap();
        let events_before = KeyhiveEventStorage::<Sendable>::load_events(&storage)
            .await
            .unwrap();
        assert_eq!(archives_before.len(), 2);
        assert!(!events_before.is_empty());

        // Compact
        let consolidated_id = StorageHash::new([10u8; 32]);
        compact::<Sendable, _, _, _, _, _, _, _>(&alice, &storage, consolidated_id)
            .await
            .unwrap();

        // After compaction: exactly 1 archive at the consolidated key
        let archives_after = KeyhiveArchiveStorage::<Sendable>::load_archives(&storage)
            .await
            .unwrap();
        assert_eq!(archives_after.len(), 1);
        assert_eq!(archives_after[0].0, consolidated_id);

        // Events that alice already knows about should have been deleted.
        // Alice already has these events in her keyhive state, so they are
        // "processed" — compaction ingests them, sees they aren't pending,
        // and removes them.
        let events_after = KeyhiveEventStorage::<Sendable>::load_events(&storage)
            .await
            .unwrap();
        assert_eq!(
            events_after.len(),
            0,
            "all events should be processed (alice already has them) and deleted"
        );

        // The consolidated archive should be loadable and deserializable
        let reloaded = load_archives::<[u8; 32], _, Sendable>(&storage)
            .await
            .unwrap();
        assert_eq!(reloaded.len(), 1);
    }
}
