//! Storage operations for keyhive data.
//!
//! This module provides high-level functions for persisting, loading, and compacting
//! keyhive state using the [`KeyhiveStorage`] trait.

use alloc::{string::ToString, sync::Arc, vec::Vec};

use keyhive_core::{
    archive::Archive, crypto::signer::async_signer::AsyncSigner, event::static_event::StaticEvent,
    keyhive::Keyhive,
};

use crate::{
    error::StorageError,
    storage::{KeyhiveStorage, StorageHash},
};

/// Serialize a value to CBOR bytes.
fn cbor_serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    let mut buf = Vec::new();
    ciborium::into_writer(value, &mut buf)
        .map_err(|e| StorageError::Serialization(e.to_string()))?;
    Ok(buf)
}

/// Deserialize a value from CBOR bytes.
fn cbor_deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    ciborium::from_reader(bytes).map_err(|e| StorageError::Deserialization(e.to_string()))
}

/// Hash event bytes using BLAKE3 to produce a storage key.
#[must_use]
pub fn hash_event_bytes(bytes: &[u8]) -> StorageHash {
    let hash = blake3::hash(bytes);
    StorageHash::new(*hash.as_bytes())
}

/// Serialize and save a keyhive archive to storage.
///
/// The archive is serialized using CBOR and stored with the provided storage ID
/// as the key. The storage ID should be a stable identifier derived from the peer ID.
///
/// # Errors
///
/// Returns [`StorageError`] if CBOR serialization or the storage write fails.
pub async fn save_keyhive_archive<T, S, K>(
    storage: &S,
    storage_id: StorageHash,
    archive: &Archive<T>,
) -> Result<(), StorageError>
where
    T: keyhive_core::content::reference::ContentRef,
    S: KeyhiveStorage<K>,
    K: futures_kind::FutureKind + ?Sized,
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
        .map_err(|e| StorageError::Save(e.to_string()))
}

/// Serialize and save an event to storage.
///
/// The event is serialized using CBOR and stored with its BLAKE3 hash as the key.
///
/// # Errors
///
/// Returns [`StorageError`] if CBOR serialization or the storage write fails.
pub async fn save_event<T, S, K>(
    storage: &S,
    event: &StaticEvent<T>,
) -> Result<StorageHash, StorageError>
where
    T: keyhive_core::content::reference::ContentRef,
    S: KeyhiveStorage<K>,
    K: futures_kind::FutureKind + ?Sized,
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
/// Returns [`StorageError`] if the storage write fails.
pub async fn save_event_bytes<S, K>(
    storage: &S,
    bytes: Vec<u8>,
) -> Result<StorageHash, StorageError>
where
    S: KeyhiveStorage<K>,
    K: futures_kind::FutureKind + ?Sized,
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
        .map_err(|e| StorageError::Save(e.to_string()))?;

    Ok(hash)
}

/// Load and deserialize all archives from storage.
///
/// # Errors
///
/// Returns [`StorageError`] if the storage read or CBOR deserialization fails.
pub async fn load_archives<T, S, K>(
    storage: &S,
) -> Result<Vec<(StorageHash, Archive<T>)>, StorageError>
where
    T: keyhive_core::content::reference::ContentRef + serde::de::DeserializeOwned,
    S: KeyhiveStorage<K>,
    K: futures_kind::FutureKind + ?Sized,
{
    let raw_archives = storage
        .load_archives()
        .await
        .map_err(|e| StorageError::Load(e.to_string()))?;

    let mut archives = Vec::with_capacity(raw_archives.len());
    for (hash, bytes) in raw_archives {
        let archive: Archive<T> = cbor_deserialize(&bytes)?;
        archives.push((hash, archive));
    }

    tracing::debug!(count = archives.len(), "loaded archives from storage");
    Ok(archives)
}

/// Load and deserialize all events from storage.
///
/// # Errors
///
/// Returns [`StorageError`] if the storage read or CBOR deserialization fails.
pub async fn load_events<T, S, K>(
    storage: &S,
) -> Result<Vec<(StorageHash, StaticEvent<T>)>, StorageError>
where
    T: keyhive_core::content::reference::ContentRef + serde::de::DeserializeOwned,
    S: KeyhiveStorage<K>,
    K: futures_kind::FutureKind + ?Sized,
{
    let raw_events = storage
        .load_events()
        .await
        .map_err(|e| StorageError::Load(e.to_string()))?;

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
/// Returns [`StorageError`] if the storage read fails.
pub async fn load_event_bytes<S, K>(
    storage: &S,
) -> Result<Vec<(StorageHash, Vec<u8>)>, StorageError>
where
    S: KeyhiveStorage<K>,
    K: futures_kind::FutureKind + ?Sized,
{
    storage
        .load_events()
        .await
        .map_err(|e| StorageError::Load(e.to_string()))
}

/// Ingest all stored archives and events into a keyhive instance.
///
/// This loads all archives and events from storage and ingests them into the
/// provided keyhive. Archives are ingested first, then events. Returns
/// any pending events.
///
/// # Errors
///
/// Returns [`StorageError`] if loading, deserialization, or archive ingestion fails.
pub async fn ingest_from_storage<Signer, T, P, C, L, R, S, K>(
    keyhive: &Keyhive<Signer, T, P, C, L, R>,
    storage: &S,
) -> Result<Vec<Arc<StaticEvent<T>>>, StorageError>
where
    Signer: AsyncSigner + Clone,
    T: keyhive_core::content::reference::ContentRef + serde::de::DeserializeOwned,
    P: for<'de> serde::Deserialize<'de>,
    C: keyhive_core::store::ciphertext::CiphertextStore<T, P> + Clone,
    L: keyhive_core::listener::membership::MembershipListener<Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    S: KeyhiveStorage<K>,
    K: futures_kind::FutureKind + ?Sized,
{
    // Load archives
    let archives: Vec<(StorageHash, Archive<T>)> = load_archives(storage).await?;

    tracing::debug!(count = archives.len(), "ingesting archives from storage");

    // Ingest each archive
    for (hash, archive) in archives {
        tracing::debug!(hash = %hash.to_hex(), "ingesting archive");
        keyhive
            .ingest_archive(archive)
            .await
            .map_err(|e| StorageError::Load(alloc::format!("archive ingestion failed: {e:?}")))?;
    }

    // Load and ingest events
    let events: Vec<(StorageHash, StaticEvent<T>)> = load_events(storage).await?;

    tracing::debug!(count = events.len(), "ingesting events from storage");

    let event_list: Vec<StaticEvent<T>> = events.into_iter().map(|(_, e)| e).collect();
    let pending = keyhive.ingest_unsorted_static_events(event_list).await;

    tracing::debug!(
        pending_count = pending.len(),
        "finished ingesting from storage"
    );

    Ok(pending)
}

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
/// Returns [`StorageError`] if any storage operation, serialization, or
/// deserialization fails.
pub async fn compact<Signer, T, P, C, L, R, S, K>(
    keyhive: &Keyhive<Signer, T, P, C, L, R>,
    storage: &S,
    storage_id: StorageHash,
) -> Result<(), StorageError>
where
    Signer: AsyncSigner + Clone,
    T: keyhive_core::content::reference::ContentRef + serde::de::DeserializeOwned,
    P: for<'de> serde::Deserialize<'de>,
    C: keyhive_core::store::ciphertext::CiphertextStore<T, P> + Clone,
    L: keyhive_core::listener::membership::MembershipListener<Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    S: KeyhiveStorage<K>,
    K: futures_kind::FutureKind + ?Sized,
{
    // Load raw data (we need hashes for cleanup)
    let raw_archives = storage
        .load_archives()
        .await
        .map_err(|e| StorageError::Load(e.to_string()))?;
    let raw_events = storage
        .load_events()
        .await
        .map_err(|e| StorageError::Load(e.to_string()))?;

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
        keyhive
            .ingest_archive(archive)
            .await
            .map_err(|e| StorageError::Load(alloc::format!("archive ingestion failed: {e:?}")))?;
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

    let pending = keyhive.ingest_unsorted_static_events(events).await;

    // Get hashes of pending events
    let pending_hashes: crate::collections::Set<[u8; 32]> = pending
        .iter()
        .filter_map(|e| {
            let bytes = cbor_serialize(e.as_ref()).ok()?;
            Some(*hash_event_bytes(&bytes).as_bytes())
        })
        .collect();

    // Save the new consolidated archive
    let archive = keyhive.into_archive().await;
    save_keyhive_archive(storage, storage_id, &archive).await?;

    let mut deleted_archive_count = 0;
    let mut deleted_event_count = 0;

    // Delete old archives
    for (hash, _) in &raw_archives {
        if *hash != storage_id {
            storage
                .delete_archive(*hash)
                .await
                .map_err(|e| StorageError::Delete(e.to_string()))?;
            deleted_archive_count += 1;
        }
    }

    // Delete processed events (and keep pending ones)
    for (event_bytes_hash, storage_hash) in &event_hash_to_storage {
        if !pending_hashes.contains(event_bytes_hash) {
            storage
                .delete_event(*storage_hash)
                .await
                .map_err(|e| StorageError::Delete(e.to_string()))?;
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
        storage::{KeyhiveStorage, MemoryKeyhiveStorage, StorageHash},
        test_utils::{keyhive_peer_id, make_keyhive},
    };
    use futures_kind::Local;

    #[tokio::test(flavor = "current_thread")]
    async fn save_and_load_archive_roundtrip() {
        let keyhive = make_keyhive().await;
        let storage = MemoryKeyhiveStorage::new();
        let storage_id = StorageHash::new([1u8; 32]);

        let archive = keyhive.into_archive().await;
        save_keyhive_archive::<_, _, Local>(&storage, storage_id, &archive)
            .await
            .unwrap();

        let loaded = load_archives::<[u8; 32], _, Local>(&storage).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, storage_id);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn compact_consolidates_archives_and_deletes_processed_events() {
        // Create two keyhives and exchange contact cards to generate real events
        let alice = make_keyhive().await;
        let bob = make_keyhive().await;

        let alice_cc = alice.contact_card().await.unwrap();
        let bob_cc = bob.contact_card().await.unwrap();
        alice.receive_contact_card(&bob_cc).await.unwrap();
        bob.receive_contact_card(&alice_cc).await.unwrap();

        let storage = MemoryKeyhiveStorage::new();

        // Save two separate archives (simulating multiple save points)
        let id1 = StorageHash::new([1u8; 32]);
        let archive1 = alice.into_archive().await;
        save_keyhive_archive::<_, _, Local>(&storage, id1, &archive1)
            .await
            .unwrap();

        let id2 = StorageHash::new([2u8; 32]);
        let archive2 = alice.into_archive().await;
        save_keyhive_archive::<_, _, Local>(&storage, id2, &archive2)
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
            save_event::<_, _, Local>(&storage, event).await.unwrap();
        }

        // Before compaction: 2 archives, N events
        let archives_before = KeyhiveStorage::<Local>::load_archives(&storage)
            .await
            .unwrap();
        let events_before = KeyhiveStorage::<Local>::load_events(&storage)
            .await
            .unwrap();
        assert_eq!(archives_before.len(), 2);
        assert!(!events_before.is_empty());

        // Compact
        let consolidated_id = StorageHash::new([10u8; 32]);
        compact::<_, _, _, _, _, _, _, Local>(&alice, &storage, consolidated_id)
            .await
            .unwrap();

        // After compaction: exactly 1 archive at the consolidated key
        let archives_after = KeyhiveStorage::<Local>::load_archives(&storage)
            .await
            .unwrap();
        assert_eq!(archives_after.len(), 1);
        assert_eq!(archives_after[0].0, consolidated_id);

        // Events that alice already knows about should have been deleted.
        // Alice already has these events in her keyhive state, so they are
        // "processed" — compaction ingests them, sees they aren't pending,
        // and removes them.
        let events_after = KeyhiveStorage::<Local>::load_events(&storage)
            .await
            .unwrap();
        assert_eq!(
            events_after.len(),
            0,
            "all events should be processed (alice already has them) and deleted"
        );

        // The consolidated archive should be loadable and deserializable
        let reloaded = load_archives::<[u8; 32], _, Local>(&storage).await.unwrap();
        assert_eq!(reloaded.len(), 1);
    }
}
