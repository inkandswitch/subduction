//! Storage operations for keyhive data.
//!
//! This module provides high-level functions for persisting, loading, and compacting
//! keyhive state using the [`KeyhiveStorage`] trait.

use alloc::{format, string::ToString, vec::Vec};
use core::fmt::Display;

use future_form::FutureForm;

use crate::{
    access::{KeyhiveAccess, NotIngested, Undecodable},
    collections::{Map, Set},
    error::StorageError,
    storage::{KeyhiveStorage, StorageHash},
};

/// The [`StorageError`] for a failed event ingestion.
pub(crate) fn event_ingestion_error(error: impl Display) -> StorageError {
    StorageError::Load(format!("event ingestion failed: {error}"))
}

/// Warn that a stored entry does not decode and is skipped.
///
/// Entries written in an older format (before the canonical codec) or
/// corrupted on disk would otherwise stop every later entry from loading.
fn warn_undecodable(hash: StorageHash, what: &'static str) {
    tracing::warn!(
        hash = %hash.to_hex(),
        "skipping stored keyhive {what} that does not decode (older format or corrupt)"
    );
}

/// Hash event bytes using BLAKE3 to produce a storage key.
#[must_use]
pub fn hash_event_bytes(bytes: &[u8]) -> StorageHash {
    let hash = blake3::hash(bytes);
    StorageHash::new(*hash.as_bytes())
}

/// Save a keyhive archive's bytes to storage.
///
/// The archive is stored with the provided storage ID as the key. The storage ID should be a
/// stable identifier derived from the peer ID.
///
/// # Errors
///
/// Returns [`StorageError`] if the storage write fails.
pub async fn save_keyhive_archive<S, Async>(
    storage: &S,
    storage_id: StorageHash,
    bytes: Vec<u8>,
) -> Result<(), StorageError>
where
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
{
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

/// Save an event's bytes to storage.
///
/// The bytes are stored with their BLAKE3 hash as the key.
///
/// # Errors
///
/// Returns [`StorageError`] if the storage write fails.
pub async fn save_event_bytes<S, Async>(
    storage: &S,
    bytes: Vec<u8>,
) -> Result<StorageHash, StorageError>
where
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
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

/// Load the bytes of every archive in storage.
///
/// # Errors
///
/// Returns [`StorageError`] if the storage read fails.
pub async fn load_archives<S, Async>(
    storage: &S,
) -> Result<Vec<(StorageHash, Vec<u8>)>, StorageError>
where
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
{
    storage
        .load_archives()
        .await
        .map_err(|e| StorageError::Load(e.to_string()))
}

/// Load the bytes of every event in storage.
///
/// # Errors
///
/// Returns [`StorageError`] if the storage read fails.
pub async fn load_event_bytes<S, Async>(
    storage: &S,
) -> Result<Vec<(StorageHash, Vec<u8>)>, StorageError>
where
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
{
    storage
        .load_events()
        .await
        .map_err(|e| StorageError::Load(e.to_string()))
}

/// Ingest stored archives into `keyhive`, returning the storage hashes of
/// those that do not decode.
async fn ingest_archives<K, Async>(
    keyhive: &K,
    archives: &[(StorageHash, Vec<u8>)],
) -> Result<Set<StorageHash>, StorageError>
where
    K: KeyhiveAccess<Async>,
    Async: FutureForm,
{
    let mut unreadable = Set::new();
    for (hash, bytes) in archives {
        tracing::debug!(hash = %hash.to_hex(), "ingesting archive");
        let ingested = keyhive
            .ingest_archive(bytes)
            .await
            .map_err(|e| StorageError::Load(format!("archive ingestion failed: {e:?}")))?;
        if let Err(Undecodable) = ingested {
            warn_undecodable(*hash, "archive");
            unreadable.insert(*hash);
        }
    }
    Ok(unreadable)
}

/// Ingest stored events into `keyhive`, logging those that do not decode.
async fn ingest_events<K, Async>(
    keyhive: &K,
    events: Vec<Vec<u8>>,
) -> Result<NotIngested, StorageError>
where
    K: KeyhiveAccess<Async>,
    Async: FutureForm,
{
    let not_ingested = keyhive
        .ingest_events(events)
        .await
        .map_err(event_ingestion_error)?;
    for bytes in &not_ingested.undecodable {
        warn_undecodable(hash_event_bytes(bytes), "event");
    }
    Ok(not_ingested)
}

/// Ingest all stored archives and events into a keyhive instance.
///
/// This loads all archives and events from storage and ingests them into the
/// provided keyhive. Archives are ingested first, then events. Returns
/// any pending events.
///
/// # Errors
///
/// Returns [`StorageError`] if loading or ingestion fails. Entries that do
/// not decode are logged and skipped, unless `keyhive` fails the call on
/// them instead.
pub async fn ingest_from_storage<K, S, Async>(
    keyhive: &K,
    storage: &S,
) -> Result<Vec<Vec<u8>>, StorageError>
where
    K: KeyhiveAccess<Async>,
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
{
    let archives = load_archives(storage).await?;
    tracing::debug!(count = archives.len(), "ingesting archives from storage");
    ingest_archives(keyhive, &archives).await?;

    let events: Vec<Vec<u8>> = load_event_bytes(storage)
        .await?
        .into_iter()
        .map(|(_, bytes)| bytes)
        .collect();
    tracing::debug!(count = events.len(), "ingesting events from storage");
    let pending = ingest_events(keyhive, events).await?.pending;

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
/// Entries that do not decode (an older format, or corruption) are skipped
/// and left in storage, never deleted, unless `keyhive` fails the call on
/// them instead.
///
/// # Errors
///
/// Returns [`StorageError`] if a storage operation, ingestion, or the
/// archive export fails.
pub async fn compact<K, S, Async>(
    keyhive: &K,
    storage: &S,
    storage_id: StorageHash,
) -> Result<(), StorageError>
where
    K: KeyhiveAccess<Async>,
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
{
    // Load raw data (we need hashes for cleanup)
    let raw_archives = load_archives(storage).await?;
    let raw_events = load_event_bytes(storage).await?;

    if raw_events.is_empty() && raw_archives.len() <= 1 {
        tracing::debug!("nothing to compact");
        return Ok(());
    }

    tracing::debug!(
        archives = raw_archives.len(),
        events = raw_events.len(),
        "starting compaction"
    );

    // Entries that do not decode are skipped, and left in storage: this
    // version cannot read them, but deleting them would rule out going back.
    let mut unreadable: Set<StorageHash> = ingest_archives(keyhive, &raw_archives).await?;

    // Build a map from event hash to storage hash for tracking pending events
    let event_hash_to_storage: Map<[u8; 32], StorageHash> = raw_events
        .iter()
        .map(|(storage_hash, bytes)| {
            let event_hash = hash_event_bytes(bytes);
            (*event_hash.as_bytes(), *storage_hash)
        })
        .collect();

    let not_ingested = ingest_events(
        keyhive,
        raw_events.into_iter().map(|(_, bytes)| bytes).collect(),
    )
    .await?;
    unreadable.extend(
        not_ingested
            .undecodable
            .iter()
            .filter_map(|bytes| event_hash_to_storage.get(hash_event_bytes(bytes).as_bytes())),
    );
    let pending = not_ingested.pending;

    // Get hashes of pending events
    let pending_hashes: Set<[u8; 32]> = pending
        .iter()
        .map(|bytes| *hash_event_bytes(bytes).as_bytes())
        .collect();

    // Save the new consolidated archive
    let archive = keyhive
        .to_archive()
        .await
        .map_err(|e| StorageError::Save(alloc::format!("archive export failed: {e}")))?;
    save_keyhive_archive(storage, storage_id, archive).await?;

    let mut deleted_archive_count = 0;
    let mut deleted_event_count = 0;

    // Delete old archives
    for (hash, _) in &raw_archives {
        if *hash != storage_id && !unreadable.contains(hash) {
            storage
                .delete_archive(*hash)
                .await
                .map_err(|e| StorageError::Delete(e.to_string()))?;
            deleted_archive_count += 1;
        }
    }

    // Delete processed events (and keep pending ones)
    for (event_bytes_hash, storage_hash) in &event_hash_to_storage {
        if !pending_hashes.contains(event_bytes_hash) && !unreadable.contains(storage_hash) {
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
        kept_unreadable = unreadable.len(),
        "compaction complete"
    );

    Ok(())
}

#[cfg(all(test, feature = "test-utils"))]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::{
        storage::{KeyhiveStorage, MemoryKeyhiveStorage, StorageHash},
        test_utils::{keyhive_peer_id, make_keyhive},
    };
    use future_form::Local;

    #[tokio::test(flavor = "current_thread")]
    async fn compact_consolidates_archives_and_deletes_processed_events() {
        // Create two keyhives and exchange contact cards to generate real events
        let alice = make_keyhive().await;
        let bob = make_keyhive().await;

        let alice_cc = alice.generate_contact_card().await.unwrap();
        let bob_cc = bob.generate_contact_card().await.unwrap();
        alice.receive_contact_card(&bob_cc).await.unwrap();
        bob.receive_contact_card(&alice_cc).await.unwrap();

        let storage = MemoryKeyhiveStorage::new();

        // Save two separate archives (simulating multiple save points)
        let id1 = StorageHash::new([1u8; 32]);
        let archive1 = KeyhiveAccess::<Local>::to_archive(&alice).await.unwrap();
        save_keyhive_archive::<_, Local>(&storage, id1, archive1)
            .await
            .unwrap();

        let id2 = StorageHash::new([2u8; 32]);
        let archive2 = KeyhiveAccess::<Local>::to_archive(&alice).await.unwrap();
        save_keyhive_archive::<_, Local>(&storage, id2, archive2)
            .await
            .unwrap();

        // Save alice's real events to storage
        let alice_peer_id = keyhive_peer_id(&alice);
        let alice_id = alice_peer_id.to_identifier().unwrap();
        let events = KeyhiveAccess::<Local>::events_for_agent(&alice, alice_id, None)
            .await
            .unwrap();
        assert!(
            !events.is_empty(),
            "contact card exchange should produce events"
        );

        for event in events.into_values() {
            save_event_bytes::<_, Local>(&storage, event).await.unwrap();
        }

        let archives_before = KeyhiveStorage::<Local>::load_archives(&storage)
            .await
            .unwrap();
        let events_before = KeyhiveStorage::<Local>::load_events(&storage)
            .await
            .unwrap();
        assert_eq!(archives_before.len(), 2);
        assert!(!events_before.is_empty());

        let consolidated_id = StorageHash::new([10u8; 32]);
        compact::<_, _, Local>(&alice, &storage, consolidated_id)
            .await
            .unwrap();

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

        let reloaded = make_keyhive().await;
        KeyhiveAccess::<Local>::ingest_archive(&reloaded, &archives_after[0].1)
            .await
            .unwrap()
            .unwrap();
        assert!(
            KeyhiveAccess::<Local>::is_known_agent(&reloaded, alice_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unreadable_entries_are_skipped_and_kept() -> testresult::TestResult {
        let alice = make_keyhive().await;
        let storage = MemoryKeyhiveStorage::new();

        let stale_archive = StorageHash::new([0u8; 32]);
        let readable_archive = StorageHash::new([1u8; 32]);
        let archive = KeyhiveAccess::<Local>::to_archive(&alice).await?;
        let stale_bytes = b"pre-codec bytes".to_vec();

        // In this order, a load that stopped at the stale archive would miss
        // the readable one.
        let reloaded = make_keyhive().await;
        let unreadable = ingest_archives::<_, Local>(
            &reloaded,
            &[
                (stale_archive, stale_bytes.clone()),
                (readable_archive, archive.clone()),
            ],
        )
        .await?;
        assert_eq!(unreadable, Set::from([stale_archive]));
        let alice_id = keyhive_peer_id(&alice).to_identifier()?;
        assert!(KeyhiveAccess::<Local>::is_known_agent(&reloaded, alice_id).await?);

        save_keyhive_archive::<_, Local>(&storage, readable_archive, archive).await?;
        KeyhiveStorage::<Local>::save_archive(&storage, stale_archive, stale_bytes).await?;
        let stale_event =
            save_event_bytes::<_, Local>(&storage, b"pre-codec event".to_vec()).await?;

        let consolidated = StorageHash::new([10u8; 32]);
        compact::<_, _, Local>(&alice, &storage, consolidated).await?;

        let raw_archives: Set<StorageHash> = KeyhiveStorage::<Local>::load_archives(&storage)
            .await?
            .into_iter()
            .map(|(hash, _)| hash)
            .collect();
        assert_eq!(raw_archives, Set::from([consolidated, stale_archive]));

        let raw_events: alloc::vec::Vec<StorageHash> =
            KeyhiveStorage::<Local>::load_events(&storage)
                .await?
                .into_iter()
                .map(|(hash, _)| hash)
                .collect();
        assert_eq!(raw_events, [stale_event]);
        Ok(())
    }
}
