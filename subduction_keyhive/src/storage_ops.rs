//! Storage operations for keyhive data.
//!
//! This module provides high-level functions for persisting, loading, and compacting
//! keyhive state using the [`KeyhiveStorage`] trait.

use alloc::{string::ToString, sync::Arc, vec::Vec};

use future_form::FutureForm;
use keyhive_codec::traits::{Decode, Encode};
use keyhive_content::{
    reference::ContentRef,
    store::{CiphertextStore, CiphertextStoreExt},
};
use keyhive_core::{
    archive::Archive, content::heads::ContentHeads, event::static_event::StaticEvent,
    keyhive::Keyhive, listener::membership::MembershipListener,
};
use keyhive_crypto::signer::async_signer::AsyncSigner;
use keyline::contract::Keyline;

use crate::{
    collections::{Map, Set},
    error::StorageError,
    storage::{KeyhiveStorage, StorageHash},
};

// Events and archives use Keyhive's canonical codec. An event's bytes are its
// statement's `KH` frame, the same bytes `keyhive_wasm` sends from JS.

/// Encode a value with Keyhive's canonical codec.
pub(crate) fn encode_value<T: Encode>(value: &T) -> Vec<u8> {
    value.encode().into_bytes()
}

/// Decode a value with Keyhive's canonical codec, accepting only its
/// canonical encoding.
pub(crate) fn decode_value<T: Decode>(bytes: &[u8]) -> Result<T, StorageError> {
    T::decode(bytes).map_err(|e| StorageError::Deserialization(e.to_string()))
}

/// Decode a stored entry, or log and skip it.
///
/// Entries written in an older format (before the canonical codec) or
/// corrupted on disk would otherwise stop every later entry from loading.
fn decode_stored<T: Decode>(hash: StorageHash, bytes: &[u8], what: &'static str) -> Option<T> {
    match decode_value(bytes) {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::warn!(
                hash = %hash.to_hex(),
                %error,
                "skipping stored keyhive {what} that does not decode (older format or corrupt)"
            );
            None
        }
    }
}

/// Hash event bytes using BLAKE3 to produce a storage key.
#[must_use]
pub fn hash_event_bytes(bytes: &[u8]) -> StorageHash {
    let hash = blake3::hash(bytes);
    StorageHash::new(*hash.as_bytes())
}

/// Serialize and save a keyhive archive to storage.
///
/// The archive is encoded with Keyhive's canonical codec and stored with the provided storage ID
/// as the key. The storage ID should be a stable identifier derived from the peer ID.
///
/// # Errors
///
/// Returns [`StorageError`] if the storage write fails.
pub async fn save_keyhive_archive<T, S, Async>(
    storage: &S,
    storage_id: StorageHash,
    archive: &Archive<T>,
) -> Result<(), StorageError>
where
    T: ContentRef,
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
{
    // The archive holds this replica's secret keys. `encode_zeroizing` writes
    // them into one exact-sized buffer, so no reallocation leaves a copy
    // behind, and the buffer moves to storage without another copy.
    let bytes = core::mem::take(&mut *archive.encode_zeroizing());

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

/// Whether `bytes` are an archive in the format this version writes, judged
/// by its header alone.
///
/// That is the question setting an archive aside needs answered: an archive
/// from an older or newer version carries another magic or format byte. It
/// costs nothing, where decoding a whole archive (keys, documents, every
/// statement) to find out is expensive.
fn in_current_archive_format<T: ContentRef>(bytes: &[u8]) -> bool {
    bytes.split_first_chunk::<4>().is_some_and(|(magic, rest)| {
        *magic == Archive::<T>::MAGIC && rest.first() == Some(&Archive::<T>::FORMAT)
    })
}

/// Copy the archive `bytes`, about to be overwritten at `storage_id`, to a key
/// derived from them, so this version replacing it doesn't lose it.
///
/// Archives are read by listing them all, so a version that can decode the
/// copy still finds it, and compaction never deletes an archive it cannot
/// decode.
///
/// # Errors
///
/// Returns [`StorageError`] if saving the copy fails.
async fn set_aside_archive<S, Async>(
    storage: &S,
    storage_id: StorageHash,
    bytes: &[u8],
) -> Result<(), StorageError>
where
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
{
    let aside = hash_event_bytes(bytes);
    tracing::warn!(
        from = %storage_id.to_hex(),
        to = %aside.to_hex(),
        "keeping an archive this version cannot decode under a new key before replacing it"
    );
    storage
        .save_archive(aside, bytes.to_vec())
        .await
        .map_err(|e| StorageError::Save(e.to_string()))
}

/// Save `archive` at `storage_id`, first setting aside an archive already
/// there in another version's format.
///
/// # Errors
///
/// Returns [`StorageError`] if loading the stored archives, saving the copy,
/// or saving `archive` fails.
pub async fn replace_keyhive_archive<T, S, Async>(
    storage: &S,
    storage_id: StorageHash,
    archive: &Archive<T>,
) -> Result<(), StorageError>
where
    T: ContentRef,
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
{
    let existing = storage
        .load_archives()
        .await
        .map_err(|e| StorageError::Load(e.to_string()))?;
    if let Some((_, bytes)) = existing
        .iter()
        .find(|(hash, bytes)| *hash == storage_id && !in_current_archive_format::<T>(bytes))
    {
        set_aside_archive(storage, storage_id, bytes).await?;
    }
    save_keyhive_archive(storage, storage_id, archive).await
}

/// Serialize and save an event to storage.
///
/// The event is encoded as its statement's `KH` frame (matching `keyhive_wasm`) and
/// stored with its BLAKE3 hash as the key.
///
/// # Errors
///
/// Returns [`StorageError`] if the storage write fails.
pub async fn save_event<T, S, Async>(
    storage: &S,
    event: &StaticEvent<T>,
) -> Result<StorageHash, StorageError>
where
    T: ContentRef,
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
{
    let bytes = encode_value(event);
    save_event_bytes(storage, bytes).await
}

/// Save raw event bytes to storage.
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

/// Load and deserialize all archives from storage.
///
/// Archives that do not decode are skipped with a warning.
///
/// # Errors
///
/// Returns [`StorageError`] if the storage read fails.
pub async fn load_archives<T, S, Async>(
    storage: &S,
) -> Result<Vec<(StorageHash, Archive<T>)>, StorageError>
where
    T: ContentRef,
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
{
    let raw_archives = storage
        .load_archives()
        .await
        .map_err(|e| StorageError::Load(e.to_string()))?;

    let archives: Vec<(StorageHash, Archive<T>)> = raw_archives
        .into_iter()
        .filter_map(|(hash, bytes)| Some((hash, decode_stored(hash, &bytes, "archive")?)))
        .collect();

    tracing::debug!(count = archives.len(), "loaded archives from storage");
    Ok(archives)
}

/// Load and deserialize all events from storage.
///
/// Events that do not decode are skipped with a warning.
///
/// # Errors
///
/// Returns [`StorageError`] if the storage read fails.
pub async fn load_events<T, S, Async>(
    storage: &S,
) -> Result<Vec<(StorageHash, StaticEvent<T>)>, StorageError>
where
    T: ContentRef,
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
{
    let raw_events = storage
        .load_events()
        .await
        .map_err(|e| StorageError::Load(e.to_string()))?;

    let events: Vec<(StorageHash, StaticEvent<T>)> = raw_events
        .into_iter()
        .filter_map(|(hash, bytes)| Some((hash, decode_stored(hash, &bytes, "event")?)))
        .collect();

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

/// Ingest all stored archives and events into a keyhive instance.
///
/// This loads all archives and events from storage and ingests them into the
/// provided keyhive. Archives are ingested first, then events. Returns
/// any pending events.
///
/// # Errors
///
/// Returns [`StorageError`] if loading, deserialization, or archive ingestion fails.
pub async fn ingest_from_storage<Async, Signer, T, P, C, L, R, K, S>(
    keyhive: &Keyhive<Async, Signer, T, P, C, L, R, K>,
    storage: &S,
) -> Result<Vec<Arc<StaticEvent<T>>>, StorageError>
where
    Signer: AsyncSigner<Async> + Clone,
    T: ContentRef,
    P: keyhive_codec::traits::Decode,
    C: CiphertextStore<Async, T, P> + CiphertextStoreExt<Async, T, P> + Clone,
    L: MembershipListener<Async, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<Async, RetentionWatermark = ContentHeads<T>>,
    K::Error: Send + Sync + 'static,
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
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
/// Entries that do not decode (an older format, or corruption) are skipped
/// and left in storage, never deleted.
///
/// # Errors
///
/// Returns [`StorageError`] if a storage operation or archive ingestion
/// fails.
pub async fn compact<Async, Signer, T, P, C, L, R, K, S>(
    keyhive: &Keyhive<Async, Signer, T, P, C, L, R, K>,
    storage: &S,
    storage_id: StorageHash,
) -> Result<(), StorageError>
where
    Signer: AsyncSigner<Async> + Clone,
    T: ContentRef,
    P: keyhive_codec::traits::Decode,
    C: CiphertextStore<Async, T, P> + CiphertextStoreExt<Async, T, P> + Clone,
    L: MembershipListener<Async, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<Async, RetentionWatermark = ContentHeads<T>>,
    K::Error: Send + Sync + 'static,
    S: KeyhiveStorage<Async>,
    Async: FutureForm,
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

    // Archives in another version's format are kept as they are, so they
    // don't count: with no events and at most one archive this version can
    // read, there is nothing to consolidate.
    let current_archives = raw_archives
        .iter()
        .filter(|(_, bytes)| in_current_archive_format::<T>(bytes))
        .count();
    if raw_events.is_empty() && current_archives <= 1 {
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
    let mut unreadable: Set<StorageHash> = Set::new();

    // Deserialize and ingest archives
    for (hash, bytes) in &raw_archives {
        let Some(archive) = decode_stored::<Archive<T>>(*hash, bytes, "archive") else {
            unreadable.insert(*hash);
            continue;
        };
        tracing::debug!(hash = %hash.to_hex(), "ingesting archive for compaction");
        keyhive
            .ingest_archive(archive)
            .await
            .map_err(|e| StorageError::Load(alloc::format!("archive ingestion failed: {e:?}")))?;
    }

    // Build a map from event hash to storage hash for tracking pending events
    let event_hash_to_storage: Map<[u8; 32], StorageHash> = raw_events
        .iter()
        .map(|(storage_hash, bytes)| {
            let event_hash = hash_event_bytes(bytes);
            (*event_hash.as_bytes(), *storage_hash)
        })
        .collect();

    // Deserialize and ingest events
    let mut events: Vec<StaticEvent<T>> = Vec::with_capacity(raw_events.len());
    for (hash, bytes) in &raw_events {
        match decode_stored(*hash, bytes, "event") {
            Some(event) => events.push(event),
            None => {
                unreadable.insert(*hash);
            }
        }
    }

    let pending = keyhive.ingest_unsorted_static_events(events).await;

    // Get hashes of pending events
    let pending_hashes: Set<[u8; 32]> = pending
        .iter()
        .map(|e| *hash_event_bytes(&encode_value(e.as_ref())).as_bytes())
        .collect();

    // Save the new consolidated archive
    let archive = keyhive
        .into_archive()
        .await
        .map_err(|e| StorageError::Save(alloc::format!("archive export failed: {e}")))?;
    // The ingest pass above already decoded what is at `storage_id`. The
    // deletion pass below only sees `raw_archives`, so a set-aside copy is
    // kept.
    if unreadable.contains(&storage_id)
        && let Some((_, bytes)) = raw_archives.iter().find(|(hash, _)| *hash == storage_id)
    {
        set_aside_archive(storage, storage_id, bytes).await?;
    }
    save_keyhive_archive(storage, storage_id, &archive).await?;

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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::{
        storage::{KeyhiveStorage, MemoryKeyhiveStorage, StorageHash},
        test_utils::{keyhive_peer_id, make_keyhive},
    };
    use future_form::Local;
    use testresult::TestResult;

    #[tokio::test(flavor = "current_thread")]
    async fn save_and_load_archive_roundtrip() {
        let keyhive = make_keyhive().await;
        let storage = MemoryKeyhiveStorage::new();
        let storage_id = StorageHash::new([1u8; 32]);

        let archive = keyhive.into_archive().await.expect("memory keyline");
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

        let alice_cc = alice.generate_contact_card().await.unwrap();
        let bob_cc = bob.generate_contact_card().await.unwrap();
        alice.receive_contact_card(&bob_cc).await.unwrap();
        bob.receive_contact_card(&alice_cc).await.unwrap();

        let storage = MemoryKeyhiveStorage::new();

        // Save two separate archives (simulating multiple save points)
        let id1 = StorageHash::new([1u8; 32]);
        let archive1 = alice.into_archive().await.expect("memory keyline");
        save_keyhive_archive::<_, _, Local>(&storage, id1, &archive1)
            .await
            .unwrap();

        let id2 = StorageHash::new([2u8; 32]);
        let archive2 = alice.into_archive().await.expect("memory keyline");
        save_keyhive_archive::<_, _, Local>(&storage, id2, &archive2)
            .await
            .unwrap();

        // Save alice's real events to storage
        let alice_peer_id = keyhive_peer_id(&alice);
        let alice_id = alice_peer_id.to_identifier().unwrap();
        let events = alice.static_events_for_agent(alice_id).await.unwrap();
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
        compact::<Local, _, _, _, _, _, _, _, _>(&alice, &storage, consolidated_id)
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

    /// The archives in `storage`, by key.
    async fn raw_archives(
        storage: &MemoryKeyhiveStorage,
    ) -> TestResult<alloc::collections::BTreeMap<StorageHash, alloc::vec::Vec<u8>>> {
        Ok(KeyhiveStorage::<Local>::load_archives(storage)
            .await?
            .into_iter()
            .collect())
    }

    /// Compaction writes its archive at `storage_id`. If what is already
    /// there does not decode, it is kept under another key, not overwritten.
    #[tokio::test(flavor = "current_thread")]
    async fn compaction_keeps_an_unreadable_archive_at_its_own_key() -> TestResult {
        let alice = make_keyhive().await;
        let storage = MemoryKeyhiveStorage::new();
        let storage_id = StorageHash::new([10u8; 32]);
        let stale = b"pre-codec archive".to_vec();
        KeyhiveStorage::<Local>::save_archive(&storage, storage_id, stale.clone()).await?;
        // Two readable archives, so there is something to consolidate.
        for key in [[1u8; 32], [2u8; 32]] {
            save_keyhive_archive::<_, _, Local>(
                &storage,
                StorageHash::new(key),
                &alice.into_archive().await?,
            )
            .await?;
        }

        compact::<Local, _, _, _, _, _, _, _, _>(&alice, &storage, storage_id).await?;

        let archives = raw_archives(&storage).await?;
        assert_eq!(
            archives.get(&hash_event_bytes(&stale)),
            Some(&stale),
            "the unreadable archive is kept"
        );
        assert!(
            archives
                .get(&storage_id)
                .is_some_and(|bytes| decode_value::<Archive<[u8; 32]>>(bytes).is_ok()),
            "storage_id holds the new, readable archive"
        );
        assert_eq!(
            archives.len(),
            2,
            "the readable source archives were consolidated"
        );
        Ok(())
    }

    /// A set-aside archive doesn't count against compaction's shortcut: with
    /// no events and one archive this version reads, there is nothing to
    /// consolidate, so nothing is rewritten.
    #[tokio::test(flavor = "current_thread")]
    async fn a_set_aside_archive_leaves_nothing_to_compact() -> TestResult {
        let alice = make_keyhive().await;
        let storage = MemoryKeyhiveStorage::new();
        let storage_id = StorageHash::new([10u8; 32]);
        let stale = b"pre-codec archive".to_vec();
        KeyhiveStorage::<Local>::save_archive(&storage, hash_event_bytes(&stale), stale).await?;
        save_keyhive_archive::<_, _, Local>(&storage, storage_id, &alice.into_archive().await?)
            .await?;
        let before = raw_archives(&storage).await?;

        // A compaction that ran would export this into a new archive.
        alice.generate_group(alloc::vec![]).await?;
        compact::<Local, _, _, _, _, _, _, _, _>(&alice, &storage, storage_id).await?;

        assert_eq!(raw_archives(&storage).await?, before);
        Ok(())
    }

    /// The same holds outside compaction, for the archive written when a
    /// large batch is ingested.
    #[tokio::test(flavor = "current_thread")]
    async fn replacing_an_archive_keeps_an_unreadable_one() -> TestResult {
        let alice = make_keyhive().await;
        let storage = MemoryKeyhiveStorage::new();
        let storage_id = StorageHash::new([10u8; 32]);
        let stale = b"pre-codec archive".to_vec();
        KeyhiveStorage::<Local>::save_archive(&storage, storage_id, stale.clone()).await?;

        replace_keyhive_archive::<_, _, Local>(&storage, storage_id, &alice.into_archive().await?)
            .await?;

        let archives = raw_archives(&storage).await?;
        assert_eq!(archives.get(&hash_event_bytes(&stale)), Some(&stale));
        assert!(
            archives
                .get(&storage_id)
                .is_some_and(|bytes| decode_value::<Archive<[u8; 32]>>(bytes).is_ok())
        );
        Ok(())
    }

    /// Replacing a readable archive just overwrites it: nothing is set aside.
    #[tokio::test(flavor = "current_thread")]
    async fn replacing_a_readable_archive_sets_nothing_aside() -> TestResult {
        let alice = make_keyhive().await;
        let storage = MemoryKeyhiveStorage::new();
        let storage_id = StorageHash::new([10u8; 32]);
        save_keyhive_archive::<_, _, Local>(&storage, storage_id, &alice.into_archive().await?)
            .await?;

        replace_keyhive_archive::<_, _, Local>(&storage, storage_id, &alice.into_archive().await?)
            .await?;

        assert_eq!(
            raw_archives(&storage)
                .await?
                .into_keys()
                .collect::<alloc::vec::Vec<_>>(),
            [storage_id]
        );
        Ok(())
    }

    /// Entries in an older format (or corrupt) are skipped on load, and
    /// compaction leaves them in storage rather than deleting them.
    #[tokio::test(flavor = "current_thread")]
    async fn unreadable_entries_are_skipped_and_kept() -> TestResult {
        let alice = make_keyhive().await;
        let storage = MemoryKeyhiveStorage::new();

        let readable_archive = StorageHash::new([1u8; 32]);
        save_keyhive_archive::<_, _, Local>(
            &storage,
            readable_archive,
            &alice.into_archive().await?,
        )
        .await?;
        let stale_archive = StorageHash::new([2u8; 32]);
        KeyhiveStorage::<Local>::save_archive(&storage, stale_archive, b"pre-codec bytes".to_vec())
            .await?;
        let stale_event =
            save_event_bytes::<_, Local>(&storage, b"pre-codec event".to_vec()).await?;

        let archives = load_archives::<[u8; 32], _, Local>(&storage).await?;
        assert_eq!(
            archives
                .iter()
                .map(|(hash, _)| *hash)
                .collect::<alloc::vec::Vec<_>>(),
            [readable_archive]
        );
        assert!(
            load_events::<[u8; 32], _, Local>(&storage)
                .await?
                .is_empty()
        );

        let consolidated = StorageHash::new([10u8; 32]);
        compact::<Local, _, _, _, _, _, _, _, _>(&alice, &storage, consolidated).await?;

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
