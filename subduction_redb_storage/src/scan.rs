//! B+tree range scans and the decode pipeline.
//!
//! Bulk loads run in phases: one blocking hop for the B+tree range scan
//! ([`scan_decoded`]), decoding in async land, then external blob files read
//! in parallel chunks on the blocking pool ([`resolve_items`]). The
//! metadata-only path ([`scan_payloads`]) decodes payloads without ever
//! reading blob bytes. Malformed or corrupt items are skipped with a warning
//! (mirroring the filesystem backend's tolerance).

use std::{path::PathBuf, sync::Arc};

use redb::ReadableTable;
use sedimentree_core::{
    blob::{Blob, has_meta::HasBlobMeta},
    codec::{decode::DecodeFields, encode::EncodeFields, schema::Schema},
    collections::Set,
    loose_commit::id::CommitId,
};
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};

use crate::{
    blob_store::{blob_file_path, hex_encode, read_blob_file_sync},
    codec::{DecodedCompound, decode_compound, split_meta},
    error::RedbStorageError,
    key::{Key96, item_id_of},
};

/// Undecoded `meta` + `blob` byte pair from one stored compound value
/// (external blobs already resolved to their file contents).
pub(crate) type RawCompound = (Vec<u8>, Vec<u8>);

/// Number of external blob files read per blocking task when bulk loads
/// resolve them (mirrors the filesystem backend's bulk-read fan-out).
const EXTERNAL_READ_CHUNK: usize = 128;

/// Decode a raw compound pair into a [`VerifiedMeta`], skip-and-warn on
/// corruption (mirrors the filesystem backend's tolerance).
fn decode_verified<T>(raw: RawCompound, what: &str) -> Option<VerifiedMeta<T>>
where
    T: HasBlobMeta + Schema + EncodeFields + DecodeFields,
{
    let (meta, blob) = raw;
    let signed: Signed<T> = match Signed::try_decode(meta) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("skipping corrupt stored {what}: {e}");
            return None;
        }
    };

    match VerifiedMeta::try_from_trusted(signed, Blob::new(blob)) {
        Ok(verified) => Some(verified),
        Err(e) => {
            tracing::warn!("skipping corrupt stored {what}: {e}");
            None
        }
    }
}

/// Scan the compound values in `range` from `table`, decoding each one
/// straight off the borrowed page (a single copy of the meta/blob bytes —
/// no intermediate whole-value `Vec`). Malformed values are skipped with a
/// warning. No file I/O.
pub(crate) fn scan_decoded(
    table: &impl ReadableTable<&'static [u8; 96], &'static [u8]>,
    lo: &Key96,
    hi: &Key96,
) -> Result<Vec<DecodedCompound>, RedbStorageError> {
    let mut out = Vec::new();
    for entry in table.range::<&[u8; 96]>(lo..=hi)? {
        let (_, value) = entry?;
        if let Some(decoded) = decode_compound(value.value()) {
            out.push(decoded);
        } else {
            tracing::warn!("skipping malformed compound value");
        }
    }
    Ok(out)
}

/// Scan the compound values in `range` and decode each one to its payload
/// `T`, **without reading blob bytes**: inline blob bytes are not copied off
/// the page, and external blob files are only `stat`-ed (existence + size),
/// never read. This is the metadata-only hydration read — `O(items)` small
/// meta copies + decodes, plus one cheap `stat` per *external* item.
///
/// Item-set parity with the blob-resolving load: an external item whose file
/// is missing or size-mismatched is skipped, exactly as `resolve_items`
/// would skip it after a full read — so metadata-only hydration sees the
/// same items as the full load. Inline items need no check (their blob is in
/// the scanned value). Malformed or corrupt items are skipped with a
/// warning. Runs entirely in the blocking closure.
pub(crate) fn scan_payloads<T>(
    table: &impl ReadableTable<&'static [u8; 96], &'static [u8]>,
    lo: &Key96,
    hi: &Key96,
    blobs_dir: &std::path::Path,
    what: &str,
) -> Result<Vec<T>, RedbStorageError>
where
    T: HasBlobMeta + Schema + EncodeFields + DecodeFields,
{
    let mut out = Vec::new();
    for entry in table.range::<&[u8; 96]>(lo..=hi)? {
        let (_, value) = entry?;
        let Some((is_external, meta)) = split_meta(value.value()) else {
            tracing::warn!("skipping malformed compound value");
            continue;
        };

        let payload = match Signed::<T>::try_decode(meta)
            .and_then(|signed| signed.try_decode_trusted_payload())
        {
            Ok(payload) => payload,
            Err(e) => {
                tracing::warn!("skipping corrupt stored {what}: {e}");
                continue;
            }
        };

        // Inline blobs are present by construction (they live in the scanned
        // value). External blobs are separate files — `stat` (not read) to
        // confirm the file exists with the signed size, matching the full
        // load's skip-on-missing / skip-on-size-mismatch behaviour.
        if is_external {
            let blob_meta = payload.blob_meta();
            let path = blob_file_path(blobs_dir, blob_meta.digest().as_bytes());
            match std::fs::metadata(&path) {
                Ok(m) if m.len() == blob_meta.size_bytes() => {}
                Ok(m) => {
                    tracing::warn!(
                        digest = %hex_encode(blob_meta.digest().as_bytes()),
                        have = m.len(),
                        need = blob_meta.size_bytes(),
                        "external blob file size mismatch (crash artifact); skipping {what}"
                    );
                    continue;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::warn!(
                        digest = %hex_encode(blob_meta.digest().as_bytes()),
                        "external blob file missing; skipping {what}"
                    );
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        out.push(payload);
    }
    Ok(out)
}

/// Decode scanned compound values into [`VerifiedMeta`]s, resolving
/// external blobs via parallel chunked file reads.
///
/// Three phases: the values arrive pre-decoded from a single B+tree scan
/// hop, the metas decode here in async land (yielding each external
/// record's blob digest from its own `BlobMeta`), and the external files
/// are then read in chunks of [`EXTERNAL_READ_CHUNK`] across blocking
/// tasks so a large tree's blob resolution is not bound by one thread's
/// sequential open/read throughput.
///
/// Undecodable metas, missing external files, and corrupt items are
/// skipped with a warning (mirroring the filesystem backend's tolerance).
pub(crate) async fn resolve_items<T>(
    blobs_dir: Arc<PathBuf>,
    items: Vec<DecodedCompound>,
    what: &str,
) -> Result<Vec<VerifiedMeta<T>>, RedbStorageError>
where
    T: HasBlobMeta + Schema + EncodeFields + DecodeFields,
{
    let mut out = Vec::with_capacity(items.len());
    let mut pending: Vec<(Signed<T>, [u8; 32], u64)> = Vec::new();

    for item in items {
        match item {
            DecodedCompound::Inline { meta, blob } => {
                if let Some(verified) = decode_verified((meta, blob), what) {
                    out.push(verified);
                }
            }
            DecodedCompound::External { meta } => match Signed::<T>::try_decode(meta) {
                Ok(signed) => match signed.try_decode_trusted_payload() {
                    Ok(payload) => {
                        let blob_meta = payload.blob_meta();
                        pending.push((
                            signed,
                            *blob_meta.digest().as_bytes(),
                            blob_meta.size_bytes(),
                        ));
                    }
                    Err(e) => tracing::warn!("skipping corrupt stored {what}: {e}"),
                },
                Err(e) => tracing::warn!("skipping corrupt stored {what}: {e}"),
            },
        }
    }

    if pending.is_empty() {
        return Ok(out);
    }

    let digests: Vec<[u8; 32]> = pending.iter().map(|(_, digest, _)| *digest).collect();
    let handles: Vec<_> = digests
        .chunks(EXTERNAL_READ_CHUNK)
        .map(|chunk| {
            let chunk = chunk.to_vec();
            let blobs_dir = Arc::clone(&blobs_dir);
            tokio::task::spawn_blocking(
                move || -> Result<Vec<Option<Vec<u8>>>, RedbStorageError> {
                    chunk
                        .iter()
                        .map(|digest| read_blob_file_sync(&blobs_dir, digest))
                        .collect()
                },
            )
        })
        .collect();

    let mut blobs = Vec::with_capacity(pending.len());
    for handle in handles {
        blobs.extend(handle.await??);
    }

    for ((signed, digest, expected_size), blob) in pending.into_iter().zip(blobs) {
        let Some(blob) = blob else {
            tracing::warn!(
                digest = %hex_encode(&digest),
                "external blob file missing; skipping {what}"
            );
            continue;
        };

        // Size-validate against the signed `BlobMeta` (the same check the
        // save-side CAS applies): a size-changed external file — a
        // truncated or extended crash artifact — must be skipped, not
        // paired silently with the meta. Such a file self-heals on the
        // next save of the same content via the size-mismatch rewrite in
        // `write_blob_file_sync`.
        //
        // This catches size changes, not same-length content corruption:
        // a same-size-corrupted file passes here (and `try_from_trusted`,
        // which does not recompute the blob digest) and is served. That is
        // not a propagation hole — the receiving peer re-verifies via
        // `VerifiedMeta::new` and rejects the blob on digest mismatch — but
        // such a file does not self-heal (its size matches, so
        // `write_blob_file_sync` won't rewrite it).
        if blob.len() as u64 != expected_size {
            tracing::warn!(
                digest = %hex_encode(&digest),
                have = blob.len(),
                need = expected_size,
                "external blob file size mismatch (crash artifact); skipping {what}"
            );
            continue;
        }

        match VerifiedMeta::try_from_trusted(signed, Blob::new(blob)) {
            Ok(verified) => out.push(verified),
            Err(e) => tracing::warn!("skipping corrupt stored {what}: {e}"),
        }
    }

    Ok(out)
}

/// Collect the item ids present in `range` (deduplicated).
pub(crate) fn scan_ids(
    table: &impl ReadableTable<&'static [u8; 96], &'static [u8]>,
    lo: &Key96,
    hi: &Key96,
) -> Result<Set<CommitId>, RedbStorageError> {
    let mut out = Set::new();
    for entry in table.range::<&[u8; 96]>(lo..=hi)? {
        let (key, _) = entry?;
        out.insert(item_id_of(key.value()));
    }
    Ok(out)
}

/// Delete every key in `range` from `table` within an open write transaction.
pub(crate) fn delete_range(
    table: &mut redb::Table<'_, &'static [u8; 96], &'static [u8]>,
    lo: &Key96,
    hi: &Key96,
) -> Result<(), RedbStorageError> {
    let keys: Vec<Key96> = table
        .range::<&[u8; 96]>(lo..=hi)?
        .map(|entry| entry.map(|(k, _)| *k.value()))
        .collect::<Result<_, _>>()?;

    for key in &keys {
        table.remove(key)?;
    }

    Ok(())
}
