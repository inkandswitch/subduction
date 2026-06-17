//! Resolving verified items into pending B+tree inserts, and staging their
//! external blob files.
//!
//! External blob files are staged ([`stage_external_blobs_sync`]) *before*
//! the write transaction opens — redb has a single database-wide writer, so
//! file I/O inside an open transaction would stall every other writer — then
//! the B+tree inserts ([`insert_compound`]) run inside the transaction.

use std::path::Path;

use redb::ReadableTable;
use sedimentree_core::{
    blob::has_meta::HasBlobMeta, crypto::digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_crypto::verified_meta::VerifiedMeta;

use crate::{
    blob_store::write_blob_file_sync,
    codec::{encode_external, encode_inline},
    error::RedbStorageError,
    key::{Key96, key96},
};

/// Resolved write for one item: key + byte payloads + the blob's own
/// content digest (names the external file when the blob is large).
pub(crate) struct PendingInsert {
    key: Key96,
    meta: Vec<u8>,
    blob: Vec<u8>,
    blob_digest: [u8; 32],
}

/// Resolve a verified commit into its key and payloads.
pub(crate) fn pending_commit(
    tree: SedimentreeId,
    verified: &VerifiedMeta<LooseCommit>,
) -> PendingInsert {
    let digest = Digest::hash(verified.payload());
    PendingInsert {
        key: key96(tree, verified.payload().head(), digest.as_bytes()),
        meta: verified.signed().as_bytes().to_vec(),
        blob: verified.blob().contents().clone(),
        blob_digest: *verified.payload().blob_meta().digest().as_bytes(),
    }
}

/// Resolve a verified fragment into its key and payloads.
pub(crate) fn pending_fragment(
    tree: SedimentreeId,
    verified: &VerifiedMeta<Fragment>,
) -> PendingInsert {
    let digest = Digest::hash(verified.payload());
    PendingInsert {
        key: key96(tree, verified.payload().head(), digest.as_bytes()),
        meta: verified.signed().as_bytes().to_vec(),
        blob: verified.blob().contents().clone(),
        blob_digest: *verified.payload().blob_meta().digest().as_bytes(),
    }
}

/// Durably write the external blob files for every over-threshold item.
///
/// Runs **before** `begin_write()`: redb has a single database-wide writer,
/// so file fsyncs done inside an open write transaction would stall every
/// other writer (all trees, all peers) for the duration of the blob I/O.
/// Staging first keeps the exclusive window down to the B+tree inserts and
/// one commit fsync, with identical crash consistency — the only invariant
/// is "file durable before the referencing transaction commits", and a
/// crash before that commit leaves only harmless content-addressed
/// orphans. Must be called from a blocking context.
pub(crate) fn stage_external_blobs_sync(
    items: &[PendingInsert],
    blobs_dir: &Path,
    inline_threshold: usize,
) -> Result<(), RedbStorageError> {
    for item in items {
        if item.blob.len() > inline_threshold {
            write_blob_file_sync(blobs_dir, &item.blob_digest, &item.blob)?;
        }
    }
    Ok(())
}

/// Insert one compound item (CAS: no-op if the key already exists).
///
/// Pure B+tree work: any external blob file must already have been staged
/// via [`stage_external_blobs_sync`] before the surrounding transaction
/// commits.
pub(crate) fn insert_compound(
    table: &mut redb::Table<'_, &'static [u8; 96], &'static [u8]>,
    item: &PendingInsert,
    inline_threshold: usize,
) -> Result<(), RedbStorageError> {
    if table.get(&item.key)?.is_some() {
        return Ok(());
    }

    if item.blob.len() > inline_threshold {
        table.insert(&item.key, encode_external(&item.meta).as_slice())?;
    } else {
        table.insert(&item.key, encode_inline(&item.meta, &item.blob).as_slice())?;
    }

    Ok(())
}
