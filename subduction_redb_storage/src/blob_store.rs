//! External content-addressed blob files for blobs over the inline
//! threshold: `blobs/{hex[0..2]}/{digest_hex}`, one flat file per blob,
//! deduplicated by content digest.
//!
//! Files are written durably (temp file fsynced before rename, bucket
//! directory fsynced after) **before** the referencing database transaction
//! commits, so a stored record always points at a complete file. A crash in
//! between leaves at most a harmless content-addressed orphan.

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::error::{FileContext, FileError, FileOp, RedbStorageError};

/// Process-wide counter distinguishing concurrent writers that target the
/// same content-addressed blob path.
static TMP_NONCE: AtomicU64 = AtomicU64::new(0);

/// Lowercase hex of a 32-byte digest.
pub(crate) fn hex_encode(digest: &[u8; 32]) -> String {
    use core::fmt::Write;
    digest
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Path of the external blob file for `digest`:
/// `blobs/{hex[0..2]}/{digest_hex}`.
pub(crate) fn blob_file_path(blobs_dir: &Path, digest: &[u8; 32]) -> PathBuf {
    let hex = hex_encode(digest);
    let (bucket, _) = hex.split_at(2);
    blobs_dir.join(bucket).join(&hex)
}

/// Durably write an external blob file (CAS: a no-op if an intact file for
/// this digest already exists).
///
/// Must complete — including fsyncs — *before* the database transaction
/// referencing the digest commits, so a stored record always points at a
/// complete file. Crash before the commit leaves only a harmless
/// content-addressed orphan. Must be called from a blocking context.
pub(crate) fn write_blob_file_sync(
    blobs_dir: &Path,
    digest: &[u8; 32],
    data: &[u8],
) -> Result<(), RedbStorageError> {
    use std::io::Write;

    let hex = hex_encode(digest);
    let (bucket, _) = hex.split_at(2);
    let bucket_dir = blobs_dir.join(bucket);
    let path = bucket_dir.join(&hex);

    // CAS: skip only if the existing file is intact (a crash-truncated file
    // is rewritten — same lesson as the filesystem backend).
    if let Ok(existing) = std::fs::metadata(&path) {
        if existing.len() == data.len() as u64 {
            return Ok(());
        }

        tracing::warn!(
            path = %path.display(),
            have = existing.len(),
            need = data.len(),
            "rewriting corrupt external blob file (size mismatch)"
        );
    }

    // Bucket dirs are one level below `blobs/`; if this one is new, fsync
    // `blobs/` so the bucket's link survives a crash (the constructor made
    // `blobs/` itself durable).
    if std::fs::metadata(&bucket_dir).is_err() {
        std::fs::create_dir_all(&bucket_dir).file_context(FileOp::CreateDir, &bucket_dir)?;
        fsync_dir_sync(blobs_dir)?;
    }

    // Temp-then-rename with the fsyncs ordered for crash consistency:
    // contents durable before the name appears, name durable before return.
    // The guard removes the temp on any early error exit (e.g. ENOSPC), so
    // failures don't strand `.tmp` files that worsen a full disk on retry.
    let nonce = TMP_NONCE.fetch_add(1, Ordering::Relaxed);
    let mut temp = TempFileGuard {
        path: path.with_extension(format!("{nonce}.tmp")),
        renamed: false,
    };

    let mut file =
        std::fs::File::create(&temp.path).file_context(FileOp::CreateTemp, &temp.path)?;
    file.write_all(data)
        .file_context(FileOp::Write, &temp.path)?;
    file.sync_all().file_context(FileOp::Sync, &temp.path)?;
    drop(file);

    std::fs::rename(&temp.path, &path).file_context(FileOp::Rename, &path)?;
    temp.renamed = true;
    fsync_dir_sync(&bucket_dir)?;

    Ok(())
}

/// Fsync a directory so its entries survive a crash.
///
/// No-op on non-Unix targets: `std` cannot open a directory handle on
/// Windows (it would need `FILE_FLAG_BACKUP_SEMANTICS`), and Windows has
/// no portable directory-fsync concept — directory-entry durability there
/// is best-effort.
pub(crate) fn fsync_dir_sync(dir: &Path) -> Result<(), RedbStorageError> {
    #[cfg(unix)]
    std::fs::File::open(dir)
        .and_then(|handle| handle.sync_all())
        .file_context(FileOp::SyncDir, dir)?;
    #[cfg(not(unix))]
    let _ = dir;

    Ok(())
}

/// Removes its temp file on drop unless the rename into place completed.
struct TempFileGuard {
    path: PathBuf,
    renamed: bool,
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if !self.renamed {
            std::fs::remove_file(&self.path).ok();
        }
    }
}

/// Read an external blob file. `Ok(None)` if absent.
pub(crate) fn read_blob_file_sync(
    blobs_dir: &Path,
    digest: &[u8; 32],
) -> Result<Option<Vec<u8>>, RedbStorageError> {
    let path = blob_file_path(blobs_dir, digest);
    match std::fs::read(&path) {
        Ok(data) => Ok(Some(data)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(FileError::new(FileOp::Read, &path, e).into()),
    }
}
