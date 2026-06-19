//! The crate error type.

use std::path::{Path, PathBuf};

use thiserror::Error;

/// Errors that can occur during redb storage operations.
#[derive(Debug, Error)]
pub enum RedbStorageError {
    /// A filesystem operation on an external blob file (or the on-disk store
    /// layout) failed. Carries the [operation](FileOp) and path involved.
    #[error(transparent)]
    File(#[from] FileError),

    /// Failed to open or create the database.
    #[error(transparent)]
    Database(#[from] redb::DatabaseError),

    /// Failed to begin a transaction.
    #[error(transparent)]
    Transaction(#[from] redb::TransactionError),

    /// Failed to open a table.
    #[error(transparent)]
    Table(#[from] redb::TableError),

    /// Low-level storage failure.
    #[error(transparent)]
    Storage(#[from] redb::StorageError),

    /// Failed to commit a transaction.
    #[error(transparent)]
    Commit(#[from] redb::CommitError),

    /// A blocking storage task panicked or was cancelled.
    #[error("blocking storage task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// A filesystem operation on an external blob file (or the on-disk store
/// layout) failed, naming the [operation](FileOp) and path involved.
///
/// The external blob store is a content-addressed file tree beside the redb
/// database. Surfacing the exact step (write, fsync, rename, …) and path makes
/// a failure such as `ENOSPC` actionable rather than an opaque "I/O error".
#[derive(Debug, Error)]
#[error("failed to {op} `{}`: {source}", path.display())]
pub struct FileError {
    op: FileOp,
    path: PathBuf,
    source: std::io::Error,
}

impl FileError {
    pub(crate) fn new(op: FileOp, path: &Path, source: std::io::Error) -> Self {
        Self {
            op,
            path: path.to_path_buf(),
            source,
        }
    }

    /// The filesystem operation that failed.
    #[must_use]
    pub const fn op(&self) -> FileOp {
        self.op
    }

    /// The path the failing operation targeted.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// The filesystem operation that produced a [`FileError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FileOp {
    /// Create the store root or a `blobs/` (sub)directory.
    CreateDir,

    /// Fsync a directory so its newly added entries are durable.
    SyncDir,

    /// Create the temporary file backing an atomic blob write.
    CreateTemp,

    /// Write blob bytes to the temporary file.
    Write,

    /// Fsync a blob file's contents.
    Sync,

    /// Rename the temporary file into its content-addressed name.
    Rename,

    /// Read an external blob file.
    Read,

    /// Stat an external blob file to check its existence and size.
    Stat,
}

impl FileOp {
    const fn as_str(self) -> &'static str {
        match self {
            Self::CreateDir => "create directory",
            Self::SyncDir => "fsync directory",
            Self::CreateTemp => "create temp file",
            Self::Write => "write blob file",
            Self::Sync => "fsync blob file",
            Self::Rename => "rename blob file into place",
            Self::Read => "read blob file",
            Self::Stat => "stat blob file",
        }
    }
}

impl core::fmt::Display for FileOp {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Attach filesystem [operation](FileOp) and path context to an
/// [`std::io::Result`], yielding a [`FileError`] on failure.
pub(crate) trait FileContext<T> {
    fn file_context(self, op: FileOp, path: &Path) -> Result<T, FileError>;
}

impl<T> FileContext<T> for std::io::Result<T> {
    fn file_context(self, op: FileOp, path: &Path) -> Result<T, FileError> {
        self.map_err(|source| FileError::new(op, path, source))
    }
}
