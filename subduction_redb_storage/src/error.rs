//! The crate error type.

use thiserror::Error;

/// Errors that can occur during redb storage operations.
#[derive(Debug, Error)]
pub enum RedbStorageError {
    /// I/O error (external blob files or directory setup).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

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
