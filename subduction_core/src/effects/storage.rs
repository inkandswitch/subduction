//! The persistence capability.

use core::error::Error;

use future_form::FutureForm;

use crate::storage::traits::Storage;

/// Persists sedimentree data.
///
/// The backend's error is surfaced as its own associated type, the way
/// [`IntoIterator`] surfaces `Item`, so callers write `E::StorageError`
/// instead of `<E::Storage as Storage<Async>>::Error`.
pub trait HasStorage<Async: FutureForm> {
    /// The storage backend type.
    type Storage: Storage<Async, Error = Self::StorageError>;

    /// The backend's error type.
    ///
    /// The bounds mirror [`Storage::Error`]: an equality bound does not carry
    /// the target's bounds over. Keep the two in sync.
    type StorageError: Error;
}
