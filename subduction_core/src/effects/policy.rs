//! The authorization capabilities.
//!
//! A node checks connections and storage access against one policy object,
//! so both capabilities share the `Policy` associated type.
//! [`HasConnectionPolicy`] refines [`HasStoragePolicy`] rather than adding
//! a second slot. A consumer that only gates fetches and puts, like
//! [`SyncHandler`](crate::handler::sync::SyncHandler), asks for
//! [`HasStoragePolicy`] and accepts policies that never see a connection.

use core::error::Error;

use future_form::FutureForm;

use crate::policy::{connection::ConnectionPolicy, storage::StoragePolicy};

/// Authorization for fetches and puts.
///
/// The policy's rejection types are surfaced as associated types, so callers
/// write `E::FetchDisallowed` instead of a projection through `E::Policy`.
pub trait HasStoragePolicy<Async: FutureForm> {
    /// The policy type.
    type Policy: StoragePolicy<
            Async,
            FetchDisallowed = Self::FetchDisallowed,
            PutDisallowed = Self::PutDisallowed,
        >;

    /// Why the policy rejected a fetch.
    ///
    /// The bounds here and on `PutDisallowed` mirror [`StoragePolicy`]'s: an
    /// equality bound does not carry the target's bounds over. Keep them in
    /// sync.
    type FetchDisallowed: Error;

    /// Why the policy rejected a put.
    type PutDisallowed: Error;
}

/// Authorization for connections, on top of [`HasStoragePolicy`].
///
/// Blanket-implemented for every [`HasStoragePolicy`] whose policy is also
/// a [`ConnectionPolicy`]. Never implement it directly.
pub trait HasConnectionPolicy<Async: FutureForm>:
    HasStoragePolicy<Async, Policy: ConnectionPolicy<Async>>
{
}

impl<Async: FutureForm, E: HasStoragePolicy<Async, Policy: ConnectionPolicy<Async>>>
    HasConnectionPolicy<Async> for E
{
}

/// The policy's connection rejection type for effects `E`.
///
/// An alias rather than an associated type: [`HasConnectionPolicy`] is
/// blanket-implemented, so it has no impl in which to name one.
pub type ConnectionDisallowed<Async, E> =
    <<E as HasStoragePolicy<Async>>::Policy as ConnectionPolicy<Async>>::ConnectionDisallowed;
