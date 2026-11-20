//! The minimized-tree capability.

use future_form::FutureForm;

use crate::minimized_tree_store::MinimizedTreeStore;

/// Keeps the minimized form of each tree the node serves.
///
/// Derived data only: see [`MinimizedTreeStore`] for the contract.
pub trait HasMinimizedTreeStore<Async: FutureForm> {
    /// The store type.
    type MinimizedTreeStore: MinimizedTreeStore<Async>;
}
