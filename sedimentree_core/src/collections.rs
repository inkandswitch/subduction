//! Conditional collection types that use `HashMap`/`HashSet` when `std` is enabled,
//! and fall back to `BTreeMap`/`BTreeSet` for `no_std` environments.

pub mod nonempty_ext;

#[cfg(feature = "std")]
mod inner {
    pub use std::collections::{HashMap as Map, HashSet as Set, hash_map::Entry};
}

#[cfg(not(feature = "std"))]
mod inner {
    pub use alloc::collections::{BTreeMap as Map, BTreeSet as Set, btree_map::Entry};
}

pub use inner::{Entry, Map, Set};
