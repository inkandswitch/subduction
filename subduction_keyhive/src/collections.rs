//! Conditional collection types that use `HashMap`/`HashSet` when `std` is enabled,
//! and fall back to `BTreeMap`/`BTreeSet` for `no_std` environments.

#[cfg(feature = "std")]
mod inner {
    pub(crate) use std::collections::{HashMap as Map, HashSet as Set};
}

#[cfg(not(feature = "std"))]
mod inner {
    pub(crate) use alloc::collections::{BTreeMap as Map, BTreeSet as Set};
}

pub(crate) use inner::Map;

#[allow(unused_imports)]
// Used by protocol + storage_ops, but only with certain feature combos
pub(crate) use inner::Set;
