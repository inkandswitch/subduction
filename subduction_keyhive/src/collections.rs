//! Conditional collection types that use `HashMap`/`HashSet` when `std` is enabled,
//! and fall back to `BTreeMap`/`BTreeSet` for `no_std` environments.

#[cfg(feature = "std")]
mod inner {
    pub(crate) use std::collections::HashMap as Map;
    #[allow(dead_code)]
    pub(crate) use std::collections::HashSet as Set;
}

#[cfg(not(feature = "std"))]
mod inner {
    pub(crate) use alloc::collections::BTreeMap as Map;
    #[allow(dead_code)]
    pub(crate) use alloc::collections::BTreeSet as Set;
}

pub(crate) use inner::Map;
#[allow(dead_code, unused_imports)]
pub(crate) use inner::Set;
