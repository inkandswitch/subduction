//! # Subduction

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod connection;
pub mod peer;
pub mod run;
pub mod sharded_map;
pub mod storage;
pub mod subduction;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub mod metrics;

pub use subduction::Subduction;

#[cfg(feature = "metrics")]
pub use storage::MetricsStorage;
