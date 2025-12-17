//! # Subduction

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod connection;
pub mod peer;
pub mod run;
pub mod storage;
pub mod subduction;

pub use subduction::Subduction;
