//! The core of the [Sedimentree] data partitioning scheme.
//!
//! This core library only defines the metadata tracking featrues of Sedimentree.
//! We assume that the actual data described by this metadata is not legible to the Sedimentree
//! (regardless of whether or not it's encrypted).
//!
//! Sedimentree is a way of organizing data into a series of layers, or strata, each of which
//! contains a set of checkpoints (hashes) that represent some fragment of a larger file or log.
//! For example, an Automerge document might be partitioned, and each fragment encrypted.
//! Sedimentree tracks just enough metadata to allow efficient diffing and synchronization
//! of these fragments.
//!
//! [Sedimentree]: https://github.com/inkandswitch/keyhive/blob/main/design/sedimentree.md

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod blob;
pub mod cbor;
pub mod collections;
pub mod commit;
pub mod depth;
pub mod fragment;
pub mod hex;
pub mod id;
pub mod loose_commit;
pub mod sedimentree;
pub mod storage;
