//! Filesystem-based storage for Sedimentree.
//!
//! This crate provides [`FsStorage`], a content-addressed filesystem storage
//! implementation that implements the [`Storage`] trait from `subduction_core`.
//!
//! # Storage Layout
//!
//! ```text
//! root/
//! ├── trees/
//! │   └── {sedimentree_id_hex}/
//! │       ├── commits/
//! │       │   └── {digest_hex}.cbor  ← Signed<LooseCommit>
//! │       └── fragments/
//! │           └── {digest_hex}.cbor  ← Signed<Fragment>
//! └── blobs/
//!     └── {digest_hex}               ← raw bytes
//! ```
//!
//! # Example
//!
//! ```no_run
//! use sedimentree_fs::storage::FsStorage;
//! use std::path::PathBuf;
//!
//! let storage = FsStorage::new(PathBuf::from("./data")).expect("failed to create storage");
//! ```

#![forbid(unsafe_code)]

pub mod storage;
