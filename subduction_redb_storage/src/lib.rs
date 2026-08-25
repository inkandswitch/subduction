//! # Subduction redb Storage
//!
//! A [redb] backend for the driver's
//! [`Storage`](subduction_runtime::storage::Storage) capability.
//!
//! ## Batching
//!
//! One transaction per storage op. The driver's op-shaped trait batches
//! at the protocol level — a `persist_items` op carries every item of an
//! ingest — so each op amortizes a single fsync across its whole batch.
//! (Legacy needed a writer-actor queue to get the same effect because
//! its trait was per-item; here the batching falls out of the shape.)
//!
//! ## Layout
//!
//! ```text
//! trees:     [u8; 32] = tree_id                → ()
//! commits:   [u8; 64] = tree_id ++ commit_id   → meta_len:u32be ++ signed ++ blob
//! fragments: [u8; 64] = tree_id ++ head_id     → meta_len:u32be ++ signed ++ blob
//! ```
//!
//! Keys sort lexicographically, so a tree's items are contiguous and
//! `delete_tree` is a single range removal per table. Re-persisting an
//! id overwrites in place (idempotent, per the conformance contract).
//!
//! ## Blocking
//!
//! redb transactions block; every op hops to the caller's blocking pool
//! (`tokio::task::spawn_blocking`) so the driver task never stalls on
//! disk IO. This makes the backend `Sendable`-native — the first real
//! storage for multithreaded drivers.
//!
//! [redb]: https://github.com/cberner/redb

pub mod storage;
