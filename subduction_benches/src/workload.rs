//! Deterministic workload generators.
//!
//! All generators take an explicit seed parameter and use [`rand::rngs::SmallRng`] internally, so
//! benchmarks are reproducible across machines and runs.
//!
//! # Module layout
//!
//! | Module      | Contents                                                        |
//! |-------------|-----------------------------------------------------------------|
//! | [`ids`]     | `CommitId`, `SedimentreeId`, `PeerId` generators                |
//! | [`blobs`]   | `Blob` and `BlobMeta` generators (hashed and synthetic-digest)  |
//! | [`commits`] | `LooseCommit` generators + DAG shapes (linear, merge-heavy, wide) |
//! | [`fragments`] | `Fragment` generators with configurable boundary / checkpoint / depth |
//! | [`trees`]   | Composed `Sedimentree` builders incl. overlap for diff benches  |
//! | [`signers`] | Deterministic `MemorySigner` construction + signed-payload helpers |
//!
//! # Philosophy
//!
//! These are _not_ property-based generators (that lives in `test_utils`); they're
//! benchmark-shaped workloads tuned for stable measurement. Randomness is cheap and
//! deterministic; no heap allocation beyond what the generated structures need.

pub mod blobs;
pub mod commits;
pub mod fragments;
pub mod ids;
pub mod signers;
pub mod trees;
