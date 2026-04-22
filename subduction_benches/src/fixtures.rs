//! Filesystem and test-vector fixtures.
//!
//! Fixtures are feature-gated so the crate stays lightweight when only workload generators are
//! needed.
//!
//! | Module            | Cargo feature         | Contents                                        |
//! |-------------------|-----------------------|-------------------------------------------------|
//! | [`tempdir`]       | `fs_fixtures`         | `tempfile::TempDir` helpers for FS storage      |
//! | [`egwalker`]      | `egwalker_fixtures`   | Embedded `.am` test vectors + metadata          |
//! | [`real_world`]    | `real_world_fixture`  | On-disk CAS dump at `automerge_subduction_wasm/data/trees` (gitignored) |

#[cfg(feature = "egwalker_fixtures")]
pub mod egwalker;

#[cfg(feature = "fs_fixtures")]
pub mod tempdir;

#[cfg(feature = "real_world_fixture")]
pub mod real_world;
