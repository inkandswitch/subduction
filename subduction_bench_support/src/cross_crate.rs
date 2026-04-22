//! Cross-crate composition helpers.
//!
//! When a bench needs generators that span multiple library crates — e.g. a signed
//! `LooseCommit` that needs both `sedimentree_core::test_utils::synthetic_commit` and
//! `subduction_crypto::test_utils::signer_from_seed` — the composition lives here.
//!
//! This module is currently empty. Helpers will be added as Phase 1.4+ benches reveal
//! concrete patterns that are worth centralising.
