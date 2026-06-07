//! Emit `cfg(cdylib_primary)` when this crate is the primary package being
//! built (i.e. `cargo build -p sedimentree_wasm` / `wasm-pack build` for the
//! standalone `@automerge/sedimentree` bundle), and NOT when it is merely a
//! dependency of another cdylib (e.g. `automerge_subduction_wasm`).
//!
//! This gates the JS logging exports (`setSubductionLogLevel`,
//! `set_subduction_logger`, `clear_subduction_logger`) so they appear in the
//! standalone bundle but do not collide with the identical exports that the
//! umbrella crate already gets via its `subduction_wasm` re-export.

fn main() {
    println!("cargo::rustc-check-cfg=cfg(cdylib_primary)");

    // `CARGO_PRIMARY_PACKAGE` is set by Cargo only for packages named on the
    // command line, not for their dependencies.
    if std::env::var_os("CARGO_PRIMARY_PACKAGE").is_some() {
        println!("cargo::rustc-cfg=cdylib_primary");
    }
}
