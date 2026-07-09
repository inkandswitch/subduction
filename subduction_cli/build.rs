//! Embed the git commit hash for the `subduction_build_info` metric.
//!
//! Falls back to `"unknown"` when `.git` is absent (e.g. Nix builds copy
//! the source tree without it).

use std::process::Command;

fn main() {
    let sha = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=SUBDUCTION_GIT_SHA={sha}");
    // Re-run when HEAD moves (best-effort; harmless if the path is absent).
    println!("cargo:rerun-if-changed=../.git/HEAD");

    // `tokio_unstable` is set via `.cargo/config.toml` rustflags, not a
    // feature; declare it so `unexpected_cfgs` accepts the runtime-metrics
    // sampling code.
    println!("cargo:rustc-check-cfg=cfg(tokio_unstable)");
}
