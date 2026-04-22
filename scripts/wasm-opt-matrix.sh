#!/usr/bin/env bash
# Compare `wasm-opt` optimisation levels for `subduction_wasm`.
#
# Builds the wasm bundle twice (once with `-Oz`, once with `-O3`), records the resulting
# binary size and per-optimisation perf numbers from the Node-hosted bench suite.
#
# ## What it reports
#
# For each of `-Oz` and `-O3`:
#   - Size of `subduction_wasm_bg.wasm` (compressed + raw)
#   - Median wall-clock per bench from `perf_crypto.rs` (JSON lines)
#
# ## Caveats
#
# - The workspace's production `wasm-opt` flags include `-Oz` already. This script _replaces_
#   the `-O` level on each pass but preserves the other flags (simd, bulk-memory, etc.) so
#   the comparison is apples-to-apples on feature surface.
# - Only builds `subduction_wasm`. Other wasm crates (`automerge_subduction_wasm`, etc.) are
#   not re-optimised. Extend the list below if needed.
# - Uses `wasm-pack build --release --target bundler`. If you want node-target numbers,
#   use `bench:wasm` instead.
#
# ## Usage
#
# ```sh
# scripts/wasm-opt-matrix.sh
# ```
#
# or via nix:
#
# ```sh
# bench:wasm:opt-matrix
# ```

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Flags to keep across every pass (everything except the `-O` level). Match what
# `subduction_wasm/Cargo.toml` sets under `[package.metadata.wasm-pack.profile.release]`.
BASE_FLAGS=(
  --enable-bulk-memory
  --enable-multivalue
  --enable-mutable-globals
  --enable-nontrapping-float-to-int
  --enable-reference-types
  --enable-sign-ext
  --enable-simd
)

OUT_DIR="$ROOT/target/wasm-opt-matrix"
mkdir -p "$OUT_DIR"

REPORT="$OUT_DIR/report.json"
: > "$REPORT"

build_one () {
  local level=$1
  local label=$2
  local out_pkg="$OUT_DIR/pkg-$label"

  echo "=== building subduction_wasm with wasm-opt $level ==="

  # Start from a fresh `target/wasm32-unknown-unknown/release` so wasm-pack rebuilds.
  cargo build --release -p subduction_wasm --target wasm32-unknown-unknown >&2

  local raw_wasm="target/wasm32-unknown-unknown/release/subduction_wasm.wasm"
  local opt_wasm="$out_pkg/subduction_wasm_bg.wasm"

  mkdir -p "$out_pkg"
  wasm-opt "$level" "${BASE_FLAGS[@]}" "$raw_wasm" -o "$opt_wasm"

  local raw_size opt_size gz_size
  raw_size=$(wc -c < "$raw_wasm")
  opt_size=$(wc -c < "$opt_wasm")
  gz_size=$(gzip -c "$opt_wasm" | wc -c)

  echo "  raw  = $raw_size bytes"
  echo "  opt  = $opt_size bytes  (level: $level)"
  echo "  gzip = $gz_size bytes"

  # Emit one JSON line per level into the aggregate report.
  jq -n \
    --arg level "$level" \
    --argjson raw "$raw_size" \
    --argjson opt "$opt_size" \
    --argjson gz "$gz_size" \
    '{level: $level, raw_bytes: $raw, opt_bytes: $opt, gzip_bytes: $gz}' \
    >> "$REPORT"
}

build_one -Oz oz
build_one -O3 o3

echo ""
echo "=== Size comparison ==="
jq -s '.[] | "  \(.level): opt=\(.opt_bytes) bytes  gzip=\(.gzip_bytes) bytes"' -r "$REPORT"

echo ""
echo "Raw per-level report at $REPORT."
echo ""
echo "Next step: to also get bench numbers, re-run your perf suite against each pkg"
echo "directory ($OUT_DIR/pkg-oz, $OUT_DIR/pkg-o3). Wiring node benches to a specific"
echo ".wasm file is out of scope for this helper — use wasm-pack's --out-dir option."
