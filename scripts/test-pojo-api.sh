#!/usr/bin/env bash
# Integration test runner: rebuild the actual wasm-bodge public package, typecheck
# a TS consumer against its emitted declarations, then run its Node API tests.
# Building first prevents stale dist/ bindings from hiding Rust/JS/type mismatches.
# Requires wasm-bodge and its prerequisites (matching wasm-bindgen, esbuild, tsc).
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -n "${CARGO_TOOLCHAIN:-}" ]]; then export RUSTUP_TOOLCHAIN="$CARGO_TOOLCHAIN"; fi
if [[ -n "${WASM_BINDGEN:-}" ]]; then export PATH="$(dirname "$WASM_BINDGEN"):$PATH"; fi
export PATH="$PWD/subduction_wasm/node_modules/.bin:$PATH"
(cd subduction_wasm && wasm-bodge build --debug-profile wasm-debug --no-wasm-opt)
tsc --strict --noEmit --target ES2022 --module ESNext --moduleResolution bundler --skipLibCheck false subduction_wasm/tests/pojo-consumer.ts
for entry in node debug-node; do
    POJO_WASM_PATH="$PWD/subduction_wasm/dist/wrapper/cjs/$entry.cjs" node --test subduction_wasm/tests/pojo-api.test.cjs
done
