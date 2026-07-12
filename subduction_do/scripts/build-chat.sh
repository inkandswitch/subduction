#!/usr/bin/env bash
#
# Build the subduction_wasm browser bundle for the standalone chat example
# (examples/chat/pkg). This is a dev/example tool only — the chat is NOT part of
# the deployed service. Run it, then serve examples/chat with any static server
# (e.g. `npm run chat`) and point it at a service with ?server=<ws-url>.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
wasm_crate="$here/../subduction_wasm"
out_dir="$here/examples/chat/pkg"

echo "building subduction_wasm (target=web) -> $out_dir"
wasm-pack build "$wasm_crate" \
  --target web \
  --release \
  --out-dir "$out_dir" \
  --out-name subduction_wasm

echo "chat client build complete"
