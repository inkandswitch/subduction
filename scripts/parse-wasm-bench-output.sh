#!/usr/bin/env bash
# Parse Wasm bench JSON summaries from `wasm-pack test --node -- --nocapture` output into the
# "customSmallerIsBetter" format consumed by github-action-benchmark.
#
# Input (stdin): raw wasm-pack test log. Bench summaries are single JSON lines shaped like:
#   {"bench":"wasm/blake3/1kb","unit":"ms","iters":50,"min":0.01,"median":0.015,"mean":0.016,"p95":0.018,"max":0.021}
#
# Output (stdout): a JSON array of the form
#   [
#     {"name":"wasm/blake3/1kb","unit":"ms","value":0.015,"extra":"iters=50, min=0.01, p95=0.018, max=0.021"},
#     ...
#   ]
#
# The `value` used for trend tracking is the **median** — more stable under CI jitter than the
# mean. Iters / min / p95 / max are attached as the `extra` comment for human review in the
# github-action-benchmark dashboard.
#
# Usage:
#   wasm-pack test --node subduction_wasm -- --nocapture \
#     | tee wasm-bench.log \
#     | scripts/parse-wasm-bench-output.sh > wasm-bench.json
set -euo pipefail

# Extract only the summary JSON lines (those starting with `{"bench":`).
grep -E '^\{"bench":' \
  | jq -s '
      [ .[] | {
          name:  .bench,
          unit:  .unit,
          value: .median,
          extra: ("iters=" + (.iters|tostring) +
                  ", min="  + (.min|tostring) +
                  ", p95="  + (.p95|tostring) +
                  ", max="  + (.max|tostring))
        } ]
    '
