#!/usr/bin/env bash
#
# Assemble the deployable static site into ./public (the wrangler `assets`
# directory). The service ships only the landing page — the chat under
# examples/chat is a standalone example and is intentionally NOT deployed here.
#
# Sources live in ./site (committed); ./public is a build artifact (gitignored).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
src="$here/site"
out="$here/public"

rm -rf "$out"
mkdir -p "$out"
cp -R "$src"/. "$out"/

echo "site assembled -> $out"
