#!/usr/bin/env bash
set -euo pipefail

# Migrate an FsStorage data directory from the legacy flat tree layout to the
# sharded layout introduced for scale:
#
#   legacy:   trees/{64-hex-id}/...
#   sharded:  trees/{first-4-hex}/{remaining-60-hex}/...
#
# The first two bytes of the (cryptographically distributed) SedimentreeId form
# a hex bucket (0000..ffff); the remaining 30 bytes form the leaf directory.
# The full id is never stored redundantly — it is reconstructed by concatenating
# bucket + leaf, which is exactly what `FsStorage::load_tree_ids` does.
#
# Properties:
#   - OFFLINE:    the subduction service MUST be stopped first. The script
#                 aborts if it detects a running subduction unit.
#   - IDEMPOTENT: already-sharded buckets (4-hex top-level dirs) are skipped.
#   - RESUMABLE:  each tree is moved with a single atomic rename; re-running
#                 after an interruption picks up where it left off.
#
# Usage:
#   scripts/migrate-trees-sharding.sh <data-dir> [--dry-run] [--force]
#
# Arguments:
#   <data-dir>   FsStorage root (the directory that contains `trees/`),
#                e.g. /var/lib/subduction
#
# Options:
#   --dry-run    Print what would be moved without changing anything.
#   --force      Skip the running-service safety check (use with care).

usage() {
  sed -n '3,30p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

data_dir=""
dry_run=false
force=false

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage 0 ;;
    --dry-run) dry_run=true ;;
    --force)   force=true ;;
    -*)
      echo "error: unknown option: $1" >&2
      usage 1
      ;;
    *)
      if [ -n "$data_dir" ]; then
        echo "error: unexpected extra argument: $1" >&2
        usage 1
      fi
      data_dir="$1"
      ;;
  esac
  shift
done

if [ -z "$data_dir" ]; then
  echo "error: missing <data-dir>" >&2
  usage 1
fi

trees_dir="$data_dir/trees"

if [ ! -d "$trees_dir" ]; then
  echo "error: no trees/ directory under '$data_dir' (looked for '$trees_dir')" >&2
  exit 1
fi

# ── Safety: refuse to run against a live server ─────────────────────────────
# Moving directories out from under a running server risks lost writes. Detect
# an active subduction systemd unit unless --force is given.
if [ "$force" = false ]; then
  if command -v systemctl >/dev/null 2>&1 \
     && systemctl is-active --quiet subduction 2>/dev/null; then
    echo "error: the 'subduction' service appears to be running." >&2
    echo "       Stop it first (e.g. 'sudo systemctl stop subduction'), or pass --force." >&2
    exit 1
  fi
fi

# A legacy tree dir name is the full 64-char hex id; a sharded bucket dir name
# is the 4-char hex prefix. We classify each top-level entry by name length.
is_hex() {
  case "$1" in
    *[!0-9a-fA-F]*) return 1 ;;
    "") return 1 ;;
    *) return 0 ;;
  esac
}

migrated=0
skipped=0

for entry in "$trees_dir"/*; do
  # Handle an empty trees/ (the glob stays literal when nothing matches).
  [ -e "$entry" ] || continue
  [ -d "$entry" ] || continue

  name="$(basename "$entry")"

  # Already-sharded bucket: 4 hex chars. Leave it in place (idempotent).
  if [ "${#name}" -eq 4 ] && is_hex "$name"; then
    skipped=$((skipped + 1))
    continue
  fi

  # Legacy tree: 64 hex chars. Move into trees/{prefix}/{rest}.
  if [ "${#name}" -eq 64 ] && is_hex "$name"; then
    prefix="${name:0:4}"
    rest="${name:4}"
    bucket="$trees_dir/$prefix"
    dest="$bucket/$rest"

    if [ -e "$dest" ]; then
      # Destination already present (e.g. a partial earlier run). Content is
      # addressed by id, so an existing dest is the same tree; skip.
      echo "skip (already at destination): $name"
      skipped=$((skipped + 1))
      continue
    fi

    if [ "$dry_run" = true ]; then
      echo "would move: trees/$name -> trees/$prefix/$rest"
    else
      mkdir -p "$bucket"
      mv "$entry" "$dest"
      echo "moved: trees/$name -> trees/$prefix/$rest"
    fi
    migrated=$((migrated + 1))
    continue
  fi

  echo "warning: unrecognized entry name, leaving in place: $name" >&2
  skipped=$((skipped + 1))
done

if [ "$dry_run" = true ]; then
  echo "dry-run complete: $migrated would be migrated, $skipped skipped."
else
  echo "migration complete: $migrated migrated, $skipped skipped."
fi
