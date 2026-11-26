#!/usr/bin/env bash
# Run configurations of sedimentree.qnt and print, per configuration, the share of
# sampled traces that reach each bad state (and deepWriteBlocked).
#
# Every dimension can be narrowed from the environment:
#
#   SAMPLES=500 STEPS=15 NODES="byz" MODES="Adopt Quarantine" TRUSTS="AuthorOnly" ./matrix.sh
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
samples="${SAMPLES:-500}"
steps="${STEPS:-15}"
read -ra node_sets <<<"${NODES:-honest byz}"
read -ra modes <<<"${MODES:-SuppressPeer Adopt Quarantine}"
read -ra walks <<<"${WALKS:-AllOrNothing Partial}"
read -ra connecteds <<<"${CONNECTEDS:-false true}"
read -ra trusts <<<"${TRUSTS:-AnyPeer AuthorOnly}"
read -ra serves <<<"${SERVES:-false true}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cp "$here/sedimentree.qnt" "$work/"

bad=(dataLossReached availabilityLossReached unavailableAfterSettle falseTombstoneReached
     strandedReached divergenceReached deepWriteBlocked)

printf '| nodes | mode | walk | connected | trust | serve q. | data loss | avail. loss | unavail. settled | false tomb. | stranded | diverged | write blocked |\n'
printf '|---|---|---|---|---|---|---|---|---|---|---|---|---|\n'

for ns in "${node_sets[@]}"; do
  if [[ $ns == honest ]]; then nodes='Set("a", "b", "c")'; faulty='Set()'
  else nodes='Set("a", "b", "z")'; faulty='Set("z")'; fi
  for mode in "${modes[@]}"; do
  for walk in "${walks[@]}"; do
  for connected in "${connecteds[@]}"; do
  for trust in "${trusts[@]}"; do
  for serve in "${serves[@]}"; do
    # Serving quarantined items only means something under Quarantine.
    [[ $serve == true && $mode != Quarantine ]] && continue
    cat > "$work/check.qnt" <<QNT
module check {
  import modes.* from "./sedimentree"
  import sedimentree(NODES = $nodes, BYZANTINE = $faulty, MAX_COMMITS = 6, MAX_DEPTH = 2,
    MODE = $mode, WALK = $walk, CONNECTED = $connected, SETTLE_ROUNDS = 3,
    TRUST = $trust, SERVE_QUARANTINED = $serve).* from "./sedimentree"
}
QNT
    out="$(quint run "$work/check.qnt" --main=check --max-samples="$samples" --max-steps="$steps" \
      --witnesses "${bad[@]}" 2>&1)"
    row="| $ns | $mode | $walk | $connected | $trust | $serve |"
    for w in "${bad[@]}"; do
      pct="$(grep -oP "^$w was witnessed in .*\(\K[0-9.]+(?=%\))" <<<"$out" || echo '?')"
      row+=" $pct |"
    done
    echo "$row"
  done
  done
  done
  done
  done
done
