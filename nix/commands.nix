# Project-specific commands not covered by nix-command-utils built-in modules
{
  pkgs,
  system,
  cmd,
}: let
  cargo = "${pkgs.cargo}/bin/cargo";
  grafana-server = "${pkgs.grafana}/bin/grafana-server";
  grafana-homepath = "${pkgs.grafana}/share/grafana";
  pnpm = "${pkgs.pnpm}/bin/pnpm";
  playwright = "${pnpm} --dir=./subduction_wasm exec playwright";
  prometheus = "${pkgs.prometheus}/bin/prometheus";
  wasm-pack = "${pkgs.wasm-pack}/bin/wasm-pack";

  # Multi-crate wasm builds (project-specific)
  # Build each crate individually with -p to avoid pulling in unrelated features.
  release = {
    "release:wasm:all" = cmd "Build all JS-wrapped wasm libraries for release"
      ''
        set -e

        echo "===> Building sedimentree_wasm..."
        ${cargo} build --release -p sedimentree_wasm --target wasm32-unknown-unknown
        cd "$WORKSPACE_ROOT/sedimentree_wasm"
        ${pnpm} build

        echo "===> Building subduction_wasm..."
        ${cargo} build --release -p subduction_wasm --target wasm32-unknown-unknown
        cd "$WORKSPACE_ROOT/subduction_wasm"
        ${pnpm} build

        echo "===> Building automerge_sedimentree_wasm..."
        ${cargo} build --release -p automerge_sedimentree_wasm --target wasm32-unknown-unknown
        cd "$WORKSPACE_ROOT/automerge_sedimentree_wasm"
        ${pnpm} build

        echo "===> Building automerge_subduction_wasm..."
        ${cargo} build --release -p automerge_subduction_wasm --target wasm32-unknown-unknown
        cd "$WORKSPACE_ROOT/automerge_subduction_wasm"
        ${pnpm} build

        echo ""
        echo "✓ All wasm packages built"

        wasm:sizes
      '';
  };

  build = {
    "build:wasm:all" = cmd "Build all JS-wrapped Wasm libraries"
      ''
        set -e

        echo "===> Building sedimentree_wasm..."
        ${cargo} build -p sedimentree_wasm --target wasm32-unknown-unknown
        cd "$WORKSPACE_ROOT/sedimentree_wasm"
        ${pnpm} build

        echo "===> Building subduction_wasm..."
        ${cargo} build -p subduction_wasm --target wasm32-unknown-unknown
        cd "$WORKSPACE_ROOT/subduction_wasm"
        ${pnpm} build

        echo "===> Building automerge_sedimentree_wasm..."
        ${cargo} build -p automerge_sedimentree_wasm --target wasm32-unknown-unknown
        cd "$WORKSPACE_ROOT/automerge_sedimentree_wasm"
        ${pnpm} build

        echo "===> Building automerge_subduction_wasm..."
        ${cargo} build -p automerge_subduction_wasm --target wasm32-unknown-unknown
        cd "$WORKSPACE_ROOT/automerge_subduction_wasm"
        ${pnpm} build

        echo ""
        echo "✓ All wasm packages built"
      '';
  };

  bench = {
    "bench:heap" = cmd "Run heap allocation profiling" ''
      ${cargo} test --package sedimentree_core --test heap_profile -- --nocapture
      ${pkgs.jq}/bin/jq '.' sedimentree_core/dhat-heap.json | ${pkgs.moreutils}/bin/sponge sedimentree_core/dhat-heap.json
      echo ""
      echo "Heap profile saved to sedimentree_core/dhat-heap.json"
    '';
  };

  test = {
    "test:no_std" = cmd "Test no_std compatibility (core crates only)" ''
      set -e

      echo "===> Testing sedimentree_core with no_std (base)..."
      ${cargo} check --package sedimentree_core --no-default-features -v

      echo ""
      echo "===> Testing subduction_core with no_std (base)..."
      ${cargo} check --package subduction_core --no-default-features -v

      echo ""
      echo "===> Testing sedimentree_wasm (no_std with alloc by default)..."
      ${cargo} check --package sedimentree_wasm --target wasm32-unknown-unknown -v

      echo ""
      echo "===> Testing subduction_wasm (no_std with alloc by default)..."
      ${cargo} check --package subduction_wasm --target wasm32-unknown-unknown -v

      echo ""
      echo "===> Testing automerge_subduction_wasm (no_std with alloc by default)..."
      ${cargo} check --package automerge_subduction_wasm --target wasm32-unknown-unknown -v

      echo ""
      echo "✓ All no_std checks passed"
    '';

    "test:std" = cmd "Test std feature enablement explicitly" ''
      set -e

      echo "===> Testing sedimentree_core with std..."
      ${cargo} test --package sedimentree_core --features std,arbitrary -- --nocapture

      echo ""
      echo "===> Testing subduction_core with std..."
      ${cargo} test --package subduction_core --features std -- --nocapture

      echo ""
      echo "===> Testing subduction_websocket with std (default)..."
      ${cargo} test --package subduction_websocket --lib -- --nocapture

      echo ""
      echo "===> Testing subduction_websocket with tokio features..."
      ${cargo} test --package subduction_websocket --features tokio_client,tokio_server -- --nocapture

      echo ""
      echo "✓ All std tests passed"
    '';

    "test:websocket:tokio" = cmd "Run all WebSocket tokio tests" ''
      set -e

      echo "===> Running tokio WebSocket tests..."
      ${cargo} test --package subduction_websocket --features tokio_client,tokio_server -- --nocapture --test-threads=1

      echo ""
      echo "✓ All tokio WebSocket tests passed"
    '';

    "test:ts:web" = cmd "Run subduction_wasm Typescript tests in Playwright" ''
      cd ./subduction_wasm
      ${pnpm} exec playwright install --with-deps
      cd ..

      ${pkgs.http-server}/bin/http-server --silent &
      bg_pid=$!

      build:wasm:all
      ${playwright} test ./subduction_wasm

      cleanup() {
        echo "Killing background process $bg_pid"
        kill "$bg_pid" 2>/dev/null || true
      }
      trap cleanup EXIT
    '';

    "test:ts:web:report:latest" = cmd "Open the latest Playwright report"
      "${playwright} show-report";

    "test:props" = cmd "Run proptests with many iterations" ''
      set -e
      echo "Running property tests with 100,000 iterations each..."
      echo ""
      export BOLERO_RANDOM_ITERATIONS=100000
      ${cargo} test --all-features proptests -- --nocapture
      echo ""
      echo "✓ All property tests passed"
    '';

    "test:props:quick" = cmd "Run proptests with default iterations"
      "${cargo} test --all-features proptests -- --nocapture";

    "test:props:intense" = cmd "Run proptests with 1M iterations" ''
      set -e
      echo "Running property tests with 1,000,000 iterations each..."
      echo ""
      export BOLERO_RANDOM_ITERATIONS=1000000
      ${cargo} test --all-features proptests -- --nocapture
      echo ""
      echo "✓ All property tests passed"
    '';
  };

  wasm = {
    "wasm:sizes" = cmd "Print wasm bundle sizes" ''
      set -e

      format_size() {
        local size=$1
        if [ "$size" -gt 1048576 ]; then
          echo "$(echo "scale=2; $size / 1048576" | ${pkgs.bc}/bin/bc) MB"
        elif [ "$size" -gt 1024 ]; then
          echo "$(echo "scale=2; $size / 1024" | ${pkgs.bc}/bin/bc) KB"
        else
          echo "$size B"
        fi
      }

      rows=""
      for dir in automerge_sedimentree_wasm automerge_subduction_wasm sedimentree_wasm subduction_wasm; do
        wasm_file="$WORKSPACE_ROOT/$dir/pkg-slim/"*.wasm 2>/dev/null || continue
        if [ -f $wasm_file ]; then
          name=$(basename "$dir")
          raw_size=$(${pkgs.coreutils}/bin/stat -c%s $wasm_file 2>/dev/null || echo "0")
          gzip_size=$(${pkgs.gzip}/bin/gzip -c $wasm_file | ${pkgs.coreutils}/bin/wc -c)
          raw_fmt=$(format_size "$raw_size")
          gz_fmt=$(format_size "$gzip_size")
          rows="$rows$name|$raw_fmt|$gz_fmt\n"
        fi
      done

      echo ""
      echo "┌───────────────────────────────────┬────────────┬────────────┐"
      echo "│ Package                           │        Raw │    Gzipped │"
      echo "├───────────────────────────────────┼────────────┼────────────┤"

      printf "$rows" | while IFS='|' read -r name raw gz; do
        printf "│ %-33s │ %10s │ %10s │\n" "$name" "$raw" "$gz"
      done

      echo "└───────────────────────────────────┴────────────┴────────────┘"
      echo ""
    '';
  };

  monitoring = {
    "monitoring:start" = cmd "Start Prometheus and Grafana for metrics" ''
      set -e

      echo "Starting monitoring stack..."
      echo "  Prometheus: http://localhost:9092"
      echo "  Grafana:    http://localhost:3939"
      echo ""

      mkdir -p /tmp/grafana-data /tmp/grafana-dashboards
      cp "$WORKSPACE_ROOT/subduction_cli/monitoring/grafana/provisioning/dashboards/subduction.json" /tmp/grafana-dashboards/

      ${prometheus} \
        --config.file="$WORKSPACE_ROOT/subduction_cli/monitoring/prometheus.yml" \
        --web.listen-address=":9092" \
        --storage.tsdb.path="/tmp/prometheus-data" \
        &
      PROM_PID=$!
      echo "Prometheus started (PID: $PROM_PID)"

      ${grafana-server} \
        --homepath="${grafana-homepath}" \
        --config="$WORKSPACE_ROOT/subduction_cli/monitoring/grafana/grafana.ini" \
        cfg:paths.data=/tmp/grafana-data \
        cfg:paths.provisioning="$WORKSPACE_ROOT/subduction_cli/monitoring/grafana/provisioning" \
        &
      GRAF_PID=$!
      echo "Grafana started (PID: $GRAF_PID)"

      echo ""
      echo "Monitoring stack running. Press Ctrl+C to stop."

      cleanup() {
        echo ""
        echo "Stopping monitoring stack..."
        kill $PROM_PID 2>/dev/null || true
        kill $GRAF_PID 2>/dev/null || true
        echo "Done."
      }
      trap cleanup EXIT INT TERM

      wait
    '';
  };
  ci = {
    "ci" = cmd "Run full CI suite (build, lint, test, wasm)" ''
      set -e

      echo "========================================"
      echo "  Subduction CI"
      echo "========================================"
      echo ""

      echo "===> [1/6] Checking formatting..."
      ${cargo} fmt --check
      echo "✓ Formatting OK"
      echo ""

      echo "===> [2/6] Running Clippy..."
      ${cargo} clippy --workspace --all-targets -- -D warnings
      echo "✓ Clippy OK"
      echo ""

      echo "===> [3/6] Building host target..."
      ${cargo} build --workspace
      echo "✓ Host build OK"
      echo ""

      echo "===> [4/6] Running host tests..."
      ${cargo} test --workspace
      echo "✓ Host tests OK"
      echo ""

      echo "===> [5/6] Building wasm packages..."
      ${wasm-pack} build --target web subduction_wasm
      echo "✓ Wasm build OK"
      echo ""

      echo "===> [6/6] Running wasm tests..."
      ${wasm-pack} test --node subduction_wasm
      echo "✓ Wasm tests OK"
      echo ""

      echo "========================================"
      echo "  ✓ All CI checks passed!"
      echo "========================================"
    '';

    "ci:no_std" = cmd "Check no_std compatibility (core crates)" ''
      set -e

      echo "========================================"
      echo "  Subduction CI: no_std"
      echo "========================================"
      echo ""

      echo "===> [1/4] Checking sedimentree_core (no_std)..."
      ${cargo} check --package sedimentree_core --no-default-features
      echo "✓ sedimentree_core OK"
      echo ""

      echo "===> [2/4] Checking subduction_core (no_std)..."
      ${cargo} check --package subduction_core --no-default-features
      echo "✓ subduction_core OK"
      echo ""

      echo "===> [3/4] Checking sedimentree_wasm (wasm32)..."
      ${cargo} check --package sedimentree_wasm --target wasm32-unknown-unknown
      echo "✓ sedimentree_wasm OK"
      echo ""

      echo "===> [4/4] Checking subduction_wasm (wasm32)..."
      ${cargo} check --package subduction_wasm --target wasm32-unknown-unknown
      echo "✓ subduction_wasm OK"
      echo ""

      echo "========================================"
      echo "  ✓ All no_std checks passed!"
      echo "========================================"
    '';

    "ci:std" = cmd "Run tests with std feature enabled" ''
      set -e

      echo "========================================"
      echo "  Subduction CI: std"
      echo "========================================"
      echo ""

      echo "===> [1/4] Testing sedimentree_core (std)..."
      ${cargo} test --package sedimentree_core --features std
      echo "✓ sedimentree_core OK"
      echo ""

      echo "===> [2/4] Testing subduction_core (std)..."
      ${cargo} test --package subduction_core --features std
      echo "✓ subduction_core OK"
      echo ""

      echo "===> [3/4] Testing subduction_websocket..."
      ${cargo} test --package subduction_websocket --features tokio_client,tokio_server
      echo "✓ subduction_websocket OK"
      echo ""

      echo "===> [4/4] Running doc tests..."
      ${cargo} test --doc --workspace
      echo "✓ Doc tests OK"
      echo ""

      echo "========================================"
      echo "  ✓ All std tests passed!"
      echo "========================================"
    '';

    "ci:all-features" = cmd "Run CI with --all-features" ''
      set -e

      echo "========================================"
      echo "  Subduction CI: all-features"
      echo "========================================"
      echo ""

      echo "===> [1/3] Running Clippy (all features)..."
      ${cargo} clippy --workspace --all-targets --all-features -- -D warnings
      echo "✓ Clippy OK"
      echo ""

      echo "===> [2/3] Building (all features)..."
      ${cargo} build --workspace --all-features
      echo "✓ Build OK"
      echo ""

      echo "===> [3/3] Testing (all features)..."
      ${cargo} test --workspace --all-features
      echo "✓ Tests OK"
      echo ""

      echo "========================================"
      echo "  ✓ All all-features checks passed!"
      echo "========================================"
    '';

    "ci:quick" = cmd "Run quick CI checks (lint, test)" ''
      set -e

      echo "===> Checking formatting..."
      ${cargo} fmt --check

      echo "===> Running Clippy..."
      ${cargo} clippy --workspace -- -D warnings

      echo "===> Running tests..."
      ${cargo} test --workspace

      echo ""
      echo "✓ Quick CI passed"
    '';

    "ci:full" = cmd "Run all CI suites (ci, no_std, std, all-features)" ''
      set -e

      echo "========================================"
      echo "  Subduction CI: Full Suite"
      echo "========================================"
      echo ""

      echo "╔════════════════════════════════════╗"
      echo "║  [1/4] Running main CI...          ║"
      echo "╚════════════════════════════════════╝"
      echo ""
      ci

      echo ""
      echo "╔════════════════════════════════════╗"
      echo "║  [2/4] Running no_std checks...    ║"
      echo "╚════════════════════════════════════╝"
      echo ""
      ci:no_std

      echo ""
      echo "╔════════════════════════════════════╗"
      echo "║  [3/4] Running std tests...        ║"
      echo "╚════════════════════════════════════╝"
      echo ""
      ci:std

      echo ""
      echo "╔════════════════════════════════════╗"
      echo "║  [4/4] Running all-features...     ║"
      echo "╚════════════════════════════════════╝"
      echo ""
      ci:all-features

      echo ""
      echo "========================================"
      echo "  ✓ All CI suites passed!"
      echo "========================================"
    '';
  };
in
  bench // build // ci // monitoring // release // test // wasm
