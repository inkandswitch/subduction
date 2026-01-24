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
  release = {
    "release:wasm:all" = cmd "Build all JS-wrapped wasm libraries for release"
      ''
        set -e
        export INITIAL_DIR="$(pwd)"
        ${cargo} build --release

        cd "$WORKSPACE_ROOT/subduction_wasm"
        ${pnpm} build

        cd "$WORKSPACE_ROOT/automerge_sedimentree_wasm"
        ${pnpm} build

        cd "$WORKSPACE_ROOT/automerge_subduction_wasm"
        ${pnpm} build

        cd $INITIAL_DIR
        unset INITIAL_DIR
      '';
  };

  build = {
    "build:wasm:all" = cmd "Build all JS-wrapped Wasm libraries"
      ''
        export INITIAL_DIR="$(pwd)"
        ${cargo} build

        cd "$WORKSPACE_ROOT/subduction_wasm"
        ${pnpm} build

        cd "$WORKSPACE_ROOT/automerge_sedimentree_wasm"
        ${pnpm} build

        cd "$WORKSPACE_ROOT/automerge_subduction_wasm"
        ${pnpm} build

        cd $INITIAL_DIR
        unset INITIAL_DIR
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
    "ci" = cmd "Run full CI suite (build, lint, test, docs)" ''
      set -e

      echo "========================================"
      echo "  Subduction CI"
      echo "========================================"
      echo ""

      echo "===> [1/8] Checking formatting..."
      ${cargo} fmt --check
      echo "✓ Formatting OK"
      echo ""

      echo "===> [2/8] Running Clippy..."
      ${cargo} clippy --workspace --all-targets -- -D warnings
      echo "✓ Clippy OK"
      echo ""

      echo "===> [3/8] Building host target..."
      ${cargo} build --workspace
      echo "✓ Host build OK"
      echo ""

      echo "===> [4/8] Running host tests..."
      ${cargo} test --workspace
      echo "✓ Host tests OK"
      echo ""

      echo "===> [5/8] Running doc tests..."
      ${cargo} test --doc --workspace
      echo "✓ Doc tests OK"
      echo ""

      echo "===> [6/8] Checking no_std compatibility..."
      ${cargo} check --package sedimentree_core --no-default-features
      ${cargo} check --package subduction_core --no-default-features
      ${cargo} check --package subduction_wasm --target wasm32-unknown-unknown
      echo "✓ no_std checks OK"
      echo ""

      echo "===> [7/8] Building wasm packages..."
      ${wasm-pack} build --target web subduction_wasm
      echo "✓ Wasm build OK"
      echo ""

      echo "===> [8/8] Running wasm tests..."
      ${wasm-pack} test --node subduction_wasm
      echo "✓ Wasm tests OK"
      echo ""

      echo "========================================"
      echo "  ✓ All CI checks passed!"
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
  };
in
  ci // release // build // bench // test // monitoring
