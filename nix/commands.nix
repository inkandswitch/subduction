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

  release = {
    "release:host" = cmd "Build release for ${system}"
      "${cargo} build --release";

    "release:wasm" = cmd "Build all JS-wrapped wasm libraries for release"
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
    "build:host" = cmd "Build for ${system}"
      "${cargo} build";

    "build:wasm" = cmd "Build all JS-wrapped Wasm libraries"
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
    "bench:host" = cmd "Run benchmarks, including test utils"
      "${cargo} bench";

    "bench:host:open" = cmd "Open host Criterion benchmarks in browser"
      "${pkgs.xdg-utils}/bin/xdg-open ./target/criterion/report/index.html";
  };

  lint = {
    "lint" = cmd "Run Clippy"
      "${cargo} clippy";

    "lint:pedantic" = cmd "Run Clippy pedantically"
      "${cargo} clippy -- -W clippy::pedantic";

    "lint:fix" = cmd "Apply non-pendantic Clippy suggestions"
      "${cargo} clippy --fix";
  };

  watch = {
    "watch:build:host" = cmd "Rebuild host target on save"
      "${cargo} watch --clear";
  };

  test = {
    "test:all" = cmd "Run Cargo tests"
      "test:host && test:docs && test:wasm";

    "test:host" = cmd "Run Cargo tests for host target"
      "${cargo} test && ${cargo} test --doc";

    "test:no_std" = cmd "Test no_std compatibility (core crates only)" ''
      set -e  # Exit on first error

      echo "===> Testing sedimentree_core with no_std (base)..."
      ${cargo} check --package sedimentree_core --no-default-features -v

      echo ""
      echo "===> Testing subduction_core with no_std (base)..."
      ${cargo} check --package subduction_core --no-default-features -v

      echo ""
      echo "Note: serde feature requires Vec/collection support which needs:"
      echo "  - alloc feature in no_std environments, OR"
      echo "  - std feature in std environments"
      echo "Core libraries are no_std compatible, but serde serialization requires alloc or std."
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
      set -e  # Exit on first error

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

    "test:websocket:no_std" = cmd "Verify subduction_websocket builds without std (should fail gracefully)" ''
      echo "===> Checking if subduction_websocket requires std (expected to fail)..."
      if ${cargo} check --package subduction_websocket --no-default-features 2>&1 | tail -20; then
        echo ""
        echo "⚠ Warning: subduction_websocket unexpectedly builds without std"
        exit 1
      else
        echo ""
        echo "✓ Confirmed: subduction_websocket requires std (as expected for networking)"
      fi
    '';

    "test:websocket:tokio" = cmd "Run all WebSocket tokio tests" ''
      set -e  # Exit on first error

      echo "===> Running tokio WebSocket tests..."
      ${cargo} test --package subduction_websocket --features tokio_client,tokio_server -- --nocapture --test-threads=1

      echo ""
      echo "✓ All tokio WebSocket tests passed"
    '';

    "test:wasm" = cmd "Run wasm-pack tests on all targets"
      "test:wasm:node && test:ts:web";

    "test:wasm:node" = cmd "Run wasm-pack tests in Node.js"
      "${wasm-pack} test --node subduction_wasm";

    "test:ts:web" = cmd "Run subduction_wasm Typescript tests in Playwright" ''
      cd ./subduction_wasm
      ${pnpm} exec playwright install --with-deps
      cd ..

      ${pkgs.http-server}/bin/http-server --silent &
      bg_pid=$!

      build:wasm
      ${playwright} test ./subduction_wasm

      cleanup() {
        echo "Killing background process $bg_pid"
        kill "$bg_pid" 2>/dev/null || true
      }
      trap cleanup EXIT
    '';

    "test:ts:web:report:latest" = cmd "Open the latest Playwright report"
      "${playwright} show-report";

    "test:wasm:chrome" = cmd "Run wasm-pack tests in headless Chrome"
      "${wasm-pack} test --chrome subduction_wasm --features='browser_test'";

    "test:wasm:firefox" = cmd "Run wasm-pack tests in headless Chrome"
      "${wasm-pack} test --firefox subduction_wasm --features='browser_test'";

    "test:wasm:safari" = cmd "Run wasm-pack tests in headless Chrome"
      "${wasm-pack} test --safari subduction_wasm --features='browser_test'";

    "test:docs" = cmd "Run Cargo doctests"
      "${cargo} test --doc";
  };

  docs = {
    "docs:build:host" = cmd "Refresh the docs"
      "${cargo} doc";

    "docs:build:wasm" = cmd "Refresh the docs with the wasm32-unknown-unknown target"
      "${cargo} doc --target=wasm32-unknown-unknown";

    "docs:open:host" = cmd "Open refreshed docs"
      "${cargo} doc --open";

    "docs:open:wasm" = cmd "Open refreshed docs"
      "${cargo} doc --open --target=wasm32-unknown-unknown";
  };

  monitoring = {
    "monitoring:start" = cmd "Start Prometheus and Grafana for metrics" ''
      set -e

      echo "Starting monitoring stack..."
      echo "  Prometheus: http://localhost:9092"
      echo "  Grafana:    http://localhost:3939"
      echo ""

      # Create temp directories for Grafana
      mkdir -p /tmp/grafana-data /tmp/grafana-dashboards
      cp "$WORKSPACE_ROOT/monitoring/grafana/provisioning/dashboards/subduction.json" /tmp/grafana-dashboards/

      # Start Prometheus in background
      ${prometheus} \
        --config.file="$WORKSPACE_ROOT/monitoring/prometheus.yml" \
        --web.listen-address=":9092" \
        --storage.tsdb.path="/tmp/prometheus-data" \
        &
      PROM_PID=$!
      echo "Prometheus started (PID: $PROM_PID)"

      # Start Grafana in background
      ${grafana-server} \
        --homepath="${grafana-homepath}" \
        --config="$WORKSPACE_ROOT/monitoring/grafana/grafana.ini" \
        cfg:paths.data=/tmp/grafana-data \
        cfg:paths.provisioning="$WORKSPACE_ROOT/monitoring/grafana/provisioning" \
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
in
  release // build // bench // lint // watch // test // docs // monitoring
