{
  description = "subduction";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-25.11";
    nixos-unstable.url = "nixpkgs/nixos-unstable-small";

    command-utils.url = "git+https://codeberg.org/expede/nix-command-utils";
    flake-utils.url = "github:numtide/flake-utils";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    flake-utils,
    nixos-unstable,
    nixpkgs,
    rust-overlay,
    command-utils
  } @ inputs:
    flake-utils.lib.eachDefaultSystem (
      system: let
        overlays = [
          (import rust-overlay)
        ];

        pkgs = import nixpkgs {
          inherit system overlays;
          config.allowUnfree = true;
        };

        unstable = import nixos-unstable {
          inherit system overlays;
          config.allowUnfree = true;
        };

        rustVersion = "1.90.0";

        rust-toolchain = pkgs.rust-bin.stable.${rustVersion}.default.override {
          extensions = [
            "cargo"
            "clippy"
            "llvm-tools-preview"
            "rust-src"
            "rust-std"
            "rustfmt"
          ];

          targets = [
            "aarch64-apple-darwin"
            "x86_64-apple-darwin"

            "x86_64-unknown-linux-musl"
            "aarch64-unknown-linux-musl"

            "wasm32-unknown-unknown"
            "thumbv6m-none-eabi"
          ];
        };

        format-pkgs = with pkgs; [
          nixpkgs-fmt
          alejandra
          taplo
        ];

        cargo-installs = with pkgs; [
          cargo-criterion
          cargo-deny
          cargo-expand
          cargo-nextest
          cargo-outdated
          cargo-sort
          cargo-udeps
          cargo-watch
          # llvmPackages.bintools
          twiggy
          cargo-component
          wasm-bindgen-cli
          wasm-tools
        ];

        cargo = "${pkgs.cargo}/bin/cargo";
        grafana = "${pkgs.grafana}/bin/grafana";
        grafana-server = "${pkgs.grafana}/bin/grafana-server";
        grafana-homepath = "${pkgs.grafana}/share/grafana";
        gzip = "${pkgs.gzip}/bin/gzip";
        node = "${pkgs.nodejs_22}/bin/node";
        pnpm = "${pkgs.pnpm}/bin/pnpm";
        playwright = "${pnpm} --dir=./subduction_wasm exec playwright";
        prometheus = "${pkgs.prometheus}/bin/prometheus";
        wasm-pack = "${pkgs.wasm-pack}/bin/wasm-pack";
        wasm-opt = "${pkgs.binaryen}/bin/wasm-opt";

        cmd = command-utils.cmd.${system};

        release = {
          "release:host" = cmd "Build release for ${system}"
            "${cargo} build --release";

          "release:wasm" = cmd "Build all JS-wrapped wasm libraries for release"
            ''
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

            build:wasm:web
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

        command_menu = command-utils.commands.${system}
          (release // build // bench // lint // watch // test // docs // monitoring);

      in rec {
        packages = {
          subduction_cli = pkgs.rustPlatform.buildRustPackage {
            pname = "subduction_cli";
            version = "0.1.0";
            meta = {
              description = "CLI tool for running Subduction with WebSockets";
              longDescription = ''
                Subduction is a peer-to-peer synchronization protocol built on top of
                Sedimentree, providing efficient data synchronization with support for
                multiple transports. This CLI tool provides WebSocket-based
                server and client implementations for running Subduction nodes.
              '';
              homepage = "https://github.com/inkandswitch/subduction";
              license = [
                pkgs.lib.licenses.mit
                pkgs.lib.licenses.asl20
              ];
              maintainers = [ pkgs.lib.maintainers.expede ];
              platforms = pkgs.lib.platforms.unix;
              mainProgram = "subduction_cli";
            };

            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
              outputHashes = {
                "wasm-tracing-3.0.0-alpha.0" = "sha256-b5XSxRM601ID/uT2aLMb0WrP3lSGALrh0bPB+7Va/6s=";
              };
            };

            buildInputs = [ pkgs.openssl ];
            nativeBuildInputs = [ pkgs.pkg-config ];

            cargoBuildFlags = [ "--bin" "subduction_cli" ];

            doCheck = !pkgs.stdenv.buildPlatform.canExecute pkgs.stdenv.hostPlatform;

            nativeCheckInputs = [
              pkgs.rustPlatform.cargoCheckHook
            ];

            checkPhase = ''
              cargo test --release --locked
            '';
          };

          default = packages.subduction_cli;
        };

        devShells.default = pkgs.mkShell {
          name = "subduction_shell";

          nativeBuildInputs =
            [
              command_menu
              rust-toolchain

              pkgs.binaryen
              pkgs.chromedriver
              pkgs.grafana
              pkgs.http-server
              pkgs.nodePackages.pnpm
              pkgs.nodePackages_latest.webpack-cli
              pkgs.nodejs_22
              pkgs.playwright-driver
              pkgs.playwright-driver.browsers
              pkgs.prometheus
              pkgs.rust-analyzer
              pkgs.tokio-console
              pkgs.typescript
              pkgs.wasm-pack
              pkgs.websocat
            ]
            ++ format-pkgs
            ++ cargo-installs
            ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
              pkgs.clang
              pkgs.llvmPackages.libclang
              pkgs.openssl.dev
              pkgs.pkg-config
            ];

         shellHook = ''
            unset SOURCE_DATE_EPOCH
            export WORKSPACE_ROOT="$(pwd)"
            menu
          '' + pkgs.lib.optionalString pkgs.stdenv.isLinux ''
            unset PKG_CONFIG_PATH
            export PKG_CONFIG_PATH=${pkgs.openssl.dev}/lib/pkgconfig

            export OPENSSL_NO_VENDOR=1
            export OPENSSL_LIB_DIR=${pkgs.openssl.out}/lib
            export OPENSSL_INCLUDE_DIR=${pkgs.openssl.dev}/include
          '';
        };

        formatter = pkgs.alejandra;
      }
    );
}
