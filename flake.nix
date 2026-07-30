{
  description = "subduction";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-26.05";
    nixos-unstable.url = "nixpkgs/nixos-unstable-small";

    command-utils.url = "git+https://tangled.org/expede.wtf/nix-command-utils";
    flake-utils.url = "github:numtide/flake-utils";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    wasm-bodge-src = {
      url = "github:alexjg/wasm-bodge/v0.4.0";
      flake = false;
    };
  };

  outputs = {
    self,
    flake-utils,
    nixos-unstable,
    nixpkgs,
    rust-overlay,
    command-utils,
    wasm-bodge-src
  } @ inputs:
    {
      nixosModules.default = import ./nix/nixos-module.nix {inherit self;};
      homeManagerModules.default = import ./nix/home-manager-module.nix {inherit self;};

      # Grafana dashboard for monitoring Subduction metrics
      grafanaDashboardsPath = ./subduction_cli/monitoring/grafana/provisioning/dashboards;
    }
    // flake-utils.lib.eachDefaultSystem (
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

        rustVersion = "1.91.0";

        rust-toolchain = pkgs.rust-bin.stable.${rustVersion}.default.override {
          extensions = [
            "cargo"
            "clippy"
            "llvm-tools-preview"
            "rust-src"
            "rust-std"
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

        # Nightly rustfmt for unstable formatting options (imports_granularity, etc.)
        # We need a combined nightly toolchain (rustc + rustfmt) because rustfmt
        # links against librustc_driver, which lives in the rustc component.
        # On macOS, symlinks break @rpath resolution, so we wrap the binary
        # with DYLD_LIBRARY_PATH pointing to the combined toolchain's lib/.
        nightly-rustfmt-unwrapped = pkgs.rust-bin.nightly.latest.minimal.override {
          extensions = [ "rustfmt" ];
        };

        nightly-rustfmt = pkgs.writeShellScriptBin "rustfmt" ''
          export DYLD_LIBRARY_PATH="${nightly-rustfmt-unwrapped}/lib''${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
          export LD_LIBRARY_PATH="${nightly-rustfmt-unwrapped}/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
          exec "${nightly-rustfmt-unwrapped}/bin/rustfmt" "$@"
        '';

        # wasm-bodge: universal npm package builder for wasm-bindgen crates
        # Not yet in nixpkgs; edition 2024 requires our rust-overlay toolchain
        wasm-bodge-rustPlatform = pkgs.makeRustPlatform {
          cargo = rust-toolchain;
          rustc = rust-toolchain;
        };

        wasm-bodge = wasm-bodge-rustPlatform.buildRustPackage {
          pname = "wasm-bodge";
          version = "0.4.0";
          src = wasm-bodge-src;
          cargoHash = "sha256-KE/AAkrdQ/tmr1X4Fya9CU/oH8e166qJax2kZ3R6jX0=";
          nativeBuildInputs = [ unstable.cargo-auditable ];
          doCheck = false; # tests require npm/puppeteer infrastructure
        };

        format-pkgs = with pkgs; [
          nixpkgs-fmt
          alejandra
          taplo
        ];

        cargo-installs = with pkgs; [
          cargo-audit
          cargo-component
          cargo-criterion
          cargo-deny
          cargo-expand
          cargo-flamegraph
          cargo-mutants
          cargo-nextest
          cargo-release
          cargo-outdated
          cargo-sort
          cargo-udeps
          cargo-watch
          # llvmPackages.bintools
          twiggy
          unstable.wasm-bindgen-cli
          wasm-tools
        ];

        # Pinned to pnpm 10: pnpm 11 stopped reading `pnpm.overrides` from
        # package.json (the wasm wrapper packages keep their esbuild
        # override there) and treats ignored build scripts as a hard
        # error, both of which break `pnpm i` in CI.
        pnpm = pkgs.pnpm_10;

        # Built-in command modules from nix-command-utils
        rust = command-utils.rust.${system};
        pnpm' = command-utils.pnpm.${system};
        wasm = command-utils.wasm.${system};
        cmd = command-utils.cmd.${system};

        # Project-specific commands (monitoring, etc.)
        projectCommands = import ./nix/commands.nix {
          inherit pkgs system cmd wasm-bodge;
        };

        command_menu = command-utils.commands.${system} [
          # Rust commands
          (rust.audit { cargo-audit = pkgs.cargo-audit; })
          (rust.build { cargo = pkgs.cargo; })
          (rust.test { cargo = pkgs.cargo; cargo-watch = pkgs.cargo-watch; })
          (rust.lint { cargo = pkgs.cargo; })
          (rust.fmt { cargo = pkgs.cargo; })
          (rust.doc { cargo = pkgs.cargo; })
          (rust.bench { cargo = pkgs.cargo; cargo-criterion = pkgs.cargo-criterion; xdg-open = pkgs.xdg-utils; })
          (rust.watch { cargo-watch = pkgs.cargo-watch; })

          # Wasm commands
          (wasm.build { wasm-pack = pkgs.wasm-pack; })
          (wasm.release { wasm-pack = pkgs.wasm-pack; gzip = pkgs.gzip; })
          (wasm.test { wasm-pack = pkgs.wasm-pack; features = "browser_test"; })
          (wasm.doc { cargo = pkgs.cargo; xdg-open = pkgs.xdg-utils; })

          # pnpm commands for wasm wrapper builds
          (pnpm'.build { pnpm = "${pnpm}/bin/pnpm"; })
          (pnpm'.install { pnpm = "${pnpm}/bin/pnpm"; })
          (pnpm'.test { pnpm = "${pnpm}/bin/pnpm"; })

          # Project-specific commands
          { commands = projectCommands; packages = []; }
        ];

      in rec {
        packages = {
          subduction_cli = pkgs.rustPlatform.buildRustPackage {
            pname = "subduction_cli";
            version = (builtins.fromTOML (builtins.readFile ./subduction_cli/Cargo.toml)).package.version;
            meta = {
              description = "CLI for running a Subduction sync server";
              longDescription = ''
                Subduction is a peer-to-peer synchronization protocol built on top of
                Sedimentree, providing efficient data synchronization with support for
                multiple transports. This CLI runs a Subduction sync server supporting
                WebSocket, HTTP long-poll, and Iroh (QUIC) transports.
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
                "automerge-0.10.0" = "sha256-WQWwl+6jYkBvNYk2oUGsnxUT87EPMhFiy1DIO/JRQDc=";
                "wasm-tracing-3.0.0-alpha.0" = "sha256-b5XSxRM601ID/uT2aLMb0WrP3lSGALrh0bPB+7Va/6s=";
              };
            };

            buildInputs = [ pkgs.openssl ];
            nativeBuildInputs = [ pkgs.pkg-config ];

            # Nix sources have no `.git`; hand the build script the rev so
            # `subduction_build_info{git_sha}` identifies the deploy instead
            # of reading "unknown".
            SUBDUCTION_GIT_SHA = self.rev or self.dirtyRev or "unknown";

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
            command_menu
            ++ [
              rust-toolchain
              nightly-rustfmt

              pkgs.binaryen
              pkgs.chromedriver
              pkgs.esbuild
              pkgs.gnuplot
              pkgs.grafana
              pkgs.grafana-loki
              pkgs.http-server
              pnpm
              pkgs.webpack-cli
              pkgs.nodejs
              pkgs.playwright-driver
              pkgs.playwright-driver.browsers
              pkgs.prometheus
              pkgs.rust-analyzer
              pkgs.tokio-console
              pkgs.typescript
              pkgs.wasm-pack
              wasm-bodge
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
            export RUSTFMT="${nightly-rustfmt}/bin/rustfmt"
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
