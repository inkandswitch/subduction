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

        rustVersion = "1.90.0";

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
        nightly-rustfmt = pkgs.rust-bin.nightly.latest.rustfmt;

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

        # Built-in command modules from nix-command-utils
        rust = command-utils.rust.${system};
        pnpm' = command-utils.pnpm.${system};
        wasm = command-utils.wasm.${system};
        cmd = command-utils.cmd.${system};

        # Project-specific commands (monitoring, etc.)
        projectCommands = import ./nix/commands.nix {
          inherit pkgs system cmd;
        };

        command_menu = command-utils.commands.${system} [
          # Rust commands
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
          (pnpm'.build { pnpm = "${pkgs.pnpm}/bin/pnpm"; })
          (pnpm'.install { pnpm = "${pkgs.pnpm}/bin/pnpm"; })
          (pnpm'.test { pnpm = "${pkgs.pnpm}/bin/pnpm"; })

          # Project-specific commands
          { commands = projectCommands; packages = []; }
        ];

        grafana =
          let
            pluginsDir = pkgs.linkFarm "grafana-plugins" [
              {
                name = "grafana-pyroscope-app";
                path = pkgs.grafanaPlugins.grafana-pyroscope-app;
              }
            ];
          in pkgs.symlinkJoin {
            name = "grafana-with-plugins";
            paths = [ pkgs.grafana ];
            nativeBuildInputs = [ pkgs.makeWrapper ];
            postBuild = ''
              wrapProgram $out/bin/grafana --set GF_PATHS_PLUGINS ${pluginsDir}
              wrapProgram $out/bin/grafana-server --set GF_PATHS_PLUGINS ${pluginsDir}
            '';
          };

      in rec {
        packages = {
          subduction_cli = pkgs.rustPlatform.buildRustPackage {
            pname = "subduction_cli";
            version = "0.4.0";
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
                "keyhive_core-0.2.0-alpha.1" = "sha256-DhnAqMRL+9851XXLPGgLEWFe4WKIlzJhnfrHIuvEdiU=";
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
              nightly-rustfmt

              pkgs.binaryen
              pkgs.chromedriver
              grafana
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
