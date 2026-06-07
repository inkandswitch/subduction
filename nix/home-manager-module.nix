{self}: {
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.subduction;
in {
  options.services.subduction = {
    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.subduction_cli;
      defaultText = lib.literalExpression "pkgs.subduction_cli";
      description = "The Subduction CLI package to use.";
    };

    server = {
      enable = lib.mkEnableOption "Subduction sync server";

      socket = lib.mkOption {
        type = lib.types.str;
        default = "127.0.0.1:8080";
        description = "Socket address for the WebSocket server.";
      };

      dataDir = lib.mkOption {
        type = lib.types.path;
        default = "${config.xdg.dataHome}/subduction";
        defaultText = lib.literalExpression ''"''${config.xdg.dataHome}/subduction"'';
        description = "Directory for storing sync data.";
      };

      keySeed = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Key seed (64 hex characters) for deterministic key generation. Mutually exclusive with keyFile.";
      };

      keyFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = ''
          Path to an existing file containing the signing key seed (32 bytes, hex or raw).
          Mutually exclusive with keySeed and ephemeralKey.
        '';
      };

      ephemeralKey = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = ''
          Use a random ephemeral key (lost on restart).
          Mutually exclusive with keySeed and keyFile.
        '';
      };

      handshakeMaxDrift = lib.mkOption {
        type = lib.types.int;
        default = 600;
        description = "Maximum clock drift allowed during handshake (in seconds).";
      };

      serviceName = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = ''
          Service name for discovery mode (e.g., `sync.example.com`).
          Clients can connect without knowing the server's peer ID.
          The name is hashed to a 32-byte identifier for the handshake.
          Defaults to the socket address if not specified.
          Omit the protocol so the same name works across `wss://`, `https://`, etc.
        '';
      };

      timeout = lib.mkOption {
        type = lib.types.int;
        default = 5;
        description = "Request timeout in seconds.";
      };

      logFormat = lib.mkOption {
        type = lib.types.enum ["text" "json"];
        default = "json";
        description = ''
          Log output format (passes `--log-format <fmt>`). `json` emits
          structured lines with span fields; `text` is human-readable.
        '';
      };

      logLevel = lib.mkOption {
        type = lib.types.str;
        default = "info";
        description = ''
          Log level filter, set via the `RUST_LOG` environment variable.
          Accepts standard `tracing` `EnvFilter` syntax. Defaults to `info`.
        '';
      };

      maxMessageSize = lib.mkOption {
        type = lib.types.int;
        default = 52428800; # 50 MiB
        description = ''
          Maximum WebSocket message size in bytes.

          Sets the aggregate-message limit passed to the server. If
          {option}`services.subduction.server.maxFrameSize` is left
          unset, individual WebSocket frames are capped at the same
          value — browsers commonly send unfragmented frames, so
          keeping the two limits equal avoids a silent 16 MiB
          rejection at the tungstenite default (see PR #123).
        '';
      };

      maxFrameSize = lib.mkOption {
        type = lib.types.nullOr lib.types.int;
        default = null;
        description = ''
          Maximum WebSocket frame size in bytes. When null (the
          default), the server uses
          {option}`services.subduction.server.maxMessageSize`.

          Most deployments should leave this unset. Only useful if
          you need WebSocket frame fragmentation with a smaller
          per-frame cap than the aggregate message size.
        '';
      };

      metricsPort = lib.mkOption {
        type = lib.types.port;
        default = 9090;
        description = "Port for Prometheus metrics endpoint.";
      };

      enableMetrics = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = "Whether to enable the Prometheus metrics server.";
      };

      metricsRefreshInterval = lib.mkOption {
        type = lib.types.int;
        default = 60;
        description = "Interval in seconds for refreshing storage metrics from disk.";
      };

      wsPeers = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [];
        example = ["ws://192.168.1.100:8080" "ws://192.168.1.101:8080"];
        description = "WebSocket peer URLs to connect to on startup for bidirectional sync.";
      };

      iroh = {
        enable = lib.mkOption {
          type = lib.types.bool;
          default = false;
          description = "Enable the Iroh (QUIC) transport for NAT-traversing P2P connections.";
        };

        peers = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = [];
          example = ["abc123..."];
          description = "Iroh peer node IDs (z32-encoded public keys) to connect to on startup.";
        };

        peerAddrs = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = [];
          example = ["192.168.1.100:12345"];
          description = "Direct socket addresses for iroh peers, added as transport hints.";
        };

        directOnly = lib.mkOption {
          type = lib.types.bool;
          default = false;
          description = "Skip iroh relay servers and only use direct connections.";
        };

        relayUrl = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = ''
            URL of an iroh relay server to route through instead of the public
            default (e.g. a self-hosted iroh-relay instance).
          '';
        };
      };

      auth = lib.mkOption {
        type = lib.types.enum ["keyhive" "open"];
        default = "keyhive";
        description = ''
          Authorization mode for the server (passes `--auth <mode>`).

          - `keyhive` (the default): keyhive-based access control and
            sync. Inbound keyhive (SUK) wire messages are delegated and
            the periodic cache refresh runs (subject to
            {option}`services.subduction.server.keyhiveCacheRefresh`).
          - `open`: allow-all storage policy with keyhive disabled.
            Inbound keyhive messages are dropped and no cache refresh
            runs.
        '';
      };

      keyhiveCacheRefresh = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = ''
          Run the periodic keyhive cache refresh task (passes
          `--keyhive-cache-refresh <bool>`).

          Only has an effect under {option}`services.subduction.server.auth`
          = `keyhive`; the refresh is always skipped in `open` mode.
        '';
      };
    };

  };

  config = let
    anyEnabled = cfg.server.enable;
    hasKeySource = cfg.server.keySeed != null || cfg.server.keyFile != null || cfg.server.ephemeralKey;

    serverArgs =
      [
        "${cfg.package}/bin/subduction_cli"
        "server"
        "--socket"
        cfg.server.socket
        "--data-dir"
        (toString cfg.server.dataDir)
        "--timeout"
        (toString cfg.server.timeout)
        "--handshake-max-drift"
        (toString cfg.server.handshakeMaxDrift)
        "--max-message-size"
        (toString cfg.server.maxMessageSize)
      ]
      ++ lib.optionals (cfg.server.maxFrameSize != null) [
        "--max-frame-size"
        (toString cfg.server.maxFrameSize)
      ]
      ++ lib.optionals cfg.server.enableMetrics [
        "--metrics"
        "--metrics-port"
        (toString cfg.server.metricsPort)
        "--metrics-refresh-interval"
        (toString cfg.server.metricsRefreshInterval)
      ]
      ++ lib.optionals (cfg.server.keySeed != null) ["--key-seed" cfg.server.keySeed]
      ++ lib.optionals (cfg.server.keyFile != null) ["--key-file" (toString cfg.server.keyFile)]
      ++ lib.optionals cfg.server.ephemeralKey ["--ephemeral-key"]
      ++ lib.optionals (cfg.server.serviceName != null) ["--service-name" cfg.server.serviceName]
      ++ ["--log-format" cfg.server.logFormat]
      ++ ["--auth" cfg.server.auth]
      ++ lib.optionals (cfg.server.auth == "keyhive") [
        "--keyhive-cache-refresh"
        (lib.boolToString cfg.server.keyhiveCacheRefresh)
      ]
      ++ lib.concatMap (peer: ["--ws-peer" peer]) cfg.server.wsPeers
      ++ lib.optionals cfg.server.iroh.enable ["--iroh"]
      ++ lib.optionals (cfg.server.iroh.enable && cfg.server.iroh.directOnly) ["--iroh-direct-only"]
      ++ lib.optionals (cfg.server.iroh.relayUrl != null) ["--iroh-relay-url" cfg.server.iroh.relayUrl]
      ++ lib.concatMap (peer: ["--iroh-peer" peer]) cfg.server.iroh.peers
      ++ lib.concatMap (addr: ["--iroh-peer-addr" addr]) cfg.server.iroh.peerAddrs;

  in
    lib.mkIf anyEnabled {
      assertions = [
        {
          assertion = !cfg.server.enable || hasKeySource;
          message = ''
            services.subduction.server requires a key source. Set one of:
              - keyFile (recommended): Path to persistent key file
              - keySeed: Hex-encoded key seed
              - ephemeralKey: Use random key (lost on restart)
          '';
        }
        {
          assertion = !cfg.server.enable || lib.length (lib.filter (x: x) [
            (cfg.server.keySeed != null)
            (cfg.server.keyFile != null)
            cfg.server.ephemeralKey
          ]) <= 1;
          message = "services.subduction.server: keySeed, keyFile, and ephemeralKey are mutually exclusive";
        }
      ];

      systemd.user.services = lib.mkIf pkgs.stdenv.isLinux (
        lib.optionalAttrs cfg.server.enable {
          subduction = {
            Unit = {
              Description = "Subduction Sync Server";
              After = ["network.target"];
            };

            Service = {
              Type = "simple";
              ExecStart = lib.escapeShellArgs serverArgs;
              Environment = ["RUST_LOG=${cfg.server.logLevel}"];
              Restart = "on-failure";
              RestartSec = 5;
            };

            Install = {
              WantedBy = ["default.target"];
            };
          };
        }
      );

      launchd.agents = lib.mkIf pkgs.stdenv.isDarwin (
        lib.optionalAttrs cfg.server.enable {
          subduction = {
            enable = true;
            config = {
              Label = "com.inkandswitch.subduction";
              ProgramArguments = serverArgs;
              RunAtLoad = true;
              KeepAlive = true;
              StandardOutPath = "${config.xdg.cacheHome}/subduction/server.log";
              StandardErrorPath = "${config.xdg.cacheHome}/subduction/server.error.log";
            };
          };
        }
      );
    };
}
