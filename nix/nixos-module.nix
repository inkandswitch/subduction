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
      default = self.packages.${pkgs.system}.subduction_cli;
      defaultText = lib.literalExpression "pkgs.subduction_cli";
      description = "The Subduction CLI package to use.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "subduction";
      description = "User under which the services run.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "subduction";
      description = "Group under which the services run.";
    };

    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Whether to open the service ports in the firewall.";
    };

    server = {
      enable = lib.mkEnableOption "Subduction sync server";

      socket = lib.mkOption {
        type = lib.types.str;
        default = "0.0.0.0:8080";
        description = "Socket address for the WebSocket server.";
      };

      dataDir = lib.mkOption {
        type = lib.types.path;
        default = "/var/lib/subduction";
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

      maxMessageSize = lib.mkOption {
        type = lib.types.int;
        default = 52428800; # 50 MB
        description = "Maximum WebSocket message size in bytes.";
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
    };

    grafana = {
      provisionDashboard = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = ''
          Whether to provision the Subduction dashboard in Grafana.
          Requires services.grafana.enable to be true.
          The dashboard expects a Prometheus datasource with UID "prometheus".
        '';
      };
    };
  };

  config = let
    anyEnabled = cfg.server.enable;
    hasKeySource = cfg.server.keySeed != null || cfg.server.keyFile != null || cfg.server.ephemeralKey;
  in
    lib.mkMerge [
      # Assertions
      {
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
      }


      # Service configuration
      (lib.mkIf anyEnabled {
        users.users.${cfg.user} = {
          isSystemUser = true;
          group = cfg.group;
          home = cfg.server.dataDir;
          createHome = true;
        };

        users.groups.${cfg.group} = {};

        systemd.services.subduction = lib.mkIf cfg.server.enable {
          description = "Subduction Sync Server";
          wantedBy = ["multi-user.target"];
          after = ["network.target"];

          serviceConfig = {
            Type = "simple";
            User = cfg.user;
            Group = cfg.group;
            ExecStart = let
              args =
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
                ++ lib.concatMap (peer: ["--ws-peer" peer]) cfg.server.wsPeers
                ++ lib.optionals cfg.server.iroh.enable ["--iroh"]
                ++ lib.optionals (cfg.server.iroh.enable && cfg.server.iroh.directOnly) ["--iroh-direct-only"]
                ++ lib.optionals (cfg.server.iroh.relayUrl != null) ["--iroh-relay-url" cfg.server.iroh.relayUrl]
                ++ lib.concatMap (peer: ["--iroh-peer" peer]) cfg.server.iroh.peers
                ++ lib.concatMap (addr: ["--iroh-peer-addr" addr]) cfg.server.iroh.peerAddrs;
            in
              lib.escapeShellArgs args;
            Restart = "on-failure";
            RestartSec = 5;

            NoNewPrivileges = true;
            ProtectSystem = "strict";
            ProtectHome = true;
            PrivateTmp = true;
            ReadWritePaths = [cfg.server.dataDir];
          };
        };

        networking.firewall = lib.mkIf cfg.openFirewall {
          allowedTCPPorts = let
            getPort = socket:
              let
                matched = builtins.match ".*:([0-9]+)$" socket;
              in
                lib.toInt (lib.head matched);
          in
            lib.optional cfg.server.enable (getPort cfg.server.socket);
        };
      })

      # Grafana dashboard provisioning
      (lib.mkIf cfg.grafana.provisionDashboard {
        services.grafana.provision.dashboards.settings.providers = [
          {
            name = "subduction";
            options.path = "${self.grafanaDashboardsPath}";
          }
        ];
      })
    ];
}
