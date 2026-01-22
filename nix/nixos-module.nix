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

      timeout = lib.mkOption {
        type = lib.types.int;
        default = 5;
        description = "Request timeout in seconds.";
      };

      peerId = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Peer ID as 64 hex characters. If null, one will be generated.";
      };

      peers = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [];
        example = ["ws://192.168.1.100:8080" "ws://192.168.1.101:8080"];
        description = "List of peer WebSocket URLs to connect to on startup for bidirectional sync.";
      };
    };

    relay = {
      enable = lib.mkEnableOption "Subduction ephemeral message relay";

      socket = lib.mkOption {
        type = lib.types.str;
        default = "0.0.0.0:8081";
        description = "Socket address for the ephemeral relay server.";
      };

      maxMessageSize = lib.mkOption {
        type = lib.types.int;
        default = 1048576; # 1 MB
        description = "Maximum message size in bytes.";
      };
    };
  };

  config = let
    anyEnabled = cfg.server.enable || cfg.relay.enable;
  in
    lib.mkIf anyEnabled {
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
              ]
              ++ lib.optionals cfg.server.enableMetrics ["--metrics" "--metrics-port" (toString cfg.server.metricsPort)]
              ++ lib.optionals (cfg.server.peerId != null) ["--peer-id" cfg.server.peerId]
              ++ lib.concatMap (peer: ["--peer" peer]) cfg.server.peers;
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

      systemd.services.subduction-relay = lib.mkIf cfg.relay.enable {
        description = "Subduction Ephemeral Message Relay";
        wantedBy = ["multi-user.target"];
        after = ["network.target"];

        serviceConfig = {
          Type = "simple";
          User = cfg.user;
          Group = cfg.group;
          ExecStart = lib.escapeShellArgs [
            "${cfg.package}/bin/subduction_cli"
            "ephemeral-relay"
            "--socket"
            cfg.relay.socket
            "--max-message-size"
            (toString cfg.relay.maxMessageSize)
          ];
          Restart = "on-failure";
          RestartSec = 5;

          NoNewPrivileges = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          PrivateTmp = true;
          ReadOnlyPaths = ["/"];
        };
      };

      networking.firewall = lib.mkIf cfg.openFirewall {
        allowedTCPPorts = let
          # Match the port after the last colon (handles IPv6 bracket notation)
          getPort = socket:
            let
              matched = builtins.match ".*:([0-9]+)$" socket;
            in
              lib.toInt (lib.head matched);
          serverPort = getPort cfg.server.socket;
          relayPort = getPort cfg.relay.socket;
        in
          (lib.optional cfg.server.enable serverPort)
          ++ (lib.optional cfg.relay.enable relayPort);
      };
    };
}
