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
        default = "127.0.0.1:8081";
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
      systemd.user.services = lib.mkIf pkgs.stdenv.isLinux (
        lib.optionalAttrs cfg.server.enable {
          subduction = {
            Unit = {
              Description = "Subduction Sync Server";
              After = ["network.target"];
            };

            Service = {
              Type = "simple";
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
            };

            Install = {
              WantedBy = ["default.target"];
            };
          };
        }
        // lib.optionalAttrs cfg.relay.enable {
          subduction-relay = {
            Unit = {
              Description = "Subduction Ephemeral Message Relay";
              After = ["network.target"];
            };

            Service = {
              Type = "simple";
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
              ProgramArguments =
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
              RunAtLoad = true;
              KeepAlive = true;
              StandardOutPath = "${config.xdg.cacheHome}/subduction/server.log";
              StandardErrorPath = "${config.xdg.cacheHome}/subduction/server.error.log";
            };
          };
        }
        // lib.optionalAttrs cfg.relay.enable {
          subduction-relay = {
            enable = true;
            config = {
              Label = "com.inkandswitch.subduction-relay";
              ProgramArguments = [
                "${cfg.package}/bin/subduction_cli"
                "ephemeral-relay"
                "--socket"
                cfg.relay.socket
                "--max-message-size"
                (toString cfg.relay.maxMessageSize)
              ];
              RunAtLoad = true;
              KeepAlive = true;
              StandardOutPath = "${config.xdg.cacheHome}/subduction/relay.log";
              StandardErrorPath = "${config.xdg.cacheHome}/subduction/relay.error.log";
            };
          };
        }
      );
    };
}
