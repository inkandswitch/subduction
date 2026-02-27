# Subduction CLI

> [!CAUTION]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

## Overview

The Subduction CLI runs a document sync server with three transport layers:

- **WebSocket** — browser-compatible, enabled by default
- **HTTP Long Poll** — fallback for restrictive networks, enabled by default
- **Iroh (QUIC)** — NAT-traversing P2P via [iroh](https://iroh.computer), opt-in

## Installation

<details>
<summary><h3>Using Nix</h3></summary>

```bash
# Run directly without installing
nix run github:inkandswitch/subduction -- --help

# Install to your profile
nix profile install github:inkandswitch/subduction

# Then run
subduction_cli server --socket 0.0.0.0:8080 --ephemeral-key
```

#### Adding to a Flake

```nix
{
  inputs.subduction.url = "github:inkandswitch/subduction";

  outputs = { nixpkgs, subduction, ... }: {
    # NixOS
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      modules = [{
        environment.systemPackages = [
          subduction.packages.x86_64-linux.default
        ];
      }];
    };

    # Home Manager
    homeConfigurations.myuser = home-manager.lib.homeManagerConfiguration {
      modules = [{
        home.packages = [
          subduction.packages.x86_64-linux.default
        ];
      }];
    };
  };
}
```

</details>

### Using Cargo

```bash
# Build from source
cargo build --release

# Run
./target/release/subduction_cli --help
```

## Key Management

The server requires an **Ed25519 signing key seed** (32 bytes). The seed is used to deterministically derive both the signing key and the public verifying key. The verifying key becomes the server's **peer ID**.

### Generating a Key

Any source of 32 cryptographically random bytes works. The CLI accepts either 64 hex characters or 32 raw bytes.

```bash
# OpenSSL
openssl rand -hex 32

# /dev/urandom
head -c 32 /dev/urandom | xxd -p -c 64

# Python
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### Providing the Key

| Flag | Description |
|------|-------------|
| `--key-seed <HEX>` | 64 hex characters on the command line |
| `--key-file <PATH>` | Path to a file containing 64 hex characters or 32 raw bytes |
| `--ephemeral-key` | Generate a random key (lost on restart) |

These are mutually exclusive. Exactly one must be provided.

`--key-file` is recommended for production. The file must contain either:
- 64 hex characters (with optional trailing newline), or
- Exactly 32 raw bytes

```bash
# Create a persistent key file
openssl rand -hex 32 > /var/lib/subduction/key
chmod 600 /var/lib/subduction/key

# Start with key file
subduction_cli server --key-file /var/lib/subduction/key
```

> [!WARNING]
> The key seed is equivalent to a private key. Do not commit it to version control or expose it in logs.

## Commands

### `server`

Start a Subduction sync node.

```bash
subduction_cli server --socket 0.0.0.0:8080 --key-file ./key
```

#### General Options

| Flag | Default | Description |
|------|---------|-------------|
| `-s, --socket <ADDR>` | `0.0.0.0:8080` | Socket address to bind to |
| `-d, --data-dir <PATH>` | `./data` | Data directory for filesystem storage |
| `-t, --timeout <SECS>` | `5` | Request timeout in seconds |
| `--handshake-max-drift <SECS>` | `600` | Maximum clock drift allowed during handshake |
| `--service-name <NAME>` | socket address | Service name for discovery mode handshake |
| `--max-message-size <BYTES>` | `52428800` (50 MB) | Maximum WebSocket message size |

#### Transport Options

| Flag | Default | Description |
|------|---------|-------------|
| `--websocket` | enabled | Enable the WebSocket transport |
| `--longpoll` | enabled | Enable the HTTP long-poll transport |
| `--iroh` | disabled | Enable the Iroh (QUIC) transport |

At least one transport must be enabled.

#### WebSocket Peer Options

| Flag | Description |
|------|-------------|
| `--ws-peer <URL>` | WebSocket peer URL to connect to on startup (repeatable) |

#### Iroh Peer Options

| Flag | Description |
|------|-------------|
| `--iroh-peer <NODE_ID>` | Iroh peer node ID to connect to (z32-encoded, repeatable) |
| `--iroh-peer-addr <IP:PORT>` | Direct address hint for iroh peers (repeatable) |
| `--iroh-direct-only` | Skip relay servers, direct connections only |
| `--iroh-relay-url <URL>` | Route through a specific relay instead of the public default |

By default, iroh routes traffic through [iroh's public relay infrastructure](https://iroh.computer) for NAT traversal. Use `--iroh-direct-only` for LAN-only deployments, or `--iroh-relay-url` to point at a self-hosted [`iroh-relay`](https://docs.rs/iroh-relay) instance.

#### Metrics Options

| Flag | Default | Description |
|------|---------|-------------|
| `--metrics` | disabled | Enable the Prometheus metrics server |
| `--metrics-port <PORT>` | `9090` | Port for Prometheus metrics endpoint |
| `--metrics-refresh-interval <SECS>` | `60` | Interval for refreshing storage metrics |

#### Other Options

| Flag | Description |
|------|-------------|
| `--ready-file <PATH>` | Write a file on startup with assigned port, peer ID, and iroh node ID |

### `purge`

Delete all stored data.

```bash
subduction_cli purge --data-dir ./data
```

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --data-dir <PATH>` | `./data` | Data directory to purge |
| `-y, --yes` | | Skip confirmation prompt |

## Examples

```bash
# Minimal server with an ephemeral key
subduction_cli server --ephemeral-key

# Server with persistent key and custom data directory
subduction_cli server --key-file ./key --data-dir /var/lib/subduction

# Two WebSocket servers syncing bidirectionally
subduction_cli server --key-file ./key1 --socket 0.0.0.0:8080 \
  --ws-peer ws://192.168.1.101:8080
subduction_cli server --key-file ./key2 --socket 0.0.0.0:8080 \
  --ws-peer ws://192.168.1.100:8080

# Iroh P2P (NAT-traversing, no WebSocket peers needed)
subduction_cli server --key-file ./key --iroh \
  --iroh-peer <remote-node-id>

# Iroh direct only (LAN, no relay)
subduction_cli server --key-file ./key --iroh --iroh-direct-only \
  --iroh-peer <node-id> --iroh-peer-addr 192.168.1.50:12345

# Discovery mode (clients connect by service name instead of peer ID)
subduction_cli server --key-file ./key --service-name sync.example.com

# Enable Prometheus metrics
subduction_cli server --key-file ./key --metrics --metrics-port 9090

# Debug logging
RUST_LOG=debug subduction_cli server --ephemeral-key
```

<details>
<summary><h2>Running as a System Service</h2></summary>

The flake provides NixOS and Home Manager modules for running Subduction as a managed service.

### NixOS (systemd)

```nix
{
  inputs.subduction.url = "github:inkandswitch/subduction";

  outputs = { nixpkgs, subduction, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        subduction.nixosModules.default
        {
          services.subduction = {
            server = {
              enable = true;
              socket = "0.0.0.0:8080";
              dataDir = "/var/lib/subduction";
              keyFile = "/var/lib/subduction/key";
              timeout = 5;

              # WebSocket peers for bidirectional sync
              wsPeers = [
                "ws://192.168.1.100:8080"
                "ws://192.168.1.101:8080"
              ];

              # Iroh P2P transport
              iroh = {
                enable = true;
                peers = ["<remote-node-id>"];
                # directOnly = true;      # LAN only, no relay
                # relayUrl = "https://..."; # self-hosted relay
              };

              # Prometheus metrics
              enableMetrics = true;
              metricsPort = 9090;
            };

            # Shared settings
            user = "subduction";
            group = "subduction";
            openFirewall = true;
          };
        }
      ];
    };
  };
}
```

This creates a systemd service: `subduction.service`

```bash
systemctl status subduction
journalctl -u subduction -f
```

### Home Manager (user service)

Works on both Linux (systemd user service) and macOS (launchd agent):

```nix
{
  inputs.subduction.url = "github:inkandswitch/subduction";

  outputs = { home-manager, subduction, ... }: {
    homeConfigurations.myuser = home-manager.lib.homeManagerConfiguration {
      modules = [
        subduction.homeManagerModules.default
        {
          services.subduction = {
            server = {
              enable = true;
              socket = "127.0.0.1:8080";
              keyFile = "/home/myuser/.config/subduction/key";
              # dataDir defaults to ~/.local/share/subduction

              wsPeers = ["ws://sync.example.com:8080"];

              iroh.enable = true;
            };
          };
        }
      ];
    };
  };
}
```

On Linux:
```bash
systemctl --user status subduction
```

On macOS:
```bash
launchctl list | grep subduction
tail -f ~/.cache/subduction/server.log
```

### Behind a Reverse Proxy (Caddy)

When running behind Caddy or another reverse proxy, bind to localhost:

```nix
services.subduction.server = {
  enable = true;
  socket = "127.0.0.1:8080";
  keyFile = "/var/lib/subduction/key";
};

services.caddy = {
  enable = true;
  virtualHosts."sync.example.com".extraConfig = ''
    reverse_proxy localhost:8080
  '';
};
```

Caddy automatically handles WebSocket upgrades and TLS certificates.

</details>

## Monitoring

### Prometheus

Enable the metrics endpoint:

```bash
subduction_cli server --key-file ./key --metrics --metrics-port 9090
```

Configure Prometheus to scrape:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'subduction'
    static_configs:
      - targets: ['localhost:9090']
```

### Development Monitoring Stack

With Nix, use the built-in command to launch Prometheus and Grafana with pre-configured dashboards:

```bash
nix develop
monitoring:start
```

This starts:
- **Prometheus** at `http://localhost:9092`
- **Grafana** at `http://localhost:3939` with pre-configured dashboards

### Grafana Dashboard

Import the dashboard from `subduction_cli/monitoring/grafana/provisioning/dashboards/subduction.json`. It includes panels for connections, messages, sync operations, and storage.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Log level filter (e.g. `debug`, `info`, `subduction_core=trace`) |
| `TOKIO_CONSOLE` | Set to any value to enable [tokio-console](https://github.com/tokio-rs/console) |
| `LOKI_URL` | Grafana Loki endpoint for log shipping (e.g. `http://localhost:3100`) |
| `LOKI_SERVICE_NAME` | Service label for Loki (default: `subduction`) |
