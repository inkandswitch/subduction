# Subduction CLI

> [!CAUTION]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

## Overview

The Subduction CLI provides multiple server modes:

- **`server`** - Subduction document sync server (persistent CRDT storage)
- **`client`** - Subduction client connecting to a server
- **`ephemeral-relay`** - Simple relay for ephemeral messages (presence, awareness)

## Installation

<details>
<summary><h3>Using Nix ❄️</h3></summary>

```bash
# Run directly without installing
nix run github:inkandswitch/subduction -- --help

# Install to your profile
nix profile install github:inkandswitch/subduction

# Then run
subduction_cli server --socket 0.0.0.0:8080
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

### Using Cargo 🦀

```bash
# Build from source
cargo build --release

# Run
./target/release/subduction_cli --help
```

## Commands

### Server Mode

Start a Subduction server for document synchronization:

```bash
# With Nix
nix run .#subduction_cli -- server --socket 0.0.0.0:8080

# With Cargo
cargo run --release -- server --socket 0.0.0.0:8080
```

Options:
- `--socket <ADDR>` - Socket address to bind to (default: `0.0.0.0:8080`)
- `--data-dir <PATH>` - Data directory for storage (default: `./data`)
- `--peer-id <ID>` - Peer ID as 64 hex characters (default: auto-generated)
- `--timeout <SECS>` - Request timeout in seconds (default: `5`)

### Client Mode

Connect as a client to a Subduction server:

```bash
# With Nix
nix run .#subduction_cli -- client --server ws://127.0.0.1:8080

# With Cargo
cargo run --release -- client --server ws://127.0.0.1:8080
```

Options:
- `--server <URL>` - WebSocket server URL to connect to
- `--data-dir <PATH>` - Data directory for local storage (default: `./client-data`)
- `--peer-id <ID>` - Peer ID as 64 hex characters (default: auto-generated)
- `--timeout <SECS>` - Request timeout in seconds (default: `5`)

### Ephemeral Relay Mode

Start a relay server for ephemeral messages (presence, awareness):

```bash
# With Nix
nix run .#subduction_cli -- ephemeral-relay --socket 0.0.0.0:8081

# With Cargo
cargo run --release -- ephemeral-relay --socket 0.0.0.0:8081
```

Alias: `relay`

Options:
- `--socket <ADDR>` - Socket address to bind to (default: `0.0.0.0:8081`)
- `--max-message-size <BYTES>` - Maximum message size in bytes (default: `1048576` = 1 MB)

#### Architecture

The ephemeral relay server provides a simple broadcast mechanism for ephemeral messages like presence, awareness, cursor positions, etc.

```
┌────────────────────────────────────────┐
│      Client (e.g. automerge-repo)      │
│  ┌──────────────┐    ┌──────────────┐  │
│  │   WS :8080   │    │   WS :8081   │  │
│  │ (subduction) │    │ (ephemeral)  │  │
│  └──────┬───────┘    └──────┬───────┘  │
└─────────┼───────────────────┼──────────┘
          │                   │
          ▼                   ▼
   ┌──────────────┐    ┌──────────────┐
   │ Subduction   │    │ Ephemeral    │
   │ Server       │    │ Relay Server │
   │ Port 8080    │    │ Port 8081    │
   └──────────────┘    └──────────────┘
    Document Sync     Presence/Awareness
    (persistent)         (ephemeral)
```

#### How It Works

**Subduction Server (default port 8080)**
- Handles document synchronization
- Persists changes to storage
- Uses Subduction protocol (CBOR-encoded Messages)
- For CRDTs, fragments, commits, batch sync

**Ephemeral Relay (default port 8081)**
- Implements automerge-repo NetworkSubsystem protocol handshake
- Responds to "join" messages with "peer" messages
- Broadcasts ephemeral messages between connected peers
- Does NOT persist messages
- For presence, awareness, cursors, temporary state
- Uses sharded deduplication with AHash for DoS-resistant message filtering

#### Client Configuration

In your automerge-repo client:

```typescript
const repo = new Repo({
  network: [
    // Document sync via Subduction
    new WebSocketClientAdapter("ws://127.0.0.1:8080", 5000, { subductionMode: true }),

    // Ephemeral messages via relay server
    new WebSocketClientAdapter("ws://127.0.0.1:8081"),
  ],
  subduction: await Subduction.hydrate(db),
})
```

#### Message Flow

**Document Changes**
```
Client → WebSocket:8080 → Subduction Server → Storage
                         ↓
                    Other Clients
```

**Presence Updates**
```
Client → WebSocket:8081 → Relay Server → Other Clients
                         (broadcast)
```

#### Benefits

- **Clean separation**: Document sync and ephemeral messages use different protocols
- **No Subduction changes**: Relay server is independent
- **Simple relay**: Just broadcasts messages, no processing
- **Stateless**: Relay server doesn't persist anything
- **Scalable**: Can run relay on different machine/port as needed
- **DoS-resistant**: Sharded deduplication prevents duplicate message floods

#### Production Considerations

For production use, you might want to:

1. **Add authentication** - Verify peer identities
2. **Add rate limiting** - Prevent spam
3. **Add targeted relay** - Parse targetId and relay specifically (vs broadcast)
4. **Add metrics** - Track connections, message rates
5. **Use single port** - Multiplex both protocols on one WebSocket (more complex)
6. **Add message authentication** - Prevent forged ephemeral messages (see code TODOs)
7. **Add timestamp validation** - Prevent replay attacks (see code TODOs)

## Typical Setup

For a complete setup supporting both document sync and presence:

**Terminal 1: Document Sync Server**
```bash
nix run .#subduction_cli -- server --socket 0.0.0.0:8080
```

**Terminal 2: Ephemeral Relay Server**
```bash
nix run .#subduction_cli -- relay --socket 0.0.0.0:8081
```

Your clients can then connect to:
- Port 8080 for document synchronization
- Port 8081 for ephemeral messages (presence, awareness, etc.)

## Environment Variables

- `RUST_LOG` - Set log level (e.g., `RUST_LOG=debug`)
- `TOKIO_CONSOLE` - Enable tokio console for debugging async tasks

## Examples

```bash
# Server with debug logging
RUST_LOG=debug nix run .#subduction_cli -- server

# Client connecting to remote server
nix run .#subduction_cli -- client --server ws://sync.example.com:8080

# Ephemeral relay on custom port
nix run .#subduction_cli -- relay --socket 0.0.0.0:9000

# Ephemeral relay with 5 MB message size limit
nix run .#subduction_cli -- relay --max-message-size 5242880
```

<details>
<summary><h2>Running as a System Service ❄️</h2></summary>

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
            # Document sync server
            server = {
              enable = true;
              socket = "0.0.0.0:8080";
              dataDir = "/var/lib/subduction";
              metricsPort = 9090;
              enableMetrics = true;
              timeout = 5;
              # peerId = "...";  # optional: 64 hex chars
            };

            # Ephemeral message relay
            relay = {
              enable = true;
              socket = "0.0.0.0:8081";
              maxMessageSize = 1048576;  # 1 MB
            };

            # Shared settings
            user = "subduction";
            group = "subduction";
            openFirewall = true;  # opens server + relay ports
          };
        }
      ];
    };
  };
}
```

This creates two systemd services:
- `subduction.service` - Document sync server
- `subduction-relay.service` - Ephemeral message relay

Manage with:
```bash
systemctl status subduction
systemctl status subduction-relay
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
              # dataDir defaults to ~/.local/share/subduction
            };

            relay = {
              enable = true;
              socket = "127.0.0.1:8081";
            };
          };
        }
      ];
    };
  };
}
```

On Linux, manage with:
```bash
systemctl --user status subduction
systemctl --user status subduction-relay
```

On macOS, manage with:
```bash
launchctl list | grep subduction
tail -f ~/.cache/subduction/server.log
```

### Behind a Reverse Proxy (Caddy)

When running behind Caddy or another reverse proxy, bind to localhost:

```nix
services.subduction = {
  server = {
    enable = true;
    socket = "127.0.0.1:8080";
  };
  relay = {
    enable = true;
    socket = "127.0.0.1:8081";
  };
  openFirewall = false;  # Caddy handles external access
};

services.caddy = {
  enable = true;
  virtualHosts."sync.example.com".extraConfig = ''
    reverse_proxy localhost:8080
  '';
  virtualHosts."relay.example.com".extraConfig = ''
    reverse_proxy localhost:8081
  '';
};
```

Caddy automatically handles WebSocket upgrades and TLS certificates.

</details>

## Monitoring

The Subduction server exposes Prometheus metrics on a configurable port (default: `9090`).

### Server Metrics Options

```bash
# Enable metrics (default)
subduction_cli server --metrics --metrics-port 9090

# Disable metrics
subduction_cli server --metrics=false
```

### Available Metrics

**Connection Metrics:**
- `subduction_connections_active` - Current active connections
- `subduction_connections_total` - Total connections established
- `subduction_connections_closed` - Total connections closed

**Message Metrics:**
- `subduction_messages_total` - Messages processed (labeled by type)
- `subduction_dispatch_duration_seconds` - Message dispatch latency histogram

**Sync Metrics:**
- `subduction_batch_sync_requests_total` - Batch sync requests received
- `subduction_batch_sync_responses_total` - Batch sync responses sent

**Storage Metrics:**
- `subduction_storage_sedimentrees` - Number of sedimentrees stored
- `subduction_storage_blobs` - Number of blobs stored
- `subduction_storage_loose_commits_saved` - Loose commits saved (labeled by `sedimentree_id`)
- `subduction_storage_fragments_saved` - Fragments saved (labeled by `sedimentree_id`)
- `subduction_storage_operation_duration_seconds` - Storage operation latency histogram

### Development Monitoring Stack

When developing locally with Nix, use the `monitoring:start` command to launch Prometheus and Grafana with pre-configured dashboards:

```bash
# Enter the dev shell
nix develop

# Start the monitoring stack
monitoring:start
```

This starts:
- **Prometheus** at `http://localhost:9092` - scrapes metrics from the server
- **Grafana** at `http://localhost:3939` - pre-configured dashboards

The Grafana dashboard includes panels for:
- Active/total/closed connections
- Messages per second by type
- Dispatch latency (p50/p95/p99)
- Batch sync request/response rates
- Storage metrics (sedimentrees, blobs, commits, fragments)
- Storage operation latency

### Production Monitoring

For production, configure your Prometheus instance to scrape the metrics endpoint:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'subduction'
    static_configs:
      - targets: ['localhost:9090']
```

Import the Grafana dashboard from `subduction_cli/monitoring/grafana/provisioning/dashboards/subduction.json`.

