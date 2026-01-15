# Subduction CLI

🚧 This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK. 🚧

## Overview

The Subduction CLI provides multiple server modes:

- **`server`** - Subduction document sync server (persistent CRDT storage)
- **`client`** - Subduction client connecting to a server
- **`ephemeral-relay`** - Simple relay for ephemeral messages (presence, awareness)

## Installation

### Using Nix (Recommended)

```bash
# Run directly from the repository
nix run .#subduction_cli -- --help

# Start Subduction server
nix run .#subduction_cli -- server --socket 0.0.0.0:8080

# Start ephemeral relay server
nix run .#subduction_cli -- ephemeral-relay --socket 0.0.0.0:8081
```

### Using Cargo

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

See [EPHEMERAL_RELAY.md](./EPHEMERAL_RELAY.md) for detailed information about the ephemeral relay server.

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
```

