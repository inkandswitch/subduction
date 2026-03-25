# automerge_subduction_ingest

CLI tool for ingesting Automerge documents into a Subduction sync network.

Reads an `.am` file, decomposes it into sedimentree fragments and loose commits, connects to a Subduction sync server via WebSocket, and uploads the data.

## Usage

```sh
automerge-subduction-ingest \
  --server wss://sync.example.com \
  --ephemeral-key \
  document.am
```

### Options

| Flag                    | Description                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------|
| `--server <URL>`        | WebSocket URL of the Subduction sync server                                                               |
| `--doc-id <ID>`         | Document ID (`automerge:<base58check>`, raw base58check, or 64 hex chars). Defaults to the filename stem. |
| `--key-seed <HEX>`      | Signing key seed (64 hex characters)                                                                      |
| `--key-file <PATH>`     | Path to a file containing the signing key seed                                                            |
| `--ephemeral-key`       | Use a random ephemeral key (identity lost on exit)                                                        |
| `--service-name <NAME>` | Service name for audience discovery (defaults to server hostname)                                         |
| `--timeout <SECS>`      | Sync timeout in seconds (default: 30)                                                                     |
| `--dry-run`             | Ingest and print stats without uploading                                                                  |

### Dry Run

To inspect the decomposition without uploading:

```sh
automerge-subduction-ingest --dry-run document.am
```

This prints the number of changes, fragments, loose commits, covered
commits, and blob sizes.
