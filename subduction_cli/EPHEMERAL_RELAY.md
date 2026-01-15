# Ephemeral Message Relay Server

The ephemeral relay server provides a simple broadcast mechanism for ephemeral messages like presence, awareness, cursor positions, etc.

## Architecture

```
┌────────────────────────────────────────┐
│      Client (e.g. automerge-repo)      │
│  ┌──────────────┐    ┌──────────────┐  │
│  │ WS :8080     │    │ WS :8081     │  │
│  │ (subduction) │    │ (ephemeral)  │  │
│  └──────┬───────┘    └──────┬───────┘  │
└─────────┼───────────────────┼──────────┘
          │                   │
          │                   │
          ▼                   ▼
   ┌──────────────┐    ┌──────────────┐
   │ Subduction   │    │ Ephemeral    │
   │ Server       │    │ Relay Server │
   │ Port 8080    │    │ Port 8081    │
   └──────────────┘    └──────────────┘

   Document Sync      Presence/Awareness
   (persistent)          (ephemeral)
```

## Running Both Servers

### Terminal 1: Subduction Server (Document Sync)
```bash
cargo run --release -- server --socket 0.0.0.0:8080
```

### Terminal 2: Ephemeral Relay (Presence)
```bash
cargo run --release -- ephemeral-relay --socket 0.0.0.0:8081
```

Or use the alias:
```bash
cargo run --release -- relay --socket 0.0.0.0:8081
```

## How It Works

### Subduction Server (Port 8080)
- Handles document synchronization
- Persists changes to storage
- Uses Subduction protocol (CBOR-encoded Messages)
- For CRDTs, fragments, commits, batch sync

### Ephemeral Relay (Port 8081)
- Broadcasts ephemeral messages between connected peers
- Does NOT persist messages
- Uses automerge-repo NetworkSubsystem protocol
- For presence, awareness, cursors, temporary state

## Client Configuration

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

## Message Flow

### Document Changes
```
Client → WebSocket:8080 → Subduction Server → Storage
                         ↓
                    Other Clients
```

### Presence Updates
```
Client → WebSocket:8081 → Relay Server → Other Clients
                         (broadcast)
```

## Benefits

- **Clean separation**: Document sync and ephemeral messages use different protocols
- **No Subduction changes**: Relay server is independent
- **Simple relay**: Just broadcasts messages, no processing
- **Stateless**: Relay server doesn't persist anything
- **Scalable**: Can run relay on different machine/port as needed

## Production Considerations

For production use, you might want to:

1. **Add authentication** - Verify peer identities
2. **Add rate limiting** - Prevent spam
3. **Add targeted relay** - Parse targetId and relay specifically (vs broadcast)
4. **Add metrics** - Track connections, message rates
5. **Use single port** - Multiplex both protocols on one WebSocket (more complex)
