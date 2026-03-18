# Subduction WebSocket

> [!WARNING]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

WebSocket transport layer for the [Subduction](https://github.com/inkandswitch/subduction) sync protocol. This is the most common transport in practice.

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                    WebSocket<T, K>                            │
│                                                               │
│  ┌─────────────┐    outbound_tx    ┌──────────────────────┐   │
│  │ send_bytes()│ ───────────────►  │   [SenderTask]       │   │
│  └─────────────┘                   │  drains to WebSocket │   │
│                                    └──────────────────────┘   │
│                                              │                │
│                                              ▼                │
│                                    ┌──────────────────────┐   │
│                                    │   WebSocketStream    │   │
│                                    │        <T>           │   │
│                                    └──────────────────────┘   │
│                                              │                │
│                                              ▼                │
│  ┌─────────────┐    inbound_reader ┌──────────────────────┐   │
│  │ recv_bytes()│ ◄───────────────  │  [ListenerTask]      │   │
│  └─────────────┘                   │  reads from WebSocket│   │
│                                    └──────────────────────┘   │
└───────────────────────────────────────────────────────────────┘
```

## `no_std` Support

This crate is `no_std` compatible when the `std` feature is disabled. It requires `alloc` for dynamic memory allocation.

## License

See the workspace [`LICENSE`](../LICENSE) file.
